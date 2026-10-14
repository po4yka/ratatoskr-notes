//! The blob receipt of the device channels (spec `notes-device-channels`, requirement "Bytes
//! arrive before the change that names them"): upload sessions of the shared chunked, resumable
//! transfer contract (`ratatoskr-blob-transfer-contracts`).
//!
//! A session belongs to one connection. Its chunks are written into one staged copy in the blob
//! store at the offset of their index, and the reference state machine of the contract decides
//! what is recorded, what is a replay and when finalize can run. Finalize hashes the staged copy:
//! bytes with the declared SHA-256 get their content name in the store, other bytes are deleted
//! and the answer is the outcome `digest_mismatch`. No blob is registered then.
//!
//! The sessions are kept in memory and are bounded: at most [`MAX_OPEN_SESSIONS`] at a time, each
//! for [`SESSION_LIFETIME`]. A restart forgets them, and the store deletes their staged copies at
//! its next open; a device then opens a new session, as for an expired one.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use ratatoskr_blob_transfer_contracts::{
    ChunkRecordOutcome, DigestHex, TransferErrorCode, TransferFailure, UploadChunkReceipt,
    UploadCompletionOutcome, UploadPlan, UploadResumptionToken, UploadSession, UploadSessionOpened,
    UploadSessionRequest, UploadStatusResponse,
};
use ratatoskr_identifiers::{BlobOwner, Extensions, WireTimestamp};
use tokio::sync::OwnedMutexGuard;
use tokio::time::Instant;
use uuid::Uuid;

use crate::blobs::BlobStore;
use crate::events::PRODUCER;

/// The most upload sessions that are open at the same time, over all connections.
pub const MAX_OPEN_SESSIONS: usize = 32;

/// How long an upload session stays open.
pub const SESSION_LIFETIME: Duration = Duration::from_hours(1);

/// Why a step of an upload did not happen.
#[derive(Debug)]
pub enum UploadRefusal {
    /// A step of the transfer contract was refused with its code.
    Transfer(TransferErrorCode),
    /// The declaration is larger than the largest file that is indexed, or its chunking is not
    /// one that the contract allows.
    Invalid,
    /// [`MAX_OPEN_SESSIONS`] sessions are open.
    Busy,
    /// The blob store failed.
    Store(std::io::Error),
}

impl From<TransferFailure> for UploadRefusal {
    fn from(failure: TransferFailure) -> Self {
        Self::Transfer(failure.code())
    }
}

/// One open session.
#[derive(Debug)]
struct Session {
    connection_id: Uuid,
    reference: UploadSession,
    plan: UploadPlan,
    declared: String,
    part: PathBuf,
    expires: Instant,
}

/// The open upload sessions of the service.
#[derive(Debug, Clone, Default)]
pub struct Uploads {
    sessions: Arc<Mutex<HashMap<String, Arc<tokio::sync::Mutex<Session>>>>>,
}

impl Uploads {
    /// Open a session of `connection_id` for the declaration `request`, whose size is at most
    /// `max_bytes`.
    ///
    /// # Errors
    ///
    /// [`UploadRefusal::Invalid`], [`UploadRefusal::Busy`], or [`UploadRefusal::Store`] when the
    /// staged copy cannot be created.
    pub async fn open(
        &self,
        blobs: &BlobStore,
        connection_id: Uuid,
        request: &UploadSessionRequest,
        max_bytes: u64,
    ) -> Result<UploadSessionOpened, UploadRefusal> {
        if request.declared_size_bytes > max_bytes {
            return Err(UploadRefusal::Invalid);
        }
        let plan = UploadPlan::new(request.declared_size_bytes, request.chunk_size_bytes)
            .map_err(|_| UploadRefusal::Invalid)?;
        let reference = UploadSession::new(request).map_err(|_| UploadRefusal::Invalid)?;
        self.forget_expired();
        if self.lock().len() >= MAX_OPEN_SESSIONS {
            return Err(UploadRefusal::Busy);
        }
        let store = blobs.clone();
        let part = tokio::task::spawn_blocking(move || store.create_part())
            .await
            .map_err(|error| UploadRefusal::Store(std::io::Error::other(error)))?
            .map_err(UploadRefusal::Store)?;
        let token = new_token().ok_or_else(|| {
            UploadRefusal::Store(std::io::Error::other(
                "a resumption token could not be made",
            ))
        })?;
        let expires_at = jiff::Timestamp::now()
            .checked_add(jiff::SignedDuration::from_secs(
                i64::try_from(SESSION_LIFETIME.as_secs()).unwrap_or(i64::MAX),
            ))
            .map_or_else(|_| WireTimestamp::now(), WireTimestamp::from_jiff);
        let session = Session {
            connection_id,
            reference,
            plan,
            declared: request.digest.hex.as_str().to_owned(),
            part,
            expires: Instant::now() + SESSION_LIFETIME,
        };
        self.lock().insert(
            token.as_str().to_owned(),
            Arc::new(tokio::sync::Mutex::new(session)),
        );
        Ok(UploadSessionOpened {
            resumption_token: token,
            chunk_size_bytes: plan.chunk_size_bytes(),
            expires_at,
            extensions: Extensions::new(),
        })
    }

    /// Record the chunk `index` of the session `token` of `connection_id`.
    ///
    /// # Errors
    ///
    /// [`UploadRefusal::Transfer`] with the code of the contract, or [`UploadRefusal::Store`].
    pub async fn chunk(
        &self,
        connection_id: Uuid,
        token: &str,
        index: u32,
        bytes: Vec<u8>,
    ) -> Result<UploadChunkReceipt, UploadRefusal> {
        let (resumption_token, mut session) = self.session(connection_id, token).await?;
        let mut next = session.reference.clone();
        let outcome = next.record(index, &bytes)?;
        if outcome == ChunkRecordOutcome::Recorded {
            let offset = u64::from(index) * u64::from(session.plan.chunk_size_bytes());
            let part = session.part.clone();
            tokio::task::spawn_blocking(move || BlobStore::write_part(&part, offset, &bytes))
                .await
                .map_err(|error| UploadRefusal::Store(std::io::Error::other(error)))?
                .map_err(UploadRefusal::Store)?;
        }
        session.reference = next;
        let view = session.reference.status_view(&resumption_token);
        Ok(UploadChunkReceipt {
            resumption_token,
            chunk_index: index,
            received_chunks_count: view.received_chunks_count,
            idempotent_replay: outcome == ChunkRecordOutcome::IdempotentReplay,
            extensions: Extensions::new(),
        })
    }

    /// The resume view of the session `token` of `connection_id`.
    ///
    /// # Errors
    ///
    /// [`UploadRefusal::Transfer`] when no open session answers the token.
    pub async fn status(
        &self,
        connection_id: Uuid,
        token: &str,
    ) -> Result<UploadStatusResponse, UploadRefusal> {
        let (resumption_token, session) = self.session(connection_id, token).await?;
        Ok(session.reference.status_view(&resumption_token))
    }

    /// Finalize the session `token` of `connection_id`: hash the staged copy, and give it its
    /// content name when the SHA-256 is the declared one. The session ends with either outcome.
    ///
    /// # Errors
    ///
    /// [`UploadRefusal::Transfer`] with `blob_transfer.finalize_incomplete` while chunks are
    /// missing, or `blob_transfer.session_unknown`; [`UploadRefusal::Store`] when the store fails.
    pub async fn finalize(
        &self,
        blobs: &BlobStore,
        connection_id: Uuid,
        token: &str,
    ) -> Result<UploadCompletionOutcome, UploadRefusal> {
        let (_, mut session) = self.session(connection_id, token).await?;
        let missing_chunks_count = session.reference.missing_chunks_count();
        if missing_chunks_count > 0 {
            return Err(TransferFailure::FinalizeIncomplete {
                missing_chunks_count,
            }
            .into());
        }
        let store = blobs.clone();
        let part = session.part.clone();
        let declared = session.declared.clone();
        let sealed = tokio::task::spawn_blocking(move || store.seal_part(&part, &declared))
            .await
            .map_err(|error| UploadRefusal::Store(std::io::Error::other(error)));
        self.lock().remove(token);
        let sealed = sealed?.map_err(UploadRefusal::Store)?;
        let owner = BlobOwner::parse(PRODUCER)
            .map_err(|_| UploadRefusal::Store(std::io::Error::other("the blob owner")))?;
        let computed = DigestHex::parse(&sealed.digest)
            .map_err(|_| UploadRefusal::Store(std::io::Error::other("the digest")))?;
        Ok(session.reference.finalize(owner, computed)?)
    }

    /// The open session `token` of `connection_id`, held until the step ends. A session of another
    /// connection answers as one that does not exist; an expired one is forgotten.
    async fn session(
        &self,
        connection_id: Uuid,
        token: &str,
    ) -> Result<(UploadResumptionToken, OwnedMutexGuard<Session>), UploadRefusal> {
        let unknown = || UploadRefusal::Transfer(TransferErrorCode::SessionUnknown);
        let resumption_token = UploadResumptionToken::parse(token).map_err(|_| unknown())?;
        let session = self.lock().get(token).cloned().ok_or_else(unknown)?;
        let session = session.lock_owned().await;
        if session.connection_id != connection_id {
            return Err(unknown());
        }
        if session.expires <= Instant::now() {
            BlobStore::discard_part(&session.part);
            self.lock().remove(token);
            return Err(UploadRefusal::Transfer(TransferErrorCode::SessionExpired));
        }
        Ok((resumption_token, session))
    }

    /// Forget each expired session that no step holds, and delete its staged copy.
    fn forget_expired(&self) {
        let now = Instant::now();
        self.lock().retain(|_, session| match session.try_lock() {
            Ok(open) if open.expires <= now => {
                BlobStore::discard_part(&open.part);
                false
            }
            _ => true,
        });
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Arc<tokio::sync::Mutex<Session>>>> {
        self.sessions.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A new resumption token: `rst_` and 52 digits of base 32 from two random UUIDs, 244 random bits.
fn new_token() -> Option<UploadResumptionToken> {
    const DIGITS: &[u8; 32] = b"0123456789abcdefghijklmnopqrstuv";
    let mut token = String::from("rst_");
    for random in [Uuid::new_v4().as_u128(), Uuid::new_v4().as_u128()] {
        for place in (0..26_u32).rev() {
            let digit = usize::try_from((random >> (place * 5)) & 0x1f).ok()?;
            token.push(char::from(*DIGITS.get(digit)?));
        }
    }
    UploadResumptionToken::parse(&token).ok()
}
