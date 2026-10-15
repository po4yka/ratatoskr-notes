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
//! The sessions are kept in memory and are bounded: at most [`MAX_SESSIONS_PER_CONNECTION`] for
//! one connection and [`MAX_OPEN_SESSIONS`] for the service at a time, each for
//! [`SESSION_LIFETIME`]. The count and the insert of a new session are one step under one lock. A restart forgets them, and the store deletes their staged copies at
//! its next open; a device then opens a new session, as for an expired one. A client that gives
//! up an upload closes its session, which deletes the staged copy and frees the slot at once.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use ratatoskr_blob_transfer_contracts::{
    ChunkRecordOutcome, DigestHex, TransferErrorCode, TransferFailure, UploadChunkReceipt,
    UploadCompletionOutcome, UploadPlan, UploadResumptionToken, UploadSession, UploadSessionOpened,
    UploadSessionRequest, UploadSessionState, UploadStatusResponse,
};
use ratatoskr_identifiers::{BlobOwner, Extensions, WireTimestamp};
use tokio::sync::OwnedMutexGuard;
use tokio::time::Instant;
use uuid::Uuid;

use crate::blobs::BlobStore;
use crate::events::PRODUCER;

/// The most upload sessions of one connection that are open at the same time, so that one device
/// cannot hold the sessions of every owner.
pub const MAX_SESSIONS_PER_CONNECTION: usize = 4;

/// The most upload sessions that are open at the same time, over all connections.
pub const MAX_OPEN_SESSIONS: usize = 64;

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
    /// The connection has [`MAX_SESSIONS_PER_CONNECTION`] open sessions, or the service has
    /// [`MAX_OPEN_SESSIONS`].
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
    /// The session was closed. A step that waited for its lock finds no session.
    closed: bool,
}

/// The place of one session in the table: its connection and its expiry, which the table reads
/// without the lock of the session, and the session.
#[derive(Debug, Clone)]
struct Slot {
    connection_id: Uuid,
    expires: Instant,
    session: Arc<tokio::sync::Mutex<Session>>,
}

/// The open upload sessions of the service.
#[derive(Debug, Clone, Default)]
pub struct Uploads {
    sessions: Arc<Mutex<HashMap<String, Slot>>>,
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
        let expires = Instant::now() + SESSION_LIFETIME;
        let session = Session {
            connection_id,
            reference,
            plan,
            declared: request.digest.hex.as_str().to_owned(),
            part,
            expires,
            closed: false,
        };
        let part = session.part.clone();
        let slot = Slot {
            connection_id,
            expires,
            session: Arc::new(tokio::sync::Mutex::new(session)),
        };
        if !self.admit(token.as_str(), slot) {
            BlobStore::discard_part(&part);
            return Err(UploadRefusal::Busy);
        }
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

    /// Close the open session `token` of `connection_id`: delete its staged copy and end it, so
    /// that it no longer counts toward the open sessions.
    ///
    /// # Errors
    ///
    /// [`UploadRefusal::Transfer`] when no open session of `connection_id` answers the token.
    pub async fn close(&self, connection_id: Uuid, token: &str) -> Result<(), UploadRefusal> {
        let (_, mut session) = self.session(connection_id, token).await?;
        session.closed = true;
        BlobStore::discard_part(&session.part);
        self.lock().remove(token);
        Ok(())
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
        let slot = self.lock().get(token).cloned().ok_or_else(unknown)?;
        let session = slot.session.lock_owned().await;
        // A step that waited for the lock can find the session ended by the step before it, for
        // example a second finalize of one token.
        let open = session
            .reference
            .status_view(&resumption_token)
            .session_state
            == UploadSessionState::Open;
        if session.connection_id != connection_id || !open || session.closed {
            return Err(unknown());
        }
        if session.expires <= Instant::now() {
            BlobStore::discard_part(&session.part);
            self.lock().remove(token);
            return Err(UploadRefusal::Transfer(TransferErrorCode::SessionExpired));
        }
        Ok((resumption_token, session))
    }

    /// Insert `slot` under `token` when its connection and the service are under their caps, in
    /// one step under the lock of the table. Each expired session that no step holds is forgotten
    /// first, and its staged copy deleted. Returns whether the slot is in the table.
    fn admit(&self, token: &str, slot: Slot) -> bool {
        let now = Instant::now();
        let mut sessions = self.lock();
        sessions.retain(|_, held| {
            if held.expires > now {
                return true;
            }
            match held.session.try_lock() {
                Ok(open) => {
                    BlobStore::discard_part(&open.part);
                    false
                }
                Err(_) => true,
            }
        });
        let of_connection = sessions
            .values()
            .filter(|held| held.connection_id == slot.connection_id)
            .count();
        if of_connection >= MAX_SESSIONS_PER_CONNECTION || sessions.len() >= MAX_OPEN_SESSIONS {
            return false;
        }
        sessions.insert(token.to_owned(), slot);
        true
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Slot>> {
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

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::pin;
    use std::task::{Context, Waker};

    use ratatoskr_blob_transfer_contracts::{TransferErrorCode, UploadSessionRequest};
    use uuid::Uuid;

    use super::{UploadRefusal, Uploads};
    use crate::blobs::BlobStore;

    /// Poll `future` once, and return whether it waits.
    fn waits<F: Future>(future: std::pin::Pin<&mut F>) -> bool {
        future
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    }

    #[tokio::test]
    async fn chunk_that_waited_for_a_closed_session_finds_no_session() {
        let root = tempfile::tempdir().expect("a blob root");
        let blobs = BlobStore::new(root.path());
        let uploads = Uploads::default();
        let connection = Uuid::now_v7();
        let bytes = b"closed while the chunk waits\n";
        let request: UploadSessionRequest = serde_json::from_value(serde_json::json!({
            "declared_size_bytes": bytes.len(),
            "media_type": "text/markdown",
            "digest": { "algorithm": "sha256", "hex": "0".repeat(64) },
            "chunk_size_bytes": 65_536,
        }))
        .expect("a declaration");
        let opened = uploads
            .open(&blobs, connection, &request, 1 << 20)
            .await
            .expect("a session");
        let token = opened.resumption_token.as_str().to_owned();

        // Hold the lock of the session. The close waits for it first and the chunk second: the
        // chunk took the session from the table before the close removes it.
        let slot = uploads.lock().get(&token).cloned().expect("the session");
        let held = slot.session.lock_owned().await;
        let mut close = pin!(uploads.close(connection, &token));
        let mut chunk = pin!(uploads.chunk(connection, &token, 0, bytes.to_vec()));
        assert!(waits(close.as_mut()), "the close waits for the lock");
        assert!(waits(chunk.as_mut()), "the chunk waits for the lock");
        drop(held);
        let closed = close.await;
        let chunk = chunk.await;

        assert!(closed.is_ok(), "{closed:?}");
        assert!(
            matches!(
                chunk,
                Err(UploadRefusal::Transfer(TransferErrorCode::SessionUnknown))
            ),
            "the chunk finds no open session: {chunk:?}"
        );
    }
}
