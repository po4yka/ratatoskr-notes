//! The encryption of the Obsidian passwords at rest (workspace design D4).
//!
//! The key is 32 random bytes in one file, `/etc/ratatoskr/notes-secrets.key`, mode 0400, owner
//! `ratatoskr-notes`. Each password is sealed with XChaCha20-Poly1305 under a fresh random 24-byte
//! nonce. The associated data is `ratatoskr-notes:<connection id>:<secret kind>`, so a ciphertext
//! opens only in the row that it was written to. The MFA code is never sealed and never stored.

use std::fmt;
use std::path::Path;

use chacha20poly1305::aead::{Aead as _, Generate as _, Payload};
use chacha20poly1305::{KeyInit as _, XChaCha20Poly1305, XNonce};
use uuid::Uuid;

/// The length of the key in the key file, in bytes.
const KEY_BYTES: usize = 32;

/// The kind of a stored secret. The MFA code has no kind, because it is never stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecretKind {
    /// The password of the Obsidian account.
    AccountPassword,
    /// The end-to-end encryption password of the remote vault.
    E2ePassword,
}

impl SecretKind {
    /// The word that `notes.connection_secrets.secret_kind` stores.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AccountPassword => "account_password",
            Self::E2ePassword => "e2e_password",
        }
    }
}

/// One sealed secret: the nonce and the ciphertext with its tag.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sealed {
    /// The kind of the secret.
    pub kind: SecretKind,
    /// The 24-byte nonce.
    pub nonce: Vec<u8>,
    /// The ciphertext with its 16-byte Poly1305 tag.
    pub ciphertext: Vec<u8>,
}

/// The key file could not be used. The message names the reason, never a byte of the key.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum KeyError {
    /// The file could not be read.
    #[error("the secrets key file could not be read")]
    Unreadable(#[source] std::io::Error),
    /// The file does not hold exactly 32 bytes.
    #[error("the secrets key file does not hold exactly 32 bytes")]
    WrongLength,
    /// The group or other users can read or write the file.
    #[error("the secrets key file is open to other users; it must be mode 0400 or 0600")]
    TooOpen,
}

/// A secret could not be sealed.
#[derive(Debug, thiserror::Error)]
#[error("a secret could not be sealed")]
pub struct SealError;

/// The key that seals the Obsidian passwords. Its `Debug` output holds no key byte.
pub struct SecretsKey {
    cipher: XChaCha20Poly1305,
}

impl fmt::Debug for SecretsKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretsKey([REDACTED])")
    }
}

impl SecretsKey {
    /// Read the key file.
    ///
    /// # Errors
    ///
    /// [`KeyError`] when the file cannot be read, does not hold exactly 32 bytes, or is open to
    /// the group or other users.
    pub fn load(path: &Path) -> Result<Self, KeyError> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = std::fs::metadata(path)
                .map_err(KeyError::Unreadable)?
                .permissions()
                .mode();
            if mode & 0o077 != 0 {
                return Err(KeyError::TooOpen);
            }
        }
        let bytes =
            secrecy::zeroize::Zeroizing::new(std::fs::read(path).map_err(KeyError::Unreadable)?);
        if bytes.len() != KEY_BYTES {
            return Err(KeyError::WrongLength);
        }
        let cipher =
            XChaCha20Poly1305::new_from_slice(&bytes).map_err(|_| KeyError::WrongLength)?;
        Ok(Self { cipher })
    }

    /// Seal one secret of one connection under a fresh random nonce.
    ///
    /// # Errors
    ///
    /// [`SealError`] when the operating system gives no random bytes or the cipher refuses the
    /// input.
    pub fn seal(
        &self,
        connection_id: Uuid,
        kind: SecretKind,
        plaintext: &[u8],
    ) -> Result<Sealed, SealError> {
        let nonce = XNonce::try_generate().map_err(|_| SealError)?;
        let aad = associated_data(connection_id, kind);
        let ciphertext = self
            .cipher
            .encrypt(
                &nonce,
                Payload {
                    msg: plaintext,
                    aad: &aad,
                },
            )
            .map_err(|_| SealError)?;
        Ok(Sealed {
            kind,
            nonce: nonce.to_vec(),
            ciphertext,
        })
    }
}

/// `ratatoskr-notes:<connection id>:<secret kind>`, as `schema.sql` states it.
fn associated_data(connection_id: Uuid, kind: SecretKind) -> Vec<u8> {
    format!("ratatoskr-notes:{connection_id}:{}", kind.as_str()).into_bytes()
}
