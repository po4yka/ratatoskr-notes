//! The first login of a `sync` connection (workspace design D4).
//!
//! The connect request carries the Obsidian email, the account password, an optional MFA code and
//! an optional end-to-end password. The service seals the two passwords into
//! `notes.connection_secrets`, writes the values of the login to credential files, and starts
//! `ratatoskr-notes-login@<id>.service`, which loads the files with `LoadCredential=` and runs
//! `ob login` and then `ob sync-setup`. When the unit fails, its exit status names the step and
//! the reason, and the connect operation fails with one of four stable codes. No secret is in a
//! log line, an error envelope or a `Debug` output: each one is a [`SecretString`].

use std::fs::{DirBuilder, OpenOptions};
use std::io::Write as _;
use std::os::unix::fs::{DirBuilderExt as _, OpenOptionsExt as _};
use std::path::{Path, PathBuf};

use secrecy::{ExposeSecret, SecretString};
use uuid::Uuid;

use crate::systemd::{BusError, JobResult, SystemdBus, UnitName};

/// The longest email that the login takes, in characters (RFC 5321 path limit).
const MAX_EMAIL_CHARS: usize = 254;

/// The longest password that the login takes, in characters.
const MAX_PASSWORD_CHARS: usize = 1024;

/// The longest MFA code that the login takes, in characters.
const MAX_MFA_CHARS: usize = 64;

/// The exit statuses of `ratatoskr-notes-login@.service`, the contract between the unit and this
/// module. The unit maps each failure of `ob` to one of them. The unit exits 10 when `ob login`
/// fails for any other reason, and every status that is not listed here, 10 among them, is
/// `notes.sync.login_failed`.
pub(crate) mod exit {
    /// `ob login` ended without a token, because the account needs an MFA code that the owner
    /// did not give.
    pub(crate) const MFA_REQUIRED: i32 = 11;
    /// `ob sync-setup` exited 2: the end-to-end password is wrong.
    pub(crate) const E2E_PASSWORD_WRONG: i32 = 12;
    /// `ob sync-setup` exited 3: the account has no remote vault with this id.
    pub(crate) const VAULT_NOT_FOUND: i32 = 13;
}

/// The Obsidian login of a `sync` connect request.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SyncLogin {
    /// The account email. It is not a secret, but it is personal data, so it is not logged.
    pub(crate) email: String,
    /// The account password.
    pub(crate) password: SecretString,
    /// The one-time MFA code, when the account uses MFA. Never stored.
    #[serde(default)]
    pub(crate) mfa_code: Option<SecretString>,
    /// The end-to-end encryption password, when the remote vault uses one.
    #[serde(default)]
    pub(crate) e2e_password: Option<SecretString>,
}

impl SyncLogin {
    /// Whether every field has a form that the login unit can take safely: no control character,
    /// no whitespace or quote in the email and the MFA code, bounded lengths, and no leading `-`
    /// in the email and the MFA code, which `ob` gets in its argv and could read as an option.
    pub(crate) fn is_valid(&self) -> bool {
        let email = &self.email;
        let email_valid = (3..=MAX_EMAIL_CHARS).contains(&email.chars().count())
            && email.contains('@')
            && !email.starts_with('-')
            && email.chars().all(|c| {
                !c.is_control() && !c.is_whitespace() && !matches!(c, '"' | '\'' | '\\' | '$' | '`')
            });
        let password_valid = |secret: &SecretString| {
            let value = secret.expose_secret();
            (1..=MAX_PASSWORD_CHARS).contains(&value.chars().count())
                && !value.chars().any(char::is_control)
        };
        let mfa_valid = self.mfa_code.as_ref().is_none_or(|code| {
            let value = code.expose_secret();
            (1..=MAX_MFA_CHARS).contains(&value.len())
                && !value.starts_with('-')
                && value.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
        });
        email_valid
            && password_valid(&self.password)
            && self.e2e_password.as_ref().is_none_or(password_valid)
            && mfa_valid
    }
}

/// Whether a vault key can name a remote vault for `ob sync-setup --vault`: the remote vault id,
/// ASCII letters, digits, `-` and `_`, and no leading `-`, which `ob` could read as an option.
pub(crate) fn valid_remote_vault_id(key: &str) -> bool {
    !key.is_empty()
        && !key.starts_with('-')
        && key
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_'))
}

/// Why a login failed. Each one is a stable code that a client may branch on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LoginFailure {
    /// `notes.sync.login_failed`.
    LoginFailed,
    /// `notes.sync.mfa_required`.
    MfaRequired,
    /// `notes.sync.e2e_password_wrong`.
    E2ePasswordWrong,
    /// `notes.sync.vault_not_found`.
    VaultNotFound,
}

impl LoginFailure {
    /// The failure that an exit status of the login unit names. An unknown status, or none,
    /// is a failed login.
    pub(crate) const fn from_exit_status(status: Option<i32>) -> Self {
        match status {
            Some(exit::MFA_REQUIRED) => Self::MfaRequired,
            Some(exit::E2E_PASSWORD_WRONG) => Self::E2ePasswordWrong,
            Some(exit::VAULT_NOT_FOUND) => Self::VaultNotFound,
            _ => Self::LoginFailed,
        }
    }

    /// The stable code and the safe message of the error envelope.
    pub(crate) const fn fields(self) -> (&'static str, &'static str) {
        match self {
            Self::LoginFailed => (
                "notes.sync.login_failed",
                "The Obsidian login failed. Check the email, the password and the MFA code.",
            ),
            Self::MfaRequired => (
                "notes.sync.mfa_required",
                "The Obsidian account needs an MFA code. Connect again with the code.",
            ),
            Self::E2ePasswordWrong => (
                "notes.sync.e2e_password_wrong",
                "The end-to-end encryption password of the remote vault is wrong.",
            ),
            Self::VaultNotFound => (
                "notes.sync.vault_not_found",
                "The Obsidian account has no remote vault with this id.",
            ),
        }
    }
}

/// The credential files of one login: `<root>/<connection id>/`, mode 0700, with one file of mode
/// 0600 for each value. The login unit names the same files in its `LoadCredential=` lines. The
/// directory is deleted when this value is dropped, so also when the request is cancelled.
#[derive(Debug)]
pub(crate) struct CredentialFiles {
    directory: PathBuf,
}

impl CredentialFiles {
    /// Writes the five files that the login unit loads. `mfa` and `e2e` are empty when the owner
    /// gave no value, because systemd fails the start of a unit whose credential file is missing.
    ///
    /// # Errors
    ///
    /// The I/O error when a directory or a file cannot be created or written. What was written is
    /// deleted.
    pub(crate) fn write(
        root: &Path,
        connection_id: Uuid,
        vault_id: &str,
        sync_login: &SyncLogin,
    ) -> std::io::Result<Self> {
        DirBuilder::new().recursive(true).mode(0o700).create(root)?;
        let directory = root.join(connection_id.to_string());
        DirBuilder::new().mode(0o700).create(&directory)?;
        let files = Self { directory };
        for (name, value) in [
            ("email", sync_login.email.as_str()),
            ("password", sync_login.password.expose_secret()),
            ("mfa", exposed_or_empty(sync_login.mfa_code.as_ref())),
            ("vault", vault_id),
            ("e2e", exposed_or_empty(sync_login.e2e_password.as_ref())),
        ] {
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(files.directory.join(name))?
                .write_all(value.as_bytes())?;
        }
        Ok(files)
    }
}

/// The value of an optional secret, or the empty string.
fn exposed_or_empty(secret: Option<&SecretString>) -> &str {
    secret.map_or("", ExposeSecret::expose_secret)
}

impl Drop for CredentialFiles {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_dir_all(&self.directory) {
            tracing::error!(%error, directory = %self.directory.display(), "the credential files of a login could not be deleted");
        }
    }
}

/// Start the login unit of a connection and wait for it. The [`CredentialFiles`] of the login must
/// exist while it runs.
///
/// Returns `None` when the login and the setup succeeded, and the failure when the unit failed.
///
/// # Errors
///
/// [`BusError`] when systemd could not run the unit or report its result.
pub(crate) async fn run<B: SystemdBus>(
    systemd: &B,
    connection_id: Uuid,
) -> Result<Option<LoginFailure>, BusError> {
    let unit = UnitName::login(connection_id);
    match systemd.start_unit(&unit).await? {
        JobResult::Done => Ok(None),
        JobResult::NotDone(result) => {
            let status = systemd.exit_status(&unit).await?;
            let failure = LoginFailure::from_exit_status(status);
            tracing::warn!(%unit, %result, ?status, code = failure.fields().0, "the login failed");
            Ok(Some(failure))
        }
    }
}
