//! The systemd template units of a `sync` connection, reached through one trait.
//!
//! Notes starts and stops `ratatoskr-notes-sync@<id>.service`,
//! `ratatoskr-notes-login@<id>.service` and `ratatoskr-notes-cleanup@<id>.service` through the
//! systemd D-Bus API (workspace design D3). Every call goes through [`SystemdBus`], so a test uses
//! a fake bus and production uses [`DbusSystemd`]. The journal of the sync units, which has no
//! D-Bus API, is read through the same trait.

use std::fmt;
use std::future::Future;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use futures_util::Stream;
use tokio::io::AsyncBufReadExt as _;
use uuid::Uuid;

/// The name of one template unit instance for one connection.
///
/// The constructors are the only way to make one, so a caller cannot ask systemd for a unit
/// outside the three templates that the polkit rule allows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnitName(String);

impl UnitName {
    /// `ratatoskr-notes-sync@<id>.service`: the continuous `ob sync` of a connection.
    #[must_use]
    pub fn sync(connection_id: Uuid) -> Self {
        Self(format!("ratatoskr-notes-sync@{connection_id}.service"))
    }

    /// `ratatoskr-notes-login@<id>.service`: the oneshot unit that runs `ob login` and
    /// `ob sync-setup` for a new connection (workspace design D4).
    #[must_use]
    pub fn login(connection_id: Uuid) -> Self {
        Self(format!("ratatoskr-notes-login@{connection_id}.service"))
    }

    /// `ratatoskr-notes-cleanup@<id>.service`: the oneshot unit that removes the working tree and
    /// the client state of a removed connection.
    #[must_use]
    pub fn cleanup(connection_id: Uuid) -> Self {
        Self(format!("ratatoskr-notes-cleanup@{connection_id}.service"))
    }

    /// The unit name as systemd reads it.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for UnitName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// The working tree of a `sync` connection on the host, which `ob` writes and the watcher reads.
#[must_use]
pub fn tree_dir(connection_id: Uuid) -> PathBuf {
    PathBuf::from(format!(
        "/mnt/nvme/ratatoskr/notes/connections/{connection_id}/tree"
    ))
}

/// The client state of a `sync` connection on the host (`ob-home`: the Sync token and the vault
/// configuration of `ob`).
#[must_use]
pub fn client_dir(connection_id: Uuid) -> PathBuf {
    PathBuf::from(format!(
        "/mnt/nvme/ratatoskr/notes/connections/{connection_id}/ob-home"
    ))
}

/// The result of one systemd job, as the `JobRemoved` signal reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobResult {
    /// `done`: the job finished. For a oneshot unit, its `ExecStart=` lines succeeded.
    Done,
    /// Any other result word, for example `failed`, `timeout` or `canceled`.
    NotDone(String),
}

/// A call to systemd that did not produce a job result.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BusError {
    /// The system bus could not be reached.
    #[error("the system bus could not be reached")]
    Unreachable(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// systemd refused the call or the job could not be followed.
    #[error("systemd refused the call")]
    Refused(#[source] Box<dyn std::error::Error + Send + Sync>),
}

/// The journal namespace of the sync units (owner decision A of 2026-10-14). The sync unit sets
/// `LogNamespace=` to it, so its lines are not in the system journal, and the service needs read
/// access to this namespace only.
pub const SYNC_LOG_NAMESPACE: &str = "ratatoskr-notes-sync";

/// One line of the journal of the sync units.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalLine {
    /// The unit that wrote the line, from the trusted field `_SYSTEMD_UNIT`, which journald sets
    /// and a process cannot.
    pub unit: String,
    /// When journald received the line, on the monotonic clock of the host (`__MONOTONIC_TIMESTAMP`).
    pub monotonic: Duration,
    /// The message of the line.
    pub message: String,
}

/// The journal of the sync units cannot be read. The text is the reason that `journalctl` gives.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("the journal of the sync units cannot be read: {0}")]
pub struct JournalUnreadable(pub String);

/// The systemd calls that Notes makes. A start or a stop waits until the job is finished.
pub trait SystemdBus: Send + Sync + 'static {
    /// Start a unit and wait for the job. For a oneshot unit the job ends when the unit's
    /// `ExecStart=` lines have run.
    fn start_unit(
        &self,
        unit: &UnitName,
    ) -> impl Future<Output = Result<JobResult, BusError>> + Send;

    /// Stop a unit and wait for the job. A unit that systemd does not know counts as stopped.
    fn stop_unit(
        &self,
        unit: &UnitName,
    ) -> impl Future<Output = Result<JobResult, BusError>> + Send;

    /// The exit status of the last main process of a unit, as `ExecMainStatus` reports it.
    /// `None` when the process did not exit by itself, for example when a signal killed it.
    /// For a oneshot unit whose start failed, it is the status of the command that failed.
    fn exit_status(
        &self,
        unit: &UnitName,
    ) -> impl Future<Output = Result<Option<i32>, BusError>> + Send;

    /// The lines that the journal namespace of the sync units receives from now on, in order. The
    /// stream ends when the journal cannot be read any more. A failure is logged.
    fn follow_journal(&self) -> impl Stream<Item = JournalLine> + Send + 'static;

    /// Whether the service can read the journal namespace of the sync units.
    fn check_journal(&self) -> impl Future<Output = Result<(), JournalUnreadable>> + Send;
}

/// The well-known name, object and interface of the systemd manager.
const SYSTEMD: &str = "org.freedesktop.systemd1";
const MANAGER_PATH: &str = "/org/freedesktop/systemd1";
const MANAGER_INTERFACE: &str = "org.freedesktop.systemd1.Manager";

/// The error name systemd answers for a unit it does not know.
const NO_SUCH_UNIT: &str = "org.freedesktop.systemd1.NoSuchUnit";

/// The interface that carries `ExecMainCode` and `ExecMainStatus`.
const SERVICE_INTERFACE: &str = "org.freedesktop.systemd1.Service";

/// `CLD_EXITED` from `<signal.h>`: the process exited, and the status is its exit code.
const CLD_EXITED: i32 = 1;

/// Where Debian 12 installs `journalctl`. The service user needs read access to the journal
/// namespace [`SYNC_LOG_NAMESPACE`], and to no other journal: the deploy role gives the group
/// `ratatoskr-notes` an ACL on the directory of the namespace.
const JOURNALCTL: &str = "/usr/bin/journalctl";

/// The stable code of a journal that the service cannot read. An expired Sync login is then not
/// found.
pub(crate) const JOURNAL_UNREADABLE: &str = "notes.sync.journal_unreadable";

/// How long the check of the journal may take.
const CHECK_TIMEOUT: Duration = Duration::from_secs(10);

/// The production bus: the systemd manager on the system bus.
///
/// The connection opens on the first call, so a developer machine without a system bus starts
/// the service, and only a call to systemd fails there. The polkit rule of the host allows the
/// service user to manage exactly the three template units (workspace design D3).
#[derive(Debug, Default)]
pub struct DbusSystemd {
    connection: tokio::sync::OnceCell<zbus::Connection>,
}

impl DbusSystemd {
    /// A bus that has not connected yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    async fn manager(&self) -> Result<zbus::Proxy<'static>, BusError> {
        let connection = self
            .connection
            .get_or_try_init(zbus::Connection::system)
            .await
            .map_err(|error| BusError::Unreachable(error.into()))?;
        zbus::Proxy::new(connection, SYSTEMD, MANAGER_PATH, MANAGER_INTERFACE)
            .await
            .map_err(|error| BusError::Unreachable(error.into()))
    }

    /// Calls `StartUnit` or `StopUnit` in mode `replace` and waits for the `JobRemoved` signal of
    /// the job it returns.
    async fn run_job(&self, method: &'static str, unit: &UnitName) -> Result<JobResult, BusError> {
        use futures_util::StreamExt as _;

        let manager = self.manager().await?;
        // systemd sends job signals only to subscribed clients. Subscribe and listen before the
        // call, so the signal of a job that finishes at once is not missed.
        manager
            .call_method("Subscribe", &())
            .await
            .map_err(|error| BusError::Refused(error.into()))?;
        let mut removed = manager
            .receive_signal("JobRemoved")
            .await
            .map_err(|error| BusError::Refused(error.into()))?;

        let job: zbus::zvariant::OwnedObjectPath =
            match manager.call(method, &(unit.as_str(), "replace")).await {
                Ok(job) => job,
                Err(zbus::Error::MethodError(name, _, _))
                    if method == "StopUnit" && name.as_str() == NO_SUCH_UNIT =>
                {
                    return Ok(JobResult::Done);
                }
                Err(error) => return Err(BusError::Refused(error.into())),
            };

        while let Some(message) = removed.next().await {
            let (_id, path, _unit, result): (u32, zbus::zvariant::OwnedObjectPath, String, String) =
                message
                    .body()
                    .deserialize()
                    .map_err(|error| BusError::Refused(error.into()))?;
            if path == job {
                return Ok(if result == "done" {
                    JobResult::Done
                } else {
                    JobResult::NotDone(result)
                });
            }
        }
        Err(BusError::Refused(
            "the signal stream ended before the job finished".into(),
        ))
    }
}

impl SystemdBus for DbusSystemd {
    async fn start_unit(&self, unit: &UnitName) -> Result<JobResult, BusError> {
        self.run_job("StartUnit", unit).await
    }

    async fn stop_unit(&self, unit: &UnitName) -> Result<JobResult, BusError> {
        self.run_job("StopUnit", unit).await
    }

    async fn exit_status(&self, unit: &UnitName) -> Result<Option<i32>, BusError> {
        let manager = self.manager().await?;
        let path: zbus::zvariant::OwnedObjectPath = manager
            .call("GetUnit", &(unit.as_str(),))
            .await
            .map_err(|error| BusError::Refused(error.into()))?;
        let service: zbus::Proxy<'_> = zbus::proxy::Builder::new(manager.connection())
            .destination(SYSTEMD)
            .and_then(|builder| builder.path(path))
            .and_then(|builder| builder.interface(SERVICE_INTERFACE))
            .map_err(|error| BusError::Refused(error.into()))?
            .cache_properties(zbus::proxy::CacheProperties::No)
            .build()
            .await
            .map_err(|error| BusError::Refused(error.into()))?;
        let code: i32 = service
            .get_property("ExecMainCode")
            .await
            .map_err(|error| BusError::Refused(error.into()))?;
        let status: i32 = service
            .get_property("ExecMainStatus")
            .await
            .map_err(|error| BusError::Refused(error.into()))?;
        Ok((code == CLD_EXITED).then_some(status))
    }

    /// Runs `journalctl --namespace=ratatoskr-notes-sync --follow --lines=0 --output=json
    /// --output-fields=MESSAGE,_SYSTEMD_UNIT` and reads its lines. `journalctl` stops when the stream
    /// is dropped. Each line that it writes to stderr is logged with [`JOURNAL_UNREADABLE`].
    fn follow_journal(&self) -> impl Stream<Item = JournalLine> + Send + 'static {
        let reader = tokio::process::Command::new(JOURNALCTL)
            .arg(format!("--namespace={SYNC_LOG_NAMESPACE}"))
            .args([
                "--follow",
                "--lines=0",
                "--output=json",
                "--output-fields=MESSAGE,_SYSTEMD_UNIT",
                "--no-pager",
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .inspect_err(|error| {
                tracing::error!(code = JOURNAL_UNREADABLE, %error, "journalctl cannot be started");
            })
            .ok()
            .and_then(|mut child| {
                if let Some(errors) = child.stderr.take() {
                    tokio::spawn(log_stderr(errors));
                }
                let output = child.stdout.take()?;
                Some((child, tokio::io::BufReader::new(output).lines()))
            });
        futures_util::stream::unfold(reader, |reader| async move {
            let (child, mut lines) = reader?;
            let line = next_journal_line(&mut lines).await?;
            Some((line, Some((child, lines))))
        })
    }

    /// Runs `journalctl --namespace=ratatoskr-notes-sync --lines=0`, which opens the files of the
    /// namespace and prints no entry. It exits 1 when it can open none of them.
    async fn check_journal(&self) -> Result<(), JournalUnreadable> {
        let run = tokio::process::Command::new(JOURNALCTL)
            .arg(format!("--namespace={SYNC_LOG_NAMESPACE}"))
            .args(["--lines=0", "--no-pager"])
            .stdin(Stdio::null())
            .kill_on_drop(true)
            .output();
        let output = match tokio::time::timeout(CHECK_TIMEOUT, run).await {
            Ok(Ok(output)) => output,
            Ok(Err(error)) => return Err(JournalUnreadable(error.to_string())),
            Err(_) => return Err(JournalUnreadable("journalctl did not finish".to_owned())),
        };
        let errors = String::from_utf8_lossy(&output.stderr).trim().to_owned();
        if output.status.success() {
            if !errors.is_empty() {
                tracing::warn!(code = JOURNAL_UNREADABLE, reason = %errors, "journalctl reports a problem with the journal of the sync units");
            }
            return Ok(());
        }
        Err(JournalUnreadable(if errors.is_empty() {
            format!("journalctl ended with {}", output.status)
        } else {
            errors
        }))
    }
}

/// Logs each line that `journalctl` writes to stderr, until it ends. The lines name files and
/// permissions, never the content of a journal entry.
async fn log_stderr(errors: tokio::process::ChildStderr) {
    let mut lines = tokio::io::BufReader::new(errors).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        tracing::error!(code = JOURNAL_UNREADABLE, reason = %line, "journalctl reports a problem with the journal of the sync units");
    }
}

/// The output of `journalctl`, line by line.
type JournalctlLines = tokio::io::Lines<tokio::io::BufReader<tokio::process::ChildStdout>>;

/// The next journal line that `journalctl` writes, or `None` when it ended.
async fn next_journal_line(lines: &mut JournalctlLines) -> Option<JournalLine> {
    loop {
        let text = match lines.next_line().await {
            Ok(Some(text)) => text,
            Ok(None) => return None,
            Err(error) => {
                tracing::error!(%error, "the journal of a unit could not be read");
                return None;
            }
        };
        if let Some(line) = journal_line(&text) {
            return Some(line);
        }
    }
}

/// One entry of `journalctl --output=json`, with the fields that the supervisor reads.
#[derive(Debug, serde::Deserialize)]
struct JournalEntry {
    #[serde(rename = "_SYSTEMD_UNIT", default)]
    unit: Option<String>,
    #[serde(rename = "__MONOTONIC_TIMESTAMP")]
    monotonic_micros: String,
    /// A string, or an array of bytes when the message is not UTF-8.
    #[serde(rename = "MESSAGE", default)]
    message: Option<serde_json::Value>,
}

/// The line of one JSON entry, or `None` when it has no unit, no time or no UTF-8 message.
fn journal_line(text: &str) -> Option<JournalLine> {
    let entry: JournalEntry = serde_json::from_str(text).ok()?;
    let micros: u64 = entry.monotonic_micros.parse().ok()?;
    let serde_json::Value::String(message) = entry.message? else {
        return None;
    };
    Some(JournalLine {
        unit: entry.unit?,
        monotonic: Duration::from_micros(micros),
        message,
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{JournalLine, journal_line};

    #[test]
    fn journalctl_json_entry_is_a_line() {
        // The form of `journalctl --output=json --output-fields=MESSAGE,_SYSTEMD_UNIT` of systemd
        // 252 in debian:12-slim.
        let entry = r#"{"__CURSOR":"s=94dc;i=3","__REALTIME_TIMESTAMP":"1791977342156364","MESSAGE":"Error: Failed to authenticate: stub","_SYSTEMD_UNIT":"ratatoskr-notes-sync@0192.service","_BOOT_ID":"022ee5327c954d1ea0c8f4a2e531e6ef","__MONOTONIC_TIMESTAMP":"18685091972"}"#;
        assert_eq!(
            journal_line(entry),
            Some(JournalLine {
                unit: "ratatoskr-notes-sync@0192.service".to_owned(),
                monotonic: Duration::from_micros(18_685_091_972),
                message: "Error: Failed to authenticate: stub".to_owned(),
            })
        );
        let binary =
            r#"{"MESSAGE":[69,255],"_SYSTEMD_UNIT":"a.service","__MONOTONIC_TIMESTAMP":"1"}"#;
        assert_eq!(journal_line(binary), None, "a message that is not UTF-8");
        let no_unit =
            r#"{"MESSAGE":"Error: Failed to authenticate: x","__MONOTONIC_TIMESTAMP":"1"}"#;
        assert_eq!(journal_line(no_unit), None, "an entry without a unit");
    }
}
