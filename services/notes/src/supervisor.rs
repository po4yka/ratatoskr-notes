//! The Sync supervisor: the continuous sync unit of each `sync` connection (workspace design D3).
//!
//! Each `sync` connection whose first login finished has one unit,
//! `ratatoskr-notes-sync@<id>.service`. The supervisor starts it after the first login, and again at
//! each start of the service, because the unit is not enabled and a restart of the host does not
//! start it. Each start of the unit runs `ob sync-config --mode pull-only` first, so a connection is
//! in pull-only mode at every start. A disconnect stops the unit (see `api::clean_up_sync_client`).
//!
//! An expired or revoked login does not stop `ob sync --continuous`: it logs the line
//! [`AUTH_FAILURE`] and tries again. The supervisor reads that one line from the journal of each
//! unit that it started, and no other: the other lines are dropped unread, and no file content is
//! taken from the journal. When the line repeats for [`EXPIRY`], the supervisor marks the connection
//! `login_expired` (`notes.sync.login_expired`) and stops the unit, once. It starts no login: the
//! owner connects again.

use std::collections::HashMap;
use std::pin::pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{Stream, StreamExt as _};
use ratatoskr_notes_persistence::Database;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use uuid::Uuid;

use crate::systemd::{JobResult, JournalLine, SystemdBus, UnitName};

/// The start of the line that `ob` 0.0.14 logs when the Sync server refuses its login token:
/// `getServer()` logs `new Error("Failed to authenticate: " + msg)` with `console.error`, so the
/// journal line is `Error: Failed to authenticate: <server message>` (`package/cli.js`).
/// PROVISIONAL: read from the code, not from an expired token. Spike 3.1 confirms it. The deploy role
/// pins `obsidian-headless`, because a release can change the line.
pub(crate) const AUTH_FAILURE: &str = "Failed to authenticate:";

/// How long the line must repeat before the login counts as expired (workspace design D3).
/// PROVISIONAL with [`AUTH_FAILURE`].
pub(crate) const EXPIRY: Duration = Duration::from_mins(5);

/// The longest pause between two failure lines of one run of failures. `ob` 0.0.14 tries to sync
/// every 30 seconds, and after each failed connection it waits up to 5 minutes before the next one.
/// A longer pause starts a new run.
const LONGEST_PAUSE: Duration = Duration::from_mins(10);

/// The stable code of an expired login.
const LOGIN_EXPIRED: &str = "notes.sync.login_expired";

/// Whether a journal line is the authentication failure of `ob`.
fn is_auth_failure(message: &str) -> bool {
    let message = message.trim_start();
    message
        .strip_prefix("Error: ")
        .unwrap_or(message)
        .starts_with(AUTH_FAILURE)
}

/// The run of authentication failures of one unit, by the monotonic time of each line.
#[derive(Debug, Default)]
struct AuthFailures {
    first: Option<Duration>,
    last: Option<Duration>,
}

impl AuthFailures {
    /// Count one failure line. Returns whether the run has lasted [`EXPIRY`].
    fn observe(&mut self, at: Duration) -> bool {
        let continues = self
            .last
            .is_some_and(|last| at.saturating_sub(last) <= LONGEST_PAUSE);
        if !continues {
            self.first = Some(at);
        }
        self.last = Some(at);
        self.first
            .is_some_and(|first| at.saturating_sub(first) >= EXPIRY)
    }
}

/// The supervisor of the sync units. Clones share one bus and one set of journal readers.
#[derive(Debug)]
pub(crate) struct Supervisor<B> {
    database: Database,
    systemd: Arc<B>,
    /// The journal reader of each connection whose unit this process started.
    readers: Arc<Mutex<HashMap<Uuid, CancellationToken>>>,
    tasks: TaskTracker,
    stopping: CancellationToken,
}

impl<B> Clone for Supervisor<B> {
    fn clone(&self) -> Self {
        Self {
            database: self.database.clone(),
            systemd: Arc::clone(&self.systemd),
            readers: Arc::clone(&self.readers),
            tasks: self.tasks.clone(),
            stopping: self.stopping.clone(),
        }
    }
}

impl<B: SystemdBus> Supervisor<B> {
    /// A supervisor that reaches systemd through `systemd`.
    pub(crate) fn new(database: Database, systemd: Arc<B>) -> Self {
        Self {
            database,
            systemd,
            readers: Arc::new(Mutex::new(HashMap::new())),
            tasks: TaskTracker::new(),
            stopping: CancellationToken::new(),
        }
    }

    /// Start the sync unit of a connection, wait for the start job, and read its journal for the
    /// authentication failure. A unit that runs already continues to run. Returns whether the job
    /// finished; a failure is logged.
    pub(crate) async fn start(&self, connection_id: Uuid) -> bool {
        let unit = UnitName::sync(connection_id);
        // Before the start, so that no line of this start is lost.
        let journal = self.systemd.follow_journal(&unit);
        match self.systemd.start_unit(&unit).await {
            Ok(JobResult::Done) => {
                tracing::info!(%unit, "the sync unit runs");
                self.read_journal(connection_id, journal);
                true
            }
            Ok(JobResult::NotDone(result)) => {
                tracing::error!(%unit, %result, "the sync unit did not start");
                false
            }
            Err(error) => {
                tracing::error!(%unit, %error, "the sync unit could not be started");
                false
            }
        }
    }

    /// Start the sync unit of each `sync` connection whose first login finished and that is
    /// `connecting` or `active`. The service calls it at start, before its listeners accept a
    /// request. A failure is logged and the start continues.
    pub(crate) async fn resume(&self) {
        let connections = match self.database.sync_connections_to_run().await {
            Ok(connections) => connections,
            Err(error) => {
                tracing::error!(%error, "the sync connections could not be read");
                return;
            }
        };
        for connection_id in connections {
            self.start(connection_id).await;
        }
    }

    /// Stop reading the journal of a connection. A disconnect calls it before it stops the unit.
    pub(crate) fn forget(&self, connection_id: Uuid) {
        if let Some(reader) = self
            .readers
            .lock()
            .ok()
            .and_then(|mut readers| readers.remove(&connection_id))
        {
            reader.cancel();
        }
    }

    /// Stop every journal reader and wait until each one ended. The units continue to run.
    pub(crate) async fn shut_down(&self) {
        self.stopping.cancel();
        self.tasks.close();
        self.tasks.wait().await;
    }

    /// Read the journal of the unit of a connection in a task of its own.
    fn read_journal(
        &self,
        connection_id: Uuid,
        journal: impl Stream<Item = JournalLine> + Send + 'static,
    ) {
        let stop = self.stopping.child_token();
        if let Ok(mut readers) = self.readers.lock()
            && let Some(earlier) = readers.insert(connection_id, stop.clone())
        {
            earlier.cancel();
        }
        let supervisor = self.clone();
        self.tasks.spawn(async move {
            supervisor.watch_login(connection_id, journal, stop).await;
        });
    }

    /// Count the authentication failures of a unit until they last [`EXPIRY`], then expire the
    /// login. Every other line is dropped unread.
    async fn watch_login(
        &self,
        connection_id: Uuid,
        journal: impl Stream<Item = JournalLine>,
        stop: CancellationToken,
    ) {
        let mut journal = pin!(journal);
        let mut failures = AuthFailures::default();
        loop {
            let line = tokio::select! {
                biased;
                () = stop.cancelled() => return,
                line = journal.next() => line,
            };
            let Some(line) = line else {
                tracing::warn!(%connection_id, "the journal of a sync unit ended; an expired login is found at the next start of the service");
                return;
            };
            if is_auth_failure(&line.message) && failures.observe(line.monotonic) {
                self.expire_login(connection_id).await;
                return;
            }
        }
    }

    /// Mark the connection `login_expired`, then stop its unit. A connection that the owner
    /// removed meanwhile is left to the disconnect.
    async fn expire_login(&self, connection_id: Uuid) {
        match self.database.expire_login(connection_id).await {
            Ok(true) => {}
            Ok(false) => return,
            Err(error) => {
                tracing::error!(%error, %connection_id, "an expired login could not be recorded");
                return;
            }
        }
        tracing::warn!(%connection_id, code = LOGIN_EXPIRED, "the Sync login of a connection expired; the owner must connect again");
        let unit = UnitName::sync(connection_id);
        match self.systemd.stop_unit(&unit).await {
            Ok(JobResult::Done) => {}
            Ok(JobResult::NotDone(result)) => {
                tracing::error!(%unit, %result, "the sync unit of an expired login did not stop");
            }
            Err(error) => {
                tracing::error!(%unit, %error, "the sync unit of an expired login could not be stopped");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{AuthFailures, is_auth_failure};

    #[test]
    fn only_the_failure_line_counts() {
        assert!(is_auth_failure(
            "Error: Failed to authenticate: token refused"
        ));
        assert!(is_auth_failure("Failed to authenticate: token refused"));
        assert!(!is_auth_failure("Connecting..."));
        assert!(!is_auth_failure(
            "Downloaded Failed to authenticate: notes.md"
        ));
    }

    #[test]
    fn a_long_pause_starts_a_new_run() {
        let mut failures = AuthFailures::default();
        assert!(!failures.observe(Duration::from_secs(0)));
        assert!(!failures.observe(Duration::from_secs(200)));
        // 11 minutes without the line: the run starts again.
        assert!(!failures.observe(Duration::from_secs(860)));
        assert!(!failures.observe(Duration::from_secs(1100)));
        assert!(failures.observe(Duration::from_secs(1160)));
    }
}
