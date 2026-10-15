//! The Sync supervisor: the continuous sync unit of each `sync` connection (workspace design D3).
//!
//! Each `sync` connection whose first login finished has one unit,
//! `ratatoskr-notes-sync@<id>.service`. The supervisor starts it after the first login, and again at
//! each start of the service, because the unit is not enabled and a restart of the host does not
//! start it. Each start of the unit runs `ob sync-config --mode pull-only` first, so a connection is
//! in pull-only mode at every start. A disconnect stops the unit (see `api::clean_up_sync_client`).
//!
//! An expired or revoked login does not stop `ob sync --continuous`: it logs the line
//! [`AUTH_FAILURE`] and tries again. The sync units log to the journal namespace
//! [`SYNC_LOG_NAMESPACE`] (owner decision A of 2026-10-14). The supervisor follows that namespace
//! with one follower for all units, and sends each line to its connection by the trusted field
//! `_SYSTEMD_UNIT`. One follower, not one for each unit, because each follower is one `journalctl`
//! process and the host has no spare memory. Only that one line counts, and only for a unit that
//! this process started: the other lines are dropped unread, and no file content is taken from the
//! journal. When the line repeats for [`EXPIRY`], the supervisor marks the connection
//! `login_expired` (`notes.sync.login_expired`) and stops the unit, once. It starts no login: the
//! owner connects again.
//!
//! At start the supervisor checks that it can read the namespace, and logs
//! `notes.sync.journal_unreadable` with the reason of `journalctl` when it cannot. A follower that
//! ends starts again after a backoff of [`FIRST_BACKOFF`] that doubles up to [`LAST_BACKOFF`], until
//! the service stops. The run of failures of a unit stays across a new follower.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{Stream, StreamExt as _};
use ratatoskr_notes_persistence::Database;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use uuid::Uuid;

use crate::api::TestHook;
use crate::operation::{self, Completion, Phase, Report};
use crate::systemd::{
    JOURNAL_UNREADABLE, JobResult, JournalLine, SYNC_LOG_NAMESPACE, SystemdBus, UnitName,
};
use crate::tree_sync::Pipeline;

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

/// The wait before the first new follower after a follower ended.
const FIRST_BACKOFF: Duration = Duration::from_secs(1);

/// The longest wait before a new follower. A follower that ran this long before it ended starts
/// the next backoff at [`FIRST_BACKOFF`] again.
const LAST_BACKOFF: Duration = Duration::from_mins(1);

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

/// A sync unit whose authentication failures count.
#[derive(Debug)]
struct Watched {
    connection_id: Uuid,
    failures: AuthFailures,
}

/// One follower of the journal namespace.
type Follower = Pin<Box<dyn Stream<Item = JournalLine> + Send>>;

/// The supervisor of the sync units. Clones share one bus, one follower and one set of units.
#[derive(Debug)]
pub(crate) struct Supervisor<B> {
    database: Database,
    systemd: Arc<B>,
    /// The units that this process started, by unit name.
    watched: Arc<Mutex<HashMap<String, Watched>>>,
    /// Whether the follower task runs.
    following: Arc<AtomicBool>,
    tasks: TaskTracker,
    stopping: CancellationToken,
    /// The watcher and the recorder of each working tree.
    pipeline: Pipeline,
    /// The stop of the pipeline of each connection whose sync unit this process started.
    pipelines: Arc<Mutex<HashMap<Uuid, CancellationToken>>>,
    /// A pause after a login expired, for tests.
    after_terminal_state: Option<TestHook>,
}

impl<B> Clone for Supervisor<B> {
    fn clone(&self) -> Self {
        Self {
            database: self.database.clone(),
            systemd: Arc::clone(&self.systemd),
            watched: Arc::clone(&self.watched),
            following: Arc::clone(&self.following),
            tasks: self.tasks.clone(),
            stopping: self.stopping.clone(),
            pipeline: self.pipeline.clone(),
            pipelines: Arc::clone(&self.pipelines),
            after_terminal_state: self.after_terminal_state.clone(),
        }
    }
}

impl<B: SystemdBus> Supervisor<B> {
    /// A supervisor that reaches systemd through `systemd`.
    pub(crate) fn new(database: Database, systemd: Arc<B>, pipeline: Pipeline) -> Self {
        Self {
            database,
            systemd,
            watched: Arc::new(Mutex::new(HashMap::new())),
            following: Arc::new(AtomicBool::new(false)),
            tasks: TaskTracker::new(),
            stopping: CancellationToken::new(),
            pipeline,
            pipelines: Arc::new(Mutex::new(HashMap::new())),
            after_terminal_state: None,
        }
    }

    /// Wait for `hook` after a login expired, before the rest of the step. For tests.
    pub(crate) fn set_after_terminal_state(&mut self, hook: TestHook) {
        self.after_terminal_state = Some(hook);
    }

    /// Use `pipeline` for the working trees of the units that start from now on.
    pub(crate) fn set_pipeline(&mut self, pipeline: Pipeline) {
        self.pipeline = pipeline;
    }

    /// Start the sync unit of a connection, wait for the start job, and count the authentication
    /// failures of the unit. A unit that runs already continues to run. Returns whether the job
    /// finished; a failure is logged.
    ///
    /// When the unit runs, the pipeline of its working tree starts: the watcher feeds the recorder,
    /// and, when `completion` is given, the first full scan completes the connect operation.
    pub(crate) async fn start(&self, connection_id: Uuid, completion: Option<Completion>) -> bool {
        let unit = UnitName::sync(connection_id);
        // Before the start, so that no line of this start is lost.
        self.follow();
        self.watch(connection_id, &unit);
        match self.systemd.start_unit(&unit).await {
            Ok(JobResult::Done) => {
                tracing::info!(%unit, "the sync unit runs");
                let completion = match completion {
                    Some(mut completion) => {
                        completion.journal_readable = self.systemd.check_journal().await.is_ok();
                        operation::report(
                            &self.database,
                            completion.owner_id,
                            completion.operation_id,
                            Report::Running(Phase::FirstSync),
                        )
                        .await;
                        Some(completion)
                    }
                    None => None,
                };
                self.run_pipeline(connection_id, completion);
                return true;
            }
            Ok(JobResult::NotDone(result)) => {
                tracing::error!(%unit, %result, "the sync unit did not start");
            }
            Err(error) => {
                tracing::error!(%unit, %error, "the sync unit could not be started");
            }
        }
        self.forget(connection_id);
        false
    }

    /// Check that the journal namespace of the sync units can be read, then start the sync unit of
    /// each `sync` connection whose first login finished and that is `connecting` or `active`. The
    /// service calls it at start, before its listeners accept a request. A failure is logged and
    /// the start continues.
    pub(crate) async fn resume(&self) {
        if let Err(error) = self.systemd.check_journal().await {
            tracing::error!(
                code = JOURNAL_UNREADABLE,
                namespace = SYNC_LOG_NAMESPACE,
                %error,
                "the service cannot read the journal of the sync units; an expired Sync login is not found"
            );
        }
        self.follow();
        let connections = match self.database.sync_connections_to_run().await {
            Ok(connections) => connections,
            Err(error) => {
                tracing::error!(%error, "the sync connections could not be read");
                return;
            }
        };
        for connection_id in connections {
            let completion = match self.database.connect_operation(connection_id).await {
                Ok(Some(operation)) => completion_of(&operation),
                Ok(None) => None,
                Err(error) => {
                    tracing::error!(%error, %connection_id, "the connect operation of a connection could not be read");
                    None
                }
            };
            self.start(connection_id, completion).await;
        }
    }

    /// Start the pipeline of the working tree of `connection_id`, in place of one that runs.
    fn run_pipeline(&self, connection_id: Uuid, completion: Option<Completion>) {
        let stop = self.stopping.child_token();
        if let Ok(mut pipelines) = self.pipelines.lock()
            && let Some(previous) = pipelines.insert(connection_id, stop.clone())
        {
            previous.cancel();
        }
        let pipeline = self.pipeline.clone();
        self.tasks
            .spawn(async move { pipeline.run(connection_id, stop, completion).await });
    }

    /// Stop counting the failures of the unit of a connection. A disconnect calls it before it
    /// stops the unit.
    pub(crate) fn forget(&self, connection_id: Uuid) {
        if let Ok(mut watched) = self.watched.lock() {
            watched.remove(UnitName::sync(connection_id).as_str());
        }
        if let Some(stop) = self
            .pipelines
            .lock()
            .ok()
            .and_then(|mut pipelines| pipelines.remove(&connection_id))
        {
            stop.cancel();
        }
    }

    /// Stop the follower and wait until it ended. The units continue to run.
    pub(crate) async fn shut_down(&self) {
        self.stopping.cancel();
        self.tasks.close();
        self.tasks.wait().await;
    }

    /// Count the failures of `unit` from now on, from an empty run.
    fn watch(&self, connection_id: Uuid, unit: &UnitName) {
        if let Ok(mut watched) = self.watched.lock() {
            watched.insert(
                unit.as_str().to_owned(),
                Watched {
                    connection_id,
                    failures: AuthFailures::default(),
                },
            );
        }
    }

    /// Start the follower task, unless it runs. The first follower starts before this returns.
    fn follow(&self) {
        if self.following.swap(true, Ordering::SeqCst) {
            return;
        }
        let journal: Follower = Box::pin(self.systemd.follow_journal());
        let supervisor = self.clone();
        self.tasks
            .spawn(async move { supervisor.keep_following(journal).await });
    }

    /// Read each follower until it ends, then start a new one after the backoff, until the service
    /// stops.
    async fn keep_following(&self, mut journal: Follower) {
        let mut backoff = FIRST_BACKOFF;
        loop {
            let began = tokio::time::Instant::now();
            if !self.read(journal).await {
                return;
            }
            if began.elapsed() >= LAST_BACKOFF {
                backoff = FIRST_BACKOFF;
            }
            tracing::error!(
                code = JOURNAL_UNREADABLE,
                namespace = SYNC_LOG_NAMESPACE,
                backoff_seconds = backoff.as_secs(),
                "the journal follower of the sync units ended; a new one starts after the backoff"
            );
            tokio::select! {
                biased;
                () = self.stopping.cancelled() => return,
                () = tokio::time::sleep(backoff) => {}
            }
            backoff = backoff.saturating_mul(2).min(LAST_BACKOFF);
            journal = Box::pin(self.systemd.follow_journal());
        }
    }

    /// Count the authentication failures of each watched unit until the follower ends, and expire
    /// a login whose failures lasted [`EXPIRY`]. Returns `false` when the service stops.
    async fn read(&self, mut journal: Follower) -> bool {
        loop {
            let line = tokio::select! {
                biased;
                () = self.stopping.cancelled() => return false,
                line = journal.next() => line,
            };
            let Some(line) = line else {
                return true;
            };
            if !is_auth_failure(&line.message) {
                continue;
            }
            if let Some(connection_id) = self.count_failure(&line) {
                self.expire_login(connection_id).await;
            }
        }
    }

    /// Count one failure line of a watched unit. Returns the connection when its login expired;
    /// the unit is then no longer watched.
    fn count_failure(&self, line: &JournalLine) -> Option<Uuid> {
        let mut watched = self.watched.lock().ok()?;
        let unit = watched.get_mut(&line.unit)?;
        if !unit.failures.observe(line.monotonic) {
            return None;
        }
        watched.remove(&line.unit).map(|unit| unit.connection_id)
    }

    /// Mark the connection `login_expired`, then stop its unit. A connection that the owner
    /// removed meanwhile is left to the disconnect.
    async fn expire_login(&self, connection_id: Uuid) {
        let operation = self
            .database
            .connect_operation(connection_id)
            .await
            .ok()
            .flatten();
        match self.database.expire_login(connection_id).await {
            Ok(true) => {}
            Ok(false) => return,
            Err(error) => {
                tracing::error!(%error, %connection_id, "an expired login could not be recorded");
                return;
            }
        }
        tracing::warn!(%connection_id, code = LOGIN_EXPIRED, "the Sync login of a connection expired; the owner must connect again");
        if let Some(hook) = &self.after_terminal_state {
            (hook.0)(connection_id).await;
        }
        self.forget(connection_id);
        // A connect whose first scan did not end fails with the code of the expired login.
        if let Some(operation) = operation.filter(|operation| operation.state == "connecting")
            && let Some(operation_id) = operation.operation_id
        {
            operation::report(
                &self.database,
                operation.owner_id,
                operation_id,
                Report::Failed(
                    LOGIN_EXPIRED,
                    "The Obsidian login expired. Connect the vault again.",
                    false,
                ),
            )
            .await;
        }
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

/// The completion of the connect operation of a connection that is still `connecting`.
fn completion_of(operation: &ratatoskr_notes_persistence::ConnectOperation) -> Option<Completion> {
    if operation.state != "connecting" {
        return None;
    }
    Some(Completion {
        connection_id: operation.connection_id,
        owner_id: operation.owner_id,
        operation_id: operation.operation_id?,
        channel: operation.channel.clone(),
        vault_display_name: operation.vault_display_name.clone(),
        journal_readable: true,
    })
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
