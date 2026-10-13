//! The systemd template units of a `sync` connection, reached through one trait.
//!
//! Notes starts and stops `ratatoskr-notes-sync@<id>.service`,
//! `ratatoskr-notes-login@<id>.service` and `ratatoskr-notes-cleanup@<id>.service` through the
//! systemd D-Bus API (workspace design D3). Every call goes through [`SystemdBus`], so a test uses
//! a fake bus and production uses [`DbusSystemd`].

use std::fmt;
use std::future::Future;

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

/// The two systemd calls that Notes makes. Each one waits until the job is finished.
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
}

/// The well-known name, object and interface of the systemd manager.
const SYSTEMD: &str = "org.freedesktop.systemd1";
const MANAGER_PATH: &str = "/org/freedesktop/systemd1";
const MANAGER_INTERFACE: &str = "org.freedesktop.systemd1.Manager";

/// The error name systemd answers for a unit it does not know.
const NO_SUCH_UNIT: &str = "org.freedesktop.systemd1.NoSuchUnit";

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
}
