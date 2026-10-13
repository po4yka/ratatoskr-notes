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

/// The production bus: `org.freedesktop.systemd1` on the system bus.
#[derive(Debug, Default)]
pub struct DbusSystemd;

impl SystemdBus for DbusSystemd {
    async fn start_unit(&self, _unit: &UnitName) -> Result<JobResult, BusError> {
        Err(BusError::Unreachable(
            "the D-Bus client is not implemented yet".into(),
        ))
    }

    async fn stop_unit(&self, _unit: &UnitName) -> Result<JobResult, BusError> {
        Err(BusError::Unreachable(
            "the D-Bus client is not implemented yet".into(),
        ))
    }
}
