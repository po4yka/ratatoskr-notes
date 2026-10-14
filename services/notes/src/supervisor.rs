//! The Sync supervisor: the continuous sync unit of each `sync` connection (workspace design D3).
//!
//! Each `sync` connection whose first login finished has one unit,
//! `ratatoskr-notes-sync@<id>.service`. The supervisor starts it after the first login, and again at
//! each start of the service, because the unit is not enabled and a restart of the host does not
//! start it. Each start of the unit runs `ob sync-config --mode pull-only` first, so a connection is
//! in pull-only mode at every start. A disconnect stops the unit (see `api::clean_up_sync_client`).

use std::sync::Arc;

use ratatoskr_notes_persistence::Database;
use uuid::Uuid;

use crate::systemd::{JobResult, SystemdBus, UnitName};

/// The supervisor of the sync units. Clones share one bus.
#[derive(Debug)]
pub(crate) struct Supervisor<B> {
    database: Database,
    systemd: Arc<B>,
}

impl<B> Clone for Supervisor<B> {
    fn clone(&self) -> Self {
        Self {
            database: self.database.clone(),
            systemd: Arc::clone(&self.systemd),
        }
    }
}

impl<B: SystemdBus> Supervisor<B> {
    /// A supervisor that reaches systemd through `systemd`.
    pub(crate) fn new(database: Database, systemd: Arc<B>) -> Self {
        Self { database, systemd }
    }

    /// Start the sync unit of a connection and wait for the start job. A unit that runs already
    /// continues to run. Returns whether the job finished; a failure is logged.
    pub(crate) async fn start(&self, connection_id: Uuid) -> bool {
        let unit = UnitName::sync(connection_id);
        match self.systemd.start_unit(&unit).await {
            Ok(JobResult::Done) => {
                tracing::info!(%unit, "the sync unit runs");
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
}
