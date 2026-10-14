//! The end of the connect operation of a `plugin` or `cli` connection (workspace design D5 and
//! D10).
//!
//! The device sends the manifest of its vault at once after its connect. The first applied
//! manifest makes the connection `active` and completes the operation in its own transaction (see
//! `manifest.rs`), as the first full scan does for a `sync` connection. The end of the bound fails
//! only a connection that is still `connecting`; it waits for the lock of the row, so it cannot
//! fail a connection whose manifest committed. When no manifest is applied within the bound, the connection becomes
//! `failed` and the operation fails with `notes.channel.manifest_missing`, so that every connect
//! reaches a terminal report. The bound is 30 minutes ([`super::DEFAULT_MANIFEST_DEADLINE`]): the
//! plugin lists and hashes the vault before its first manifest, which takes minutes for a large
//! vault on a phone, and 30 minutes is half of the shortest stale window of Platform's
//! operation reaper (`RATATOSKR__OPERATIONS__STALE_AFTER_SECONDS`, at least 1 hour, 24 hours by
//! default), so the stable code of notes arrives before `platform.operation.stale`.
//!
//! The wait is a task that the service tracks and ends when the service stops. At the next start,
//! [`ApiState::resume_device_connects`] waits again for the rest of the bound of each connection.

use std::time::Duration;

use uuid::Uuid;

use super::ApiState;
use crate::operation::{self, Report};
use crate::systemd::SystemdBus;

/// The failure of a device connect whose first manifest did not arrive in time.
const MANIFEST_MISSING: (&str, &str) = (
    "notes.channel.manifest_missing",
    "The device sent no manifest of the vault in time. Connect the vault again.",
);

impl<B: SystemdBus> ApiState<B> {
    /// Wait `wait` for the first manifest of `connection_id`, then fail the connection and its
    /// operation when it is still `connecting`.
    pub(super) fn wait_for_manifest(&self, connection_id: Uuid, wait: Duration) {
        let state = self.clone();
        self.connects.spawn(async move {
            tokio::select! {
                biased;
                () = state.stopping.cancelled() => {}
                () = tokio::time::sleep(wait) => state.expire_device_connect(connection_id).await,
            }
        });
    }

    /// Wait again for the first manifest of each `plugin` or `cli` connection that is
    /// `connecting`, for the rest of its bound. The service calls it at start, before the
    /// listeners accept a request.
    pub async fn resume_device_connects(&self) {
        match self.database.waiting_device_connects().await {
            Ok(waiting) => {
                for (connection_id, age) in waiting {
                    self.wait_for_manifest(
                        connection_id,
                        self.manifest_deadline.saturating_sub(age),
                    );
                }
            }
            Err(error) => {
                tracing::error!(%error, "the device connects that wait for a manifest could not be read");
            }
        }
    }

    /// Fail a device connect that is still `connecting` after its bound, and its operation. The
    /// wait of the bound calls it; a test calls it to run the end of the bound at one step.
    pub async fn expire_device_connect(&self, connection_id: Uuid) {
        let operation = match self.database.connect_operation(connection_id).await {
            Ok(Some(operation)) => operation,
            Ok(None) => return,
            Err(error) => {
                tracing::error!(%error, %connection_id, "a device connect could not be read");
                return;
            }
        };
        match self.database.fail_connection(connection_id).await {
            Ok(true) => {
                tracing::info!(%connection_id, code = MANIFEST_MISSING.0, "a device connect failed: no manifest in time");
            }
            Ok(false) => return,
            Err(error) => {
                tracing::error!(%error, %connection_id, "a device connect could not be failed");
                return;
            }
        }
        if let Some(operation_id) = operation.operation_id {
            let (code, message) = MANIFEST_MISSING;
            operation::report(
                &self.database,
                operation.owner_id,
                operation_id,
                Report::Failed(code, message, true),
            )
            .await;
        }
    }
}
