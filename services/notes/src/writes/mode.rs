//! The write mode of a connection (workspace design D8, spec `notes-write-command`, requirement
//! "The owner turns write mode on or off").
//!
//! Every connection starts with write mode off. Edge mints the operation
//! `notes.connection.write_mode` and forwards the switch; notes is its only reporter (lane
//! `ratatoskr-notes`). Turning write mode off fails every write of the connection that waits in a
//! queue and was not delivered, with `notes.write.disabled`. A delivered write finishes, because
//! the vault may already hold it.

use uuid::Uuid;

use super::queue::Locked;
use super::report::{ReportRow, Step};
use super::{DISABLED, WriteError, Writes};

/// The code of a write-mode change whose sync client could not be configured.
pub const RECONFIGURE_FAILED: &str = "notes.sync.reconfigure_failed";

/// The kind of the operation of a write-mode change.
pub const WRITE_MODE_OPERATION_KIND: &str = "notes.connection.write_mode";

/// What [`Writes::set_write_mode`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModeChange {
    /// The connection has the mode now.
    Changed {
        /// The wire word of the channel of the connection.
        channel: String,
    },
    /// The owner holds no connection with the id that is `connecting` or `active`. Nothing
    /// changed and nothing is reported.
    NotFound,
}

impl Writes {
    /// Set the write mode of `connection_id` of `owner_id` to on or off in one transaction under
    /// the lock of the connection row. Off fails each write that waits in a queue with
    /// `notes.write.disabled`. When `report` is true, the report `succeeded` of the operation
    /// `operation_id` is written in the same transaction.
    ///
    /// # Errors
    ///
    /// [`WriteError`] when the database fails; nothing is changed then.
    pub async fn set_write_mode(
        &self,
        owner_id: Uuid,
        connection_id: Uuid,
        operation_id: Uuid,
        on: bool,
        report: bool,
    ) -> Result<ModeChange, WriteError> {
        let database = self.recorder.database();
        let Some((mut transaction, connection)) = database.begin_file_change(connection_id).await?
        else {
            return Ok(ModeChange::NotFound);
        };
        if connection.owner_id != owner_id {
            return Ok(ModeChange::NotFound);
        }
        transaction.set_write_mode(on).await?;
        let mut locked = Locked {
            recorder: &self.recorder,
            transaction: &mut transaction,
            connection: &connection,
            connection_id,
            raised: Vec::new(),
        };
        if !on {
            for write in locked.transaction.queued_writes().await? {
                locked.fail(&write, DISABLED).await?;
            }
        }
        if report {
            write_report(
                &mut transaction,
                owner_id,
                operation_id,
                Step::Succeeded(None),
            )
            .await?;
        }
        transaction.commit().await?;
        Ok(ModeChange::Changed {
            channel: connection.channel,
        })
    }
}

impl Writes {
    /// Write the report `succeeded` of the write-mode operation `operation_id` of `owner_id`, or
    /// `failed` with `code` when one is given.
    ///
    /// # Errors
    ///
    /// [`WriteError`] when the database fails.
    pub async fn report_mode(
        &self,
        owner_id: Uuid,
        operation_id: Uuid,
        failed: Option<&str>,
    ) -> Result<(), WriteError> {
        let step = match failed {
            Some(code) => Step::Failed(code, None),
            None => Step::Succeeded(None),
        };
        match ReportRow::new(owner_id, operation_id, step) {
            Ok(row) => self.recorder.database().insert_event(&row.event()).await?,
            Err(error) => {
                tracing::error!(%error, %operation_id, "a report of a write-mode change could not be built");
            }
        }
        Ok(())
    }
}

/// Write `step` of `operation_id` in `transaction`.
async fn write_report(
    transaction: &mut ratatoskr_notes_persistence::FileTransaction,
    owner_id: Uuid,
    operation_id: Uuid,
    step: Step<'_>,
) -> Result<(), WriteError> {
    match ReportRow::new(owner_id, operation_id, step) {
        Ok(row) => transaction.write_event(&row.event()).await?,
        Err(error) => {
            tracing::error!(%error, %operation_id, "a report of a write-mode change could not be built");
        }
    }
    Ok(())
}
