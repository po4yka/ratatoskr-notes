//! A command that stopped the write path by a defect (OBS-024 tasks 8.10 to 8.25): the write
//! consumer runs each command in a task of its own and ends one that panicked by what it
//! committed.

use ratatoskr_event_envelope::CommandEnvelope;

use super::request::Request;
use super::{INTERNAL, Writes};

/// What [`Writes::fail_defect`] did with a command that stopped the write path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Defect {
    /// The command committed its write before the defect: the write goes on.
    Committed,
    /// Its operation failed with `notes.write.internal`.
    Failed,
    /// The failure could not be recorded: the command must be delivered again.
    Unreported,
}

impl Writes {
    /// Fail the operation of `command`, which stopped the write path by a defect, with
    /// `notes.write.internal`. Nothing of the command was committed. A failure is logged.
    pub(crate) async fn fail_defect(&self, command: &CommandEnvelope) -> Defect {
        let Some((owner_id, operation_id)) = Request::operation_of(command) else {
            return Defect::Failed;
        };
        // A defect after the commit: the write goes on, and its operation ends with it.
        match self
            .recorder
            .database()
            .write_of_operation(operation_id)
            .await
        {
            Ok(Some(_)) => return Defect::Committed,
            Ok(None) => {}
            Err(error) => {
                tracing::error!(%error, %operation_id, "the write of a failed command could not be read");
                return Defect::Unreported;
            }
        }
        match self.refused(owner_id, operation_id, INTERNAL).await {
            Ok(()) => Defect::Failed,
            Err(error) => {
                tracing::error!(%error, %operation_id, "the failure of a write could not be reported");
                Defect::Unreported
            }
        }
    }
}
