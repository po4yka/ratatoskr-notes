//! The disconnects that run now, so that a connect whose login a disconnect stops can wait for it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::watch;
use uuid::Uuid;

/// The disconnects that run now in this process, by connection. The channel of each one closes
/// when the disconnect ends.
#[derive(Debug, Clone, Default)]
pub(super) struct Removals(Arc<Mutex<HashMap<Uuid, watch::Receiver<()>>>>);

/// One disconnect that runs. Its drop removes it from [`Removals`] and closes its channel.
pub(super) struct Removal {
    removals: Removals,
    connection_id: Uuid,
    ended: watch::Sender<()>,
}

impl Removals {
    /// Record a disconnect of `connection_id` until the returned value is dropped.
    pub(super) fn begin(&self, connection_id: Uuid) -> Removal {
        let (ended, receiver) = watch::channel(());
        if let Ok(mut running) = self.0.lock() {
            running.insert(connection_id, receiver);
        }
        Removal {
            removals: self.clone(),
            connection_id,
            ended,
        }
    }

    /// Wait until no disconnect of `connection_id` that runs now runs any more. Returns whether
    /// one ran.
    pub(super) async fn wait(&self, connection_id: Uuid) -> bool {
        let running = self
            .0
            .lock()
            .ok()
            .and_then(|running| running.get(&connection_id).cloned());
        let Some(mut ended) = running else {
            return false;
        };
        while ended.changed().await.is_ok() {}
        true
    }
}

impl Drop for Removal {
    fn drop(&mut self) {
        if let Ok(mut running) = self.removals.0.lock() {
            // A later disconnect of the same connection holds its own entry.
            if running
                .get(&self.connection_id)
                .is_some_and(|receiver| receiver.same_channel(&self.ended.subscribe()))
            {
                running.remove(&self.connection_id);
            }
        }
    }
}
