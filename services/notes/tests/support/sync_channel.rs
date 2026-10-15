//! A fake `sync` channel for the write path: an active `sync` connection with write mode on, whose
//! working tree the test stands for. The test reports what the Headless client leaves in the tree
//! as the watcher would, through [`FileRecorder::record`].

#![allow(
    clippy::expect_used,
    dead_code,
    reason = "test scaffolding; not every test binary uses every helper"
)]

use std::path::PathBuf;

use ratatoskr_notes::files::{FileRecorder, Recorded};
use ratatoskr_notes::watcher::TreeChange;
use uuid::Uuid;

use crate::support::channel::OWNER;
use crate::support::writes::Writer;

impl Writer {
    /// The recorder of the files, over the database and the blob store of this writer.
    pub(crate) fn recorder(&self) -> FileRecorder {
        FileRecorder::new(
            self.channel.fixture.database.clone(),
            self.channel.blobs.clone(),
        )
    }

    /// An active `sync` connection of [`OWNER`] whose first login finished, with write mode on,
    /// whose working tree holds `files`.
    pub(crate) async fn sync_vault(&self, files: &[(&str, &[u8])]) -> Uuid {
        let connection_id = Uuid::now_v7();
        sqlx::query(
            "insert into notes.connections
                 (connection_id, owner_id, channel, vault_key, vault_display_name, state,
                  logged_in_at, write_mode, created_at, updated_at)
             values ($1, $2, 'sync', $3, 'Personal', 'active', now(), 'on', now(), now())",
        )
        .bind(connection_id)
        .bind(OWNER)
        .bind(format!("remote-vault-{}", connection_id.simple()))
        .execute(self.channel.fixture.pool())
        .await
        .expect("a sync connection is inserted");
        for (path, bytes) in files {
            self.tree_change(connection_id, path, bytes).await;
        }
        connection_id
    }

    /// Put `bytes` in the blob store and let `connection_id` name them, as an upload of the
    /// owner's session does.
    pub(crate) async fn keep_for_sync(&self, connection_id: Uuid, bytes: &[u8]) {
        let digest = self.store(bytes);
        self.channel
            .fixture
            .database
            .record_connection_blob(connection_id, &digest, bytes.len() as u64)
            .await
            .expect("the blob is registered");
    }

    /// The watcher reports that `path` of the working tree of `connection_id` has `bytes` now.
    pub(crate) async fn tree_change(
        &self,
        connection_id: Uuid,
        path: &str,
        bytes: &[u8],
    ) -> Recorded {
        let digest = self.store(bytes);
        self.recorder()
            .record(
                connection_id,
                &TreeChange::Revision {
                    path: PathBuf::from(path),
                    digest,
                    length: bytes.len() as u64,
                },
            )
            .await
            .expect("the tree change is recorded")
    }

    /// Store `bytes` in the blob store, and return their SHA-256.
    pub(crate) fn store(&self, bytes: &[u8]) -> String {
        let scratch = tempfile::NamedTempFile::new().expect("a scratch file");
        std::fs::write(scratch.path(), bytes).expect("the scratch file is written");
        self.channel
            .blobs
            .store_file(scratch.path())
            .expect("the blob is stored")
            .digest
    }
}
