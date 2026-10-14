//! The watcher of the working tree of a `sync` connection (workspace design D3).
//!
//! Notes takes file changes only from the working tree, never from the output of `ob`. The watcher
//! reads inotify events of the tree on Linux, and reads the whole tree at its start and every
//! [`RESCAN_INTERVAL`], which finds what an event did not report: a lost event, a full event queue,
//! or a directory that the watcher did not watch yet. A file is settled when its size and
//! modification time have not changed for [`SETTLE_WINDOW`]. Each settled state of a file becomes
//! one revision: the watcher copies the bytes into the blob root and sends
//! [`TreeChange::Revision`]. A state that already became a revision does not become one again.
//!
//! The watcher does not follow a symbolic link and reads regular files only. Other platforms than
//! Linux have no inotify, and there the watcher only rescans. Paths are not logged: a file name is
//! the owner's content.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::blobs::BlobStore;

/// How long the size and the modification time of a file must stay the same before the file is
/// settled.
pub const SETTLE_WINDOW: Duration = Duration::from_secs(2);

/// How often the watcher reads the whole tree.
pub const RESCAN_INTERVAL: Duration = Duration::from_mins(15);

/// One change of the working tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TreeChange {
    /// A file has a new settled state. Its bytes are in the blob root under `digest`.
    Revision {
        /// The path of the file, relative to the root of the tree.
        path: PathBuf,
        /// The lowercase hex SHA-256 of the bytes.
        digest: String,
        /// The number of bytes.
        length: u64,
    },
    /// A file that became a revision is no longer in the tree.
    Removed {
        /// The path of the file, relative to the root of the tree.
        path: PathBuf,
    },
}

/// Watch `tree` until `stop`, and send each change to `changes`. The watcher also stops when the
/// receiver of `changes` is dropped.
#[must_use]
pub fn watch(
    tree: PathBuf,
    blobs: BlobStore,
    changes: mpsc::Sender<TreeChange>,
    stop: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut events = events::Events::open();
        let mut watcher = Watcher {
            tree,
            blobs,
            changes,
            settled: HashMap::new(),
            pending: HashMap::new(),
        };
        // The first tick is at once: the read of the tree at the start.
        let mut rescan = tokio::time::interval(RESCAN_INTERVAL);
        rescan.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            let next_due = watcher.next_due();
            let open = tokio::select! {
                biased;
                () = stop.cancelled() => return,
                _ = rescan.tick() => watcher.scan(&mut events).await,
                notice = events.next() => match notice {
                    events::Notice::Path(path) => watcher.observe(path).await,
                    events::Notice::Rescan => watcher.scan(&mut events).await,
                },
                () = sleep_until(next_due) => watcher.settle().await,
            };
            if !open {
                return;
            }
        }
    })
}

/// Sleep until `due`, or for ever when nothing is due.
async fn sleep_until(due: Option<Instant>) {
    match due {
        Some(due) => tokio::time::sleep_until(due).await,
        None => std::future::pending().await,
    }
}

/// The size and the modification time of a regular file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileState {
    length: u64,
    modified: Option<SystemTime>,
}

/// A file whose state changed and is not settled yet.
#[derive(Debug, Clone, Copy)]
struct Pending {
    /// The state when it last changed.
    seen: FileState,
    /// When the state is settled if it does not change again.
    due: Instant,
}

/// The state of one watcher. Every method that sends returns `false` when the receiver is gone.
struct Watcher {
    tree: PathBuf,
    blobs: BlobStore,
    changes: mpsc::Sender<TreeChange>,
    /// The state of each file that became a revision.
    settled: HashMap<PathBuf, FileState>,
    pending: HashMap<PathBuf, Pending>,
}

impl Watcher {
    /// The state of a regular file of the tree, or `None` when it is not one.
    fn stat(&self, path: &Path) -> Option<FileState> {
        let metadata = std::fs::symlink_metadata(self.tree.join(path)).ok()?;
        metadata.is_file().then(|| FileState {
            length: metadata.len(),
            modified: metadata.modified().ok(),
        })
    }

    /// The earliest time at which a pending file is settled.
    fn next_due(&self) -> Option<Instant> {
        self.pending.values().map(|pending| pending.due).min()
    }

    /// A file of the tree may have changed.
    async fn observe(&mut self, path: PathBuf) -> bool {
        match self.stat(&path) {
            Some(state) if self.pending.get(&path).is_some_and(|p| p.seen == state) => true,
            Some(state) if self.settled.get(&path) == Some(&state) => {
                self.pending.remove(&path);
                true
            }
            Some(state) => {
                let due = Instant::now() + SETTLE_WINDOW;
                self.pending.insert(path, Pending { seen: state, due });
                true
            }
            None => self.removed(path).await,
        }
    }

    /// A file is not in the tree. It was removed only when it became a revision before.
    async fn removed(&mut self, path: PathBuf) -> bool {
        self.pending.remove(&path);
        if self.settled.remove(&path).is_none() {
            return true;
        }
        self.changes
            .send(TreeChange::Removed { path })
            .await
            .is_ok()
    }

    /// Read the whole tree: watch each directory, observe each file, and report each file that
    /// became a revision and is gone.
    async fn scan(&mut self, events: &mut events::Events) -> bool {
        let mut files = HashSet::new();
        let mut directories = vec![PathBuf::new()];
        while let Some(directory) = directories.pop() {
            events.watch_directory(&self.tree, &directory);
            let entries = match std::fs::read_dir(self.tree.join(&directory)) {
                Ok(entries) => entries,
                Err(error) => {
                    tracing::warn!(%error, "a directory of a working tree could not be read");
                    continue;
                }
            };
            for entry in entries.flatten() {
                let path = directory.join(entry.file_name());
                match entry.file_type() {
                    Ok(kind) if kind.is_dir() => directories.push(path),
                    Ok(kind) if kind.is_file() => {
                        files.insert(path);
                    }
                    _ => {}
                }
            }
        }
        let gone: Vec<PathBuf> = self
            .settled
            .keys()
            .filter(|path| !files.contains(*path))
            .cloned()
            .collect();
        for path in files.into_iter().chain(gone) {
            if !self.observe(path).await {
                return false;
            }
        }
        true
    }

    /// Turn each pending file whose window ended into a revision when its state did not change.
    async fn settle(&mut self) -> bool {
        let now = Instant::now();
        let due: Vec<(PathBuf, Pending)> = self
            .pending
            .iter()
            .filter(|(_, pending)| pending.due <= now)
            .map(|(path, pending)| (path.clone(), *pending))
            .collect();
        for (path, pending) in due {
            self.pending.remove(&path);
            let open = match self.stat(&path) {
                Some(state) if state == pending.seen => self.record(path, state).await,
                Some(_) => self.observe(path).await,
                None => self.removed(path).await,
            };
            if !open {
                return false;
            }
        }
        true
    }

    /// Copy a settled file into the blob root and send its revision. When the file changed during
    /// the copy, it is pending again.
    async fn record(&mut self, path: PathBuf, state: FileState) -> bool {
        if self.settled.get(&path) == Some(&state) {
            return true;
        }
        let blobs = self.blobs.clone();
        let source = self.tree.join(&path);
        let stored = tokio::task::spawn_blocking(move || blobs.store_file(&source)).await;
        let blob = match stored {
            Ok(Ok(blob)) => blob,
            Ok(Err(error)) => {
                // The next event or the next rescan tries again.
                tracing::warn!(%error, "a settled file could not be stored in the blob root");
                return true;
            }
            Err(error) => {
                tracing::error!(%error, "the copy of a settled file stopped");
                return true;
            }
        };
        if self.stat(&path) != Some(state) {
            return self.observe(path).await;
        }
        self.settled.insert(path.clone(), state);
        self.changes
            .send(TreeChange::Revision {
                path,
                digest: blob.digest,
                length: blob.length,
            })
            .await
            .is_ok()
    }
}

/// The inotify events of a tree, on Linux.
#[cfg(target_os = "linux")]
mod events {
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};

    use futures_util::StreamExt as _;
    use inotify::{EventMask, EventStream, Inotify, WatchDescriptor, WatchMask};

    /// What an event tells the watcher.
    pub(super) enum Notice {
        /// A file may have changed.
        Path(PathBuf),
        /// The watcher must read the whole tree: a directory changed, or events were lost.
        Rescan,
    }

    /// The events that change a file or a directory of the tree.
    const MASK: WatchMask = WatchMask::CREATE
        .union(WatchMask::MODIFY)
        .union(WatchMask::CLOSE_WRITE)
        .union(WatchMask::ATTRIB)
        .union(WatchMask::DELETE)
        .union(WatchMask::MOVED_FROM)
        .union(WatchMask::MOVED_TO)
        .union(WatchMask::DONT_FOLLOW)
        .union(WatchMask::ONLYDIR);

    /// The inotify instance of one tree and the directory of each watch.
    pub(super) struct Events {
        stream: Option<EventStream<Vec<u8>>>,
        directories: HashMap<WatchDescriptor, PathBuf>,
    }

    impl Events {
        /// A new inotify instance. When it cannot be made, the watcher only rescans.
        pub(super) fn open() -> Self {
            let stream = Inotify::init()
                .and_then(|inotify| inotify.into_event_stream(vec![0; 16 * 1024]))
                .inspect_err(|error| {
                    tracing::error!(%error, "inotify is not available; the working tree is only rescanned");
                })
                .ok();
            Self {
                stream,
                directories: HashMap::new(),
            }
        }

        /// Watch one directory of the tree. Watching a directory again changes nothing.
        pub(super) fn watch_directory(&mut self, tree: &Path, directory: &Path) {
            let Some(stream) = &self.stream else {
                return;
            };
            match stream.watches().add(tree.join(directory), MASK) {
                Ok(descriptor) => {
                    self.directories.insert(descriptor, directory.to_owned());
                }
                Err(error) => {
                    tracing::warn!(%error, "a directory of a working tree could not be watched");
                }
            }
        }

        /// The next notice. Cancel safe: an event is taken only when it is returned.
        pub(super) async fn next(&mut self) -> Notice {
            loop {
                let Some(stream) = &mut self.stream else {
                    return std::future::pending().await;
                };
                let event = match stream.next().await {
                    Some(Ok(event)) => event,
                    Some(Err(error)) => {
                        tracing::error!(%error, "inotify failed; the working tree is only rescanned");
                        self.stream = None;
                        continue;
                    }
                    None => {
                        self.stream = None;
                        continue;
                    }
                };
                if event.mask.contains(EventMask::Q_OVERFLOW) {
                    return Notice::Rescan;
                }
                if event.mask.contains(EventMask::IGNORED) {
                    self.directories.remove(&event.wd);
                    continue;
                }
                if event.mask.contains(EventMask::ISDIR) {
                    return Notice::Rescan;
                }
                let (Some(directory), Some(name)) = (self.directories.get(&event.wd), event.name)
                else {
                    continue;
                };
                return Notice::Path(directory.join(name));
            }
        }
    }
}

/// No inotify: the watcher only rescans.
#[cfg(not(target_os = "linux"))]
mod events {
    use std::path::{Path, PathBuf};

    /// What an event tells the watcher. No event comes on this platform.
    #[expect(dead_code, reason = "the same type as the Linux implementation")]
    pub(super) enum Notice {
        /// A file may have changed.
        Path(PathBuf),
        /// The watcher must read the whole tree.
        Rescan,
    }

    /// No events.
    pub(super) struct Events;

    impl Events {
        /// No events.
        pub(super) fn open() -> Self {
            Self
        }

        /// Nothing to watch.
        #[expect(
            clippy::unused_self,
            reason = "the same signature as the Linux implementation"
        )]
        pub(super) fn watch_directory(&mut self, _tree: &Path, _directory: &Path) {}

        /// Never ready.
        pub(super) async fn next(&mut self) -> Notice {
            std::future::pending().await
        }
    }
}
