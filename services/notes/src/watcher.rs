//! The watcher of the working tree of a `sync` connection (workspace design D3).
//!
//! Notes takes file changes only from the working tree, never from the output of `ob`. The watcher
//! reads inotify events of the tree on Linux, and reads the whole tree at its start, after a full
//! event queue, and every [`RESCAN_INTERVAL`], which finds what an event did not report. A new
//! directory makes the watcher read that directory only, and a removed directory makes it check the
//! files that it knew under it. Each read of the tree runs on the blocking pool. A file is settled
//! when its size and
//! modification time have not changed for [`SETTLE_WINDOW`]. Each settled state of a file becomes
//! one revision: the watcher copies the bytes into the blob root and sends
//! [`TreeChange::Revision`]. A state that already became a revision does not become one again.
//!
//! The watcher reads regular files only, and never through a symbolic link or outside the tree: on
//! Linux it opens each file and each directory with `openat2` and
//! `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS` from the root of the tree, and elsewhere it opens each
//! component with `O_NOFOLLOW`. The state of a file is the `fstat` of the open file, and the bytes
//! of a revision come from the same open file. So a link in the tree, also one that replaces a
//! watched directory, never makes a revision of a file outside it. Other platforms than Linux have
//! no inotify, and there the watcher only rescans. Paths are not logged: a file name is the owner's
//! content.

use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::fs::File;
use std::os::unix::ffi::OsStrExt as _;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use rustix::fs::{AtFlags, Dir, FileType, OFlags};

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::blobs::{BlobStore, StoredBlob};

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
                _ = rescan.tick() => watcher.scan(&mut events, PathBuf::new()).await,
                notice = events.next() => match notice {
                    events::Notice::Path(path) => watcher.observe(path).await,
                    events::Notice::Directory(path) => watcher.scan(&mut events, path).await,
                    events::Notice::DirectoryGone(path) => watcher.directory_gone(&path).await,
                    events::Notice::Rescan => watcher.scan(&mut events, PathBuf::new()).await,
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

impl FileState {
    /// The state of an open file, or `None` when it is not a regular file.
    fn of(file: &File) -> Option<Self> {
        let metadata = file.metadata().ok()?;
        metadata.is_file().then(|| Self {
            length: metadata.len(),
            modified: metadata.modified().ok(),
        })
    }
}

/// Open the regular file at `path` of the tree, never through a symbolic link and never outside
/// the tree, and take its state from the open file.
fn open_file(tree: &Path, path: &Path) -> Option<(File, FileState)> {
    let file = File::from(beneath::open(tree, path, OFlags::RDONLY | OFlags::NONBLOCK).ok()?);
    let state = FileState::of(&file)?;
    Some((file, state))
}

/// Copy the file at `path` of the tree into the blob root when it is still in the settled state
/// `state`, before and after the copy, which reads the same open file. `None` when the file
/// changed or left the tree. It blocks.
fn copy_settled(
    tree: &Path,
    blobs: &BlobStore,
    path: &Path,
    state: FileState,
) -> std::io::Result<Option<StoredBlob>> {
    let Some((file, before)) = open_file(tree, path) else {
        return Ok(None);
    };
    if before != state {
        return Ok(None);
    }
    let blob = blobs.store_open_file(&file)?;
    Ok((FileState::of(&file) == Some(state)).then_some(blob))
}

/// What a read of a part of the tree found.
#[derive(Debug, Default)]
struct Found {
    /// Each directory that was read.
    directories: Vec<PathBuf>,
    /// Each regular file and its state.
    files: Vec<(PathBuf, FileState)>,
}

/// Read the directory `top` of the tree and every directory under it. It blocks.
fn walk(tree: &Path, top: PathBuf) -> Found {
    let mut found = Found::default();
    let mut open = vec![top];
    while let Some(directory) = open.pop() {
        let opened = beneath::open(tree, &directory, OFlags::RDONLY | OFlags::DIRECTORY)
            .and_then(|descriptor| Ok(Dir::new(descriptor)?));
        let mut entries = match opened {
            Ok(entries) => entries,
            Err(error) => {
                tracing::warn!(%error, "a directory of a working tree could not be read");
                continue;
            }
        };
        while let Some(Ok(entry)) = entries.read() {
            let name = OsStr::from_bytes(entry.file_name().to_bytes());
            if name == "." || name == ".." {
                continue;
            }
            let path = directory.join(name);
            match kind_of(&entries, name, entry.file_type()) {
                FileType::Directory => open.push(path),
                FileType::RegularFile => {
                    if let Some((_, state)) = open_file(tree, &path) {
                        found.files.push((path, state));
                    }
                }
                _ => {}
            }
        }
        found.directories.push(directory);
    }
    found
}

/// The type of an entry of `directory`. A file system that does not give the type in the entry
/// gives it to `fstatat`, which does not follow a symbolic link.
fn kind_of(directory: &Dir, name: &OsStr, kind: FileType) -> FileType {
    if kind != FileType::Unknown {
        return kind;
    }
    directory
        .fd()
        .and_then(|descriptor| rustix::fs::statat(descriptor, name, AtFlags::SYMLINK_NOFOLLOW))
        .map_or(FileType::Unknown, |stat| {
            FileType::from_raw_mode(stat.st_mode)
        })
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
        open_file(&self.tree, path).map(|(_, state)| state)
    }

    /// The earliest time at which a pending file is settled.
    fn next_due(&self) -> Option<Instant> {
        self.pending.values().map(|pending| pending.due).min()
    }

    /// A file of the tree may have changed.
    async fn observe(&mut self, path: PathBuf) -> bool {
        let state = self.stat(&path);
        self.observe_state(path, state).await
    }

    /// A file of the tree has the state `state` now, or is not a regular file of the tree.
    async fn observe_state(&mut self, path: PathBuf, state: Option<FileState>) -> bool {
        match state {
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

    /// Read the directory `top` of the tree and every directory under it on the blocking pool:
    /// watch each directory, observe each file, and check each known file under `top` that the
    /// read did not find. The empty path is the whole tree.
    async fn scan(&mut self, events: &mut events::Events, top: PathBuf) -> bool {
        let tree = self.tree.clone();
        let walked = tokio::task::spawn_blocking({
            let top = top.clone();
            move || walk(&tree, top)
        })
        .await;
        let found = match walked {
            Ok(found) => found,
            Err(error) => {
                tracing::error!(%error, "a read of a working tree stopped");
                return true;
            }
        };
        for directory in &found.directories {
            events.watch_directory(&self.tree, directory);
        }
        let seen: HashSet<&PathBuf> = found.files.iter().map(|(path, _)| path).collect();
        let missing: Vec<PathBuf> = self
            .known_under(&top)
            .into_iter()
            .filter(|path| !seen.contains(path))
            .collect();
        for (path, state) in found.files {
            if !self.observe_state(path, Some(state)).await {
                return false;
            }
        }
        for path in missing {
            if !self.observe(path).await {
                return false;
            }
        }
        true
    }

    /// A directory left the tree: check each file under it that the watcher knows.
    async fn directory_gone(&mut self, directory: &Path) -> bool {
        for path in self.known_under(directory) {
            if !self.observe(path).await {
                return false;
            }
        }
        true
    }

    /// Each file under `directory` that is settled or pending.
    fn known_under(&self, directory: &Path) -> HashSet<PathBuf> {
        self.settled
            .keys()
            .chain(self.pending.keys())
            .filter(|path| path.starts_with(directory))
            .cloned()
            .collect()
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

    /// Copy a settled file into the blob root and send its revision. When the file changed or
    /// left the tree since it settled, or changed during the copy, it is observed again.
    async fn record(&mut self, path: PathBuf, state: FileState) -> bool {
        if self.settled.get(&path) == Some(&state) {
            return true;
        }
        let tree = self.tree.clone();
        let blobs = self.blobs.clone();
        let source = path.clone();
        let copied =
            tokio::task::spawn_blocking(move || copy_settled(&tree, &blobs, &source, state)).await;
        let blob = match copied {
            Ok(Ok(Some(blob))) => blob,
            Ok(Ok(None)) => return self.observe(path).await,
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

/// Opens a path of a tree without a symbolic link in any component and without leaving the tree.
mod beneath {
    use std::os::fd::OwnedFd;
    use std::path::Path;

    use rustix::fs::{Mode, OFlags};

    /// Open `path`, relative to the root of `tree`, with `flags`. The empty path is the root.
    #[cfg(target_os = "linux")]
    pub(super) fn open(tree: &Path, path: &Path, flags: OFlags) -> std::io::Result<OwnedFd> {
        use rustix::fs::ResolveFlags;

        let root = rustix::fs::open(
            tree,
            OFlags::PATH | OFlags::DIRECTORY | OFlags::CLOEXEC,
            Mode::empty(),
        )?;
        let path = if path.as_os_str().is_empty() {
            Path::new(".")
        } else {
            path
        };
        Ok(rustix::fs::openat2(
            &root,
            path,
            flags | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
            ResolveFlags::BENEATH | ResolveFlags::NO_SYMLINKS,
        )?)
    }

    /// Open `path`, relative to the root of `tree`, with `flags`, one component at a time, each
    /// with `O_NOFOLLOW`. The empty path is the root.
    #[cfg(not(target_os = "linux"))]
    pub(super) fn open(tree: &Path, path: &Path, flags: OFlags) -> std::io::Result<OwnedFd> {
        use std::path::Component;

        let directory_flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC;
        let mut directory = rustix::fs::open(tree, directory_flags, Mode::empty())?;
        let mut components = path.components().peekable();
        while let Some(component) = components.next() {
            let Component::Normal(name) = component else {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "a path of a tree has only names",
                ));
            };
            let last = components.peek().is_none();
            let flags = if last { flags } else { directory_flags };
            directory = rustix::fs::openat(
                &directory,
                name,
                flags | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )?;
        }
        Ok(directory)
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
        /// A directory came into the tree: the watcher reads it.
        Directory(PathBuf),
        /// A directory left the tree.
        DirectoryGone(PathBuf),
        /// The watcher must read the whole tree: events were lost.
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

        /// What one event tells the watcher, or `None` when it tells nothing.
        fn notice(
            &self,
            mask: EventMask,
            descriptor: &WatchDescriptor,
            name: Option<&std::ffi::OsStr>,
        ) -> Option<Notice> {
            if mask.contains(EventMask::Q_OVERFLOW) {
                return Some(Notice::Rescan);
            }
            let directory = mask.contains(EventMask::ISDIR);
            let (Some(parent), Some(name)) = (self.directories.get(descriptor), name) else {
                // A directory event of a directory that the watcher does not know.
                return directory.then_some(Notice::Rescan);
            };
            let path = parent.join(name);
            if !directory {
                Some(Notice::Path(path))
            } else if mask.intersects(EventMask::CREATE | EventMask::MOVED_TO) {
                Some(Notice::Directory(path))
            } else if mask.intersects(EventMask::DELETE | EventMask::MOVED_FROM) {
                Some(Notice::DirectoryGone(path))
            } else {
                None
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
                if event.mask.contains(EventMask::IGNORED) {
                    self.directories.remove(&event.wd);
                    continue;
                }
                if let Some(notice) = self.notice(event.mask, &event.wd, event.name.as_deref()) {
                    return notice;
                }
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
        /// A directory came into the tree.
        Directory(PathBuf),
        /// A directory left the tree.
        DirectoryGone(PathBuf),
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

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::open_file;

    #[test]
    fn a_file_opens_only_beneath_the_tree_and_without_a_link() {
        let host = tempfile::tempdir().expect("a host directory");
        let tree = host.path().join("tree");
        let outside = host.path().join("outside");
        std::fs::create_dir_all(tree.join("Daily")).expect("a folder of the vault");
        std::fs::create_dir(&outside).expect("a directory outside the tree");
        std::fs::write(tree.join("Daily/note.md"), "# Note\n").expect("a note");
        std::fs::write(outside.join("secret.md"), "outside\n").expect("a file outside");
        std::os::unix::fs::symlink(&outside, tree.join("Linked")).expect("a linked folder");
        std::os::unix::fs::symlink(outside.join("secret.md"), tree.join("link.md"))
            .expect("a linked file");

        assert!(open_file(&tree, Path::new("Daily/note.md")).is_some());
        assert!(open_file(&tree, Path::new("Linked/secret.md")).is_none());
        assert!(open_file(&tree, Path::new("link.md")).is_none());
        assert!(open_file(&tree, Path::new("../outside/secret.md")).is_none());
        assert!(
            open_file(&tree, Path::new("Daily")).is_none(),
            "a directory"
        );
    }
}
