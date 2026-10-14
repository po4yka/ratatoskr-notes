//! A process of another user that reads `/proc/*/cmdline` again and again while a test runs.
//!
//! Linux only: the reader needs `/proc`. It runs as `nobody`. When the test runs as root, the
//! reader drops to `nobody` itself; otherwise it starts through `sudo -n -u nobody`, which the
//! GitHub runner allows without a password. If neither works, the test fails: it does not skip.

#![allow(
    clippy::expect_used,
    dead_code,
    reason = "test scaffolding; not every test binary uses every helper"
)]

use std::collections::HashSet;
use std::os::unix::fs::MetadataExt as _;
use std::path::PathBuf;
use std::process::Stdio;

use tokio::io::{AsyncBufReadExt as _, BufReader};
use uuid::Uuid;

/// The uid and gid of `nobody` on Debian and Ubuntu.
const NOBODY: u32 = 65_534;

/// The most passes the reader makes, so that a reader whose test died stops by itself.
const MAX_PASSES: u32 = 3_000;

/// Prints its own uid, then every command line of every process, one per line, until the stop
/// file exists. One `grep` reads all the files of one pass.
const READER: &str = r#"id -u
n=0
while [ ! -e "$1" ] && [ "$n" -lt "$2" ]; do
    n=$((n + 1))
    grep -a -H '' /proc/[0-9]*/cmdline 2>/dev/null
done
"#;

/// What the reader saw.
#[derive(Debug)]
pub(crate) struct Seen {
    /// The uid that the reader ran as.
    pub(crate) reader_uid: u32,
    /// Each distinct line, `/proc/<pid>/cmdline:<arguments>`, with the NUL separators as spaces.
    pub(crate) lines: HashSet<String>,
}

/// A running reader.
#[derive(Debug)]
pub(crate) struct CmdlineReader {
    child: tokio::process::Child,
    stop: PathBuf,
    stopped: bool,
    reader_uid: u32,
    lines: tokio::task::JoinHandle<HashSet<String>>,
}

/// The uid of this test process.
pub(crate) fn own_uid() -> u32 {
    std::fs::metadata("/proc/self")
        .expect("/proc/self exists on Linux")
        .uid()
}

impl CmdlineReader {
    /// Starts the reader and waits until it runs.
    pub(crate) async fn start() -> Self {
        // In the shared temporary directory, so that `nobody` can see it.
        let stop = std::env::temp_dir().join(format!("notes-cmdline-stop-{}", Uuid::now_v7()));
        let mut command = if own_uid() == 0 {
            let mut command = tokio::process::Command::new("/bin/sh");
            command.uid(NOBODY).gid(NOBODY);
            command
        } else {
            let mut command = tokio::process::Command::new("sudo");
            command.args(["-n", "-u", "nobody", "--", "/bin/sh"]);
            command
        };
        let mut child = command
            .arg("-c")
            .arg(READER)
            .arg("reader")
            .arg(&stop)
            .arg(MAX_PASSES.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .expect("the reader starts as another user");
        let mut stdout = BufReader::new(child.stdout.take().expect("the reader's stdout"));
        let mut first = Vec::new();
        stdout
            .read_until(b'\n', &mut first)
            .await
            .expect("the reader prints its uid");
        let reader_uid = String::from_utf8_lossy(&first)
            .trim()
            .parse()
            .expect("the reader runs as another user: sudo -n -u nobody must work");
        let lines = tokio::spawn(async move {
            let mut lines = HashSet::new();
            let mut line = Vec::new();
            while stdout.read_until(b'\n', &mut line).await.unwrap_or(0) > 0 {
                lines.insert(String::from_utf8_lossy(&line).replace('\0', " "));
                line.clear();
            }
            lines
        });
        Self {
            child,
            stop,
            stopped: false,
            reader_uid,
            lines,
        }
    }

    /// Stops the reader and returns what it saw.
    pub(crate) async fn stop(mut self) -> Seen {
        std::fs::write(&self.stop, b"").expect("the stop file is written");
        self.child.wait().await.expect("the reader stops");
        self.stopped = true;
        let lines = (&mut self.lines).await.expect("the lines are collected");
        Seen {
            reader_uid: self.reader_uid,
            lines,
        }
    }
}

impl Drop for CmdlineReader {
    fn drop(&mut self) {
        if self.stopped {
            let _ = std::fs::remove_file(&self.stop);
        } else {
            // A reader whose test failed early stops at its next pass, also behind sudo. The
            // stop file stays, because the reader may not have seen it yet.
            let _ = std::fs::write(&self.stop, b"");
        }
    }
}
