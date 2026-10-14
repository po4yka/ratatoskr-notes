//! Captures every log line that the service writes while a test runs, at every level.

#![allow(
    clippy::expect_used,
    dead_code,
    reason = "test scaffolding; not every test binary uses every helper"
)]

use std::io::Write;
use std::sync::{Arc, Mutex};

use tracing::subscriber::DefaultGuard;
use tracing_subscriber::fmt::MakeWriter;

/// The captured log output. Clones share one buffer.
#[derive(Debug, Clone, Default)]
pub(crate) struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

impl CapturedLogs {
    /// Installs a subscriber at `TRACE` for the current thread. The capture stops when the guard
    /// is dropped. A `#[tokio::test]` runs on one thread, so it captures the routes too.
    pub(crate) fn install() -> (Self, DefaultGuard) {
        let logs = Self::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(logs.clone())
            .with_max_level(tracing::Level::TRACE)
            .with_ansi(false)
            .finish();
        let guard = tracing::subscriber::set_default(subscriber);
        (logs, guard)
    }

    /// Everything written so far.
    pub(crate) fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().expect("the log buffer")).into_owned()
    }
}

impl Write for CapturedLogs {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .expect("the log buffer")
            .extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for CapturedLogs {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}
