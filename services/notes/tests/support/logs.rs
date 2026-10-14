//! Captures every log line that the service writes while a test binary runs, at every level.
//!
//! The subscriber is the global default of the test binary, not a per-thread default: with a
//! per-thread default, a callsite that another test thread reaches first can keep the interest of a
//! thread without a subscriber, and the capture then misses its events (seen on macOS with five
//! tests in parallel). The buffer therefore holds the lines of every test in the binary.

#![allow(
    clippy::expect_used,
    dead_code,
    reason = "test scaffolding; not every test binary uses every helper"
)]

use std::io::Write;
use std::sync::{Arc, Mutex, OnceLock};

use tracing_subscriber::fmt::MakeWriter;

/// The one capture of this test binary.
static GLOBAL: OnceLock<CapturedLogs> = OnceLock::new();

/// The captured log output. Clones share one buffer.
#[derive(Debug, Clone, Default)]
pub(crate) struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

impl CapturedLogs {
    /// Installs a subscriber at `TRACE` as the global default of the test binary, once, and
    /// returns its buffer.
    pub(crate) fn install() -> Self {
        GLOBAL
            .get_or_init(|| {
                let logs = Self::default();
                let subscriber = tracing_subscriber::fmt()
                    .with_writer(logs.clone())
                    .with_max_level(tracing::Level::TRACE)
                    .with_ansi(false)
                    .finish();
                tracing::subscriber::set_global_default(subscriber)
                    .expect("no other global subscriber in this test binary");
                logs
            })
            .clone()
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
