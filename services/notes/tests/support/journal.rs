//! The journal of the sync units of the fake systemd bus.
//!
//! A stub `ob` of a sync unit logs lines under paused tokio time: [`Journals::log_auth_failures`]
//! runs a paused runtime on its own thread, so that 30 seconds between two lines take no real time
//! and the runtime of the test, which talks to a real database, keeps the real clock. Each line
//! carries the monotonic time of the fake host, as journald gives it to a reader. The fake records
//! the time of the last line that a reader took when the service stops a unit.

#![allow(
    clippy::expect_used,
    dead_code,
    reason = "test scaffolding; not every test binary uses every helper"
)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// The monotonic time of the fake host at the first line.
const BOOT_OFFSET: Duration = Duration::from_secs(1000);

/// How often ob 0.0.14 tries to sync again in continuous mode (spike 3.2).
const RETRY: Duration = Duration::from_secs(30);

/// One line of a journal: the monotonic time and the message.
pub(crate) type Line = (Duration, String);

/// The journals of the sync units. Clones share one record.
#[derive(Debug, Clone, Default)]
pub(crate) struct Journals(Arc<Mutex<State>>);

#[derive(Debug, Default)]
struct State {
    /// The lines of each unit, in order.
    lines: HashMap<String, Vec<Line>>,
    /// The monotonic time of the last line, from [`BOOT_OFFSET`].
    clock: Duration,
    /// The time of the last line that a reader took, by unit.
    last_read: HashMap<String, Duration>,
    /// The time of the last line that a reader took at each stop of a unit.
    read_at_stop: HashMap<String, Vec<Option<Duration>>>,
}

/// The times from its start at which the stub `ob` logs, in `duration` of paused time.
fn retry_offsets(duration: Duration) -> Vec<Duration> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .expect("a paused runtime");
    runtime.block_on(async {
        let start = tokio::time::Instant::now();
        let mut offsets = Vec::new();
        while start.elapsed() <= duration {
            offsets.push(start.elapsed());
            tokio::time::sleep(RETRY).await;
        }
        offsets
    })
}

impl Journals {
    /// The stub `ob` of `unit` cannot authenticate: each 30 seconds of paused time for `duration`,
    /// it logs `Connecting...` and then the error line of ob 0.0.14,
    /// `Error: Failed to authenticate: <server message>`.
    pub(crate) fn log_auth_failures(&self, unit: &str, duration: Duration) {
        // A thread of its own, because a runtime cannot start inside the runtime of the test.
        let offsets = std::thread::scope(|scope| {
            scope
                .spawn(|| retry_offsets(duration))
                .join()
                .expect("the stub ob logs")
        });
        let mut state = self.0.lock().expect("the journals");
        let base = state.clock.max(BOOT_OFFSET);
        for offset in &offsets {
            let at = base + *offset;
            let lines = state.lines.entry(unit.to_owned()).or_default();
            lines.push((at, "Connecting...".to_owned()));
            lines.push((at, "Error: Failed to authenticate: stub refusal".to_owned()));
        }
        state.clock = base + offsets.last().copied().unwrap_or_default();
    }

    /// The lines of `unit` from index `from` on.
    pub(crate) fn lines_from(&self, unit: &str, from: usize) -> Vec<Line> {
        self.0
            .lock()
            .expect("the journals")
            .lines
            .get(unit)
            .and_then(|lines| lines.get(from..))
            .map(<[Line]>::to_vec)
            .unwrap_or_default()
    }

    /// A reader of `unit` took the line of time `at`.
    pub(crate) fn mark_read(&self, unit: &str, at: Duration) {
        self.0
            .lock()
            .expect("the journals")
            .last_read
            .insert(unit.to_owned(), at);
    }

    /// The service stops `unit`.
    pub(crate) fn record_stop(&self, unit: &str) {
        let mut state = self.0.lock().expect("the journals");
        let read = state.last_read.get(unit).copied();
        state
            .read_at_stop
            .entry(unit.to_owned())
            .or_default()
            .push(read);
    }

    /// For each stop of `unit`, the time of the last line that a reader took then, from the first
    /// line of the unit.
    pub(crate) fn read_at_stops(&self, unit: &str) -> Vec<Option<Duration>> {
        let state = self.0.lock().expect("the journals");
        let first = state
            .lines
            .get(unit)
            .and_then(|lines| lines.first())
            .map(|(at, _)| *at)
            .unwrap_or_default();
        state
            .read_at_stop
            .get(unit)
            .map(|stops| {
                stops
                    .iter()
                    .map(|read| read.map(|at| at.saturating_sub(first)))
                    .collect()
            })
            .unwrap_or_default()
    }
}
