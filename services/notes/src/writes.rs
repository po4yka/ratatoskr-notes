//! The write path of a connected vault (workspace change `add-notes-write-back`, OBS-024).
//!
//! Edge publishes each write as `notes.file.write_requested.v1` on the command stream. Notes reads
//! the commands through the durable that Edge creates for it.

mod consumer;

pub use consumer::{COMMAND_STREAM, DurableError, WRITE_DURABLE, WRITE_FILTER, WriteConsumer};
