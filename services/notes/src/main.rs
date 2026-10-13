//! The `ratatoskr-notes` deployable.
//!
//! This binary owns the `notes` schema: it applies `schema.sql` to a fresh database at startup.
//! There is one process per deployment and one schema owner.

use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    if std::env::args().nth(1).as_deref() == Some("check-config") {
        return ratatoskr_notes::check_config();
    }
    ratatoskr_notes::run().await
}
