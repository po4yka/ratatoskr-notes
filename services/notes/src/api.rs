//! The domain API on the loopback listener that Edge forwards `/v1/notes` to.

use axum::Router;
use ratatoskr_notes_persistence::Database;

/// What the domain handlers share.
#[derive(Debug, Clone)]
pub struct ApiState {
    #[expect(dead_code, reason = "the connection routes read it (OBS-023 task 5.3)")]
    database: Database,
}

impl ApiState {
    /// The state of the domain routes.
    #[must_use]
    pub const fn new(database: Database) -> Self {
        Self { database }
    }
}

/// The domain router. It is served only on the domain listener, never on the operator listener.
pub fn domain_router(state: ApiState) -> Router {
    Router::new().with_state(state)
}
