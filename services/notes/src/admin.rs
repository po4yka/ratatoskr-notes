//! The operator plane: liveness, readiness, metrics and version, on the operator listener only.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use metrics_exporter_prometheus::PrometheusHandle;
use ratatoskr_notes_persistence::Database;

/// The facts that readiness is computed from.
#[derive(Debug)]
pub struct RuntimeState {
    database: Database,
    started: AtomicBool,
    stopping: AtomicBool,
}

impl RuntimeState {
    /// A state that is not ready until [`RuntimeState::mark_started`].
    #[must_use]
    pub const fn new(database: Database) -> Self {
        Self {
            database,
            started: AtomicBool::new(false),
            stopping: AtomicBool::new(false),
        }
    }

    /// Both listeners are bound; readiness may answer 200.
    pub fn mark_started(&self) {
        self.started.store(true, Ordering::SeqCst);
    }

    /// A stop signal arrived; readiness answers 503 from now on.
    pub fn mark_stopping(&self) {
        self.stopping.store(true, Ordering::SeqCst);
    }
}

#[derive(Clone)]
struct AdminState {
    runtime: Arc<RuntimeState>,
    metrics: PrometheusHandle,
}

/// The admin router. It is served only on the operator listener, never on the domain listener.
pub fn admin_router(runtime: Arc<RuntimeState>, metrics: PrometheusHandle) -> Router {
    Router::new()
        .route("/health/live", get(live))
        .route("/health/ready", get(ready))
        .route("/metrics", get(render_metrics))
        .route("/version", get(version))
        .with_state(AdminState { runtime, metrics })
}

async fn live() -> StatusCode {
    StatusCode::OK
}

/// 200 only after startup completed, before the stop signal, and while the database answers.
async fn ready(State(state): State<AdminState>) -> StatusCode {
    let runtime = &state.runtime;
    if !runtime.started.load(Ordering::SeqCst) || runtime.stopping.load(Ordering::SeqCst) {
        return StatusCode::SERVICE_UNAVAILABLE;
    }
    match runtime.database.ping().await {
        Ok(()) => StatusCode::OK,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

async fn render_metrics(State(state): State<AdminState>) -> impl IntoResponse {
    state.metrics.render()
}

async fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
