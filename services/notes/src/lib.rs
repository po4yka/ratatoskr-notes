//! The `ratatoskr-notes` service library: the process lifecycle and the routes.
//!
//! The binary's `main` calls [`run`] or [`check_config`] and nothing else. The library is separate
//! from the binary so that integration tests drive the real routes.
//!
//! # Exit codes
//!
//! | Code | Meaning |
//! |---|---|
//! | `0` | Clean start and clean shutdown |
//! | `1` | Runtime startup failure: telemetry, the database, the schema, or a listener |
//! | `78` | `EX_CONFIG`: the configuration is unreadable or invalid; nothing was bound |

pub mod admin;
pub mod api;
pub mod config;
pub mod events;
pub mod systemd;

use std::process::ExitCode;
use std::sync::Arc;

use axum::Router;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use ratatoskr_notes_persistence::Database;
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;

use crate::admin::{RuntimeState, admin_router};
use crate::api::{ApiState, domain_router};
use crate::config::{LogFormat, NotesConfig, TelemetryConfig};
use crate::systemd::DbusSystemd;

/// The whole process lifecycle.
///
/// 1. Load the configuration; on failure write the report to stderr and exit 78.
/// 2. Initialise logging and the metrics recorder; on failure exit 1.
/// 3. Connect the database and apply the schema; on failure exit 1.
/// 4. Bind the operator and the domain listeners; on failure exit 1.
/// 5. Serve until SIGTERM or SIGINT, then let in-flight requests finish within the grace window.
pub async fn run() -> ExitCode {
    let config = match config::load() {
        Ok(config) => config,
        Err(error) => {
            eprintln!("{}", error.report());
            return ExitCode::from(error.exit_code());
        }
    };
    let metrics = match init_telemetry(&config.telemetry) {
        Ok(metrics) => metrics,
        Err(reason) => {
            eprintln!("ratatoskr-notes: refusing to start; {reason}");
            return ExitCode::FAILURE;
        }
    };
    tracing::info!(config = ?config, version = env!("CARGO_PKG_VERSION"), "effective configuration");

    match serve(&config, metrics).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(reason) => {
            tracing::error!(%reason, "the process stopped with a failure");
            ExitCode::FAILURE
        }
    }
}

/// `ratatoskr-notes check-config`: load and validate without binding anything, and exit 0 or 78.
///
/// Both outputs go to stderr: the workspace forbids writing to stdout.
#[must_use]
pub fn check_config() -> ExitCode {
    match config::load() {
        Ok(config) => {
            // Safe by type: the database URL renders as `[REDACTED]`.
            eprintln!("ratatoskr-notes: configuration is valid.\n{config:#?}");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("{}", error.report());
            ExitCode::from(error.exit_code())
        }
    }
}

fn init_telemetry(telemetry: &TelemetryConfig) -> Result<PrometheusHandle, String> {
    let filter = EnvFilter::try_new(&telemetry.log_filter)
        .map_err(|_| "the log filter could not be parsed".to_owned())?;
    let registry = tracing_subscriber::registry().with(filter);
    let installed = match telemetry.log_format {
        LogFormat::Text => registry
            .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
            .try_init(),
        LogFormat::Json => registry
            .with(
                tracing_subscriber::fmt::layer()
                    .json()
                    .with_writer(std::io::stderr),
            )
            .try_init(),
    };
    installed.map_err(|_| "the log subscriber could not be installed".to_owned())?;
    PrometheusBuilder::new()
        .install_recorder()
        .map_err(|_| "the metrics recorder could not be installed".to_owned())
}

async fn serve(config: &NotesConfig, metrics: PrometheusHandle) -> Result<(), String> {
    let database = Database::connect(
        &config.database.url,
        config.database.max_connections,
        config.database.acquire_timeout(),
    )
    .await
    .map_err(|error| {
        format!("the database could not be reached: {error}; check RATATOSKR__DATABASE__URL")
    })?;
    database
        .apply_schema()
        .await
        .map_err(|error| format!("the notes schema could not be applied: {error}"))?;

    let runtime = Arc::new(RuntimeState::new(database.clone()));
    let admin = bind(config.admin.bind, "operator").await?;
    let api = bind(config.api.listen_address, "domain").await?;

    let (stop_sender, stop_receiver) = tokio::sync::watch::channel(false);
    let admin_task = spawn_server(
        admin,
        admin_router(Arc::clone(&runtime), metrics),
        stop_receiver.clone(),
    );
    let api_task = spawn_server(
        api,
        domain_router(ApiState::new(database.clone(), DbusSystemd::new())),
        stop_receiver,
    );

    runtime.mark_started();
    tracing::info!(
        admin = %config.admin.bind,
        api = %config.api.listen_address,
        "startup complete"
    );

    shutdown_signal().await;
    runtime.mark_stopping();
    tracing::info!("stop signal received; draining");
    let _ = stop_sender.send(true);

    let grace = std::time::Duration::from_secs(config.shutdown.grace_seconds);
    let drained = tokio::time::timeout(grace, async {
        let _ = admin_task.await;
        let _ = api_task.await;
    })
    .await
    .is_ok();
    tracing::info!(graceful = drained, "the process stopped");
    database.close().await;
    Ok(())
}

async fn bind(address: std::net::SocketAddr, name: &str) -> Result<TcpListener, String> {
    TcpListener::bind(address)
        .await
        .map_err(|error| format!("the {name} listener could not bind {address}: {error}"))
}

fn spawn_server(
    listener: TcpListener,
    router: Router,
    mut stop: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let served = axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                let _ = stop.wait_for(|stopping| *stopping).await;
            })
            .await;
        if let Err(error) = served {
            tracing::error!(%error, "a listener stopped with an error");
        }
    })
}

async fn shutdown_signal() {
    let interrupt = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut terminate = match tokio::signal::unix::signal(
            tokio::signal::unix::SignalKind::terminate(),
        ) {
            Ok(signal) => signal,
            Err(error) => {
                tracing::error!(%error, "SIGTERM cannot be observed; only SIGINT stops the process");
                let _ = interrupt.await;
                return;
            }
        };
        tokio::select! {
            _ = interrupt => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = interrupt.await;
    }
}
