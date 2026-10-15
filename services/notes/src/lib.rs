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
//! | `1` | Runtime startup failure: telemetry, the blob store, the database, the schema, or a listener |
//! | `78` | `EX_CONFIG`: the configuration is unreadable or invalid; nothing was bound |

pub mod admin;
pub mod api;
pub mod blobs;
pub mod channel;
pub mod config;
pub mod events;
pub mod files;
mod login;
pub mod manifest;
pub mod operation;
pub mod outbox;
pub mod secrets;
mod supervisor;
pub mod systemd;
mod tree_sync;
pub mod uploads;
pub mod watcher;
pub mod writes;

use std::future::Future;
use std::path::PathBuf;
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
use crate::api::{ApiState, DEFAULT_CREDENTIALS_DIR, domain_router};
use crate::blobs::BlobStore;
use crate::config::{LogFormat, NotesConfig, TelemetryConfig};
use crate::files::FileRecorder;
use crate::outbox::{BusSettings, OutboxRelay};
use crate::systemd::{DbusSystemd, SystemdBus};
use crate::writes::{WriteConsumer, Writes};

/// The whole process lifecycle.
///
/// 1. Load the configuration; on failure write the report to stderr and exit 78.
/// 2. Initialise logging and the metrics recorder; on failure exit 1.
/// 3. Open the blob store, which refuses a umask stricter than 0027; on failure exit 1.
/// 4. Connect the database and apply the schema; on failure exit 1.
/// 5. Bind the operator and the domain listeners; on failure exit 1.
/// 6. Fail each `sync` login that a stop of the previous process interrupted, and start the sync
///    unit of each `sync` connection whose login finished, before the listeners accept a request.
/// 7. Start the outbox relay and the consumer of the write commands when `RATATOSKR__BUS__URL` is
///    set. Readiness then needs the durable of the write commands.
/// 8. Serve until SIGTERM or SIGINT. Then, within the grace window, let in-flight requests finish,
///    and fail each connect whose login still runs. Then stop the relay and the consumer.
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

    let served = serve(
        &config,
        metrics,
        DbusSystemd::new(),
        PathBuf::from(DEFAULT_CREDENTIALS_DIR),
        shutdown_signal(),
    )
    .await;
    match served {
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

/// Serve with a loaded configuration until `shutdown` completes, then stop within the grace window.
///
/// Steps 3 to 7 of [`run`]. [`run`] gives it the systemd manager on the system bus,
/// [`DEFAULT_CREDENTIALS_DIR`] and the arrival of SIGTERM or SIGINT. A test gives it a fake bus, a
/// temporary directory and a future that the test completes.
///
/// # Errors
///
/// The reason, when the blob store, the database, the schema or a listener cannot be used.
pub async fn serve<B: SystemdBus>(
    config: &NotesConfig,
    metrics: PrometheusHandle,
    systemd: B,
    credentials_dir: PathBuf,
    shutdown: impl Future<Output = ()> + Send,
) -> Result<(), String> {
    // First, so that a umask that can remove the setgid bit of a blob directory stops the start
    // (workspace design D12). The read routes read the bytes from this store.
    let blobs = BlobStore::open(&config.blobs.root)
        .map_err(|error| format!("the blob store cannot be used: {error}"))?;
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

    // The relay publishes what the outbox holds, also the events of an earlier process.
    let relay_stop = tokio_util::sync::CancellationToken::new();
    let relay = config.bus.url.as_ref().map(|url| {
        let settings = BusSettings {
            url: url.clone(),
            nkey_seed_path: config.bus.nkey_seed_path.clone(),
        };
        // Readiness needs the durable of the write commands from now on.
        let writes = Writes::new(
            FileRecorder::new(database.clone(), blobs.clone())
                .with_max_file_bytes(config.files.max_file_bytes),
        );
        let consumer = WriteConsumer::new(settings.clone(), Arc::clone(&runtime), writes);
        let relay = OutboxRelay::new(database.clone(), settings);
        (
            tokio::spawn(relay.run(relay_stop.clone())),
            tokio::spawn(consumer.run(relay_stop.clone())),
        )
    });
    if relay.is_none() {
        tracing::warn!("RATATOSKR__BUS__URL is not set; the events stay in the outbox");
    }

    let state = ApiState::new(database.clone(), systemd)
        .with_blobs(blobs)
        .with_max_file_bytes(config.files.max_file_bytes)
        .with_key_file(config.secrets.key_file.clone())
        .with_credentials_dir(credentials_dir)
        .with_max_logins(config.connections.max_logins);
    // After the binds, so that a second process that cannot bind changes nothing, and before the
    // listeners accept, so that no connect of this process runs yet.
    state.reconcile_interrupted_logins().await;
    state.resume_sync_connections().await;
    state.resume_device_connects().await;

    let (stop_sender, stop_receiver) = tokio::sync::watch::channel(false);
    let admin_task = spawn_server(
        admin,
        admin_router(Arc::clone(&runtime), metrics),
        stop_receiver.clone(),
    );
    let api_task = spawn_server(api, domain_router(state.clone()), stop_receiver);

    runtime.mark_started();
    tracing::info!(
        admin = %config.admin.bind,
        api = %config.api.listen_address,
        "startup complete"
    );

    shutdown.await;
    runtime.mark_stopping();
    tracing::info!("stop signal received; draining");
    let _ = stop_sender.send(true);

    let grace = std::time::Duration::from_secs(config.shutdown.grace_seconds);
    let drained = tokio::time::timeout(grace, async {
        tokio::join!(state.shut_down(), async {
            let _ = admin_task.await;
            let _ = api_task.await;
        })
    })
    .await
    .is_ok();
    relay_stop.cancel();
    if let Some((relay, consumer)) = relay {
        let _ = relay.await;
        let _ = consumer.await;
    }
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
