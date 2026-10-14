//! A umask that can remove the setgid bit of a blob directory fails the start of the service
//! (workspace design D12, review item M-1 of tasks 6.11 to 6.18).
//!
//! The umask is the umask of the process, so this file holds one test only. Linux only: only Linux
//! gives the umask without a change, and the blob store checks it there.

#![cfg(target_os = "linux")]
#![allow(
    clippy::expect_used,
    reason = "test assertions read the result of the start"
)]

use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use metrics_exporter_prometheus::PrometheusBuilder;
use ratatoskr_notes::config::{
    AdminConfig, ApiConfig, BlobsConfig, ConnectionsConfig, DatabaseConfig, NotesConfig,
    SecretsConfig, ShutdownConfig, TelemetryConfig,
};
use ratatoskr_notes::systemd::DbusSystemd;
use rustix::fs::Mode;
use secrecy::SecretString;

#[tokio::test]
async fn strict_umask_fails_the_start() {
    let host = tempfile::tempdir().expect("a host directory");
    let any_port = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
    let config = NotesConfig {
        admin: AdminConfig { bind: any_port },
        api: ApiConfig {
            listen_address: any_port,
        },
        blobs: BlobsConfig {
            root: host.path().join("blobs"),
        },
        connections: ConnectionsConfig::default(),
        // No database answers on port 1: a start that reaches the database fails there.
        database: DatabaseConfig {
            url: SecretString::from("postgres://notes:notes@127.0.0.1:1/notes"),
            max_connections: 1,
            acquire_timeout_seconds: 1,
        },
        secrets: SecretsConfig::default(),
        shutdown: ShutdownConfig { grace_seconds: 1 },
        telemetry: TelemetryConfig::default(),
    };
    let metrics = PrometheusBuilder::new().build_recorder().handle();

    let previous = rustix::process::umask(Mode::from_raw_mode(0o077));
    let started = tokio::time::timeout(
        Duration::from_secs(30),
        ratatoskr_notes::serve(
            &config,
            metrics,
            DbusSystemd::new(),
            host.path().join("credentials"),
            std::future::pending(),
        ),
    )
    .await;
    rustix::process::umask(previous);

    let error = started
        .expect("the start ends")
        .expect_err("the start fails");
    assert!(
        error.contains("umask"),
        "the start fails because of the umask, before it reaches the database: {error}"
    );
}
