//! The durable of the write consumer (workspace change `add-notes-write-back`, task 4.1).
//!
//! Edge creates `ratatoskr_notes_write_requests` on the command stream; notes never creates it.
//! The consumer looks the durable up by its name and verifies its filter and its ack wait. Any
//! other value, or no durable at all, keeps the service not ready.

#![allow(
    clippy::expect_used,
    reason = "test assertions read the answers of the broker that they set up"
)]

use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream::consumer::{AckPolicy, DeliverPolicy, ReplayPolicy, pull};
use async_nats::jetstream::{self, stream};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use metrics_exporter_prometheus::PrometheusBuilder;
use ratatoskr_notes::admin::{RuntimeState, admin_router};
use ratatoskr_notes::outbox::BusSettings;
use ratatoskr_notes::writes::{COMMAND_STREAM, WRITE_DURABLE, WRITE_FILTER, WriteConsumer};
use ratatoskr_notes_persistence::test_support::TestDatabase;
use tower::ServiceExt as _;

/// Where the test reaches a NATS server with `JetStream`. `NOTES_TEST_NATS_URL` overrides it; CI
/// starts a server there.
#[expect(
    clippy::disallowed_methods,
    reason = "a test-only location of a scratch NATS server, not a setting of the service"
)]
fn nats_url() -> String {
    std::env::var("NOTES_TEST_NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".to_owned())
}

/// The durable as Edge provisions it: the `standard` row of Platform's `DOMAIN_CONSUMERS`.
fn durable(filter: &str, ack_wait: Duration) -> pull::Config {
    pull::Config {
        durable_name: Some(WRITE_DURABLE.to_owned()),
        filter_subject: filter.to_owned(),
        ack_policy: AckPolicy::Explicit,
        deliver_policy: DeliverPolicy::All,
        ack_wait,
        max_deliver: -1,
        replay_policy: ReplayPolicy::Instant,
        ..pull::Config::default()
    }
}

/// Replace the durable with `config`, or remove it when `config` is `None`.
async fn provision(stream: &stream::Stream, config: Option<pull::Config>) {
    let _ = stream.delete_consumer(WRITE_DURABLE).await;
    if let Some(config) = config {
        stream
            .create_consumer(config)
            .await
            .expect("the test durable is created");
    }
}

/// The status of `/health/ready` of `runtime`.
async fn readiness(runtime: &Arc<RuntimeState>) -> StatusCode {
    let metrics = PrometheusBuilder::new().build_recorder().handle();
    admin_router(Arc::clone(runtime), metrics)
        .oneshot(
            Request::get("/health/ready")
                .body(Body::empty())
                .expect("a request"),
        )
        .await
        .expect("the admin router answers")
        .status()
}

#[tokio::test]
async fn verifies_write_request_durable() {
    let fixture = TestDatabase::create().await.expect("a test database");
    let client = async_nats::connect(nats_url())
        .await
        .expect("the test NATS server can be reached");
    let context = jetstream::new(client);
    let stream = context
        .get_or_create_stream(stream::Config {
            name: COMMAND_STREAM.to_owned(),
            subjects: vec!["cmd.>".to_owned()],
            ..Default::default()
        })
        .await
        .expect("the command stream exists");
    let runtime = Arc::new(RuntimeState::new(fixture.database.clone()));
    runtime.mark_started();
    let consumer = WriteConsumer::new(
        BusSettings {
            url: nats_url(),
            nkey_seed_path: None,
        },
        Arc::clone(&runtime),
    );
    let standard = Duration::from_secs(30);

    provision(&stream, None).await;
    let missing = consumer.verify().await;
    let missing_ready = readiness(&runtime).await;

    provision(
        &stream,
        Some(durable("cmd.notes.file.other_requested.v1", standard)),
    )
    .await;
    let other_filter = consumer.verify().await;
    let other_filter_ready = readiness(&runtime).await;

    provision(&stream, Some(durable(WRITE_FILTER, Duration::from_mins(1)))).await;
    let other_wait = consumer.verify().await;
    let other_wait_ready = readiness(&runtime).await;

    provision(&stream, Some(durable(WRITE_FILTER, standard))).await;
    let provisioned = consumer.verify().await;
    let provisioned_ready = readiness(&runtime).await;

    provision(&stream, Some(durable(WRITE_FILTER, Duration::from_secs(5)))).await;
    let changed = consumer.verify().await;
    let changed_ready = readiness(&runtime).await;

    assert_eq!(WRITE_DURABLE, "ratatoskr_notes_write_requests");
    assert_eq!(WRITE_FILTER, "cmd.notes.file.write_requested.v1");
    assert!(missing.is_err(), "a missing durable is refused");
    assert_eq!(missing_ready, StatusCode::SERVICE_UNAVAILABLE);
    assert!(other_filter.is_err(), "another filter is refused");
    assert_eq!(other_filter_ready, StatusCode::SERVICE_UNAVAILABLE);
    assert!(other_wait.is_err(), "another ack wait is refused");
    assert_eq!(other_wait_ready, StatusCode::SERVICE_UNAVAILABLE);
    assert!(provisioned.is_ok(), "the durable of Edge is accepted");
    assert_eq!(provisioned_ready, StatusCode::OK);
    assert!(changed.is_err(), "a durable that changed later is refused");
    assert_eq!(
        changed_ready,
        StatusCode::SERVICE_UNAVAILABLE,
        "readiness follows the last verification"
    );

    provision(&stream, None).await;
    fixture
        .cleanup()
        .await
        .expect("the test database is dropped");
}
