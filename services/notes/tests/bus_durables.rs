//! The durable of the write consumer (workspace change `add-notes-write-back`, task 4.1).
//!
//! Edge creates `ratatoskr_notes_write_requests` on the command stream; notes never creates it.
//! The consumer looks the durable up by its name and verifies its filter and its ack wait. Any
//! other value, or no durable at all, keeps the service not ready. The consumer hands each command
//! of the durable to the write path and acknowledges it after the write path took it.

#![allow(
    clippy::expect_used,
    reason = "test assertions read the answers of the broker that they set up"
)]

mod support;

use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream::consumer::{AckPolicy, DeliverPolicy, ReplayPolicy, pull};
use async_nats::jetstream::{self, stream};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use metrics_exporter_prometheus::PrometheusBuilder;
use ratatoskr_notes::admin::{RuntimeState, admin_router};
use ratatoskr_notes::blobs::BlobStore;
use ratatoskr_notes::files::FileRecorder;
use ratatoskr_notes::outbox::BusSettings;
use ratatoskr_notes::writes::Outcome;
use ratatoskr_notes::writes::{
    COMMAND_STREAM, Handled, WRITE_DURABLE, WRITE_FILTER, WriteConsumer, Writes,
};
use ratatoskr_notes_persistence::test_support::TestDatabase;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt as _;

use crate::support::channel::sha256;
use crate::support::writes::{Writer, command, update};

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

/// The tests of this file change the one durable of the broker, one at a time.
static BUS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// The settings of the scratch broker.
fn settings() -> BusSettings {
    BusSettings {
        url: nats_url(),
        nkey_seed_path: None,
    }
}

/// The write path of `fixture`, with a blob store that no test reads.
fn writes_of(fixture: &TestDatabase) -> Writes {
    let root = std::env::temp_dir().join(format!("notes-bus-{}", uuid::Uuid::now_v7().simple()));
    Writes::new(FileRecorder::new(
        fixture.database.clone(),
        BlobStore::new(root),
    ))
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
    let _bus = BUS.lock().await;
    let consumer = WriteConsumer::new(settings(), Arc::clone(&runtime), writes_of(&fixture));
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

#[tokio::test]
async fn delivers_each_command_to_the_write_path() {
    let writer = Writer::new().await;
    let base = b"# Plan\n\nIn the vault.\n";
    let mine = b"# Plan\n\nSent through the bus.\n";
    let connection = writer.vault(&[("Plan.md", base)]).await;
    let blob = writer.stage(connection, mine).await;
    let write = command(connection, &update("Plan.md", &sha256(base), &blob));
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
    let _bus = BUS.lock().await;
    stream.purge().await.expect("the command stream is emptied");
    provision(
        &stream,
        Some(durable(WRITE_FILTER, Duration::from_secs(30))),
    )
    .await;
    context
        .publish(
            WRITE_FILTER,
            serde_json::to_vec(&write.envelope)
                .expect("the command is JSON")
                .into(),
        )
        .await
        .expect("the command is published")
        .await
        .expect("the command is stored");
    let runtime = Arc::new(RuntimeState::new(writer.channel.fixture.database.clone()));
    let stop = CancellationToken::new();
    let consumer = WriteConsumer::new(settings(), runtime, writer.writes.clone());
    let running = tokio::spawn(consumer.run(stop.clone()));

    let mut outcome = None;
    for _ in 0..100 {
        outcome = writer.outcome(&write).await;
        if outcome.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let mut info = stream
        .get_consumer::<pull::Config>(WRITE_DURABLE)
        .await
        .expect("the durable exists");
    let mut acknowledged = false;
    for _ in 0..50 {
        let state = info.info().await.expect("the durable can be read");
        if state.num_ack_pending == 0 && state.num_pending == 0 {
            acknowledged = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    stop.cancel();
    let _ = running.await;
    let delivered = writer.deliveries(connection).await;

    assert_eq!(outcome, Some(Outcome::Pending), "the command is queued");
    assert_eq!(
        delivered.iter().map(|d| d.operation_id).collect::<Vec<_>>(),
        vec![write.operation_id]
    );
    assert!(
        acknowledged,
        "the message is acknowledged after the write is queued"
    );
    assert!(matches!(
        writer.writes.handle(&write.envelope).await,
        Ok(Handled::Repeated { .. })
    ));

    provision(&stream, None).await;
    writer.cleanup().await;
}

/// How many times the durable delivered a message, or `None` when it cannot be read.
async fn deliveries_of(durable: &mut jetstream::consumer::Consumer<pull::Config>) -> Option<u64> {
    durable
        .info()
        .await
        .ok()
        .map(|info| info.delivered.consumer_sequence)
}

#[tokio::test]
async fn a_failed_write_is_delivered_again() {
    let writer = Writer::new().await;
    let base = b"# Plan\n\nIn the vault.\n";
    let mine = b"# Plan\n\nSent while the database fails.\n";
    let connection = writer.vault(&[("Plan.md", base)]).await;
    let blob = writer.stage(connection, mine).await;
    let write = command(connection, &update("Plan.md", &sha256(base), &blob));
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
    let _bus = BUS.lock().await;
    stream.purge().await.expect("the command stream is emptied");
    provision(
        &stream,
        Some(durable(WRITE_FILTER, Duration::from_secs(30))),
    )
    .await;
    let mut durable = stream
        .get_consumer::<pull::Config>(WRITE_DURABLE)
        .await
        .expect("the durable exists");
    context
        .publish(
            WRITE_FILTER,
            serde_json::to_vec(&write.envelope)
                .expect("the command is JSON")
                .into(),
        )
        .await
        .expect("the command is published")
        .await
        .expect("the command is stored");

    // The write path fails while its table is away. Nothing between the rename and the restore
    // can panic, so the table always comes back before an assertion runs.
    let pool = writer.channel.fixture.pool();
    sqlx::query("alter table notes.writes rename to writes_away")
        .execute(pool)
        .await
        .expect("the table is renamed");
    let runtime = Arc::new(RuntimeState::new(writer.channel.fixture.database.clone()));
    let stop = CancellationToken::new();
    let consumer = WriteConsumer::new(settings(), runtime, writer.writes.clone());
    let running = tokio::spawn(consumer.run(stop.clone()));
    // A second delivery proves that the first one failed and was answered with a nak.
    let mut redelivered = false;
    for _ in 0..150 {
        if deliveries_of(&mut durable)
            .await
            .is_some_and(|count| count >= 2)
        {
            redelivered = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let restored = sqlx::query("alter table notes.writes_away rename to writes")
        .execute(pool)
        .await;

    let mut outcome = None;
    for _ in 0..150 {
        outcome = writer.outcome(&write).await;
        if outcome.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    stop.cancel();
    let _ = running.await;

    assert!(restored.is_ok(), "the table is back: {restored:?}");
    assert!(
        redelivered,
        "the failed command is delivered again within 15 s, by its nak and not the ack wait"
    );
    assert_eq!(
        outcome,
        Some(Outcome::Pending),
        "a later delivery queues the write"
    );

    provision(&stream, None).await;
    writer.cleanup().await;
}

#[tokio::test]
async fn a_panic_in_one_write_does_not_stop_the_consumer() {
    let writer = Writer::new().await;
    let a = b"# A\n";
    let base = b"# Plan\n\nIn the vault.\n";
    let mine = b"# Plan\n\nSent through the bus after a defect.\n";
    let broken = writer.vault(&[("A.md", a), ("Index.md", b"[[A]]\n")]).await;
    let other = writer.vault(&[("Plan.md", base)]).await;
    let blob = writer.stage(other, mine).await;
    let rename = command(
        broken,
        &serde_json::json!({ "rename": { "path": "A.md", "base_hash": sha256(a), "target_path": "B.md" } }),
    );
    let write = command(other, &update("Plan.md", &sha256(base), &blob));
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
    let _bus = BUS.lock().await;
    stream.purge().await.expect("the command stream is emptied");
    provision(
        &stream,
        Some(durable(WRITE_FILTER, Duration::from_secs(30))),
    )
    .await;
    for command in [&rename, &write] {
        context
            .publish(
                WRITE_FILTER,
                serde_json::to_vec(&command.envelope)
                    .expect("the command is JSON")
                    .into(),
            )
            .await
            .expect("the command is published")
            .await
            .expect("the command is stored");
    }
    let runtime = Arc::new(RuntimeState::new(writer.channel.fixture.database.clone()));
    let stop = CancellationToken::new();
    let defective = writer.writes.clone().with_panic_in_plan("A.md");
    let consumer = WriteConsumer::new(settings(), Arc::clone(&runtime), defective);
    let running = tokio::spawn(consumer.run(stop.clone()));

    let mut outcome = None;
    for _ in 0..100 {
        outcome = writer.outcome(&write).await;
        if outcome.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let ready_while_running = readiness(&runtime).await;
    let reports = writer.reports(rename.operation_id).await;
    stop.cancel();
    let _ = running.await;

    assert_eq!(
        outcome,
        Some(Outcome::Pending),
        "the write of another connection lands after a write that panicked"
    );
    assert_eq!(
        ready_while_running,
        StatusCode::OK,
        "the consumer still runs"
    );
    assert_eq!(
        reports
            .last()
            .map(|report| (report["status"].clone(), report["error"]["code"].clone())),
        Some((
            serde_json::json!("failed"),
            serde_json::json!("notes.write.internal")
        )),
        "the write that panicked fails its operation: {reports:?}"
    );
    assert_eq!(
        readiness(&runtime).await,
        StatusCode::SERVICE_UNAVAILABLE,
        "the service is not ready once the consumer ended"
    );

    provision(&stream, None).await;
    writer.cleanup().await;
}
