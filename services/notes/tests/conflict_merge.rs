//! A write on a stale base raises a conflict with its three sides and, for text, a three-way
//! merge proposal (workspace change `add-notes-write-back`, spec `notes-write-command`, task 5.1).

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions read the JSON events they wait for"
)]

mod support;

use ratatoskr_notes::writes::Outcome;
use ratatoskr_notes_contracts::ConflictRaised;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::support::channel::{OWNER, sha256};
use crate::support::writes::{Writer, command, command_with, resolve, update};

/// The event type of a raised conflict.
const RAISED: &str = "notes.conflict.raised.v1";

/// A stale update of `path` from `origin`: the vault holds `base`, the device changes it to
/// `phone`, and the writer sends `mine` on `base`. Returns the connection.
async fn stale_update(
    writer: &Writer,
    path: &str,
    media_type: &str,
    [base, phone, mine]: [&[u8]; 3],
    origin: &str,
) -> Uuid {
    let connection = writer.vault(&[(path, base)]).await;
    writer.vault_change(connection, path, phone).await;
    let blob = writer.stage_for(OWNER, connection, mine, media_type).await;
    let write = command_with(
        OWNER,
        connection,
        origin,
        &Uuid::now_v7().to_string(),
        &update(path, &sha256(base), &blob),
    );
    writer.send(&write).await;
    connection
}

#[tokio::test]
async fn conflict_event_carries_hashes_only() {
    let writer = Writer::new().await;
    let base: &[u8] = b"# Plan\n\nThe marker-base-6c1f text.\n";
    let phone: &[u8] = b"# Plan\n\nThe marker-phone-6c1f text.\n";
    let mine: &[u8] = b"# Plan\n\nThe marker-mine-6c1f text.\n";
    let connection = stale_update(
        &writer,
        "Plan.md",
        "text/markdown",
        [base, phone, mine],
        "platform",
    )
    .await;

    let raised = writer.events_of(connection, RAISED).await;
    let rows = writer.outbox_text(RAISED).await;

    assert_eq!(raised.len(), 1, "exactly one conflict is raised");
    let event: ConflictRaised =
        serde_json::from_value(raised[0].clone()).expect("the event satisfies the contract");
    assert_eq!(event.path.as_str(), "Plan.md");
    assert_eq!(event.media_type.as_str(), "text/markdown");
    assert_eq!(
        event
            .base_hash
            .as_ref()
            .map(|hash| hash.as_str().to_owned()),
        Some(sha256(base))
    );
    assert_eq!(
        event
            .current_hash
            .as_ref()
            .map(|hash| hash.as_str().to_owned()),
        Some(sha256(phone))
    );
    assert_eq!(
        event
            .writer_hash
            .as_ref()
            .map(|hash| hash.as_str().to_owned()),
        Some(sha256(mine))
    );
    for row in &rows {
        assert!(
            !row.contains("marker-"),
            "no file content in the event: {row}"
        );
    }

    writer.cleanup().await;
}

#[tokio::test]
async fn conflict_repeats_the_origin() {
    let writer = Writer::new().await;
    let base: &[u8] = b"# Page\n\nPublished.\n";
    let phone: &[u8] = b"# Page\n\nChanged in Obsidian.\n";
    let mine: &[u8] = b"# Page\n\nChanged on the site.\n";
    let connection = stale_update(
        &writer,
        "Page.md",
        "text/markdown",
        [base, phone, mine],
        "site:docs",
    )
    .await;

    let raised = writer.events_of(connection, RAISED).await;

    assert_eq!(raised.len(), 1, "exactly one conflict is raised");
    assert_eq!(raised[0]["origin"], "site:docs");

    writer.cleanup().await;
}

#[tokio::test]
async fn disjoint_edits_merge_cleanly() {
    let writer = Writer::new().await;
    let base: &[u8] =
        b"# Plan\n\nThe first paragraph.\n\nThe middle paragraph.\n\nThe last paragraph.\n";
    let phone: &[u8] =
        b"# Plan\n\nThe first paragraph, edited on the phone.\n\nThe middle paragraph.\n\nThe last paragraph.\n";
    let mine: &[u8] =
        b"# Plan\n\nThe first paragraph.\n\nThe middle paragraph.\n\nThe last paragraph, edited by the owner.\n";
    let merged = "# Plan\n\nThe first paragraph, edited on the phone.\n\nThe middle paragraph.\n\nThe last paragraph, edited by the owner.\n";
    let connection = stale_update(
        &writer,
        "Plan.md",
        "text/markdown",
        [base, phone, mine],
        "platform",
    )
    .await;

    let raised = writer.events_of(connection, RAISED).await;
    let conflict_id = raised
        .first()
        .and_then(|event| event["conflict_id"].as_str())
        .unwrap_or_default()
        .to_owned();
    let read = writer.conflict(OWNER, &conflict_id).await;

    assert_eq!(raised.len(), 1, "exactly one conflict is raised");
    assert_eq!(
        raised[0]["merge_proposal"],
        json!({ "merged_hash": sha256(merged.as_bytes()), "has_overlapping_hunks": false })
    );
    assert_eq!(read.body["merge_proposal"], raised[0]["merge_proposal"]);
    assert_eq!(read.body["texts"]["merged"], Value::from(merged));

    writer.cleanup().await;
}

#[tokio::test]
async fn attachment_conflict_has_no_proposal() {
    let writer = Writer::new().await;
    let base: &[u8] = b"\x89PNG\r\n\x1a\nthe photo as it was";
    let phone: &[u8] = b"\x89PNG\r\n\x1a\nthe photo cropped on the phone";
    let mine: &[u8] = b"\x89PNG\r\n\x1a\nthe photo cropped by the owner";
    let connection = stale_update(
        &writer,
        "assets/photo.png",
        "image/png",
        [base, phone, mine],
        "platform",
    )
    .await;

    let raised = writer.events_of(connection, RAISED).await;

    assert_eq!(raised.len(), 1, "exactly one conflict is raised");
    assert_eq!(raised[0]["media_type"], "image/png");
    assert_eq!(raised[0].get("merge_proposal"), None);

    writer.cleanup().await;
}

/// The event type of a resolved conflict.
const RESOLVED: &str = "notes.conflict.resolved.v1";

/// The id of the last conflict that `connection` raised.
async fn last_conflict(writer: &Writer, connection: Uuid) -> String {
    writer
        .events_of(connection, RAISED)
        .await
        .last()
        .and_then(|event| event["conflict_id"].as_str().map(str::to_owned))
        .expect("a conflict is raised")
}

#[tokio::test]
async fn resolution_races_vault_change() {
    let writer = Writer::new().await;
    let base: &[u8] = b"# Plan\n\nThe draft that the writer read.\n";
    let phone: &[u8] = b"# Plan\n\nThe draft that the phone saved.\n";
    let mine: &[u8] = b"# Plan\n\nThe draft that the writer saved.\n";
    let later: &[u8] = b"# Plan\n\nThe phone saved again meanwhile.\n";
    let connection = stale_update(
        &writer,
        "Plan.md",
        "text/markdown",
        [base, phone, mine],
        "platform",
    )
    .await;
    let first = last_conflict(&writer, connection).await;

    writer.vault_change(connection, "Plan.md", later).await;
    let resolution = command(connection, &resolve(&first, "keep_writer"));
    writer.send(&resolution).await;
    let raised = writer.events_of(connection, RAISED).await;
    let read = writer.conflict(OWNER, &first).await;

    assert!(
        writer.deliveries(connection).await.is_empty(),
        "nothing is delivered over the newer revision"
    );
    assert_eq!(raised.len(), 2, "a second conflict is raised");
    assert_ne!(raised[1]["conflict_id"], json!(first));
    assert_eq!(raised[1]["current_hash"], json!(sha256(later)));
    assert_eq!(raised[1]["writer_hash"], json!(sha256(mine)));
    assert_eq!(raised[1]["base_hash"], json!(sha256(phone)));
    assert!(
        matches!(
            writer.outcome(&resolution).await,
            Some(Outcome::Failed { ref code, conflict_id: Some(_) }) if code == "notes.write.conflict"
        ),
        "{:?}",
        writer.outcome(&resolution).await
    );
    assert_eq!(
        writer.current(connection, "Plan.md").await,
        Some(sha256(later))
    );
    assert_eq!(read.body["state"], "open", "the first conflict stays open");
    assert!(writer.events_of(connection, RESOLVED).await.is_empty());

    writer.cleanup().await;
}

#[tokio::test]
async fn resolution_is_published_on_acknowledgement() {
    let writer = Writer::new().await;
    let base: &[u8] = b"# Plan\n\nThe draft that the writer read.\n";
    let phone: &[u8] = b"# Plan\n\nThe draft that the phone saved.\n";
    let mine: &[u8] = b"# Plan\n\nThe draft that the writer saved.\n";
    let connection = stale_update(
        &writer,
        "Plan.md",
        "text/markdown",
        [base, phone, mine],
        "site:docs",
    )
    .await;
    let conflict_id = last_conflict(&writer, connection).await;

    // The site that raised the conflict also resolves it; the resolved event names who resolved.
    let resolution = command_with(
        OWNER,
        connection,
        "site:docs",
        "site-resolution",
        &resolve(&conflict_id, "keep_writer"),
    );
    writer.send(&resolution).await;
    let delivered = writer.deliveries(connection).await;
    let before = writer.events_of(connection, RESOLVED).await;
    for delivery in &delivered {
        writer.acknowledge(connection, delivery).await;
    }
    let resolved = writer.events_of(connection, RESOLVED).await;
    let read = writer.conflict(OWNER, &conflict_id).await;

    assert_eq!(delivered.len(), 1, "{delivered:?}");
    assert_eq!(delivered[0].kind, "resolve_conflict");
    assert_eq!(delivered[0].base_hash, Some(sha256(phone)));
    assert_eq!(delivered[0].content_hash, Some(sha256(mine)));
    assert!(
        before.is_empty(),
        "nothing is resolved before the acknowledgement"
    );
    assert_eq!(resolved.len(), 1);
    assert_eq!(resolved[0]["conflict_id"], json!(conflict_id));
    assert_eq!(resolved[0]["revision"], json!(sha256(mine)));
    assert_eq!(resolved[0]["origin"], "site:docs");
    assert_eq!(resolved[0]["path"], "Plan.md");
    assert_eq!(
        writer.current(connection, "Plan.md").await,
        Some(sha256(mine))
    );
    assert_eq!(read.body["state"], "resolved");

    writer.cleanup().await;
}
