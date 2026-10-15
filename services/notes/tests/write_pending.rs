//! A write is pending until its channel acknowledges it (workspace change
//! `add-notes-write-back`, specs `notes-write-command` and `notes-write-delivery`, task 4.7).

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions read the deliveries they wait for"
)]

mod support;

use axum::http::StatusCode;
use ratatoskr_notes::writes::{Outcome, Refusal};

use crate::support::channel::{OWNER, sha256};
use crate::support::writes::{Writer, command, update};

/// The code of a write that met a revision that its writer did not see.
const CONFLICT: &str = "notes.write.conflict";

#[tokio::test]
async fn refused_delivery_publishes_nothing() {
    let writer = Writer::new().await;
    let base = b"# Plan\n\nIn the vault.\n";
    let mine = b"# Plan\n\nRefused by the vault.\n";
    let connection = writer.vault(&[("Plan.md", base)]).await;
    let blob = writer.stage(connection, mine).await;

    let write = command(connection, &update("Plan.md", &sha256(base), &blob));
    writer.send(&write).await;
    let delivered = writer.deliveries(connection).await;
    let while_delivering = writer.current(connection, "Plan.md").await;
    writer
        .refuse(
            connection,
            &delivered[0],
            Refusal::Failed {
                code: "notes.channel.refused".to_owned(),
            },
        )
        .await;
    let (status, bytes) = writer.channel.read_file(OWNER, connection, "Plan.md").await;

    assert_eq!(
        while_delivering,
        Some(sha256(base)),
        "a delivered write is not current before its acknowledgement"
    );
    assert_eq!(
        writer.current(connection, "Plan.md").await,
        Some(sha256(base))
    );
    assert!(
        !writer.changed(connection).await.contains(&sha256(mine)),
        "no notes.file.changed.v1 names the refused write"
    );
    assert_eq!(status, StatusCode::OK);
    assert_eq!(bytes, base, "a read returns the previous revision");
    assert_eq!(
        writer.outcome(&write).await,
        Some(Outcome::Failed {
            code: "notes.channel.refused".to_owned(),
            conflict_id: None
        })
    );

    writer.cleanup().await;
}

#[tokio::test]
async fn acknowledged_write_publishes_once() {
    let writer = Writer::new().await;
    let base = b"# Plan\n\nIn the vault.\n";
    let mine = b"# Plan\n\nAcknowledged by the vault.\n";
    let connection = writer.vault(&[("Plan.md", base)]).await;
    let blob = writer.stage(connection, mine).await;

    let write = command(connection, &update("Plan.md", &sha256(base), &blob));
    writer.send(&write).await;
    let delivered = writer.deliveries(connection).await;
    let before = writer.changed(connection).await;
    writer.acknowledge(connection, &delivered[0]).await;
    let after = writer.changed(connection).await;

    assert!(
        !before.contains(&sha256(mine)),
        "nothing is published before the acknowledgement"
    );
    assert_eq!(
        after
            .iter()
            .filter(|revision| **revision == sha256(mine))
            .count(),
        1,
        "exactly one notes.file.changed.v1 names the acknowledged write"
    );
    assert_eq!(
        writer.current(connection, "Plan.md").await,
        Some(sha256(mine))
    );

    writer.cleanup().await;
}

#[tokio::test]
async fn device_base_mismatch_raises_conflict() {
    let writer = Writer::new().await;
    let base = b"# Plan\n\nIn the vault.\n";
    let local = b"# Plan\n\nEdited on the phone, not sent yet.\n";
    let mine = b"# Plan\n\nWritten by the owner.\n";
    let connection = writer.vault(&[("Plan.md", base)]).await;
    let blob = writer.stage(connection, mine).await;

    let write = command(connection, &update("Plan.md", &sha256(base), &blob));
    writer.send(&write).await;
    let delivered = writer.deliveries(connection).await;
    writer
        .refuse(
            connection,
            &delivered[0],
            Refusal::BaseMismatch {
                local_hash: Some(sha256(local)),
            },
        )
        .await;

    assert_eq!(
        writer.current(connection, "Plan.md").await,
        Some(sha256(base)),
        "the previous revision stays current"
    );
    assert!(!writer.changed(connection).await.contains(&sha256(mine)));
    assert!(
        matches!(
            writer.outcome(&write).await,
            Some(Outcome::Failed { ref code, .. }) if code == CONFLICT
        ),
        "{:?}",
        writer.outcome(&write).await
    );

    writer.cleanup().await;
}
