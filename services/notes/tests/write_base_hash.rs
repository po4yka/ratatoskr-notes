//! The base of a write is checked at the head of its queue (workspace change
//! `add-notes-write-back`, spec `notes-write-command`, task 4.5).

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions read the deliveries they wait for"
)]

mod support;

use ratatoskr_notes::writes::{Handled, Outcome};

use crate::support::channel::sha256;
use crate::support::writes::{Writer, command, create, update};

/// The code of a write that met a revision that its writer did not see.
const CONFLICT: &str = "notes.write.conflict";

#[tokio::test]
async fn stale_base_makes_a_conflict() {
    let writer = Writer::new().await;
    let base = b"# Plan\n\nThe draft that the writer read.\n";
    let phone = b"# Plan\n\nThe draft that the phone saved later.\n";
    let mine = b"# Plan\n\nThe draft that the writer saved.\n";
    let connection = writer.vault(&[("Plan.md", base)]).await;
    writer.vault_change(connection, "Plan.md", phone).await;
    let blob = writer.stage(connection, mine).await;
    let events_before = writer.changed(connection).await;

    let stale = command(connection, &update("Plan.md", &sha256(base), &blob));
    let handled = writer.send(&stale).await;

    assert!(matches!(handled, Handled::Queued { .. }), "{handled:?}");
    assert!(
        writer.deliveries(connection).await.is_empty(),
        "a write on an old revision is not delivered"
    );
    assert!(
        matches!(
            writer.outcome(&stale).await,
            Some(Outcome::Failed { ref code, .. }) if code == CONFLICT
        ),
        "{:?}",
        writer.outcome(&stale).await
    );
    assert_eq!(
        writer.current(connection, "Plan.md").await,
        Some(sha256(phone)),
        "the current revision stays the one of the vault"
    );
    assert_eq!(
        writer.changed(connection).await,
        events_before,
        "no notes.file.changed.v1 is published for the write"
    );
    assert!(!writer.changed(connection).await.contains(&sha256(mine)));

    writer.cleanup().await;
}

#[tokio::test]
async fn case_folded_create_collision() {
    let writer = Writer::new().await;
    let upper = b"# Plan\n\nWritten as Ideas/Plan.md.\n";
    let lower = b"# plan\n\nWritten as ideas/plan.md.\n";
    let connection = writer.vault(&[("Home.md", b"home\n")]).await;
    let upper_blob = writer.stage(connection, upper).await;
    let lower_blob = writer.stage(connection, lower).await;

    let first = command(connection, &create("Ideas/Plan.md", &upper_blob));
    let second = command(connection, &create("ideas/plan.md", &lower_blob));
    writer.send(&first).await;
    writer.send(&second).await;
    let mut delivered = Vec::new();
    for _ in 0..2 {
        let Some(delivery) = writer.deliveries(connection).await.into_iter().next() else {
            break;
        };
        writer.acknowledge(connection, &delivery).await;
        delivered.push(delivery.operation_id);
    }

    assert_eq!(
        delivered,
        vec![first.operation_id],
        "exactly one of the two creates is delivered"
    );
    assert_eq!(
        writer.outcome(&first).await,
        Some(Outcome::Succeeded {
            revision: Some(sha256(upper))
        })
    );
    assert!(
        matches!(
            writer.outcome(&second).await,
            Some(Outcome::Failed { ref code, .. }) if code == CONFLICT
        ),
        "{:?}",
        writer.outcome(&second).await
    );
    assert_eq!(
        writer.current(connection, "Ideas/Plan.md").await,
        Some(sha256(upper))
    );
    assert_eq!(writer.current(connection, "ideas/plan.md").await, None);

    writer.cleanup().await;
}
