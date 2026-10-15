//! The writes to one file wait in one queue (workspace change `add-notes-write-back`, spec
//! `notes-write-command`, task 4.3).

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions read the deliveries they wait for"
)]

mod support;

use ratatoskr_notes::writes::{Handled, Outcome};

use crate::support::channel::sha256;
use crate::support::writes::{Writer, command, update};

#[tokio::test]
async fn chained_edits_apply_in_order() {
    let writer = Writer::new().await;
    let base = b"# Plan\n\nThe first draft.\n";
    let first = b"# Plan\n\nThe second draft.\n";
    let second = b"# Plan\n\nThe third draft.\n";
    let connection = writer.vault(&[("Plan.md", base)]).await;
    let first_blob = writer.stage(connection, first).await;
    let second_blob = writer.stage(connection, second).await;

    // An editor that saves twice: B names the hash that A will produce.
    let a = command(connection, update("Plan.md", &sha256(base), first_blob));
    let b = command(connection, update("Plan.md", &sha256(first), second_blob));
    let a_handled = writer.send(&a).await;
    let b_handled = writer.send(&b).await;
    let before_ack = writer.deliveries(connection).await;
    if let Some(delivered) = before_ack.first() {
        writer.acknowledge(connection, delivered).await;
    }
    let after_ack = writer.deliveries(connection).await;
    if let Some(delivered) = after_ack.first() {
        writer.acknowledge(connection, delivered).await;
    }

    assert!(matches!(a_handled, Handled::Queued { .. }), "{a_handled:?}");
    assert!(matches!(b_handled, Handled::Queued { .. }), "{b_handled:?}");
    let operations: Vec<_> = before_ack.iter().map(|d| d.operation_id).collect();
    assert_eq!(
        operations,
        vec![a.operation_id],
        "only A is delivered before its acknowledgement"
    );
    let operations: Vec<_> = after_ack.iter().map(|d| d.operation_id).collect();
    assert_eq!(
        operations,
        vec![b.operation_id],
        "B is delivered after A's acknowledgement"
    );
    assert_eq!(
        writer.outcome(&a).await,
        Some(Outcome::Succeeded {
            revision: Some(sha256(first))
        })
    );
    assert_eq!(
        writer.outcome(&b).await,
        Some(Outcome::Succeeded {
            revision: Some(sha256(second))
        })
    );
    assert_eq!(
        writer.current(connection, "Plan.md").await,
        Some(sha256(second))
    );
    assert!(writer.deliveries(connection).await.is_empty());

    writer.cleanup().await;
}
