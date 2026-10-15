//! A rename and its link rewrites are one write group (workspace change `add-notes-write-back`,
//! spec `notes-link-rewrite`, requirements "The rename and its rewrites are one write group" and
//! "Revisions of a group become current as the channel acknowledges them", task 8.4).

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions read the answers they wait for"
)]

mod support;

use axum::http::StatusCode;
use ratatoskr_notes::sync_write::SyncWriter;
use ratatoskr_notes::writes::{Handled, Outcome};
use serde_json::{Value, json};

use crate::support::channel::{OWNER, sha256};
use crate::support::device_of;
use crate::support::writes::{Writer, command, update};

/// The code of a write that met a revision that its writer did not see.
const CONFLICT: &str = "notes.write.conflict";

/// The code of a write group that the channel applied only in part.
const PARTIALLY_APPLIED: &str = "notes.write.partially_applied";

/// The event of a raised conflict.
const RAISED: &str = "notes.conflict.raised.v1";

/// `rename` of `path` on `base` to `target`.
fn rename(path: &str, base: &str, target: &str) -> Value {
    json!({ "rename": { "path": path, "base_hash": base, "target_path": target } })
}

#[tokio::test]
async fn stale_linked_file_blocks_rename() {
    let writer = Writer::new().await;
    let a0 = b"# A\n";
    let a1 = b"# A\n\nEdited before the rename.\n";
    let index0 = b"See [[A]].\n";
    let index_phone = b"See [[A]], edited on the phone.\n";
    let connection = writer.vault(&[("A.md", a0), ("Index.md", index0)]).await;
    // An edit of A.md waits for its acknowledgement, so the rename waits behind it.
    let blob = writer.stage(connection, a1).await;
    let edit = command(connection, &update("A.md", &sha256(a0), &blob));
    assert!(matches!(writer.send(&edit).await, Handled::Queued { .. }));
    let renamed = command(connection, &rename("A.md", &sha256(a1), "B.md"));
    assert!(matches!(
        writer.send(&renamed).await,
        Handled::Queued { .. }
    ));

    // Index.md changes in the vault before the rename reaches the head of its queue.
    writer
        .vault_change(connection, "Index.md", index_phone)
        .await;
    let delivered = writer.deliveries(connection).await;
    assert_eq!(
        delivered.len(),
        1,
        "only the edit is delivered: {delivered:?}"
    );
    writer.acknowledge(connection, &delivered[0]).await;

    assert!(
        writer.deliveries(connection).await.is_empty(),
        "neither the rename nor any rewrite is delivered"
    );
    let outcome = writer.outcome(&renamed).await;
    assert!(
        matches!(outcome, Some(Outcome::Failed { ref code, .. }) if code == CONFLICT),
        "the rename fails with {CONFLICT}: {outcome:?}"
    );
    let conflict_id = match outcome {
        Some(Outcome::Failed { conflict_id, .. }) => conflict_id,
        _ => None,
    };
    assert!(conflict_id.is_some(), "the failure names its conflict");
    let raised = writer.events_of(connection, RAISED).await;
    assert_eq!(raised.len(), 1, "one conflict: {raised:?}");
    assert_eq!(
        raised[0]["path"], "Index.md",
        "the conflict names the stale file"
    );
    assert_eq!(
        raised[0]["conflict_id"],
        json!(conflict_id.map(|id| id.to_string()))
    );
    assert_eq!(writer.current(connection, "A.md").await, Some(sha256(a1)));
    assert_eq!(writer.current(connection, "B.md").await, None);
    assert_eq!(
        writer.current(connection, "Index.md").await,
        Some(sha256(index_phone)),
        "the change of the vault stays current"
    );

    writer.cleanup().await;
}

#[tokio::test]
async fn partial_application_fails_with_a_conflict() {
    let writer = Writer::new().await;
    let a = b"# A\n";
    let x0 = b"[[A]]\n";
    let x1 = b"[[B]]\n";
    let y0 = b"See [[A]] too.\n";
    let y_local = b"See [[A]] too, edited on the device.\n";
    let connection = writer
        .vault(&[("A.md", a), ("X.md", x0), ("Y.md", y0)])
        .await;
    let renamed = command(connection, &rename("A.md", &sha256(a), "B.md"));
    assert!(matches!(
        writer.send(&renamed).await,
        Handled::Queued { .. }
    ));

    let device = device_of(OWNER);
    let read = writer
        .device_operations(OWNER, device, connection, None)
        .await;
    assert_eq!(read.status, StatusCode::OK, "{:?}", read.body);
    let operations = read.body["operations"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(
        operations.len(),
        1,
        "the device reads the rename and not each rewrite: {:?}",
        read.body
    );
    let rewrites: Vec<(Value, Value)> = operations[0]["rewrites"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|rewrite| (rewrite["path"].clone(), rewrite["base_hash"].clone()))
        .collect();
    assert_eq!(
        rewrites,
        [
            (json!("X.md"), json!(sha256(x0))),
            (json!("Y.md"), json!(sha256(y0)))
        ],
        "the rename names the links that it rewrites: {:?}",
        operations[0]
    );

    // The device renamed the file and rewrote X.md; Y.md had a local edit.
    writer.keep(OWNER, connection, x1).await;
    let operation_id = operations[0]["operation_id"].as_str().expect("an id");
    let answer = writer
        .device_result(
            OWNER,
            device,
            connection,
            operation_id,
            json!({
                "outcome": "applied",
                "sha256": sha256(a),
                "rewrites": [
                    { "path": "X.md", "outcome": "applied", "sha256": sha256(x1) },
                    { "path": "Y.md", "outcome": "base_mismatch", "local_sha256": sha256(y_local) },
                ],
            }),
        )
        .await;
    assert_eq!(answer.status, StatusCode::OK, "{:?}", answer.body);

    assert_eq!(writer.current(connection, "B.md").await, Some(sha256(a)));
    assert_eq!(writer.current(connection, "A.md").await, None);
    assert_eq!(writer.current(connection, "X.md").await, Some(sha256(x1)));
    assert_eq!(
        writer.current(connection, "Y.md").await,
        Some(sha256(y0)),
        "the file that was not applied keeps its revision"
    );
    let outcome = writer.outcome(&renamed).await;
    assert!(
        matches!(outcome, Some(Outcome::Failed { ref code, .. }) if code == PARTIALLY_APPLIED),
        "the rename fails with {PARTIALLY_APPLIED}: {outcome:?}"
    );
    let conflict_id = match outcome {
        Some(Outcome::Failed { conflict_id, .. }) => conflict_id,
        _ => None,
    };
    let raised = writer.events_of(connection, RAISED).await;
    assert_eq!(raised.len(), 1, "one conflict: {raised:?}");
    assert_eq!(
        raised[0]["path"], "Y.md",
        "the conflict names the file that was not applied"
    );
    assert_eq!(
        raised[0]["conflict_id"],
        json!(conflict_id.map(|id| id.to_string()))
    );

    writer.cleanup().await;
}

#[tokio::test]
async fn sync_writes_the_rewrites_after_the_rename() {
    let writer = Writer::new().await;
    let a0 = b"# A\n\n## Part\n\nSee [[A#Part]].\n";
    let b1 = b"# A\n\n## Part\n\nSee [[B#Part]].\n";
    let index0 = b"[[A]]\n";
    let index1 = b"[[B]]\n";
    let connection = writer
        .sync_vault(&[("A.md", a0), ("Index.md", index0)])
        .await;
    let trees = tempfile::tempdir().expect("a trees root");
    let tree = trees.path().join(connection.to_string());
    std::fs::create_dir_all(&tree).expect("the working tree");
    std::fs::write(tree.join("A.md"), a0).expect("A.md is in the tree");
    std::fs::write(tree.join("Index.md"), index0).expect("Index.md is in the tree");
    let renamed = command(connection, &rename("A.md", &sha256(a0), "B.md"));
    assert!(matches!(
        writer.send(&renamed).await,
        Handled::Queued { .. }
    ));
    assert_eq!(
        writer.deliveries(connection).await.len(),
        2,
        "the rename and the rewrite of Index.md are delivered together"
    );

    let sync = SyncWriter::new(writer.recorder(), trees.path());
    sync.deliver(connection).await.expect("the delivery runs");

    assert!(!tree.join("A.md").exists(), "the file moved");
    assert_eq!(
        std::fs::read(tree.join("B.md")).expect("B.md"),
        b1,
        "the renamed note gets its own rewritten link"
    );
    assert_eq!(
        std::fs::read(tree.join("Index.md")).expect("Index.md"),
        index1,
        "the linking note names the new path"
    );

    // The next pass finds the rename in the tree, and the watcher reports the rewrite back.
    sync.deliver(connection).await.expect("the delivery runs");
    writer.tree_change(connection, "Index.md", index1).await;
    assert_eq!(
        writer.outcome(&renamed).await,
        Some(Outcome::Succeeded {
            revision: Some(sha256(b1))
        })
    );
    assert_eq!(writer.current(connection, "B.md").await, Some(sha256(b1)));
    assert_eq!(
        writer.current(connection, "Index.md").await,
        Some(sha256(index1))
    );

    writer.cleanup().await;
}

#[tokio::test]
async fn sync_writes_no_rewrite_when_the_rename_fails() {
    let writer = Writer::new().await;
    let a0 = b"# A\n";
    let a_phone = b"# A\n\nChanged on the phone after the queue check.\n";
    let index0 = b"[[A]]\n";
    let connection = writer
        .sync_vault(&[("A.md", a0), ("Index.md", index0)])
        .await;
    let trees = tempfile::tempdir().expect("a trees root");
    let tree = trees.path().join(connection.to_string());
    std::fs::create_dir_all(&tree).expect("the working tree");
    std::fs::write(tree.join("Index.md"), index0).expect("Index.md is in the tree");
    let renamed = command(connection, &rename("A.md", &sha256(a0), "B.md"));
    assert!(matches!(
        writer.send(&renamed).await,
        Handled::Queued { .. }
    ));
    // The Headless client changes A.md after the queue check.
    std::fs::write(tree.join("A.md"), a_phone).expect("the client writes A.md");

    SyncWriter::new(writer.recorder(), trees.path())
        .deliver(connection)
        .await
        .expect("the delivery runs");

    assert_eq!(
        std::fs::read(tree.join("Index.md")).expect("Index.md"),
        index0,
        "no link names B.md while A.md did not move"
    );
    let outcome = writer.outcome(&renamed).await;
    assert!(
        matches!(&outcome, Some(Outcome::Failed { code, .. }) if code == CONFLICT),
        "the rename fails with a conflict: {outcome:?}"
    );

    writer.cleanup().await;
}

#[tokio::test]
async fn a_retried_result_settles_the_rest_of_the_group() {
    let writer = Writer::new().await;
    let a = b"# A\n";
    let x0 = b"[[A]]\n";
    let x1 = b"[[B]]\n";
    let y0 = b"See [[A]] too.\n";
    let y1 = b"See [[B]] too.\n";
    let connection = writer
        .vault(&[("A.md", a), ("X.md", x0), ("Y.md", y0)])
        .await;
    let renamed = command(connection, &rename("A.md", &sha256(a), "B.md"));
    assert!(matches!(
        writer.send(&renamed).await,
        Handled::Queued { .. }
    ));
    let device = device_of(OWNER);
    let read = writer
        .device_operations(OWNER, device, connection, None)
        .await;
    let operation_id = read.body["operations"][0]["operation_id"]
        .as_str()
        .expect("the rename is in the queue")
        .to_owned();

    // A first result recorded the rename and X.md, and then the service failed before Y.md.
    let deliveries = writer.deliveries(connection).await;
    for delivery in &deliveries {
        let digest = match delivery.path.as_str() {
            "A.md" => sha256(a),
            "X.md" => sha256(x1),
            _ => continue,
        };
        assert!(
            writer
                .writes
                .acknowledge(connection, delivery.write_id, Some(&digest))
                .await
                .expect("the acknowledgement is recorded"),
            "{delivery:?}"
        );
    }

    // The device sends the result again.
    let answer = writer
        .device_result(
            OWNER,
            device,
            connection,
            &operation_id,
            json!({
                "outcome": "applied",
                "sha256": sha256(a),
                "rewrites": [
                    { "path": "X.md", "outcome": "applied", "sha256": sha256(x1) },
                    { "path": "Y.md", "outcome": "applied", "sha256": sha256(y1) },
                ],
            }),
        )
        .await;
    assert_eq!(answer.status, StatusCode::OK, "{:?}", answer.body);

    assert_eq!(
        writer.current(connection, "Y.md").await,
        Some(sha256(y1)),
        "the retry settles the rewrite that was still delivering"
    );
    assert_eq!(
        writer.outcome(&renamed).await,
        Some(Outcome::Succeeded {
            revision: Some(sha256(a))
        })
    );
    assert_eq!(answer.body["state"], "succeeded", "{:?}", answer.body);

    writer.cleanup().await;
}

#[tokio::test]
async fn a_rename_that_the_device_fails_ends_the_group_with_its_code() {
    let writer = Writer::new().await;
    let a = b"# A\n";
    let x0 = b"[[A]]\n";
    let connection = writer.vault(&[("A.md", a), ("X.md", x0)]).await;
    let renamed = command(connection, &rename("A.md", &sha256(a), "B.md"));
    assert!(matches!(
        writer.send(&renamed).await,
        Handled::Queued { .. }
    ));
    let device = device_of(OWNER);
    let read = writer
        .device_operations(OWNER, device, connection, None)
        .await;
    let operation_id = read.body["operations"][0]["operation_id"]
        .as_str()
        .expect("the rename is in the queue")
        .to_owned();

    let answer = writer
        .device_result(
            OWNER,
            device,
            connection,
            &operation_id,
            json!({ "outcome": "failed", "code": "notes.write.unsupported" }),
        )
        .await;
    assert_eq!(answer.status, StatusCode::OK, "{:?}", answer.body);

    assert_eq!(
        writer.outcome(&renamed).await,
        Some(Outcome::Failed {
            code: "notes.write.unsupported".to_owned(),
            conflict_id: None
        })
    );
    let terminal: Vec<Value> = writer
        .reports(renamed.operation_id)
        .await
        .into_iter()
        .filter(|report| report["status"] == "failed")
        .map(|report| report["error"]["code"].clone())
        .collect();
    assert_eq!(
        terminal,
        [json!("notes.write.unsupported")],
        "one terminal report with the code of the device"
    );
    assert_eq!(
        writer.events_of(connection, RAISED).await,
        Vec::<Value>::new(),
        "a failure of the device is no conflict"
    );
    assert_eq!(writer.current(connection, "X.md").await, Some(sha256(x0)));

    writer.cleanup().await;
}
