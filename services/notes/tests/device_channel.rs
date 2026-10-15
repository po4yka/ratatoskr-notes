//! The device channel routes (workspace spec `notes-device-channels`, group 8): change batches with
//! the device sequence, the blob receipt of the shared transfer contract, and the manifest that
//! reconciles a vault after a gap.

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions read the JSON answers they build"
)]

mod support;

use axum::http::StatusCode;
use serde_json::{Value, json};

use crate::support::channel::{
    Channel, OTHER_OWNER, OWNER, SECOND_DEVICE, SHORT_DEADLINE, SYNC_HINT, change, entry, sha256,
};
use crate::support::{device_of, terminal_report};

#[tokio::test]
async fn duplicate_batch_is_idempotent() {
    let channel = Channel::new().await;
    let connection = channel
        .plugin_connection(OWNER, "vault-key-duplicate")
        .await;
    for bytes in [&b"alpha v1\n"[..], b"beta v1\n", b"alpha v2\n"] {
        channel.hold(OWNER, connection, bytes).await;
    }
    let changes = vec![
        change(1, "created", "Alpha.md", b"alpha v1\n"),
        change(2, "created", "Beta.md", b"beta v1\n"),
        change(3, "updated", "Alpha.md", b"alpha v2\n"),
    ];

    let first = channel.batch(OWNER, connection, changes.clone()).await;
    let second = channel.batch(OWNER, connection, changes).await;

    assert_eq!(first.status, StatusCode::OK, "{:?}", first.body);
    assert_eq!(second.status, StatusCode::OK, "{:?}", second.body);
    assert_eq!(first.body["committed_sequence"], 3);
    assert_eq!(
        second.body["committed_sequence"], 3,
        "both answers name the same committed sequence"
    );
    assert_eq!(
        channel.revisions(connection).await,
        3,
        "one revision per change"
    );
    assert_eq!(channel.events(connection).await, 3, "one event per change");

    channel.cleanup().await;
}

#[tokio::test]
async fn sequence_gap_commits_nothing() {
    let channel = Channel::new().await;
    let connection = channel.plugin_connection(OWNER, "vault-key-gap").await;
    let mut first = Vec::new();
    for sequence in 1..=10_u64 {
        let bytes = format!("note {sequence}\n");
        channel.hold(OWNER, connection, bytes.as_bytes()).await;
        first.push(change(
            sequence,
            "created",
            &format!("n{sequence}.md"),
            bytes.as_bytes(),
        ));
    }
    let committed = channel.batch(OWNER, connection, first).await;
    channel.hold(OWNER, connection, b"late\n").await;

    let gap = channel
        .batch(
            OWNER,
            connection,
            vec![
                change(12, "created", "late.md", b"late\n"),
                change(13, "updated", "n1.md", b"late\n"),
            ],
        )
        .await;

    assert_eq!(committed.status, StatusCode::OK, "{:?}", committed.body);
    assert_eq!(gap.status, StatusCode::CONFLICT, "{:?}", gap.body);
    assert_eq!(gap.body["code"], "notes.channel.sequence_gap");
    assert_eq!(
        gap.body["committed_sequence"], 10,
        "the answer names the committed sequence"
    );
    assert_eq!(
        channel.events(connection).await,
        10,
        "nothing of the batch is committed"
    );

    channel.cleanup().await;
}

#[tokio::test]
async fn oversized_batch_commits_nothing() {
    let channel = Channel::new().await;
    let connection = channel
        .plugin_connection(OWNER, "vault-key-oversized")
        .await;
    channel.hold(OWNER, connection, b"one body\n").await;
    let changes: Vec<Value> = (1..=257_u64)
        .map(|sequence| {
            change(
                sequence,
                "created",
                &format!("f{sequence}.md"),
                b"one body\n",
            )
        })
        .collect();

    let answer = channel.batch(OWNER, connection, changes).await;

    assert_eq!(
        answer.status,
        StatusCode::PAYLOAD_TOO_LARGE,
        "{:?}",
        answer.body
    );
    assert_eq!(answer.body["code"], "notes.channel.batch_too_large");
    assert_eq!(channel.events(connection).await, 0, "nothing is committed");
    assert_eq!(channel.revisions(connection).await, 0, "no revision");

    channel.cleanup().await;
}

#[tokio::test]
async fn foreign_connection_changes_are_not_found() {
    let channel = Channel::new().await;
    let foreign = channel
        .plugin_connection(OTHER_OWNER, "vault-key-foreign")
        .await;
    channel.hold(OTHER_OWNER, foreign, b"intruder\n").await;

    let answer = channel
        .batch(
            OWNER,
            foreign,
            vec![change(1, "created", "Intruder.md", b"intruder\n")],
        )
        .await;

    assert_eq!(answer.status, StatusCode::NOT_FOUND, "{:?}", answer.body);
    assert_eq!(answer.body["code"], "notes.connection.not_found");
    assert_eq!(
        channel.events(foreign).await,
        0,
        "no change is recorded for the connection of the other owner"
    );

    channel.cleanup().await;
}

#[tokio::test]
async fn export_agent_on_plugin_connection_is_channel_mismatch() {
    let channel = Channel::new().await;
    let connection = channel.plugin_connection(OWNER, "vault-key-kind").await;
    channel.hold(OWNER, connection, b"from the agent\n").await;

    let answer = channel
        .batch_from(
            OWNER,
            device_of(OWNER),
            "export_agent",
            connection,
            vec![change(1, "created", "Agent.md", b"from the agent\n")],
        )
        .await;

    assert_eq!(answer.status, StatusCode::CONFLICT, "{:?}", answer.body);
    assert_eq!(answer.body["code"], "notes.channel_mismatch");
    assert_eq!(channel.events(connection).await, 0, "no change is recorded");

    channel.cleanup().await;
}

#[tokio::test]
async fn second_device_for_one_connection_is_refused() {
    let channel = Channel::new().await;
    let connection = channel.plugin_connection(OWNER, "vault-key-copied").await;
    channel.hold(OWNER, connection, b"from the copy\n").await;

    let connect = channel
        .connect_from(OWNER, SECOND_DEVICE, "vault-key-copied")
        .await;
    let batch = channel
        .batch_from(
            OWNER,
            SECOND_DEVICE,
            "obsidian_plugin",
            connection,
            vec![change(1, "created", "Copy.md", b"from the copy\n")],
        )
        .await;

    for (step, answer) in [("connect", &connect), ("batch", &batch)] {
        assert_eq!(
            answer.status,
            StatusCode::CONFLICT,
            "{step}: {:?}",
            answer.body
        );
        assert_eq!(answer.body["code"], "notes.channel_mismatch", "{step}");
        assert!(
            answer.body["message"]
                .as_str()
                .is_some_and(|message| message.contains(SYNC_HINT)),
            "{step}: the message points the owner to Obsidian Sync: {:?}",
            answer.body
        );
    }
    assert_eq!(
        channel.device(connection).await,
        Some(device_of(OWNER)),
        "the connection keeps its first device"
    );
    assert_eq!(channel.events(connection).await, 0, "no change is recorded");

    channel.cleanup().await;
}

#[tokio::test]
async fn upload_with_wrong_digest_is_refused() {
    let channel = Channel::new().await;
    let connection = channel.plugin_connection(OWNER, "vault-key-upload").await;
    let declared_bytes = b"the declared bytes\n";
    let uploaded_bytes = b"the uploaded bytes\n";
    let declared = sha256(declared_bytes);

    let [opened, chunk, finalized] = channel.upload(connection, &declared, uploaded_bytes).await;
    let batch = channel
        .batch(
            OWNER,
            connection,
            vec![change(1, "created", "Declared.md", declared_bytes)],
        )
        .await;

    assert_eq!(opened.status, StatusCode::CREATED, "{:?}", opened.body);
    assert_eq!(chunk.status, StatusCode::OK, "{:?}", chunk.body);
    assert_eq!(finalized.status, StatusCode::OK, "{:?}", finalized.body);
    assert_eq!(
        finalized.body["outcome"], "digest_mismatch",
        "the transfer contract answers the mismatch as its outcome"
    );
    assert_eq!(finalized.body["declared_sha256_hex"], declared.as_str());
    assert_eq!(
        finalized.body["computed_sha256_hex"],
        sha256(uploaded_bytes).as_str()
    );
    for digest in [declared.clone(), sha256(uploaded_bytes)] {
        let blob = channel.blobs.path_of(&digest).expect("a blob path");
        assert!(!blob.exists(), "no blob is registered for {digest}");
    }
    assert_eq!(batch.status, StatusCode::CONFLICT, "{:?}", batch.body);
    assert_eq!(batch.body["code"], "notes.channel.bytes_missing");

    channel.cleanup().await;
}

#[tokio::test]
async fn manifest_reconciles_offline_changes() {
    let channel = Channel::new().await;
    let connection = channel.plugin_connection(OWNER, "vault-key-manifest").await;
    let original = |n: u32| format!("file {n:02} as it was\n");
    let mut first = Vec::new();
    for n in 1..=20_u32 {
        channel
            .hold(OWNER, connection, original(n).as_bytes())
            .await;
        first.push(change(
            u64::from(n),
            "created",
            &format!("f{n:02}.md"),
            original(n).as_bytes(),
        ));
    }
    let committed = channel.batch(OWNER, connection, first).await;
    // While the plugin was disabled: f01 is edited, f02 is deleted, f03 is renamed and new.md is
    // created. The new bytes are only on the device.
    let edited = b"file 01 edited offline\n";
    let created = b"a note made offline\n";
    let mut entries: Vec<Value> = (4..=20_u32)
        .map(|n| entry(&format!("f{n:02}.md"), original(n).as_bytes()))
        .collect();
    entries.push(entry("f01.md", edited));
    entries.push(entry("Moved/f03.md", original(3).as_bytes()));
    entries.push(entry("new.md", created));
    let manifest = json!({ "entries": entries });

    let asked = channel
        .device_call(connection, "manifest", manifest.clone())
        .await;
    let uploads = [
        channel.upload(connection, &sha256(edited), edited).await,
        channel.upload(connection, &sha256(created), created).await,
    ];
    let applied = channel.device_call(connection, "manifest", manifest).await;

    assert_eq!(committed.status, StatusCode::OK, "{:?}", committed.body);
    assert_eq!(asked.status, StatusCode::OK, "{:?}", asked.body);
    let mut needed = vec![sha256(edited), sha256(created)];
    needed.sort();
    assert_eq!(
        asked.body["needed_sha256"],
        json!(needed),
        "the first answer names the bytes that the service needs"
    );
    assert_eq!(asked.body["committed_sequence"], 20);
    for [_, _, finalized] in &uploads {
        assert_eq!(finalized.body["outcome"], "stored", "{:?}", finalized.body);
    }
    assert_eq!(applied.status, StatusCode::OK, "{:?}", applied.body);
    assert_eq!(applied.body["needed_sha256"], json!([]));
    let changes = channel.changes(connection).await;
    let offline: Vec<(String, String, Value)> = changes
        .iter()
        .skip(20)
        .map(|change| {
            (
                change["change_kind"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned(),
                change["path"].as_str().unwrap_or_default().to_owned(),
                change["previous_path"].clone(),
            )
        })
        .collect();
    let mut kinds: Vec<&str> = offline.iter().map(|(kind, _, _)| kind.as_str()).collect();
    kinds.sort_unstable();
    assert_eq!(
        kinds,
        ["created", "deleted", "renamed", "updated"],
        "exactly one change of each kind: {offline:#?}"
    );
    assert!(
        offline.contains(&("updated".to_owned(), "f01.md".to_owned(), Value::Null)),
        "{offline:#?}"
    );
    assert!(
        offline.contains(&("deleted".to_owned(), "f02.md".to_owned(), Value::Null)),
        "{offline:#?}"
    );
    assert!(
        offline.contains(&(
            "renamed".to_owned(),
            "Moved/f03.md".to_owned(),
            json!("f03.md")
        )),
        "{offline:#?}"
    );
    assert!(
        offline.contains(&("created".to_owned(), "new.md".to_owned(), Value::Null)),
        "{offline:#?}"
    );

    channel.cleanup().await;
}

#[tokio::test]
async fn first_manifest_completes_the_device_connect() {
    let channel = Channel::new().await;
    let connection = channel
        .plugin_connection(OWNER, "vault-key-first-manifest")
        .await;
    channel.hold(OWNER, connection, b"the first note\n").await;

    let applied = channel
        .device_call(
            connection,
            "manifest",
            json!({ "entries": [entry("First.md", b"the first note\n")] }),
        )
        .await;
    let report = terminal_report(&channel.fixture, connection).await;

    assert_eq!(applied.status, StatusCode::OK, "{:?}", applied.body);
    assert_eq!(applied.body["needed_sha256"], json!([]));
    assert_eq!(
        report["status"], "succeeded",
        "the first manifest ends the connect operation: {report:?}"
    );
    assert_eq!(channel.connection_state(connection).await, "active");
    assert_eq!(channel.connected_events(connection).await, 1);

    channel.cleanup().await;
}

#[tokio::test]
async fn device_connect_without_manifest_fails_in_its_bound() {
    let channel = Channel::with(|state| state.with_manifest_deadline(SHORT_DEADLINE)).await;
    let connection = channel
        .plugin_connection(OWNER, "vault-key-no-manifest")
        .await;

    let report = terminal_report(&channel.fixture, connection).await;

    assert_eq!(report["status"], "failed", "{report:?}");
    assert_eq!(report["error"]["code"], "notes.channel.manifest_missing");
    assert_eq!(channel.connection_state(connection).await, "failed");
    assert_eq!(channel.connected_events(connection).await, 0);

    channel.cleanup().await;
}

#[tokio::test]
async fn manifest_deadline_holds_after_a_restart() {
    let channel = Channel::new().await;
    let connection = channel.plugin_connection(OWNER, "vault-key-restart").await;

    // The next start of the service finds the connection that still waits for its manifest.
    let restarted = channel.state().with_manifest_deadline(SHORT_DEADLINE);
    restarted.resume_device_connects().await;
    let report = terminal_report(&channel.fixture, connection).await;

    assert_eq!(report["status"], "failed", "{report:?}");
    assert_eq!(report["error"]["code"], "notes.channel.manifest_missing");
    assert_eq!(channel.connection_state(connection).await, "failed");

    channel.cleanup().await;
}

#[tokio::test]
async fn empty_file_needs_no_upload() {
    let channel = Channel::new().await;
    let connection = channel.plugin_connection(OWNER, "vault-key-empty").await;

    // The transfer contract refuses a declaration of 0 bytes, so a device cannot upload an empty
    // file. Notes takes the SHA-256 of no bytes without an upload.
    let opened = channel
        .device_call(
            connection,
            "blobs",
            json!({
                "declared_size_bytes": 0,
                "media_type": "text/markdown",
                "digest": { "algorithm": "sha256", "hex": sha256(b"") },
                "chunk_size_bytes": 65_536,
            }),
        )
        .await;
    let batch = channel
        .batch(
            OWNER,
            connection,
            vec![change(1, "created", "Empty.md", b"")],
        )
        .await;
    let manifest = channel
        .device_call(
            connection,
            "manifest",
            json!({ "entries": [entry("Empty.md", b""), entry("Blank.canvas", b"")] }),
        )
        .await;

    assert_eq!(opened.status, StatusCode::BAD_REQUEST, "{:?}", opened.body);
    assert_eq!(batch.status, StatusCode::OK, "{:?}", batch.body);
    assert_eq!(batch.body["committed_sequence"], 1);
    assert_eq!(manifest.status, StatusCode::OK, "{:?}", manifest.body);
    assert_eq!(
        manifest.body["needed_sha256"],
        json!([]),
        "no bytes are needed for an empty file"
    );
    let changes = channel.changes(connection).await;
    let paths: Vec<&str> = changes
        .iter()
        .map(|change| change["path"].as_str().unwrap_or_default())
        .collect();
    assert_eq!(paths, ["Empty.md", "Blank.canvas"]);
    assert_eq!(changes[0]["blob"]["length_bytes"], 0);

    channel.cleanup().await;
}

#[tokio::test]
async fn foreign_bytes_are_missing_for_another_connection() {
    let channel = Channel::new().await;
    let secret = b"the private note of the other owner\n";
    let theirs = channel
        .plugin_connection(OTHER_OWNER, "vault-key-theirs")
        .await;
    let mine = channel.plugin_connection(OWNER, "vault-key-mine").await;
    let [_, _, uploaded] = channel
        .upload_as(OTHER_OWNER, theirs, &sha256(secret), secret)
        .await;
    let committed = channel
        .batch(
            OTHER_OWNER,
            theirs,
            vec![change(1, "created", "Secret.md", secret)],
        )
        .await;

    // Owner A knows the hash of the file of owner B, but not its bytes.
    let named = channel
        .batch(OWNER, mine, vec![change(1, "created", "Stolen.md", secret)])
        .await;
    let manifest = channel
        .device_call(
            mine,
            "manifest",
            json!({ "entries": [entry("Stolen.md", secret)] }),
        )
        .await;
    let read = channel.read_file(OWNER, mine, "Stolen.md").await;
    // Owner B may name its own hash again.
    let again = channel
        .batch(
            OTHER_OWNER,
            theirs,
            vec![change(2, "created", "Copy.md", secret)],
        )
        .await;
    // Owner A uploads the same bytes itself, and may then name them.
    let [_, _, own_upload] = channel.upload(mine, &sha256(secret), secret).await;
    let after_upload = channel
        .batch(OWNER, mine, vec![change(1, "created", "Mine.md", secret)])
        .await;

    assert_eq!(uploaded.body["outcome"], "stored", "{:?}", uploaded.body);
    assert_eq!(committed.status, StatusCode::OK, "{:?}", committed.body);
    assert_eq!(named.status, StatusCode::CONFLICT, "{:?}", named.body);
    assert_eq!(named.body["code"], "notes.channel.bytes_missing");
    assert_eq!(named.body["missing_sha256"], json!([sha256(secret)]));
    assert_eq!(manifest.status, StatusCode::OK, "{:?}", manifest.body);
    assert_eq!(
        manifest.body["needed_sha256"],
        json!([sha256(secret)]),
        "the manifest asks for the bytes as well"
    );
    assert_eq!(
        read.0,
        StatusCode::NOT_FOUND,
        "owner A reads no bytes of owner B"
    );
    assert_ne!(read.1, secret.to_vec());
    assert_eq!(again.status, StatusCode::OK, "{:?}", again.body);
    assert_eq!(
        own_upload.body["outcome"], "stored",
        "{:?}",
        own_upload.body
    );
    assert_eq!(
        after_upload.status,
        StatusCode::OK,
        "{:?}",
        after_upload.body
    );
    let paths: Vec<String> = channel
        .changes(mine)
        .await
        .iter()
        .map(|change| change["path"].as_str().unwrap_or_default().to_owned())
        .collect();
    assert_eq!(
        paths,
        ["Mine.md"],
        "only the bytes that owner A uploaded are recorded"
    );

    channel.cleanup().await;
}

#[tokio::test]
async fn bound_after_a_committed_manifest_changes_nothing() {
    // The end of the manifest bound runs at once after the first manifest is applied and before
    // its answer: the one step where it could still find the connection `connecting`.
    let channel = Channel::with(|state| {
        let bound = state.clone();
        state.with_after_manifest(move |connection_id| {
            let bound = bound.clone();
            async move { bound.expire_device_connect(connection_id).await }
        })
    })
    .await;
    let connection = channel
        .plugin_connection(OWNER, "vault-key-interleaved")
        .await;
    channel
        .hold(OWNER, connection, b"committed before the bound\n")
        .await;

    let applied = channel
        .device_call(
            connection,
            "manifest",
            json!({ "entries": [entry("Note.md", b"committed before the bound\n")] }),
        )
        .await;
    let report = terminal_report(&channel.fixture, connection).await;

    assert_eq!(applied.status, StatusCode::OK, "{:?}", applied.body);
    assert_eq!(
        report["status"], "succeeded",
        "a bound that ends after the manifest committed does not fail the connect: {report:?}"
    );
    assert_eq!(channel.connection_state(connection).await, "active");
    assert_eq!(channel.connected_events(connection).await, 1);

    channel.cleanup().await;
}

/// One `deleted` change.
fn deleted(sequence: u64, path: &str) -> Vec<Value> {
    vec![json!({ "sequence": sequence, "kind": "deleted", "path": path })]
}

#[tokio::test]
async fn removed_connection_is_gone_and_a_failed_one_is_not_found() {
    let channel = Channel::new().await;
    let removed = channel.plugin_connection(OWNER, "vault-key-removed").await;
    let disconnect = crate::support::send(
        &channel.router,
        axum::http::Method::DELETE,
        &format!("/v1/notes/connections/{removed}"),
        OWNER,
        None,
    )
    .await;
    // The manifest bound fails a connect without a manifest.
    let bounded = Channel::with(|state| state.with_manifest_deadline(SHORT_DEADLINE)).await;
    let failed = bounded.plugin_connection(OWNER, "vault-key-failed").await;
    let failed_report = terminal_report(&bounded.fixture, failed).await;

    let gone_batch = channel.batch(OWNER, removed, deleted(1, "Old.md")).await;
    let gone_manifest = channel
        .device_call(removed, "manifest", json!({ "entries": [] }))
        .await;
    let gone_upload = channel
        .device_call(
            removed,
            "blobs",
            json!({
                "declared_size_bytes": 1,
                "media_type": "text/markdown",
                "digest": { "algorithm": "sha256", "hex": sha256(b"x") },
                "chunk_size_bytes": 65_536,
            }),
        )
        .await;
    let foreign_batch = channel
        .batch(OTHER_OWNER, removed, deleted(1, "Old.md"))
        .await;
    let failed_batch = bounded.batch(OWNER, failed, deleted(1, "Old.md")).await;

    assert_eq!(disconnect.status, StatusCode::NO_CONTENT);
    assert_eq!(failed_report["status"], "failed", "{failed_report:?}");
    for (step, answer) in [
        ("batch", &gone_batch),
        ("manifest", &gone_manifest),
        ("upload", &gone_upload),
    ] {
        assert_eq!(answer.status, StatusCode::GONE, "{step}: {:?}", answer.body);
        assert_eq!(answer.body["code"], "notes.connection.removed", "{step}");
    }
    assert_eq!(
        foreign_batch.status,
        StatusCode::NOT_FOUND,
        "another owner learns nothing: {:?}",
        foreign_batch.body
    );
    assert_eq!(foreign_batch.body["code"], "notes.connection.not_found");
    assert_eq!(
        failed_batch.status,
        StatusCode::NOT_FOUND,
        "{:?}",
        failed_batch.body
    );
    assert_eq!(failed_batch.body["code"], "notes.connection.not_found");

    channel.cleanup().await;
    bounded.cleanup().await;
}
