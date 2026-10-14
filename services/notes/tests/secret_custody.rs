//! Secret custody (workspace spec `notes-vault-connections`, requirements "Obsidian secrets never
//! leave the notes service" and "Secrets are kept as ciphertext and a failed login has a stable
//! code", workspace design D4).
//!
//! Every secret is an obvious marker. The search for a marker covers every column of every table
//! in the `notes` schema, the outbox and the events in it, the log lines of the service, and the
//! output of the units. The ciphertext column is the one place that holds a form of a secret: the
//! encrypted form. The search reads that column too, and asserts that the plaintext marker is not
//! in it. `stored_secret_is_ciphertext_without_mfa` then checks the ciphertext itself.

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions read the rows and JSON answers they build"
)]

mod support;

use axum::Router;
use axum::http::{Method, StatusCode};
use chacha20poly1305::aead::{Aead as _, Payload};
use chacha20poly1305::{KeyInit as _, XChaCha20Poly1305, XNonce};
use ratatoskr_notes::api::{ApiState, domain_router};
use ratatoskr_notes_persistence::test_support::TestDatabase;
use uuid::Uuid;

use crate::support::logs::CapturedLogs;
use crate::support::systemd::{FakeSystemd, KEY_BYTES};
use crate::support::{SyncSecrets, connect_sync, connection_id, send};

const OWNER: Uuid = Uuid::from_u128(0x0192_0000_0000_7000_8000_0000_0000_00c1);
const REMOTE_VAULT_ID: &str = "remote-vault-0042";

const ACCOUNT_PASSWORD: &str = "MARKER-ACCOUNT-PW-5f1c0a";
const MFA_CODE: &str = "MARKER-MFA-4e7a21";
const E2E_PASSWORD: &str = "MARKER-E2E-PW-9b2d77";

/// The three secrets of a connect request. None of them may appear outside the ciphertext.
const MARKERS: [&str; 3] = [ACCOUNT_PASSWORD, MFA_CODE, E2E_PASSWORD];

const SECRETS: SyncSecrets<'static> = SyncSecrets {
    password: ACCOUNT_PASSWORD,
    mfa_code: MFA_CODE,
    e2e_password: E2E_PASSWORD,
};

fn router(fixture: &TestDatabase, systemd: &FakeSystemd) -> Router {
    domain_router(
        ApiState::new(fixture.database.clone(), systemd.clone()).with_key_file(systemd.key_file()),
    )
}

/// Every `table.column` of the `notes` schema that holds `marker`. A `bytea` column is searched
/// for the bytes of the marker, every other column for its text.
async fn columns_holding(fixture: &TestDatabase, marker: &str) -> Vec<String> {
    let columns: Vec<(String, String, String)> = sqlx::query_as(
        "select table_name::text, column_name::text, data_type::text
         from information_schema.columns
         where table_schema = 'notes'
         order by table_name, column_name",
    )
    .fetch_all(fixture.pool())
    .await
    .expect("the columns of the notes schema can be listed");
    assert!(
        columns
            .iter()
            .any(|(table, column, _)| table == "connection_secrets" && column == "ciphertext"),
        "the search covers the ciphertext column: {columns:?}"
    );
    assert!(
        columns
            .iter()
            .any(|(table, column, _)| table == "outbox" && column == "payload"),
        "the search covers the outbox: {columns:?}"
    );

    let mut holding = Vec::new();
    for (table, column, data_type) in columns {
        let hits: i64 = if data_type == "bytea" {
            sqlx::query_scalar(&format!(
                r#"select count(*) from notes."{table}" where position($1::bytea in "{column}") > 0"#
            ))
            .bind(marker.as_bytes())
            .fetch_one(fixture.pool())
            .await
        } else {
            sqlx::query_scalar(&format!(
                r#"select count(*) from notes."{table}" where strpos("{column}"::text, $1) > 0"#
            ))
            .bind(marker)
            .fetch_one(fixture.pool())
            .await
        }
        .expect("a column can be searched");
        if hits > 0 {
            holding.push(format!("{table}.{column}"));
        }
    }
    holding
}

async fn assert_no_marker_in_tables(fixture: &TestDatabase) {
    for marker in MARKERS {
        let holding = columns_holding(fixture, marker).await;
        assert!(
            holding.is_empty(),
            "the marker {marker} is stored in {holding:?}"
        );
    }
}

fn assert_no_marker_in(place: &str, text: &str) {
    for marker in MARKERS {
        assert!(!text.contains(marker), "{place} holds the marker {marker}");
    }
}

#[tokio::test]
async fn marker_secrets_appear_nowhere_in_notes() {
    let (logs, _capture) = CapturedLogs::install();
    let fixture = TestDatabase::create().await.expect("a test database");
    let systemd = FakeSystemd::new();
    let router = router(&fixture, &systemd);

    let connected = connect_sync(&router, OWNER, REMOTE_VAULT_ID, SECRETS).await;
    let id = connection_id(&connected);
    assert_no_marker_in("the connect answer", &connected.body.to_string());
    assert_no_marker_in_tables(&fixture).await;

    // A disconnect writes the event that the relay publishes.
    let removed = send(
        &router,
        Method::DELETE,
        &format!("/v1/notes/connections/{id}"),
        OWNER,
        None,
    )
    .await;
    assert_eq!(removed.status, StatusCode::NO_CONTENT, "{:?}", removed.body);
    let events: Vec<serde_json::Value> = sqlx::query_scalar("select payload from notes.outbox")
        .fetch_all(fixture.pool())
        .await
        .expect("the outbox can be read");
    assert_eq!(events.len(), 1, "the disconnect event is in the outbox");
    for event in &events {
        assert_no_marker_in("an event", &event.to_string());
    }
    assert_no_marker_in_tables(&fixture).await;

    let journal = logs.text();
    assert!(
        journal.contains("a connection was created"),
        "the log capture works: {journal}"
    );
    assert_no_marker_in("the journal of the service", &journal);
    assert_no_marker_in("the journal of the units", &systemd.journal());

    fixture
        .cleanup()
        .await
        .expect("the test database is dropped");
}

#[tokio::test]
async fn wrong_e2e_password_fails_with_its_code() {
    let fixture = TestDatabase::create().await.expect("a test database");
    let systemd = FakeSystemd::new().refuse_e2e_password();
    let router = router(&fixture, &systemd);

    let answer = connect_sync(&router, OWNER, REMOTE_VAULT_ID, SECRETS).await;
    assert_eq!(
        answer.status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{:?}",
        answer.body
    );
    assert_eq!(
        answer.body["code"], "notes.sync.e2e_password_wrong",
        "{:?}",
        answer.body
    );
    assert_no_marker_in("the error envelope", &answer.body.to_string());

    let active: i64 = sqlx::query_scalar(
        "select count(*) from notes.connections where owner_id = $1 and state = 'active'",
    )
    .bind(OWNER)
    .fetch_one(fixture.pool())
    .await
    .expect("the connections can be counted");
    assert_eq!(active, 0, "no connection becomes active");
    let secrets: i64 = sqlx::query_scalar("select count(*) from notes.connection_secrets")
        .fetch_one(fixture.pool())
        .await
        .expect("the secrets can be counted");
    assert_eq!(secrets, 0, "a failed login keeps no ciphertext");

    // The failed connection does not hold the vault key: the owner can try again.
    let again = connect_sync(&router, OWNER, REMOTE_VAULT_ID, SECRETS).await;
    assert_eq!(
        again.body["code"], "notes.sync.e2e_password_wrong",
        "{:?}",
        again.body
    );

    fixture
        .cleanup()
        .await
        .expect("the test database is dropped");
}

/// The associated data that binds a ciphertext to its connection and its kind (`schema.sql`).
fn associated_data(connection_id: Uuid, kind: &str) -> Vec<u8> {
    format!("ratatoskr-notes:{connection_id}:{kind}").into_bytes()
}

fn open(key: &[u8; 32], nonce: &[u8], ciphertext: &[u8], aad: &[u8]) -> Option<Vec<u8>> {
    let cipher = XChaCha20Poly1305::new_from_slice(key).expect("a 32-byte key");
    let nonce = XNonce::try_from(nonce).expect("a 24-byte nonce");
    cipher
        .decrypt(
            &nonce,
            Payload {
                msg: ciphertext,
                aad,
            },
        )
        .ok()
}

#[tokio::test]
async fn stored_secret_is_ciphertext_without_mfa() {
    let fixture = TestDatabase::create().await.expect("a test database");
    let systemd = FakeSystemd::new();
    let router = router(&fixture, &systemd);

    let id = connection_id(&connect_sync(&router, OWNER, REMOTE_VAULT_ID, SECRETS).await);

    let rows: Vec<(String, Vec<u8>, Vec<u8>)> = sqlx::query_as(
        "select secret_kind, nonce, ciphertext from notes.connection_secrets
         where connection_id = $1 order by secret_kind",
    )
    .bind(id)
    .fetch_all(fixture.pool())
    .await
    .expect("the secrets can be read");
    let kinds: Vec<&str> = rows.iter().map(|(kind, _, _)| kind.as_str()).collect();
    assert_eq!(
        kinds,
        ["account_password", "e2e_password"],
        "no MFA code row"
    );

    let key_file: [u8; 32] = std::fs::read(systemd.key_file())
        .expect("the key file can be read")
        .try_into()
        .expect("the key file holds 32 bytes");
    assert_eq!(key_file, KEY_BYTES);
    let other_key = [0x5a_u8; 32];
    for ((kind, nonce, ciphertext), plaintext) in rows.iter().zip([ACCOUNT_PASSWORD, E2E_PASSWORD])
    {
        assert_eq!(nonce.len(), 24, "{kind}: an XChaCha20 nonce");
        assert_eq!(
            ciphertext.len(),
            plaintext.len() + 16,
            "{kind}: the ciphertext and its Poly1305 tag"
        );
        assert!(
            !ciphertext
                .windows(plaintext.len())
                .any(|window| window == plaintext.as_bytes()),
            "{kind}: the stored bytes are not the plaintext"
        );
        let aad = associated_data(id, kind);
        assert_eq!(
            open(&KEY_BYTES, nonce, ciphertext, &aad).as_deref(),
            Some(plaintext.as_bytes()),
            "{kind}: the key file opens the ciphertext"
        );
        assert_eq!(
            open(&other_key, nonce, ciphertext, &aad),
            None,
            "{kind}: without the key file the ciphertext does not open"
        );
    }

    assert!(
        columns_holding(&fixture, MFA_CODE).await.is_empty(),
        "the MFA code is stored nowhere"
    );

    fixture
        .cleanup()
        .await
        .expect("the test database is dropped");
}
