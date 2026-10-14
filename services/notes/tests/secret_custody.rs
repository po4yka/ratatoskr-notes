//! Secret custody (workspace spec `notes-vault-connections`, requirements "Obsidian secrets never
//! leave the notes service" and "Secrets are kept as ciphertext and a failed login has a stable
//! code", workspace design D4).
//!
//! Every secret is an obvious marker. The search for a marker covers every column of every table
//! in the `notes` schema, the outbox and the events in it, the log lines of the service, and the
//! output of the units. The ciphertext column is the one place that holds a form of a secret: the
//! encrypted form. The search reads that column too, and asserts that the plaintext marker is not
//! in it. `stored_secret_is_ciphertext_without_mfa` then checks the ciphertext itself.
//!
//! `login_unit_gets_secrets_only_as_credentials` and `no_secret_in_any_cmdline` run the shipped
//! login unit through the fake systemd bus (requirement "The login runs as a oneshot unit that
//! receives secrets as credentials"). The second one needs `/proc`, so it runs on Linux only.
//!
//! The tests after them check the review follow-ups of workspace task 5.12: each login failure has
//! its stable code, a key file that is open to others, has the wrong length, is a symbolic link or
//! has another owner is refused, and no value that reaches the argv of `ob` starts with `-`.
//! `start_removes_stale_credentials` (workspace task 5.14) checks that the start of the service
//! removes the credential files that a crash left behind.

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions read the rows and JSON answers they build"
)]

mod support;

use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;

use axum::Router;
use axum::http::{Method, StatusCode};
use chacha20poly1305::aead::{Aead as _, Payload};
use chacha20poly1305::{KeyInit as _, XChaCha20Poly1305, XNonce};
use ratatoskr_notes::api::{ApiState, domain_router};
use ratatoskr_notes_persistence::test_support::TestDatabase;
use uuid::Uuid;

use crate::support::cmdline::{CmdlineReader, own_uid};
use crate::support::logs::CapturedLogs;
use crate::support::systemd::{FakeSystemd, KEY_BYTES};
use crate::support::{OBSIDIAN_EMAIL, SyncSecrets, connect_sync, connection_id, send};

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
        ApiState::new(fixture.database.clone(), systemd.clone())
            .with_key_file(systemd.key_file())
            .with_credentials_dir(systemd.credentials_root()),
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
    let logs = CapturedLogs::install();
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

/// The shipped login unit, as the host installs it.
const LOGIN_UNIT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../deploy/systemd/ratatoskr-notes-login@.service"
);

/// The directives of the login unit, `(key, value)`, without comments.
fn directives(unit: &str) -> Vec<(String, String)> {
    unit.lines()
        .map(str::trim)
        .filter(|line| !line.starts_with('#'))
        .filter_map(|line| line.split_once('='))
        .map(|(key, value)| (key.to_owned(), value.to_owned()))
        .collect()
}

#[tokio::test]
async fn login_unit_gets_secrets_only_as_credentials() {
    let fixture = TestDatabase::create().await.expect("a test database");
    let systemd = FakeSystemd::new();
    let refusing = FakeSystemd::new().refuse_e2e_password();
    let refusing_router = router(&fixture, &refusing);
    let router = router(&fixture, &systemd);

    let id = connection_id(&connect_sync(&router, OWNER, REMOTE_VAULT_ID, SECRETS).await);

    // The service wrote one file for each value of the login, readable by its own user only.
    let snapshot = systemd
        .credential_snapshot(id)
        .expect("the service wrote the credential files before it started the login unit");
    assert_eq!(snapshot.directory_mode, 0o700, "{snapshot:?}");
    for file in &snapshot.files {
        assert_eq!(file.mode, 0o600, "{}: {snapshot:?}", file.name);
    }
    let contents = |name: &str| {
        snapshot
            .files
            .iter()
            .find(|file| file.name == name)
            .map(|file| file.contents.as_str())
    };
    assert_eq!(contents("password"), Some(ACCOUNT_PASSWORD), "{snapshot:?}");
    assert_eq!(contents("e2e"), Some(E2E_PASSWORD), "{snapshot:?}");

    // The unit loads every file of that directory with LoadCredential=, and nothing else names a
    // password.
    let unit = std::fs::read_to_string(LOGIN_UNIT).expect("the login unit ships");
    let directives = directives(&unit);
    let credentials: Vec<(&str, &str)> = directives
        .iter()
        .filter(|(key, _)| key == "LoadCredential")
        .map(|(_, value)| value.split_once(':').expect("LoadCredential=name:path"))
        .collect();
    for file in &snapshot.files {
        let source = format!("/run/ratatoskr-notes/credentials/%i/{}", file.name);
        assert!(
            credentials.contains(&(file.name.as_str(), source.as_str())),
            "the unit loads {source} as the credential {}: {credentials:?}",
            file.name
        );
    }
    assert!(
        directives
            .iter()
            .any(|(key, value)| key == "User" && value == "ratatoskr-notes-sync"),
        "the login unit runs as the owner of ob-home"
    );
    assert!(
        !unit.contains("--password"),
        "no password is given to ob in argv"
    );

    // ob read each password on standard input.
    assert_eq!(systemd.ob_stdin("login").as_deref(), Some(ACCOUNT_PASSWORD));
    assert_eq!(
        systemd.ob_stdin("sync-setup").as_deref(),
        Some(E2E_PASSWORD)
    );

    // The files are gone after the unit stopped, also after a failed login.
    assert!(
        !systemd.credentials_root().join(id.to_string()).exists(),
        "the credential files of a login are deleted when the unit stops"
    );
    let failed = connect_sync(&refusing_router, OWNER, "remote-vault-0043", SECRETS).await;
    assert_eq!(
        failed.status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{:?}",
        failed.body
    );
    let left: Vec<_> = std::fs::read_dir(refusing.credentials_root())
        .expect("the credentials root can be listed")
        .collect();
    assert!(
        left.is_empty(),
        "the credential files of a failed login are deleted: {left:?}"
    );

    fixture
        .cleanup()
        .await
        .expect("the test database is dropped");
}

/// The two passwords. The MFA code is a one-time code, and it may be in argv (workspace spec
/// `notes-vault-connections`, requirement "The login runs as a oneshot unit that receives secrets
/// as credentials").
const PASSWORD_MARKERS: [&str; 2] = [ACCOUNT_PASSWORD, E2E_PASSWORD];

#[tokio::test]
#[cfg_attr(
    not(target_os = "linux"),
    ignore = "reads /proc/*/cmdline, which only Linux has"
)]
async fn no_secret_in_any_cmdline() {
    let reader = CmdlineReader::start().await;
    let fixture = TestDatabase::create().await.expect("a test database");
    // The stub ob sleeps in each step, so that the reader sees its processes.
    let systemd = FakeSystemd::new().ob_sleeps();
    let router = router(&fixture, &systemd);

    let connected = connect_sync(&router, OWNER, REMOTE_VAULT_ID, SECRETS).await;
    let seen = reader.stop().await;
    connection_id(&connected);

    assert_ne!(
        seen.reader_uid,
        own_uid(),
        "the reader runs as another user than the login"
    );
    // The reader saw both steps of the login while they ran.
    for step in [" login --email ", " sync-setup --vault "] {
        assert!(
            seen.lines.iter().any(|line| line.contains(step)),
            "the reader saw the process of `ob{step}` while it ran"
        );
    }
    for marker in PASSWORD_MARKERS {
        let holding: Vec<&String> = seen
            .lines
            .iter()
            .filter(|line| line.contains(marker))
            .collect();
        assert!(
            holding.is_empty(),
            "a command line holds the password marker {marker}: {holding:?}"
        );
    }

    fixture
        .cleanup()
        .await
        .expect("the test database is dropped");
}

#[tokio::test]
async fn start_removes_stale_credentials() {
    let fixture = TestDatabase::create().await.expect("a test database");
    let systemd = FakeSystemd::new();
    // What a crash of the service during a login leaves on the tmpfs.
    let stale = systemd.credentials_root().join(Uuid::now_v7().to_string());
    std::fs::create_dir(&stale).expect("a credentials directory is created");
    std::fs::write(stale.join("password"), ACCOUNT_PASSWORD).expect("a credential file is written");

    // The service builds its domain router once, at start.
    let _router = router(&fixture, &systemd);
    assert!(
        !stale.exists(),
        "the start removes the credential files that a crash left behind"
    );

    fixture
        .cleanup()
        .await
        .expect("the test database is dropped");
}

/// A `sync` connect body with the marker passwords and the given email, vault id and MFA code.
fn sync_body(email: &str, vault_id: &str, mfa_code: Option<&str>) -> serde_json::Value {
    let mut sync_login = serde_json::json!({
        "email": email,
        "password": ACCOUNT_PASSWORD,
        "e2e_password": E2E_PASSWORD,
    });
    if let Some(mfa_code) = mfa_code {
        sync_login["mfa_code"] = mfa_code.into();
    }
    serde_json::json!({
        "channel": "sync",
        "vault_key": vault_id,
        "vault_display_name": "Personal",
        "sync_login": sync_login,
    })
}

async fn count(fixture: &TestDatabase, query: &str) -> i64 {
    sqlx::query_scalar(query)
        .fetch_one(fixture.pool())
        .await
        .expect("the rows can be counted")
}

#[tokio::test]
async fn each_login_failure_has_its_code() {
    let fixture = TestDatabase::create().await.expect("a test database");
    let cases = [
        (
            "notes.sync.login_failed",
            FakeSystemd::new().refuse_login(),
            Some(MFA_CODE),
        ),
        (
            "notes.sync.mfa_required",
            FakeSystemd::new().need_mfa(),
            None,
        ),
        (
            "notes.sync.vault_not_found",
            FakeSystemd::new().find_no_vault(),
            Some(MFA_CODE),
        ),
    ];

    let mut wrong = Vec::new();
    for (index, (code, systemd, mfa_code)) in cases.into_iter().enumerate() {
        let router = router(&fixture, &systemd);
        let body = sync_body(
            OBSIDIAN_EMAIL,
            &format!("remote-vault-010{index}"),
            mfa_code,
        );
        let answer = send(
            &router,
            Method::POST,
            "/v1/notes/connections",
            OWNER,
            Some(body),
        )
        .await;
        assert_no_marker_in("the error envelope", &answer.body.to_string());
        if answer.status != StatusCode::UNPROCESSABLE_ENTITY || answer.body["code"] != code {
            wrong.push(format!("{code}: {} {}", answer.status, answer.body));
        }
    }
    assert!(
        wrong.is_empty(),
        "a login failure has the wrong answer: {wrong:#?}"
    );
    assert_eq!(
        count(&fixture, "select count(*) from notes.connection_secrets").await,
        0,
        "a failed login keeps no ciphertext"
    );

    fixture
        .cleanup()
        .await
        .expect("the test database is dropped");
}

/// Connects with a key file that the service must refuse, and returns why the answer is wrong, or
/// `None` when the service refused the key file.
async fn refusal_of_key_file(
    fixture: &TestDatabase,
    systemd: &FakeSystemd,
    vault_id: &str,
) -> Option<String> {
    let router = router(fixture, systemd);
    let answer = connect_sync(&router, OWNER, vault_id, SECRETS).await;
    let refused = answer.status == StatusCode::SERVICE_UNAVAILABLE
        && answer.body["code"] == "notes.service.unavailable";
    let started = systemd.calls();
    if refused && started.is_empty() {
        None
    } else {
        Some(format!(
            "{} {}, units {started:?}",
            answer.status, answer.body
        ))
    }
}

/// Makes the key file of a fake into one that the service must refuse.
type PrepareKeyFile = fn(&Path);

fn set_mode(path: &Path, mode: u32) {
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
        .expect("the mode of the key file can be set");
}

#[tokio::test]
async fn unsafe_key_file_is_refused() {
    let fixture = TestDatabase::create().await.expect("a test database");
    let cases: [(&str, PrepareKeyFile); 3] = [
        ("mode 0644", |key| set_mode(key, 0o644)),
        ("31 bytes", |key| {
            set_mode(key, 0o600);
            std::fs::write(key, &KEY_BYTES[..31]).expect("the key file can be written");
        }),
        ("a symbolic link to a good key file", |key| {
            let target = key.with_extension("target");
            std::fs::rename(key, &target).expect("the key file can be moved");
            std::os::unix::fs::symlink(&target, key).expect("a symbolic link can be made");
        }),
    ];

    let mut accepted = Vec::new();
    for (index, (case, prepare)) in cases.into_iter().enumerate() {
        let systemd = FakeSystemd::new();
        prepare(&systemd.key_file());
        let vault_id = format!("remote-vault-020{index}");
        if let Some(answer) = refusal_of_key_file(&fixture, &systemd, &vault_id).await {
            accepted.push(format!("{case}: {answer}"));
        }
    }
    assert!(
        accepted.is_empty(),
        "the service used a key file that it must refuse: {accepted:#?}"
    );
    assert_eq!(
        count(&fixture, "select count(*) from notes.connections").await,
        0,
        "no connection is created without a usable key"
    );

    fixture
        .cleanup()
        .await
        .expect("the test database is dropped");
}

/// The uid and gid of `nobody` on Debian and Ubuntu.
const NOBODY: u32 = 65_534;

#[tokio::test]
#[cfg_attr(
    not(target_os = "linux"),
    ignore = "gives a file to another user, which needs root or `sudo -n`"
)]
async fn key_file_of_another_owner_is_refused() {
    let fixture = TestDatabase::create().await.expect("a test database");
    let systemd = FakeSystemd::new();
    let key_file = systemd.key_file();
    // As root, the service can read a file of any owner, so only the owner check refuses it. That
    // is the run that this test is for: the Linux container of DEVELOPMENT.md runs it as root.
    if own_uid() == 0 {
        std::os::unix::fs::chown(&key_file, Some(NOBODY), Some(NOBODY))
            .expect("root can give the key file to nobody");
    } else {
        let status = std::process::Command::new("sudo")
            .args(["-n", "chown", &format!("{NOBODY}:{NOBODY}")])
            .arg(&key_file)
            .status()
            .expect("sudo can be started");
        assert!(status.success(), "sudo -n gives the key file to nobody");
    }

    let wrong = refusal_of_key_file(&fixture, &systemd, REMOTE_VAULT_ID).await;
    assert_eq!(wrong, None, "the service used a key file of another owner");

    fixture
        .cleanup()
        .await
        .expect("the test database is dropped");
}

#[tokio::test]
async fn leading_dash_is_refused() {
    let fixture = TestDatabase::create().await.expect("a test database");
    let systemd = FakeSystemd::new();
    let router = router(&fixture, &systemd);
    let dashed_mfa = format!("-{MFA_CODE}");
    let cases = [
        (
            "the email",
            sync_body("-owner@example.test", "remote-vault-0301", Some(MFA_CODE)),
        ),
        (
            "the vault id",
            sync_body(OBSIDIAN_EMAIL, "-remote-vault-0302", Some(MFA_CODE)),
        ),
        (
            "the MFA code",
            sync_body(OBSIDIAN_EMAIL, "remote-vault-0303", Some(&dashed_mfa)),
        ),
    ];

    let mut accepted = Vec::new();
    for (case, body) in cases {
        let answer = send(
            &router,
            Method::POST,
            "/v1/notes/connections",
            OWNER,
            Some(body),
        )
        .await;
        if answer.status != StatusCode::BAD_REQUEST
            || answer.body["code"] != "notes.request.invalid"
        {
            accepted.push(format!("{case}: {} {}", answer.status, answer.body));
        }
    }
    assert!(
        accepted.is_empty(),
        "a value that ob could read as an option was not refused: {accepted:#?}"
    );
    assert_eq!(systemd.calls(), [], "no unit starts for an invalid request");

    fixture
        .cleanup()
        .await
        .expect("the test database is dropped");
}
