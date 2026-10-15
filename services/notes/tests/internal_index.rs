//! The internal index route of a connection (workspace change `add-site-publications`, design D3,
//! tasks 4.1 and 4.3): `GET /internal/connections/{connection}/files` on the loopback domain
//! listener, which only `ratatoskr-sites` calls.

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions read the answers they wait for"
)]

mod support;

use axum::http::{Method, StatusCode};
use serde_json::{Value, json};

use crate::support::channel::{OWNER, sha256};
use crate::support::writes::Writer;
use crate::support::{free_address, run_service, send_tcp};

const HOME: &[u8] = b"---\ntitle: Home\ntags: [site]\n---\n# Home\n\n## Part\n\nSee [[Beta#Section One|beta]], ![[photo.png]] and [[Missing]]. #topic\n\nA block. ^blk1\n";
const BETA: &[u8] = b"# Beta\n\n## Section One\n";
const PHOTO: &[u8] = b"\x89PNG\r\n\x1a\n not a real image";

/// The file of `answer` at `path`.
fn file<'a>(answer: &'a Value, path: &str) -> &'a Value {
    answer["files"]
        .as_array()
        .and_then(|files| files.iter().find(|file| file["path"] == path))
        .unwrap_or(&Value::Null)
}

#[tokio::test]
async fn index_route_returns_resolved_links_embeds_frontmatter_and_revision() {
    let writer = Writer::new().await;
    let connection = writer
        .vault(&[
            ("Home.md", HOME),
            ("Notes/Beta.md", BETA),
            ("photo.png", PHOTO),
        ])
        .await;
    let api = free_address();
    let route = format!("/internal/connections/{connection}/files");
    let request = send_tcp(api, Method::GET, &route, OWNER, None);
    let (answer, stopped) = run_service(
        &writer.channel.fixture,
        &writer.channel.systemd,
        request,
        api,
    )
    .await;
    assert!(stopped, "serve stops without a failure");

    assert_eq!(answer.status, StatusCode::OK, "{:?}", answer.body);
    let home = file(&answer.body, "Home.md");
    assert_eq!(
        home["revision"],
        json!({
            "owner_service": "ratatoskr-notes",
            "digest": { "algorithm": "sha256", "hex": sha256(HOME) },
            "media_type": "text/markdown",
            "length_bytes": HOME.len(),
        }),
        "the current revision of the note"
    );
    assert_eq!(
        home["frontmatter"],
        json!({ "title": "Home", "tags": ["site"] })
    );
    assert_eq!(home["tags"], json!(["#site", "#topic"]));
    assert_eq!(
        home["headings"],
        json!([{ "level": 1, "heading": "Home" }, { "level": 2, "heading": "Part" }])
    );
    assert_eq!(home["blocks"], json!(["blk1"]));
    assert_eq!(
        home["links"],
        json!([
            { "link": "Beta#Section One", "display_text": "beta", "form": "wiki", "target": "Notes/Beta.md" },
            { "link": "Missing", "display_text": "Missing", "form": "wiki", "target": null },
        ]),
        "each link names the file that it resolves to, or null"
    );
    assert_eq!(
        home["embeds"],
        json!([
            { "link": "photo.png", "display_text": "photo.png", "form": "wiki", "target": "photo.png" },
        ])
    );

    let photo = file(&answer.body, "photo.png");
    assert_eq!(photo["revision"]["digest"]["hex"], sha256(PHOTO));
    assert_eq!(photo["revision"]["media_type"], "image/png");
    assert_eq!(photo["frontmatter"], Value::Null);
    assert_eq!(photo["links"], json!([]));
    assert_eq!(
        answer.body["files"].as_array().map(Vec::len),
        Some(3),
        "every present file of the connection: {:?}",
        answer.body
    );

    writer.cleanup().await;
}
