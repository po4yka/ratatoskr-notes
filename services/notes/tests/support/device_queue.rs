//! The requests of a device to the queue of the writes of its connection (workspace spec
//! `notes-write-delivery`, requirement "Device channels read an ordered queue through Edge").

#![allow(
    clippy::expect_used,
    dead_code,
    reason = "test scaffolding; not every test binary uses every helper"
)]

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use http_body_util::BodyExt as _;
use serde_json::Value;
use tower::ServiceExt as _;
use uuid::Uuid;

use crate::support::writes::Writer;
use crate::support::{Answer, DEVICE_ID_HEADER, DEVICE_KIND_HEADER, USER_HEADER, send_with};

impl Writer {
    /// `GET /v1/notes/connections/{id}/device-operations[?after=<cursor>]` from `device` of
    /// `owner`, a plugin.
    pub(crate) async fn device_operations(
        &self,
        owner: Uuid,
        device: Uuid,
        connection_id: Uuid,
        after: Option<&str>,
    ) -> Answer {
        let query = after
            .map(|cursor| format!("?after={cursor}"))
            .unwrap_or_default();
        send_with(
            &self.channel.router,
            Method::GET,
            &format!("/v1/notes/connections/{connection_id}/device-operations{query}"),
            owner,
            &plugin_claims(device),
            None,
        )
        .await
    }

    /// `POST /v1/notes/connections/{id}/device-operations/{operation_id}/result` from `device` of
    /// `owner`, a plugin.
    pub(crate) async fn device_result(
        &self,
        owner: Uuid,
        device: Uuid,
        connection_id: Uuid,
        operation_id: &str,
        result: Value,
    ) -> Answer {
        send_with(
            &self.channel.router,
            Method::POST,
            &format!(
                "/v1/notes/connections/{connection_id}/device-operations/{operation_id}/result"
            ),
            owner,
            &plugin_claims(device),
            Some(result),
        )
        .await
    }

    /// `GET .../device-operations/{operation_id}/content` from `device` of `owner`: the status and
    /// the bytes.
    pub(crate) async fn device_content(
        &self,
        owner: Uuid,
        device: Uuid,
        connection_id: Uuid,
        operation_id: &str,
    ) -> (StatusCode, Vec<u8>) {
        let mut request = Request::builder()
            .method(Method::GET)
            .uri(format!(
                "/v1/notes/connections/{connection_id}/device-operations/{operation_id}/content"
            ))
            .header(USER_HEADER, owner.to_string());
        for (name, value) in plugin_claims(device) {
            request = request.header(name, value);
        }
        let response = self
            .channel
            .router
            .clone()
            .oneshot(request.body(Body::empty()).expect("a request"))
            .await
            .expect("the router answers");
        let status = response.status();
        let bytes = response
            .into_body()
            .collect()
            .await
            .expect("a body")
            .to_bytes();
        (status, bytes.to_vec())
    }
}

/// The claims that Edge mints for a plugin device.
fn plugin_claims(device: Uuid) -> [(&'static str, String); 2] {
    [
        (DEVICE_ID_HEADER, device.to_string()),
        (DEVICE_KIND_HEADER, "obsidian_plugin".to_owned()),
    ]
}
