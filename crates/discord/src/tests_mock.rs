//! End-to-end tests against a local axum server that emulates the Discord
//! webhook REST API and CDN, so no real network access is required.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::{delete, get, post};
use axum::{Router, body::Bytes as AxumBytes};
use bytes::Bytes;
use frame_streamer::{BoxError, UploadByteStream};
use futures_util::{TryStreamExt, stream};
use serde_json::json;

use crate::client::DiscordCore;
use crate::webhook::Webhook;

fn body(data: &'static [u8]) -> UploadByteStream {
    Box::pin(stream::once(async move {
        Ok::<_, BoxError>(Bytes::from_static(data))
    }))
}

const BODY: &[u8] = b"0123456789abcdefghij";

#[derive(Clone)]
struct MockState {
    base: String,
    /// Webhook id that should answer uploads with 404 (a "dead" webhook).
    dead_id: Option<String>,
}

async fn start_mock(dead_id: Option<String>) -> (String, SocketAddr) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base = format!("http://{addr}");
    let state = MockState {
        base: base.clone(),
        dead_id,
    };
    let app = Router::new()
        .route("/webhooks/{id}/{token}", post(upload))
        .route("/webhooks/{id}/{token}/messages/{mid}", get(message))
        .route("/webhooks/{id}/{token}/messages/{mid}", delete(remove))
        .route("/cdn", get(cdn))
        .with_state(state);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (base, addr)
}

async fn upload(
    State(state): State<MockState>,
    Path((id, _token)): Path<(String, String)>,
    _body: AxumBytes,
) -> impl IntoResponse {
    if state.dead_id.as_deref() == Some(id.as_str()) {
        return (StatusCode::NOT_FOUND, Json(json!({ "code": 10015 }))).into_response();
    }
    Json(json!({
        "id": "msg1",
        "attachments": [{ "url": format!("{}/cdn?ex=6a000000", state.base) }],
    }))
    .into_response()
}

async fn message(State(state): State<MockState>) -> impl IntoResponse {
    Json(json!({
        "id": "msg1",
        "attachments": [{ "url": format!("{}/cdn?ex=6a000000", state.base) }],
    }))
}

async fn remove() -> impl IntoResponse {
    StatusCode::NO_CONTENT
}

async fn cdn(headers: HeaderMap) -> impl IntoResponse {
    let range = headers
        .get(header::RANGE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("bytes="))
        .and_then(|value| value.split_once('-'))
        .map(|(start, end)| {
            let start: usize = start.parse().unwrap();
            let end: usize = end.parse().unwrap();
            start..(end + 1).min(BODY.len())
        })
        .unwrap_or(0..BODY.len());
    (StatusCode::PARTIAL_CONTENT, BODY[range].to_vec())
}

fn webhooks(ids: &[&str]) -> Vec<Webhook> {
    ids.iter()
        .map(|id| Webhook {
            id: (*id).to_owned(),
            token: "tok".to_owned(),
        })
        .collect()
}

#[tokio::test]
async fn uploads_then_resolves_and_downloads_a_range() {
    let (base, _) = start_mock(None).await;
    let core = Arc::new(DiscordCore::with_base(webhooks(&["1"]), base).unwrap());

    let stored = core.post_attachment(body(b"frame")).await.unwrap();
    assert_eq!(stored.uri, "discord://1/tok/msg1");
    let cached = stored.cached_url.unwrap();
    assert!(cached.as_str().ends_with("/cdn?ex=6a000000"));
    assert!(cached.expires_at().is_some());

    let url = core.resolve_attachment("1", "tok", "msg1").await.unwrap();
    let bytes: Vec<Bytes> = core
        .cdn_range(url.as_str(), 2..8)
        .try_collect()
        .await
        .unwrap();
    let joined: Vec<u8> = bytes.into_iter().flatten().collect();
    assert_eq!(joined, b"234567");
}

#[tokio::test]
async fn a_dead_webhook_errors_and_is_pruned() {
    let (base, _) = start_mock(Some("1".to_owned())).await;
    // Webhook "1" returns 404 (Unknown Webhook); "2" works.
    let core = Arc::new(DiscordCore::with_base(webhooks(&["1", "2"]), base).unwrap());
    // First upload hits the least-used "1": no retry, the error surfaces.
    assert!(core.post_attachment(body(b"frame")).await.is_err());
    // "1" is now pruned, so the next upload lands on the live "2".
    let stored = core.post_attachment(body(b"frame")).await.unwrap();
    assert_eq!(stored.uri, "discord://2/tok/msg1");
}
