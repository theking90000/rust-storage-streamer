use std::ops::Range;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use frame_streamer::{BoxError, EncryptedByteStream, SignedUrl, StoredObject, UploadByteStream};
use futures_util::StreamExt;
use reqwest::StatusCode;
use reqwest::header::RANGE;
use reqwest::multipart::{Form, Part};
use serde::Deserialize;

use crate::ratelimit::KeyedRateLimiter;
use crate::registry::WebhookRegistry;
use crate::webhook::{Webhook, format_uri};

/// Discord caps each segment at 150 physical frames (one attachment per object).
pub(crate) const FRAMES_PER_SEGMENT: u64 = 150;

const DEFAULT_API_BASE: &str = "https://discord.com/api/v10";
/// Bounded retries for a resolve on transient 429s.
const RESOLVE_ATTEMPTS: usize = 4;

#[derive(Deserialize)]
struct Message {
    id: String,
    #[serde(default)]
    attachments: Vec<Attachment>,
}

#[derive(Deserialize)]
struct Attachment {
    url: String,
}

/// Shared layer used by both the upload and download backends: two connection
/// pools (webhook REST + raw CDN), the per-webhook rate limiter, and the
/// least-used webhook registry.
pub(crate) struct DiscordCore {
    api_client: reqwest::Client,
    cdn_client: reqwest::Client,
    limiter: KeyedRateLimiter,
    registry: WebhookRegistry,
    api_base: String,
}

impl DiscordCore {
    #[cfg(test)]
    pub fn new(webhooks: Vec<Webhook>) -> Result<Self, BoxError> {
        Self::with_proxy(webhooks, None)
    }

    #[cfg(test)]
    pub fn with_base(webhooks: Vec<Webhook>, api_base: String) -> Result<Self, BoxError> {
        Self::with_base_and_proxy(webhooks, api_base, None)
    }

    pub fn with_proxy(webhooks: Vec<Webhook>, proxy_url: Option<&str>) -> Result<Self, BoxError> {
        Self::with_base_and_proxy(webhooks, DEFAULT_API_BASE.to_owned(), proxy_url)
    }

    fn with_base_and_proxy(
        webhooks: Vec<Webhook>,
        api_base: String,
        proxy_url: Option<&str>,
    ) -> Result<Self, BoxError> {
        let api_client = client(proxy_url)?;
        let cdn_client = client(None)?;
        Ok(Self {
            api_client,
            cdn_client,
            limiter: KeyedRateLimiter::new(),
            registry: WebhookRegistry::new(webhooks),
            api_base,
        })
    }

    /// Streams one segment to the least-used alive webhook as a chunked
    /// multipart body (no buffering, no Content-Length, no retry — failure is
    /// surfaced). Returns a self-describing `discord://` URI plus the freshly
    /// minted attachment URL (so the immediate post-upload read skips a resolve).
    pub async fn post_attachment(&self, body: UploadByteStream) -> Result<StoredObject, BoxError> {
        let idx = self
            .registry
            .pick()
            .ok_or_else(|| BoxError::from("no live webhook available"))?;
        let id = self.registry.slot(idx).id.clone();
        let token = self.registry.slot(idx).token.clone();

        self.limiter.acquire(&id).await;
        let part = Part::stream(reqwest::Body::wrap_stream(body)).file_name("segment");
        let form = Form::new().part("file", part);
        let response = self
            .api_client
            .post(format!("{}/webhooks/{id}/{token}?wait=true", self.api_base))
            .multipart(form)
            .send()
            .await;

        let response = match response {
            Ok(response) => response,
            Err(error) => {
                self.registry.finish(idx);
                return Err(boxed(error));
            }
        };
        let status = response.status();
        self.limiter
            .update_from_headers(&id, response.headers(), status);
        if is_dead(status) {
            self.registry.mark_dead(idx);
            self.registry.finish(idx);
            return Err(BoxError::from(format!("webhook {id} is dead ({status})")));
        }
        if !status.is_success() {
            self.registry.finish(idx);
            return Err(BoxError::from(format!("upload failed: {status}")));
        }

        let message: Message = response.json().await.map_err(boxed)?;
        self.registry.finish(idx);
        let url = first_attachment(message.attachments)?;
        let expires = parse_expiry(&url);
        Ok(StoredObject {
            uri: format_uri(&id, &token, &message.id),
            cached_url: Some(SignedUrl::new(url, expires)),
        })
    }

    /// Resolves a webhook message to its current (signed, expiring) attachment
    /// URL. Concurrency/caching is owned by the layer above; this only respects
    /// the webhook's rate limit.
    pub async fn resolve_attachment(
        &self,
        id: &str,
        token: &str,
        message_id: &str,
    ) -> Result<SignedUrl, BoxError> {
        for _ in 0..RESOLVE_ATTEMPTS {
            self.limiter.acquire(id).await;
            let response = self
                .api_client
                .get(format!(
                    "{}/webhooks/{id}/{token}/messages/{message_id}",
                    self.api_base
                ))
                .send()
                .await
                .map_err(boxed)?;
            let status = response.status();
            self.limiter
                .update_from_headers(id, response.headers(), status);
            if status == StatusCode::TOO_MANY_REQUESTS {
                continue;
            }
            if !status.is_success() {
                return Err(BoxError::from(format!("resolve failed: {status}")));
            }
            let message: Message = response.json().await.map_err(boxed)?;
            let url = first_attachment(message.attachments)?;
            let expires = parse_expiry(&url);
            return Ok(SignedUrl::new(url, expires));
        }
        Err(BoxError::from("resolve exhausted retries"))
    }

    /// Deletes the message backing an object. A missing message is treated as
    /// already-deleted.
    pub async fn delete_message(
        &self,
        id: &str,
        token: &str,
        message_id: &str,
    ) -> Result<(), BoxError> {
        self.limiter.acquire(id).await;
        let response = self
            .api_client
            .delete(format!(
                "{}/webhooks/{id}/{token}/messages/{message_id}",
                self.api_base
            ))
            .send()
            .await
            .map_err(boxed)?;
        let status = response.status();
        self.limiter
            .update_from_headers(id, response.headers(), status);
        if status.is_success() || status == StatusCode::NOT_FOUND {
            Ok(())
        } else {
            Err(BoxError::from(format!("delete failed: {status}")))
        }
    }

    /// Streams a byte range straight from the CDN attachment URL. The CDN is a
    /// separate host, so this bypasses the webhook rate limiter and its pool.
    pub fn cdn_range(&self, url: &str, range: Range<u64>) -> EncryptedByteStream {
        let client = self.cdn_client.clone();
        let url = url.to_owned();
        Box::pin(async_stream::try_stream! {
            let header = format!("bytes={}-{}", range.start, range.end.saturating_sub(1));
            let response = client
                .get(&url)
                .header(RANGE, header)
                .send()
                .await
                .map_err(boxed)?;
            if !response.status().is_success() {
                Err(BoxError::from(format!("cdn download failed: {}", response.status())))?;
            }
            let mut stream = response.bytes_stream();
            while let Some(chunk) = stream.next().await {
                yield chunk.map_err(boxed)?;
            }
        })
    }
}

fn pool() -> reqwest::ClientBuilder {
    reqwest::Client::builder()
        .pool_idle_timeout(Duration::from_secs(120))
        .pool_max_idle_per_host(64)
        .tcp_keepalive(Some(Duration::from_secs(60)))
        .tcp_nodelay(true)
}

fn client(proxy_url: Option<&str>) -> Result<reqwest::Client, BoxError> {
    let mut builder = pool();
    if let Some(url) = proxy_url {
        let scheme = url
            .split_once("://")
            .map(|(scheme, _)| scheme)
            .unwrap_or_default();
        if !matches!(scheme, "http" | "https" | "socks5" | "socks5h") {
            return Err(BoxError::from(format!(
                "unsupported proxy scheme '{scheme}'; expected http, https, socks5, or socks5h"
            )));
        }
        builder = builder.proxy(reqwest::Proxy::all(url).map_err(boxed)?);
    }
    builder.build().map_err(boxed)
}

fn first_attachment(attachments: Vec<Attachment>) -> Result<String, BoxError> {
    attachments
        .into_iter()
        .next()
        .map(|attachment| attachment.url)
        .ok_or_else(|| BoxError::from("message has no attachment"))
}

/// 401/403 (bad token) and 404 (Unknown Webhook) mean the webhook is gone.
fn is_dead(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN | StatusCode::NOT_FOUND
    )
}

/// Discord CDN URLs carry their expiry as a hex unix timestamp in `?ex=`.
fn parse_expiry(url: &str) -> Option<SystemTime> {
    let query = url.split('?').nth(1)?;
    for pair in query.split('&') {
        if let Some(value) = pair.strip_prefix("ex=") {
            let secs = u64::from_str_radix(value, 16).ok()?;
            return Some(UNIX_EPOCH + Duration::from_secs(secs));
        }
    }
    None
}

fn boxed(error: reqwest::Error) -> BoxError {
    Box::new(error)
}

#[cfg(test)]
mod proxy_tests {
    use super::*;

    fn webhook() -> Vec<Webhook> {
        vec![Webhook {
            id: "1".to_owned(),
            token: "token".to_owned(),
        }]
    }

    #[test]
    fn accepts_supported_proxy_schemes_and_no_proxy() {
        for proxy in [
            None,
            Some("http://127.0.0.1:8080"),
            Some("https://127.0.0.1:8443"),
            Some("socks5://127.0.0.1:1080"),
            Some("socks5h://127.0.0.1:1080"),
        ] {
            DiscordCore::with_base_and_proxy(webhook(), "http://localhost".to_owned(), proxy)
                .unwrap();
        }
    }

    #[test]
    fn rejects_unknown_or_invalid_proxy_urls() {
        for proxy in ["socks4://127.0.0.1:1080", "ftp://127.0.0.1", "http://"] {
            assert!(
                DiscordCore::with_base_and_proxy(
                    webhook(),
                    "http://localhost".to_owned(),
                    Some(proxy),
                )
                .is_err()
            );
        }
    }
}
