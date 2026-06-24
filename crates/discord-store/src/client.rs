use std::collections::HashMap;
use std::ops::Range;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use frame_streamer::{BoxError, EncryptedByteStream, SignedUrl, StoredObject, UploadByteStream};
use futures_util::StreamExt;
use quota_engine::{Bucket, Pin, Pool, QuotaEngine, QuotaHandle, Reservation, Resource};
use reqwest::StatusCode;
use reqwest::header::{HeaderMap, RANGE};
use reqwest::multipart::{Form, Part};
use serde::Deserialize;

use crate::webhook::{Webhook, format_uri};

/// Discord caps each segment at 150 physical frames (one attachment per object).
pub(crate) const FRAMES_PER_SEGMENT: u64 = 150;

const DEFAULT_API_BASE: &str = "https://discord.com/api/v10";
/// Bounded retries for a resolve on transient 429s.
const RESOLVE_ATTEMPTS: usize = 4;
const QUOTA_DEADLINE: Duration = Duration::from_secs(30);
const QUOTA_VALIDITY: Duration = Duration::from_secs(60);
const WEBHOOK_CAPACITY: u32 = 5;
const CHANNEL_CAPACITY: u32 = 30;
const IP_CAPACITY: u32 = 10_000;
const GLOBAL_CAPACITY: u32 = 50;

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

/// Shared layer used by both the upload and download backends: one quota engine
/// over webhook/channel and egress/global resources plus API/CDN pools.
pub(crate) struct DiscordCore {
    api_clients: Vec<reqwest::Client>,
    cdn_client: reqwest::Client,
    quota: QuotaHandle<usize>,
    webhooks: Vec<Webhook>,
    webhook_by_id: HashMap<String, usize>,
    api_base: String,
}

impl DiscordCore {
    #[cfg(test)]
    pub fn new(webhooks: Vec<Webhook>) -> Result<Self, BoxError> {
        Self::with_proxies(webhooks, &[])
    }

    #[cfg(test)]
    pub fn with_base(webhooks: Vec<Webhook>, api_base: String) -> Result<Self, BoxError> {
        Self::with_base_and_proxies(webhooks, api_base, &[])
    }

    pub fn with_proxies(webhooks: Vec<Webhook>, proxy_urls: &[String]) -> Result<Self, BoxError> {
        Self::with_base_and_proxies(webhooks, DEFAULT_API_BASE.to_owned(), proxy_urls)
    }

    fn with_base_and_proxies(
        webhooks: Vec<Webhook>,
        api_base: String,
        proxy_urls: &[String],
    ) -> Result<Self, BoxError> {
        let api_clients = if proxy_urls.is_empty() {
            vec![client(None)?]
        } else {
            proxy_urls
                .iter()
                .map(|url| client(Some(url.as_str())))
                .collect::<Result<Vec<_>, _>>()?
        };
        let webhook_by_id = webhooks
            .iter()
            .enumerate()
            .map(|(idx, webhook)| (webhook.id.clone(), idx))
            .collect();
        let quota = QuotaHandle::new(quota_engine(webhooks.len(), api_clients.len()));
        Ok(Self {
            api_clients,
            cdn_client: client(None)?,
            quota,
            webhooks,
            webhook_by_id,
            api_base,
        })
    }

    #[cfg(test)]
    fn api_client_count(&self) -> usize {
        self.api_clients.len()
    }

    /// Streams one segment to the least-loaded alive webhook as a chunked
    /// multipart body. The quota commit is held until the final body chunk.
    pub async fn post_attachment(&self, body: UploadByteStream) -> Result<StoredObject, BoxError> {
        let reservation = self.reserve(&[Pin::Free, Pin::Free])?;
        let webhook_idx = reservation.picks[0];
        let egress_idx = reservation.picks[1];
        let id = self.webhooks[webhook_idx].id.clone();
        let token = self.webhooks[webhook_idx].token.clone();

        let gated_body = gate_last_chunk(body, self.quota.clone(), reservation);
        let part = Part::stream(reqwest::Body::wrap_stream(gated_body)).file_name("segment");
        let form = Form::new().part("file", part);
        let response = self.api_clients[egress_idx]
            .post(format!("{}/webhooks/{id}/{token}?wait=true", self.api_base))
            .multipart(form)
            .send()
            .await
            .map_err(boxed)?;

        let status = response.status();
        self.update_from_headers(webhook_idx, egress_idx, response.headers(), status);
        if is_dead(status) {
            self.quota.set_alive(0, &webhook_idx, false);
            return Err(BoxError::from(format!("webhook {id} is dead ({status})")));
        }
        if !status.is_success() {
            return Err(BoxError::from(format!("upload failed: {status}")));
        }

        let message: Message = response.json().await.map_err(boxed)?;
        let url = first_attachment(message.attachments)?;
        let expires = parse_expiry(&url);
        Ok(StoredObject {
            uri: format_uri(&id, &token, &message.id),
            cached_url: Some(SignedUrl::new(url, expires)),
        })
    }

    /// Resolves a webhook message to its current (signed, expiring) attachment
    /// URL. Concurrency/caching is owned by the layer above.
    pub async fn resolve_attachment(
        &self,
        id: &str,
        token: &str,
        message_id: &str,
    ) -> Result<SignedUrl, BoxError> {
        let webhook_idx = self.webhook_index(id)?;
        for _ in 0..RESOLVE_ATTEMPTS {
            let mut reservation = self.reserve(&[Pin::Fixed(webhook_idx), Pin::Free])?;
            let egress_idx = reservation.picks[1];
            self.quota.commit(&mut reservation).await;
            let response = self.api_clients[egress_idx]
                .get(format!(
                    "{}/webhooks/{id}/{token}/messages/{message_id}",
                    self.api_base
                ))
                .send()
                .await
                .map_err(boxed)?;
            let status = response.status();
            self.update_from_headers(webhook_idx, egress_idx, response.headers(), status);
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
        let webhook_idx = self.webhook_index(id)?;
        let mut reservation = self.reserve(&[Pin::Fixed(webhook_idx), Pin::Free])?;
        let egress_idx = reservation.picks[1];
        self.quota.commit(&mut reservation).await;
        let response = self.api_clients[egress_idx]
            .delete(format!(
                "{}/webhooks/{id}/{token}/messages/{message_id}",
                self.api_base
            ))
            .send()
            .await
            .map_err(boxed)?;
        let status = response.status();
        self.update_from_headers(webhook_idx, egress_idx, response.headers(), status);
        if status.is_success() || status == StatusCode::NOT_FOUND {
            Ok(())
        } else {
            Err(BoxError::from(format!("delete failed: {status}")))
        }
    }

    /// Streams a byte range straight from the CDN attachment URL. The CDN is a
    /// separate host, so this bypasses the webhook quota and proxy pool.
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

    fn reserve(&self, demand: &[Pin<usize>]) -> Result<Reservation<usize>, BoxError> {
        self.quota
            .reserve(demand, Instant::now() + QUOTA_DEADLINE, QUOTA_VALIDITY)
            .ok_or_else(|| BoxError::from("quota backlog exceeded deadline"))
    }

    fn webhook_index(&self, id: &str) -> Result<usize, BoxError> {
        self.webhook_by_id
            .get(id)
            .copied()
            .ok_or_else(|| BoxError::from(format!("unknown webhook id {id}")))
    }

    fn update_from_headers(
        &self,
        webhook_idx: usize,
        egress_idx: usize,
        headers: &HeaderMap,
        status: StatusCode,
    ) {
        if let Some(limit) = header_u32(headers, "x-ratelimit-limit") {
            let remaining = header_u32(headers, "x-ratelimit-remaining")
                .unwrap_or_else(|| limit.saturating_sub(1));
            self.quota
                .update(0, &webhook_idx, "webhook", remaining, limit);
        } else if status == StatusCode::TOO_MANY_REQUESTS {
            self.quota
                .update(0, &webhook_idx, "webhook", 0, WEBHOOK_CAPACITY);
        }

        if is_global_ratelimit(headers) {
            self.quota
                .update(1, &egress_idx, "global", 0, GLOBAL_CAPACITY);
        }
    }
}

fn quota_engine(webhooks: usize, egresses: usize) -> QuotaEngine<usize> {
    let now = Instant::now();
    QuotaEngine::new(
        vec![],
        vec![
            Pool::new(
                (0..webhooks)
                    .map(|idx| {
                        Resource::new(
                            idx,
                            vec![
                                Bucket::new("webhook", WEBHOOK_CAPACITY, 2.5, 0, now),
                                Bucket::new("channel", CHANNEL_CAPACITY, 0.5, 0, now),
                            ],
                        )
                    })
                    .collect(),
            ),
            Pool::new(
                (0..egresses)
                    .map(|idx| {
                        Resource::new(
                            idx,
                            vec![
                                Bucket::new("ip", IP_CAPACITY, 1000.0 / 60.0, 0, now),
                                Bucket::new("global", GLOBAL_CAPACITY, 50.0, 0, now),
                            ],
                        )
                    })
                    .collect(),
            ),
        ],
    )
}

fn gate_last_chunk(
    mut body: UploadByteStream,
    quota: QuotaHandle<usize>,
    mut reservation: Reservation<usize>,
) -> UploadByteStream {
    Box::pin(async_stream::try_stream! {
        let mut last: Option<Bytes> = None;
        while let Some(chunk) = body.next().await {
            if let Some(previous) = last.replace(chunk?) {
                yield previous;
            }
        }
        quota.commit(&mut reservation).await;
        if let Some(last) = last {
            yield last;
        }
    })
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

fn header_u32(headers: &HeaderMap, name: &str) -> Option<u32> {
    headers.get(name)?.to_str().ok()?.parse().ok()
}

fn is_global_ratelimit(headers: &HeaderMap) -> bool {
    headers
        .get("x-ratelimit-global")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("true"))
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
    use futures_util::stream;

    fn webhook() -> Vec<Webhook> {
        vec![Webhook {
            id: "1".to_owned(),
            token: "token".to_owned(),
        }]
    }

    #[test]
    fn accepts_supported_proxy_schemes_and_empty_proxy_list() {
        let core =
            DiscordCore::with_base_and_proxies(webhook(), "http://localhost".to_owned(), &[])
                .unwrap();
        assert_eq!(core.api_client_count(), 1);

        for proxy in [
            "http://127.0.0.1:8080",
            "https://127.0.0.1:8443",
            "socks5://127.0.0.1:1080",
            "socks5h://127.0.0.1:1080",
        ] {
            let proxies = vec![proxy.to_owned()];
            DiscordCore::with_base_and_proxies(webhook(), "http://localhost".to_owned(), &proxies)
                .unwrap();
        }
    }

    #[test]
    fn accepts_multiple_proxy_urls() {
        let proxies = vec![
            "http://127.0.0.1:8080".to_owned(),
            "socks5h://127.0.0.1:1080".to_owned(),
        ];
        let core =
            DiscordCore::with_base_and_proxies(webhook(), "http://localhost".to_owned(), &proxies)
                .unwrap();
        assert_eq!(core.api_client_count(), 2);
    }

    #[test]
    fn rejects_unknown_or_invalid_proxy_urls() {
        for proxy in ["socks4://127.0.0.1:1080", "ftp://127.0.0.1", "http://"] {
            let proxies = vec![proxy.to_owned()];
            assert!(
                DiscordCore::with_base_and_proxies(
                    webhook(),
                    "http://localhost".to_owned(),
                    &proxies,
                )
                .is_err()
            );
        }
    }

    #[tokio::test]
    async fn gated_body_holds_last_chunk_until_commit() {
        let quota = QuotaHandle::new(QuotaEngine::new(
            vec![Bucket::new("global", 1, 20.0, 0, Instant::now())],
            vec![],
        ));
        let mut spent = quota
            .reserve(
                &[],
                Instant::now() + Duration::from_secs(1),
                Duration::from_secs(1),
            )
            .unwrap();
        quota.commit(&mut spent).await;
        let reservation = quota
            .reserve(
                &[],
                Instant::now() + Duration::from_secs(1),
                Duration::from_secs(1),
            )
            .unwrap();
        let body: UploadByteStream = Box::pin(stream::iter([
            Ok(Bytes::from_static(b"a")),
            Ok(Bytes::from_static(b"b")),
        ]));
        let mut gated = gate_last_chunk(body, quota, reservation);

        let start = Instant::now();
        assert_eq!(
            gated.next().await.unwrap().unwrap(),
            Bytes::from_static(b"a")
        );
        assert!(start.elapsed() < Duration::from_millis(25));
        assert_eq!(
            gated.next().await.unwrap().unwrap(),
            Bytes::from_static(b"b")
        );
        assert!(start.elapsed() >= Duration::from_millis(40));
        assert!(gated.next().await.is_none());
    }
}
