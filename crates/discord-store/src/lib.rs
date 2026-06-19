//! Discord-webhook storage backend for the frame streamer.
//!
//! Each encrypted segment (≤ `frame_size * 150` bytes) is stored as one webhook
//! message attachment. A shared layer ([`client::DiscordCore`]) gives upload and
//! download a common per-webhook rate limiter and two connection pools (webhook
//! REST + raw CDN). Objects are addressed by a self-describing
//! `discord://{webhook_id}/{token}/{message_id}` URI.

mod client;
mod download;
mod ratelimit;
mod registry;
mod upload;
mod webhook;

#[cfg(test)]
mod tests_mock;

use std::sync::Arc;

use frame_streamer::{BoxError, StorageBackend, StreamUploadBackend, UploadBackend};

use crate::client::DiscordCore;
use crate::download::DiscordDownload;
use crate::upload::DiscordEncryptedUpload;

pub use crate::webhook::{Webhook, load_webhooks};

/// Builds Discord-flavored upload and download backends from a webhook list.
///
/// `frame_size` MUST match the server's `ServerConfig.frame_size`, otherwise the
/// GCM framing on upload and download will not line up.
pub fn create(webhooks: Vec<Webhook>, frame_size: usize) -> Result<StorageBackend, BoxError> {
    create_with_proxy(webhooks, frame_size, None)
}

/// Builds Discord backends whose API and CDN clients use the same optional
/// HTTP(S) or SOCKS5 proxy.
pub fn create_with_proxy(
    webhooks: Vec<Webhook>,
    frame_size: usize,
    proxy_url: Option<&str>,
) -> Result<StorageBackend, BoxError> {
    if webhooks.is_empty() {
        return Err(BoxError::from("webhook list must not be empty"));
    }
    let core = Arc::new(DiscordCore::with_proxy(webhooks, proxy_url)?);
    let encrypted_upload = Arc::new(DiscordEncryptedUpload::new(core.clone(), frame_size));
    let upload =
        Arc::new(StreamUploadBackend::new(encrypted_upload, frame_size)?) as Arc<dyn UploadBackend>;
    let download = Arc::new(DiscordDownload::new(core));
    Ok(StorageBackend { upload, download })
}
