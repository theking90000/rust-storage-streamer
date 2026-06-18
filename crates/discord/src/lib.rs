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

use frame_streamer::{
    BoxError, EncryptedBytesDownloadBackend, StreamUploadBackend, UploadBackend,
};

use crate::client::DiscordCore;
use crate::download::DiscordDownload;
use crate::upload::DiscordEncryptedUpload;

pub use crate::webhook::Webhook;

/// The pair of backends produced for the server/CLI to wire up.
pub struct DiscordBackends {
    pub upload_backend: Arc<dyn UploadBackend>,
    pub download_backend: Arc<dyn EncryptedBytesDownloadBackend>,
}

/// Builds Discord-flavored upload and download backends from a webhook list.
///
/// `frame_size` MUST match the server's `ServerConfig.frame_size`, otherwise the
/// GCM framing on upload and download will not line up.
pub fn create_discord_backend(
    webhooks: Vec<Webhook>,
    frame_size: usize,
) -> Result<DiscordBackends, BoxError> {
    if webhooks.is_empty() {
        return Err(BoxError::from("webhook list must not be empty"));
    }
    let core = Arc::new(DiscordCore::new(webhooks)?);
    let encrypted_upload = Arc::new(DiscordEncryptedUpload::new(core.clone(), frame_size));
    let upload_backend =
        Arc::new(StreamUploadBackend::new(encrypted_upload, frame_size)?) as Arc<dyn UploadBackend>;
    let download_backend =
        Arc::new(DiscordDownload::new(core)) as Arc<dyn EncryptedBytesDownloadBackend>;
    Ok(DiscordBackends {
        upload_backend,
        download_backend,
    })
}
