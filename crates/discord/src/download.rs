use std::ops::Range;
use std::sync::Arc;

use frame_streamer::{
    EncryptedByteStream, EncryptedBytesDownloadBackend, ObjectMeta, SignedUrl, UrlTicket,
};

use crate::client::DiscordCore;
use crate::webhook::parse_uri;

/// Download backend that reads encrypted physical bytes back from Discord.
/// `resolve_url` rebuilds the attachment URL from the object URI; `download`
/// streams a byte range from the CDN.
pub(crate) struct DiscordDownload {
    core: Arc<DiscordCore>,
}

impl DiscordDownload {
    pub fn new(core: Arc<DiscordCore>) -> Self {
        Self { core }
    }
}

impl EncryptedBytesDownloadBackend for DiscordDownload {
    fn resolve_url(&self, object: &ObjectMeta) -> UrlTicket {
        let core = self.core.clone();
        let uri = object.uri.clone();
        Box::pin(async move {
            let parsed = parse_uri(&uri)?;
            core.resolve_attachment(&parsed.id, &parsed.token, &parsed.message_id)
                .await
        })
    }

    fn download(
        &self,
        _object: &ObjectMeta,
        url: SignedUrl,
        physical_bytes: Range<u64>,
    ) -> EncryptedByteStream {
        self.core.cdn_range(url.as_str(), physical_bytes)
    }
}
