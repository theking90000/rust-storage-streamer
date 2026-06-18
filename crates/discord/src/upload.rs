use std::sync::Arc;

use frame_streamer::{
    DeleteFuture, EncryptedBytesUploadBackend, ObjectId, StoredObject, UploadByteStream,
    UploadFuture,
};

use crate::client::{DiscordCore, FRAMES_PER_SEGMENT};
use crate::webhook::parse_uri;

/// Lowest storage layer for Discord. It buffers one segment of encrypted
/// physical frames and posts it as a single webhook attachment. The GCM framing
/// above is handled by `frame_streamer::StreamUploadBackend`.
pub(crate) struct DiscordEncryptedUpload {
    core: Arc<DiscordCore>,
    frame_size: usize,
}

impl DiscordEncryptedUpload {
    pub fn new(core: Arc<DiscordCore>, frame_size: usize) -> Self {
        Self { core, frame_size }
    }
}

impl EncryptedBytesUploadBackend for DiscordEncryptedUpload {
    fn max_physical_bytes_per_segment(&self) -> u64 {
        self.frame_size as u64 * FRAMES_PER_SEGMENT
    }

    fn upload(
        &self,
        _id: ObjectId,
        _physical_size_hint: Option<u64>,
        bytes: UploadByteStream,
    ) -> UploadFuture {
        let core = self.core.clone();
        Box::pin(async move { core.post_attachment(bytes).await })
    }

    fn delete(&self, object: StoredObject) -> DeleteFuture {
        let core = self.core.clone();
        Box::pin(async move {
            let parsed = parse_uri(&object.uri)?;
            core.delete_message(&parsed.id, &parsed.token, &parsed.message_id)
                .await
        })
    }
}
