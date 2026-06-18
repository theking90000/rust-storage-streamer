use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use frame_streamer::{
    EncryptedByteStream, EncryptedBytesDownloadBackend, ObjectMeta, SignedUrl, UrlTicket,
};

use crate::Catalog;

pub(crate) struct CatalogDownloadBackend {
    catalog: Catalog,
    inner: Arc<dyn EncryptedBytesDownloadBackend>,
}

impl CatalogDownloadBackend {
    pub(crate) fn new(catalog: Catalog, inner: Arc<dyn EncryptedBytesDownloadBackend>) -> Self {
        Self { catalog, inner }
    }
}

impl EncryptedBytesDownloadBackend for CatalogDownloadBackend {
    fn resolve_url(&self, object: &ObjectMeta) -> UrlTicket {
        let catalog = self.catalog.clone();
        let inner = self.inner.clone();
        let object = object.clone();
        Box::pin(async move {
            let valid_after = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .saturating_add(Duration::from_secs(300))
                .as_secs() as i64;
            if let Some(url) = catalog.cached_url(object.id.as_str(), valid_after).await? {
                return Ok(url);
            }
            let url = inner.resolve_url(&object).await?;
            catalog.cache_url(object.id.as_str(), &url).await?;
            Ok(url)
        })
    }

    fn download(
        &self,
        object: &ObjectMeta,
        url: SignedUrl,
        physical_bytes: Range<u64>,
    ) -> EncryptedByteStream {
        self.inner.download(object, url, physical_bytes)
    }
}
