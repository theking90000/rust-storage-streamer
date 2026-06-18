mod catalog;
mod download;

use std::io;
use std::ops::Range;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use frame_streamer::{
    ByteRate, ByteRequest, ByteStream, ByteStreamConfig, ByteUpload, DecryptKey,
    EncryptedBytesDownloadBackend, FrameBudget, ObjectId, UploadBackend, UploadError, UploadObject,
};
use futures_util::{sink, stream};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use download::CatalogDownloadBackend;

pub use catalog::{Catalog, CatalogError};

#[derive(Clone, Copy, Debug)]
pub struct ServerConfig {
    pub frame_size: usize,
    pub max_file_size: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            frame_size: 1 << 16,
            max_file_size: 20 * 1024 * 1024 * 1024,
        }
    }
}

#[derive(Clone)]
pub struct AppState {
    catalog: Catalog,
    upload_backend: Arc<dyn UploadBackend>,
    download_backend: Arc<dyn EncryptedBytesDownloadBackend>,
    frame_budget: FrameBudget,
    stream_config: ByteStreamConfig,
    allocated_rate: ByteRate,
    config: ServerConfig,
}

impl AppState {
    pub fn new(
        catalog: Catalog,
        upload_backend: Arc<dyn UploadBackend>,
        download_backend: Arc<dyn EncryptedBytesDownloadBackend>,
        frame_budget: FrameBudget,
        stream_config: ByteStreamConfig,
        allocated_rate: ByteRate,
        config: ServerConfig,
    ) -> Self {
        let download_backend = Arc::new(CatalogDownloadBackend::new(
            catalog.clone(),
            download_backend,
        ));
        Self {
            catalog,
            upload_backend,
            download_backend,
            frame_budget,
            stream_config,
            allocated_rate,
            config,
        }
    }
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/files", post(create_file))
        .route("/files/{id}", get(download_file))
        .route("/files/{id}/segments/{index}", put(upload_segment))
        .route("/files/{id}/complete", post(complete_file))
        .with_state(state)
}

#[derive(Deserialize)]
struct CreateFile {
    name: String,
    #[serde(default = "default_content_type")]
    content_type: String,
    expected_size: u64,
}

fn default_content_type() -> String {
    "application/octet-stream".to_string()
}

#[derive(Serialize)]
struct FileCreated {
    id: String,
    expected_size: u64,
}

async fn create_file(
    State(state): State<AppState>,
    Json(request): Json<CreateFile>,
) -> Result<impl IntoResponse, ApiError> {
    if request.expected_size > state.config.max_file_size {
        return Err(ApiError::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "file allocation exceeds server limit",
        ));
    }
    if request.name.is_empty() {
        return Err(ApiError::new(StatusCode::BAD_REQUEST, "file name is required"));
    }
    let id = Uuid::new_v4().to_string();
    state
        .catalog
        .create_file(&id, &request.name, &request.content_type, request.expected_size)
        .await?;
    Ok((
        StatusCode::CREATED,
        Json(FileCreated {
            id,
            expected_size: request.expected_size,
        }),
    ))
}

#[derive(Serialize)]
struct SegmentCreated {
    id: String,
    size: u64,
    frame_count: u32,
    checksum: String,
}

async fn upload_segment(
    State(state): State<AppState>,
    Path((file_id, index)): Path<(String, u32)>,
    headers: HeaderMap,
    body: Body,
) -> Result<impl IntoResponse, ApiError> {
    state.catalog.reserve_segment(&file_id, index).await?;
    let segment_id = Uuid::new_v4().to_string();
    let key: [u8; 32] = rand::random();
    let content_length = headers
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok());
    let uploader = ByteUpload::new(state.upload_backend.clone(), state.config.frame_size)
        .map_err(ApiError::internal)?;
    let result = uploader
        .upload(
            UploadObject {
                id: ObjectId::new(&segment_id),
                key: DecryptKey::new(key),
            },
            body.into_data_stream(),
            content_length,
        )
        .await;
    let result = match result {
        Ok(result) => result,
        Err(error) => {
            state.catalog.cancel_reservation(&file_id, index).await;
            return Err(ApiError::upload(error));
        }
    };
    if let Err(error) = state
        .catalog
        .attach_segment(&file_id, index, &segment_id, &key, &result)
        .await
    {
        let _ = state
            .upload_backend
            .delete(result.stored_object.clone())
            .await;
        state.catalog.cancel_reservation(&file_id, index).await;
        return Err(error.into());
    }
    Ok((
        StatusCode::CREATED,
        Json(SegmentCreated {
            id: segment_id,
            size: result.plaintext_size,
            frame_count: result.frame_count,
            checksum: hex(&result.checksum),
        }),
    ))
}

#[derive(Serialize)]
struct FileCompleted {
    id: String,
    size: u64,
    completed_at: i64,
}

async fn complete_file(
    State(state): State<AppState>,
    Path(file_id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let payload_size = state
        .config
        .frame_size
        .checked_sub(frame_streamer::TAG_SIZE)
        .ok_or_else(|| ApiError::internal("invalid frame size"))?;
    let (size, completed_at) = state.catalog.complete_file(&file_id, payload_size).await?;
    Ok(Json(FileCompleted {
        id: file_id,
        size,
        completed_at,
    }))
}

async fn download_file(
    State(state): State<AppState>,
    Path(file_id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let file = state.catalog.completed_file(&file_id).await?;
    let size = file.size;
    let objects = file.objects;
    let range = parse_range(headers.get(header::RANGE), size)?;
    let partial = range != (0..size);
    let length = range.end - range.start;
    let mut builder = Response::builder()
        .status(if partial {
            StatusCode::PARTIAL_CONTENT
        } else {
            StatusCode::OK
        })
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CONTENT_LENGTH, length.to_string())
        .header(header::CONTENT_TYPE, file.content_type)
        .header(
            header::CONTENT_DISPOSITION,
            content_disposition(&file.name),
        );
    if partial {
        builder = builder.header(
            header::CONTENT_RANGE,
            format!("bytes {}-{}/{}", range.start, range.end - 1, size),
        );
    }
    if length == 0 {
        return builder.body(Body::empty()).map_err(ApiError::internal);
    }
    let byte_stream = ByteStream::new(
        stream::iter(objects.into_iter().map(Ok::<_, frame_streamer::BoxError>)),
        state.download_backend,
        ByteRequest::new(range, state.allocated_rate).map_err(ApiError::internal)?,
        state.frame_budget,
        state.stream_config,
    )
    .map_err(ApiError::internal)?;
    let (sender, receiver) = tokio::sync::mpsc::channel::<Result<bytes::Bytes, io::Error>>(1);
    let error_sender = sender.clone();
    let output = sink::unfold(sender, |sender, bytes| async move {
        sender
            .send(Ok(bytes))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "HTTP client disconnected"))?;
        Ok::<_, io::Error>(sender)
    });
    tokio::spawn(async move {
        if let Err(error) = byte_stream.pipe_into(output).await {
            let _ = error_sender
                .send(Err(io::Error::other(error.to_string())))
                .await;
        }
    });
    builder
        .body(Body::from_stream(
            tokio_stream::wrappers::ReceiverStream::new(receiver),
        ))
        .map_err(ApiError::internal)
}

fn parse_range(value: Option<&HeaderValue>, size: u64) -> Result<Range<u64>, ApiError> {
    let Some(value) = value else {
        return Ok(0..size);
    };
    let value = value.to_str().map_err(|_| range_error(size))?;
    let value = value
        .strip_prefix("bytes=")
        .filter(|value| !value.contains(','))
        .ok_or_else(|| range_error(size))?;
    let (start, end) = value.split_once('-').ok_or_else(|| range_error(size))?;
    let range = if start.is_empty() {
        let suffix: u64 = end.parse().map_err(|_| range_error(size))?;
        if suffix == 0 || size == 0 {
            return Err(range_error(size));
        }
        size.saturating_sub(suffix)..size
    } else {
        let start: u64 = start.parse().map_err(|_| range_error(size))?;
        if start >= size {
            return Err(range_error(size));
        }
        let end = if end.is_empty() {
            size
        } else {
            end.parse::<u64>()
                .map_err(|_| range_error(size))?
                .saturating_add(1)
                .min(size)
        };
        if start >= end {
            return Err(range_error(size));
        }
        start..end
    };
    Ok(range)
}

fn range_error(size: u64) -> ApiError {
    let mut error = ApiError::new(
        StatusCode::RANGE_NOT_SATISFIABLE,
        format!("invalid byte range for {size}-byte file"),
    );
    error.content_range = Some(format!("bytes */{size}"));
    error
}

/// Builds an `attachment` Content-Disposition with an ASCII-safe `filename`
/// and an RFC 5987 `filename*` carrying the exact UTF-8 name.
fn content_disposition(name: &str) -> String {
    let ascii: String = name
        .chars()
        .map(|c| if c.is_ascii() && c != '"' && c != '\\' { c } else { '_' })
        .collect();
    let mut encoded = String::with_capacity(name.len());
    for byte in name.as_bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            encoded.push(*byte as char);
        } else {
            encoded.push('%');
            encoded.push(char::from_digit((byte >> 4) as u32, 16).unwrap());
            encoded.push(char::from_digit((byte & 15) as u32, 16).unwrap());
        }
    }
    format!("inline; filename=\"{ascii}\"; filename*=UTF-8''{encoded}")
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(DIGITS[(byte >> 4) as usize] as char);
        output.push(DIGITS[(byte & 15) as usize] as char);
    }
    output
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    message: String,
    content_range: Option<String>,
}

impl ApiError {
    fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
            content_range: None,
        }
    }
    fn internal(error: impl std::fmt::Display) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
    }
    fn upload(error: frame_streamer::BoxError) -> Self {
        let status = match error.downcast_ref::<UploadError>() {
            Some(UploadError::TooLarge { .. } | UploadError::TooManyFrames { .. }) => {
                StatusCode::PAYLOAD_TOO_LARGE
            }
            Some(UploadError::LengthMismatch { .. } | UploadError::Empty) => {
                StatusCode::BAD_REQUEST
            }
            _ => StatusCode::BAD_GATEWAY,
        };
        Self::new(status, error.to_string())
    }
}

impl From<CatalogError> for ApiError {
    fn from(error: CatalogError) -> Self {
        let status = match error {
            CatalogError::NotFound => StatusCode::NOT_FOUND,
            CatalogError::NotCompleted => StatusCode::CONFLICT,
            CatalogError::Database(_) => StatusCode::INTERNAL_SERVER_ERROR,
            _ => StatusCode::CONFLICT,
        };
        Self::new(status, error.to_string())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut response = (
            self.status,
            Json(serde_json::json!({ "error": self.message })),
        )
            .into_response();
        if let Some(value) = self.content_range.and_then(|value| value.parse().ok()) {
            response.headers_mut().insert(header::CONTENT_RANGE, value);
        }
        response
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use axum::http::{Request, header};
    use std::ops::Range;
    use std::time::Duration;

    use frame_streamer::{
        ByteTransferModel, DeleteFuture, EncryptedByteStream, EncryptedBytesUploadBackend,
        ObjectMeta, SignedUrl, StoredObject, StreamUploadBackend, UploadByteStream, UploadFuture,
        UrlTicket,
    };
    use futures_util::StreamExt;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use super::*;

    #[derive(Default)]
    struct MemoryUpload {
        deleted: Mutex<Vec<String>>,
    }

    struct NoDownload;

    impl EncryptedBytesDownloadBackend for NoDownload {
        fn resolve_url(&self, _object: &ObjectMeta) -> UrlTicket {
            Box::pin(async { Err(io::Error::other("no download").into()) })
        }

        fn download(
            &self,
            _object: &ObjectMeta,
            _url: SignedUrl,
            _bytes: Range<u64>,
        ) -> EncryptedByteStream {
            Box::pin(stream::once(async {
                Err(io::Error::other("no download").into())
            }))
        }
    }

    #[derive(Default)]
    struct MemoryStore {
        objects: Arc<Mutex<HashMap<String, Vec<u8>>>>,
        resolves: AtomicUsize,
    }

    impl EncryptedBytesUploadBackend for MemoryStore {
        fn max_physical_bytes_per_segment(&self) -> u64 {
            2400
        }

        fn upload(
            &self,
            id: ObjectId,
            _hint: Option<u64>,
            mut bytes: UploadByteStream,
        ) -> UploadFuture {
            let uri = format!("memory://{}", id.as_str());
            let objects = self.objects.clone();
            Box::pin(async move {
                let mut output = Vec::new();
                while let Some(chunk) = bytes.next().await {
                    output.extend_from_slice(&chunk?);
                }
                objects.lock().unwrap().insert(uri.clone(), output);
                Ok(StoredObject {
                    uri,
                    cached_url: None,
                })
            })
        }

        fn delete(&self, object: StoredObject) -> DeleteFuture {
            self.objects.lock().unwrap().remove(&object.uri);
            Box::pin(async { Ok(()) })
        }
    }

    impl EncryptedBytesDownloadBackend for MemoryStore {
        fn resolve_url(&self, object: &ObjectMeta) -> UrlTicket {
            self.resolves.fetch_add(1, Ordering::Relaxed);
            let uri = object.uri.clone();
            Box::pin(async move {
                Ok(SignedUrl::new(
                    uri,
                    Some(std::time::SystemTime::now() + Duration::from_secs(3600)),
                ))
            })
        }

        fn download(
            &self,
            object: &ObjectMeta,
            _url: SignedUrl,
            bytes: Range<u64>,
        ) -> EncryptedByteStream {
            let data = self.objects.lock().unwrap()[&object.uri]
                [bytes.start as usize..bytes.end as usize]
                .to_vec();
            Box::pin(stream::once(async move { Ok(bytes::Bytes::from(data)) }))
        }
    }

    impl UploadBackend for MemoryUpload {
        fn max_frames_per_segment(&self) -> u32 {
            100
        }

        fn upload(
            &self,
            object: UploadObject,
            _hint: Option<u32>,
            mut frames: UploadByteStream,
        ) -> UploadFuture {
            Box::pin(async move {
                while let Some(frame) = frames.next().await {
                    frame?;
                }
                Ok(StoredObject {
                    uri: format!("memory://{}", object.id.as_str()),
                    cached_url: None,
                })
            })
        }

        fn delete(&self, object: StoredObject) -> DeleteFuture {
            self.deleted.lock().unwrap().push(object.uri);
            Box::pin(async { Ok(()) })
        }
    }

    async fn app(max_file_size: u64) -> (Router, Arc<MemoryUpload>) {
        let catalog = Catalog::connect("sqlite::memory:").await.unwrap();
        let backend = Arc::new(MemoryUpload::default());
        let rate = ByteRate::new(1_000_000.0).unwrap();
        let stream_config = ByteStreamConfig::new(
            24,
            rate,
            ByteTransferModel {
                object_rate: rate,
                data_ttfb: Duration::from_millis(1),
                url_latency: Duration::from_millis(1),
                frames_per_object: 100,
            },
        )
        .unwrap();
        let state = AppState::new(
            catalog,
            backend.clone(),
            Arc::new(NoDownload),
            FrameBudget::new(100).unwrap(),
            stream_config,
            rate,
            ServerConfig {
                frame_size: 24,
                max_file_size,
            },
        );
        (router(state), backend)
    }

    async fn app_with_store() -> (Router, Arc<MemoryStore>) {
        let catalog = Catalog::connect("sqlite::memory:").await.unwrap();
        let store = Arc::new(MemoryStore::default());
        let upload = Arc::new(StreamUploadBackend::new(store.clone(), 24).unwrap());
        let rate = ByteRate::new(1_000_000.0).unwrap();
        let stream_config = ByteStreamConfig::new(
            24,
            rate,
            ByteTransferModel {
                object_rate: rate,
                data_ttfb: Duration::from_millis(1),
                url_latency: Duration::from_millis(1),
                frames_per_object: 100,
            },
        )
        .unwrap();
        let state = AppState::new(
            catalog,
            upload,
            store.clone(),
            FrameBudget::new(100).unwrap(),
            stream_config,
            rate,
            ServerConfig {
                frame_size: 24,
                max_file_size: 100,
            },
        );
        (router(state), store)
    }

    async fn create(app: &Router, expected_size: u64) -> (StatusCode, serde_json::Value) {
        let response = app
            .clone()
            .oneshot(
                Request::post("/files")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(format!(
                        r#"{{"name":"test.bin","content_type":"text/plain","expected_size":{expected_size}}}"#
                    )))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let value =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        (status, value)
    }

    #[tokio::test]
    async fn creates_uploads_and_completes_a_file() {
        let (app, _) = app(100).await;
        let (status, file) = create(&app, 20).await;
        assert_eq!(status, StatusCode::CREATED);
        let id = file["id"].as_str().unwrap();

        let response = app
            .clone()
            .oneshot(
                Request::put(format!("/files/{id}/segments/0"))
                    .body(Body::from("hello"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        let response = app
            .clone()
            .oneshot(
                Request::post(format!("/files/{id}/complete"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(body["size"], 5);
    }

    #[tokio::test]
    async fn enforces_file_limit_and_immutable_indices() {
        let (app, backend) = app(10).await;
        assert_eq!(create(&app, 11).await.0, StatusCode::PAYLOAD_TOO_LARGE);
        let (_, file) = create(&app, 3).await;
        let id = file["id"].as_str().unwrap();
        let request = || {
            Request::put(format!("/files/{id}/segments/0"))
                .body(Body::from("abc"))
                .unwrap()
        };
        assert_eq!(
            app.clone().oneshot(request()).await.unwrap().status(),
            StatusCode::CREATED
        );
        assert_eq!(
            app.clone().oneshot(request()).await.unwrap().status(),
            StatusCode::CONFLICT
        );

        let overflow = Request::put(format!("/files/{id}/segments/1"))
            .body(Body::from("x"))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(overflow).await.unwrap().status(),
            StatusCode::CONFLICT
        );
        assert_eq!(backend.deleted.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn rejects_gaps_and_misaligned_middle_segments() {
        let (app, _) = app(100).await;
        let (_, file) = create(&app, 100).await;
        let id = file["id"].as_str().unwrap();
        let upload = |index, body| {
            Request::put(format!("/files/{id}/segments/{index}"))
                .body(Body::from(body))
                .unwrap()
        };
        assert_eq!(
            app.clone()
                .oneshot(upload(1, "abcdefgh"))
                .await
                .unwrap()
                .status(),
            StatusCode::CREATED
        );
        assert_eq!(
            app.clone()
                .oneshot(
                    Request::post(format!("/files/{id}/complete"))
                        .body(Body::empty())
                        .unwrap()
                )
                .await
                .unwrap()
                .status(),
            StatusCode::CONFLICT
        );

        assert_eq!(
            app.clone()
                .oneshot(upload(0, "short"))
                .await
                .unwrap()
                .status(),
            StatusCode::CREATED
        );
        assert_eq!(
            app.clone()
                .oneshot(
                    Request::post(format!("/files/{id}/complete"))
                        .body(Body::empty())
                        .unwrap()
                )
                .await
                .unwrap()
                .status(),
            StatusCode::CONFLICT
        );
    }

    #[tokio::test]
    async fn serializes_concurrent_allocation_and_blocks_finalize_during_upload() {
        let catalog = Catalog::connect("sqlite::memory:").await.unwrap();
        catalog
            .create_file("pending", "pending.bin", "application/octet-stream", 8)
            .await
            .unwrap();
        catalog.reserve_segment("pending", 0).await.unwrap();
        assert!(matches!(
            catalog.complete_file("pending", 8).await,
            Err(CatalogError::UploadInProgress)
        ));

        let (app, backend) = app(8).await;
        let (_, file) = create(&app, 8).await;
        let id = file["id"].as_str().unwrap().to_owned();
        let first = app.clone().oneshot(
            Request::put(format!("/files/{id}/segments/0"))
                .body(Body::from("abcdefgh"))
                .unwrap(),
        );
        let second = app.clone().oneshot(
            Request::put(format!("/files/{id}/segments/1"))
                .body(Body::from("abcdefgh"))
                .unwrap(),
        );
        let (first, second) = tokio::join!(first, second);
        let statuses = [first.unwrap().status(), second.unwrap().status()];
        assert_eq!(statuses.iter().filter(|&&status| status == StatusCode::CREATED).count(), 1);
        assert_eq!(statuses.iter().filter(|&&status| status == StatusCode::CONFLICT).count(), 1);
        assert_eq!(backend.deleted.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn downloads_full_file_ranges_and_reuses_cached_urls() {
        let (app, store) = app_with_store().await;
        let (_, file) = create(&app, 20).await;
        let id = file["id"].as_str().unwrap();
        for (index, body) in [(0, "abcdefgh"), (1, "ijk")] {
            let response = app
                .clone()
                .oneshot(
                    Request::put(format!("/files/{id}/segments/{index}"))
                        .body(Body::from(body))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::CREATED);
        }
        assert_eq!(
            app.clone()
                .oneshot(
                    Request::post(format!("/files/{id}/complete"))
                        .body(Body::empty())
                        .unwrap()
                )
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );

        let response = app
            .clone()
            .oneshot(
                Request::get(format!("/files/{id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[header::CONTENT_TYPE], "text/plain");
        assert_eq!(
            response.headers()[header::CONTENT_DISPOSITION],
            "attachment; filename=\"test.bin\"; filename*=UTF-8''test.bin"
        );
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            "abcdefghijk"
        );

        let response = app
            .clone()
            .oneshot(
                Request::get(format!("/files/{id}"))
                    .header(header::RANGE, "bytes=2-8")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(response.headers()[header::CONTENT_RANGE], "bytes 2-8/11");
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            "cdefghi"
        );

        let response = app
            .clone()
            .oneshot(
                Request::get(format!("/files/{id}"))
                    .header(header::RANGE, "bytes=-3")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            "ijk"
        );
        assert_eq!(store.resolves.load(Ordering::Relaxed), 2);

        let response = app
            .clone()
            .oneshot(
                Request::get(format!("/files/{id}"))
                    .header(header::RANGE, "bytes=99-")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
        assert_eq!(response.headers()[header::CONTENT_RANGE], "bytes */11");
    }
}
