mod catalog;

use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{post, put};
use axum::{Json, Router};
use frame_streamer::{ByteUpload, DecryptKey, ObjectId, UploadBackend, UploadError, UploadObject};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub use catalog::{Catalog, CatalogError};

#[derive(Clone, Copy, Debug)]
pub struct ServerConfig {
    pub frame_size: usize,
    pub max_file_size: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self { frame_size: 1 << 16, max_file_size: 20 * 1024 * 1024 * 1024 }
    }
}

#[derive(Clone)]
pub struct AppState {
    pub catalog: Catalog,
    pub upload_backend: Arc<dyn UploadBackend>,
    pub config: ServerConfig,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/files", post(create_file))
        .route("/files/{id}/segments/{index}", put(upload_segment))
        .route("/files/{id}/complete", post(complete_file))
        .with_state(state)
}

#[derive(Deserialize)]
struct CreateFile { expected_size: u64 }

#[derive(Serialize)]
struct FileCreated { id: String, expected_size: u64 }

async fn create_file(
    State(state): State<AppState>,
    Json(request): Json<CreateFile>,
) -> Result<impl IntoResponse, ApiError> {
    if request.expected_size > state.config.max_file_size {
        return Err(ApiError::new(StatusCode::PAYLOAD_TOO_LARGE, "file allocation exceeds server limit"));
    }
    let id = Uuid::new_v4().to_string();
    state.catalog.create_file(&id, request.expected_size).await?;
    Ok((StatusCode::CREATED, Json(FileCreated { id, expected_size: request.expected_size })))
}

#[derive(Serialize)]
struct SegmentCreated { id: String, size: u64, frame_count: u32, checksum: String }

async fn upload_segment(
    State(state): State<AppState>,
    Path((file_id, index)): Path<(String, u32)>,
    headers: HeaderMap,
    body: Body,
) -> Result<impl IntoResponse, ApiError> {
    state.catalog.reserve_segment(&file_id, index).await?;
    let segment_id = Uuid::new_v4().to_string();
    let key: [u8; 32] = rand::random();
    let content_length = headers.get(axum::http::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok());
    let uploader = ByteUpload::new(state.upload_backend.clone(), state.config.frame_size)
        .map_err(ApiError::internal)?;
    let result = uploader.upload(
        UploadObject { id: ObjectId::new(&segment_id), key: DecryptKey::new(key) },
        body.into_data_stream(),
        content_length,
    ).await;
    let result = match result {
        Ok(result) => result,
        Err(error) => {
            state.catalog.cancel_reservation(&file_id, index).await;
            return Err(ApiError::upload(error));
        }
    };
    if let Err(error) = state.catalog.attach_segment(&file_id, index, &segment_id, &key, &result).await {
        let _ = state.upload_backend.delete(result.stored_object.clone()).await;
        state.catalog.cancel_reservation(&file_id, index).await;
        return Err(error.into());
    }
    Ok((StatusCode::CREATED, Json(SegmentCreated {
        id: segment_id,
        size: result.plaintext_size,
        frame_count: result.frame_count,
        checksum: hex(&result.checksum),
    })))
}

#[derive(Serialize)]
struct FileCompleted { id: String, size: u64, completed_at: i64 }

async fn complete_file(
    State(state): State<AppState>,
    Path(file_id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let payload_size = state.config.frame_size.checked_sub(frame_streamer::TAG_SIZE)
        .ok_or_else(|| ApiError::internal("invalid frame size"))?;
    let (size, completed_at) = state.catalog.complete_file(&file_id, payload_size).await?;
    Ok(Json(FileCompleted { id: file_id, size, completed_at }))
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
struct ApiError { status: StatusCode, message: String }

impl ApiError {
    fn new(status: StatusCode, message: impl Into<String>) -> Self { Self { status, message: message.into() } }
    fn internal(error: impl std::fmt::Display) -> Self { Self::new(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()) }
    fn upload(error: frame_streamer::BoxError) -> Self {
        let status = match error.downcast_ref::<UploadError>() {
            Some(UploadError::TooLarge { .. }) => StatusCode::PAYLOAD_TOO_LARGE,
            Some(UploadError::LengthMismatch { .. } | UploadError::Empty) => StatusCode::BAD_REQUEST,
            _ => StatusCode::BAD_GATEWAY,
        };
        Self::new(status, error.to_string())
    }
}

impl From<CatalogError> for ApiError {
    fn from(error: CatalogError) -> Self {
        let status = match error {
            CatalogError::NotFound => StatusCode::NOT_FOUND,
            CatalogError::Database(_) => StatusCode::INTERNAL_SERVER_ERROR,
            _ => StatusCode::CONFLICT,
        };
        Self::new(status, error.to_string())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(serde_json::json!({ "error": self.message }))).into_response()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use axum::http::{Request, header};
    use frame_streamer::{DeleteFuture, StoredObject, UploadByteStream, UploadFuture};
    use futures_util::StreamExt;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use super::*;

    #[derive(Default)]
    struct MemoryUpload {
        deleted: Mutex<Vec<String>>,
    }

    impl UploadBackend for MemoryUpload {
        fn max_frames_per_segment(&self) -> u32 { 100 }

        fn upload(
            &self,
            object: UploadObject,
            _hint: Option<u32>,
            mut frames: UploadByteStream,
        ) -> UploadFuture {
            Box::pin(async move {
                while let Some(frame) = frames.next().await { frame?; }
                Ok(StoredObject { uri: format!("memory://{}", object.id.as_str()), cached_url: None })
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
        let state = AppState {
            catalog,
            upload_backend: backend.clone(),
            config: ServerConfig { frame_size: 24, max_file_size },
        };
        (router(state), backend)
    }

    async fn create(app: &Router, expected_size: u64) -> (StatusCode, serde_json::Value) {
        let response = app.clone().oneshot(
            Request::post("/files")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(format!(r#"{{"expected_size":{expected_size}}}"#)))
                .unwrap(),
        ).await.unwrap();
        let status = response.status();
        let value = serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
        (status, value)
    }

    #[tokio::test]
    async fn creates_uploads_and_completes_a_file() {
        let (app, _) = app(100).await;
        let (status, file) = create(&app, 20).await;
        assert_eq!(status, StatusCode::CREATED);
        let id = file["id"].as_str().unwrap();

        let response = app.clone().oneshot(
            Request::put(format!("/files/{id}/segments/0"))
                .body(Body::from("hello"))
                .unwrap(),
        ).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        let response = app.clone().oneshot(
            Request::post(format!("/files/{id}/complete"))
                .body(Body::empty())
                .unwrap(),
        ).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value = serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
        assert_eq!(body["size"], 5);
    }

    #[tokio::test]
    async fn enforces_file_limit_and_immutable_indices() {
        let (app, backend) = app(10).await;
        assert_eq!(create(&app, 11).await.0, StatusCode::PAYLOAD_TOO_LARGE);
        let (_, file) = create(&app, 3).await;
        let id = file["id"].as_str().unwrap();
        let request = || Request::put(format!("/files/{id}/segments/0"))
            .body(Body::from("abc"))
            .unwrap();
        assert_eq!(app.clone().oneshot(request()).await.unwrap().status(), StatusCode::CREATED);
        assert_eq!(app.clone().oneshot(request()).await.unwrap().status(), StatusCode::CONFLICT);

        let overflow = Request::put(format!("/files/{id}/segments/1"))
            .body(Body::from("x"))
            .unwrap();
        assert_eq!(app.clone().oneshot(overflow).await.unwrap().status(), StatusCode::CONFLICT);
        assert_eq!(backend.deleted.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn rejects_gaps_and_misaligned_middle_segments() {
        let (app, _) = app(100).await;
        let (_, file) = create(&app, 100).await;
        let id = file["id"].as_str().unwrap();
        let upload = |index, body| Request::put(format!("/files/{id}/segments/{index}"))
            .body(Body::from(body)).unwrap();
        assert_eq!(app.clone().oneshot(upload(1, "abcdefgh")).await.unwrap().status(), StatusCode::CREATED);
        assert_eq!(app.clone().oneshot(Request::post(format!("/files/{id}/complete")).body(Body::empty()).unwrap()).await.unwrap().status(), StatusCode::CONFLICT);

        assert_eq!(app.clone().oneshot(upload(0, "short")).await.unwrap().status(), StatusCode::CREATED);
        assert_eq!(app.clone().oneshot(Request::post(format!("/files/{id}/complete")).body(Body::empty()).unwrap()).await.unwrap().status(), StatusCode::CONFLICT);
    }
}
