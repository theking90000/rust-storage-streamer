//! Production HTTP server: assembles the Discord store with the files gateway.

mod config;

use std::time::Duration;

use axum::response::Html;
use axum::routing::get;
use files_gateway::{AppState, Catalog, ServerConfig, router};
use frame_streamer::{BoxError, ByteRate, ByteStreamConfig, ByteTransferModel, FrameBudget};
use tower_http::cors::{Any, CorsLayer};

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let cfg = config::resolve()?;

    let webhooks = discord_store::load_webhooks(&cfg.webhooks_file).await?;
    if webhooks.is_empty() {
        return Err(BoxError::from(format!(
            "{} contained no webhooks",
            cfg.webhooks_file.display()
        )));
    }
    let storage =
        discord_store::create_with_proxy(webhooks, cfg.frame_size, cfg.proxy_url.as_deref())?;
    let frames_per_object = storage.upload.max_frames_per_segment();

    let catalog = Catalog::connect(&cfg.database_url).await?;
    let rate = ByteRate::new(cfg.target_rate)?;
    let stream_config = ByteStreamConfig::new(
        cfg.frame_size,
        rate,
        ByteTransferModel {
            object_rate: ByteRate::new(cfg.object_rate)?,
            data_ttfb: Duration::from_millis(cfg.data_ttfb_ms),
            url_latency: Duration::from_millis(cfg.url_latency_ms),
            frames_per_object,
        },
    )?;
    let state = AppState::new(
        catalog,
        storage,
        FrameBudget::new(cfg.frame_budget)?,
        stream_config,
        rate,
        ServerConfig {
            frame_size: cfg.frame_size,
            max_file_size: cfg.max_file_size,
        },
    );

    let listener = tokio::net::TcpListener::bind(&cfg.bind).await?;
    eprintln!(
        "streamer-files-discord listening on {}",
        listener.local_addr()?
    );
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);
    let app = router(state)
        .route(
            "/",
            get(|| async { Html(include_str!("upload-test.html")) }),
        )
        .route(
            "/robots.txt",
            get(|| async { "User-agent: *\nDisallow: /\n" }),
        )
        .layer(cors);
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            tokio::signal::ctrl_c().await.ok();
        })
        .await?;
    Ok(())
}
