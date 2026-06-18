//! Production HTTP server: assembles the Discord storage backend with the
//! `file-router` Axum component and serves it.

mod config;

use std::time::Duration;

use file_router::{AppState, Catalog, ServerConfig, router};
use frame_streamer::{
    BoxError, ByteRate, ByteStreamConfig, ByteTransferModel, FrameBudget,
};

/// Discord caps each object at 150 physical frames; must match the upload backend.
const FRAMES_PER_OBJECT: u32 = 150;

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let cfg = config::resolve()?;

    let webhooks = discord::load_webhooks(&cfg.webhooks_file).await?;
    if webhooks.is_empty() {
        return Err(BoxError::from(format!(
            "{} contained no webhooks",
            cfg.webhooks_file.display()
        )));
    }
    let backends = discord::create_discord_backend(webhooks, cfg.frame_size)?;

    let catalog = Catalog::connect(&cfg.database_url).await?;
    let rate = ByteRate::new(cfg.target_rate)?;
    let stream_config = ByteStreamConfig::new(
        cfg.frame_size,
        rate,
        ByteTransferModel {
            object_rate: ByteRate::new(cfg.object_rate)?,
            data_ttfb: Duration::from_millis(cfg.data_ttfb_ms),
            url_latency: Duration::from_millis(cfg.url_latency_ms),
            frames_per_object: FRAMES_PER_OBJECT,
        },
    )?;
    let state = AppState::new(
        catalog,
        backends.upload_backend,
        backends.download_backend,
        FrameBudget::new(cfg.frame_budget)?,
        stream_config,
        rate,
        ServerConfig {
            frame_size: cfg.frame_size,
            max_file_size: cfg.max_file_size,
        },
    );

    let listener = tokio::net::TcpListener::bind(&cfg.bind).await?;
    eprintln!("discord-host listening on {}", listener.local_addr()?);
    axum::serve(listener, router(state))
        .with_graceful_shutdown(async {
            tokio::signal::ctrl_c().await.ok();
        })
        .await?;
    Ok(())
}
