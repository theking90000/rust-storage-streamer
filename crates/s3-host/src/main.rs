use std::path::PathBuf;
use std::time::Duration;

use axum::Router;
use axum::error_handling::HandleError;
use axum::http::{Response, StatusCode};
use clap::{Parser, Subcommand};
use frame_streamer::{BoxError, ByteRate, ByteStreamConfig, ByteTransferModel, FrameBudget};
use s3_router::{Catalog, DatabaseAuth, S3Storage};
use s3s::service::S3ServiceBuilder;
use s3s::{Body, HttpError};

const FRAMES_PER_OBJECT: u32 = 150;

#[derive(Parser)]
#[command(about = "S3-compatible encrypted Discord storage")]
struct Cli {
    #[arg(
        long,
        env = "S3_DATABASE_URL",
        default_value = "sqlite:s3-catalog.db?mode=rwc"
    )]
    database_url: String,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Serve {
        #[arg(long, env = "S3_BIND", default_value = "0.0.0.0:8080")]
        bind: String,
        #[arg(long, env = "DH_WEBHOOKS_FILE")]
        webhooks_file: PathBuf,
        #[arg(long, env = "DH_PROXY_URL")]
        proxy_url: Option<String>,
        #[arg(long, env = "S3_FRAME_SIZE", default_value_t = 1 << 16)]
        frame_size: usize,
        #[arg(long, env = "S3_MAX_OBJECT_SIZE", default_value_t = 20 * 1024 * 1024 * 1024_u64)]
        max_object_size: u64,
        #[arg(long, env = "S3_TARGET_RATE", default_value_t = 60_000_000.0)]
        target_rate: f64,
        #[arg(long, env = "S3_OBJECT_RATE", default_value_t = 60_000_000.0)]
        object_rate: f64,
        #[arg(long, env = "S3_DATA_TTFB_MS", default_value_t = 100)]
        data_ttfb_ms: u64,
        #[arg(long, env = "S3_URL_LATENCY_MS", default_value_t = 0)]
        url_latency_ms: u64,
        #[arg(long, env = "S3_FRAME_BUDGET", default_value_t = 415)]
        frame_budget: usize,
    },
    Credential {
        #[command(subcommand)]
        command: CredentialCommand,
    },
}

#[derive(Subcommand)]
enum CredentialCommand {
    Create {
        #[arg(long)]
        can_create_buckets: bool,
    },
    Revoke {
        access_key: String,
    },
    Grant {
        access_key: String,
        bucket: String,
        #[arg(long)]
        read_only: bool,
    },
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let cli = Cli::parse();
    let catalog = Catalog::connect(&cli.database_url).await?;
    match cli.command {
        Command::Credential { command } => credential(catalog, command).await,
        Command::Serve {
            bind,
            webhooks_file,
            proxy_url,
            frame_size,
            max_object_size,
            target_rate,
            object_rate,
            data_ttfb_ms,
            url_latency_ms,
            frame_budget,
        } => {
            serve(
                catalog,
                bind,
                webhooks_file,
                proxy_url,
                frame_size,
                max_object_size,
                target_rate,
                object_rate,
                data_ttfb_ms,
                url_latency_ms,
                frame_budget,
            )
            .await
        }
    }
}

async fn credential(catalog: Catalog, command: CredentialCommand) -> Result<(), BoxError> {
    match command {
        CredentialCommand::Create { can_create_buckets } => {
            let access_key = format!("SS{}", uuid::Uuid::new_v4().simple()).to_uppercase();
            let secret_key = hex::encode(rand::random::<[u8; 32]>());
            catalog
                .create_credential(&access_key, &secret_key, can_create_buckets)
                .await?;
            println!("access_key={access_key}\nsecret_key={secret_key}");
        }
        CredentialCommand::Revoke { access_key } => {
            if !catalog.revoke_credential(&access_key).await? {
                return Err("credential not found".into());
            }
        }
        CredentialCommand::Grant {
            access_key,
            bucket,
            read_only,
        } => {
            catalog.grant(&access_key, &bucket, !read_only).await?;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn serve(
    catalog: Catalog,
    bind: String,
    webhooks_file: PathBuf,
    proxy_url: Option<String>,
    frame_size: usize,
    max_object_size: u64,
    target_rate: f64,
    object_rate: f64,
    data_ttfb_ms: u64,
    url_latency_ms: u64,
    frame_budget: usize,
) -> Result<(), BoxError> {
    let webhooks = discord::load_webhooks(&webhooks_file).await?;
    if webhooks.is_empty() {
        return Err(format!("{} contained no webhooks", webhooks_file.display()).into());
    }
    let backends =
        discord::create_discord_backend_with_proxy(webhooks, frame_size, proxy_url.as_deref())?;
    let rate = ByteRate::new(target_rate)?;
    let stream_config = ByteStreamConfig::new(
        frame_size,
        rate,
        ByteTransferModel {
            object_rate: ByteRate::new(object_rate)?,
            data_ttfb: Duration::from_millis(data_ttfb_ms),
            url_latency: Duration::from_millis(url_latency_ms),
            frames_per_object: FRAMES_PER_OBJECT,
        },
    )?;
    let storage = S3Storage::new(
        catalog.clone(),
        backends.upload_backend,
        backends.download_backend,
        FrameBudget::new(frame_budget)?,
        stream_config,
        rate,
        frame_size,
        max_object_size,
    );
    let _ = storage.collect_garbage().await;
    let garbage_collector = storage.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(3600)).await;
            let _ = garbage_collector.collect_garbage().await;
        }
    });
    let mut builder = S3ServiceBuilder::new(storage);
    builder.set_auth(DatabaseAuth::new(catalog));
    let service = HandleError::new(builder.build(), handle_s3_error);
    let app = Router::new().fallback_service(service);
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    eprintln!("s3-host listening on http://{}", listener.local_addr()?);
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            tokio::signal::ctrl_c().await.ok();
        })
        .await?;
    Ok(())
}

async fn handle_s3_error(error: HttpError) -> Response<Body> {
    eprintln!("S3 HTTP error: {error:?}");
    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .body(Body::from("Internal Server Error".to_owned()))
        .unwrap()
}
