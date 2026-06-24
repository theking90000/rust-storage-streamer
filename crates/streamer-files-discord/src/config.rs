//! Layered configuration with precedence **env > CLI > TOML**.
//!
//! Each source is parsed into the same all-`Option` [`Partial`]; they are then
//! merged so an earlier source's `Some` wins, falling back to [`Partial::defaults`].

use std::path::PathBuf;
use std::str::FromStr;

use clap::Parser;
use frame_streamer::BoxError;
use serde::Deserialize;

/// Fully resolved configuration handed to `main`.
pub struct Config {
    pub bind: String,
    pub database_url: String,
    pub webhooks_file: PathBuf,
    pub proxy_urls: Vec<String>,
    pub frame_size: usize,
    pub max_file_size: u64,
    pub target_rate: f64,
    pub object_rate: f64,
    pub data_ttfb_ms: u64,
    pub url_latency_ms: u64,
    pub frame_budget: usize,
}

/// A partial config: every field optional so sources can be merged by precedence.
#[derive(Default, Deserialize)]
#[serde(default)]
struct Partial {
    bind: Option<String>,
    database_url: Option<String>,
    webhooks_file: Option<PathBuf>,
    proxy_urls: Option<Vec<String>>,
    proxy_url: Option<String>,
    frame_size: Option<usize>,
    max_file_size: Option<u64>,
    target_rate: Option<f64>,
    object_rate: Option<f64>,
    data_ttfb_ms: Option<u64>,
    url_latency_ms: Option<u64>,
    frame_budget: Option<usize>,
}

/// CLI layer. Mirrors [`Partial`] plus `--config` for the TOML file path. We do
/// NOT use clap's `env` attribute — it would make CLI override env, the opposite
/// of the wanted env > CLI order; env is read separately in [`Partial::from_env`].
#[derive(Parser)]
#[command(about = "Files gateway backed by Discord webhooks")]
struct Cli {
    /// Path to a TOML config file (lowest precedence).
    #[arg(long)]
    config: Option<PathBuf>,
    #[arg(long)]
    bind: Option<String>,
    #[arg(long)]
    database_url: Option<String>,
    #[arg(long)]
    webhooks_file: Option<PathBuf>,
    #[arg(long = "proxy-url")]
    proxy_url: Vec<String>,
    #[arg(long)]
    frame_size: Option<usize>,
    #[arg(long)]
    max_file_size: Option<u64>,
    #[arg(long)]
    target_rate: Option<f64>,
    #[arg(long)]
    object_rate: Option<f64>,
    #[arg(long)]
    data_ttfb_ms: Option<u64>,
    #[arg(long)]
    url_latency_ms: Option<u64>,
    #[arg(long)]
    frame_budget: Option<usize>,
}

impl Cli {
    fn into_partial(self) -> Partial {
        Partial {
            bind: self.bind,
            database_url: self.database_url,
            webhooks_file: self.webhooks_file,
            proxy_urls: (!self.proxy_url.is_empty()).then_some(self.proxy_url),
            proxy_url: None,
            frame_size: self.frame_size,
            max_file_size: self.max_file_size,
            target_rate: self.target_rate,
            object_rate: self.object_rate,
            data_ttfb_ms: self.data_ttfb_ms,
            url_latency_ms: self.url_latency_ms,
            frame_budget: self.frame_budget,
        }
    }
}

impl Partial {
    fn from_env() -> Self {
        // ponytail: unparseable env values fall through to the next layer rather
        // than erroring; tighten to hard-fail if a silent typo ever bites.
        Self {
            bind: env_str("STREAMER_BIND"),
            database_url: env_str("STREAMER_DATABASE_URL"),
            webhooks_file: env_str("DISCORD_WEBHOOKS_FILE").map(PathBuf::from),
            proxy_urls: env_list("DISCORD_PROXY_URL"),
            proxy_url: None,
            frame_size: env_parse("STREAMER_FRAME_SIZE"),
            max_file_size: env_parse("FILES_MAX_FILE_SIZE"),
            target_rate: env_parse("STREAMER_TARGET_RATE"),
            object_rate: env_parse("STREAMER_OBJECT_RATE"),
            data_ttfb_ms: env_parse("STREAMER_DATA_TTFB_MS"),
            url_latency_ms: env_parse("STREAMER_URL_LATENCY_MS"),
            frame_budget: env_parse("STREAMER_FRAME_BUDGET"),
        }
    }

    fn from_toml(path: &PathBuf) -> Result<Self, BoxError> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| BoxError::from(format!("reading config {}: {e}", path.display())))?;
        toml::from_str(&text)
            .map_err(|e| BoxError::from(format!("parsing config {}: {e}", path.display())))
    }

    /// Fills each `None` field from `other`. `self` therefore has precedence.
    fn or(self, other: Partial) -> Partial {
        Partial {
            bind: self.bind.or(other.bind),
            database_url: self.database_url.or(other.database_url),
            webhooks_file: self.webhooks_file.or(other.webhooks_file),
            proxy_urls: self.proxy_urls.or(other.proxy_urls),
            proxy_url: self.proxy_url.or(other.proxy_url),
            frame_size: self.frame_size.or(other.frame_size),
            max_file_size: self.max_file_size.or(other.max_file_size),
            target_rate: self.target_rate.or(other.target_rate),
            object_rate: self.object_rate.or(other.object_rate),
            data_ttfb_ms: self.data_ttfb_ms.or(other.data_ttfb_ms),
            url_latency_ms: self.url_latency_ms.or(other.url_latency_ms),
            frame_budget: self.frame_budget.or(other.frame_budget),
        }
    }

    /// Lowest layer. Streaming-model values are calibration knobs against real
    /// Discord — overridable by any source. `webhooks_file` has no default: it
    /// is the one required field.
    fn defaults() -> Partial {
        Partial {
            bind: Some("0.0.0.0:8080".to_owned()),
            database_url: Some("sqlite:catalog.db?mode=rwc".to_owned()),
            webhooks_file: None,
            proxy_urls: None,
            proxy_url: None,
            frame_size: Some(1 << 16),
            max_file_size: Some(20 * 1024 * 1024 * 1024),
            target_rate: Some(60_000_000.0),
            object_rate: Some(60_000_000.0),
            data_ttfb_ms: Some(100),
            url_latency_ms: Some(400),
            frame_budget: Some(415),
        }
    }
}

/// Parses all three sources and merges them in precedence order env > CLI > TOML.
pub fn resolve() -> Result<Config, BoxError> {
    let cli = Cli::parse();
    let env = Partial::from_env();
    // The TOML path itself follows the same env > CLI precedence.
    let config_path = env_str("FILES_CONFIG")
        .map(PathBuf::from)
        .or_else(|| cli.config.clone());
    let toml = match config_path {
        Some(path) => Partial::from_toml(&path)?,
        None => Partial::default(),
    };

    let merged = env.or(cli.into_partial()).or(toml).or(Partial::defaults());
    let proxy_urls = merged
        .proxy_urls
        .or_else(|| merged.proxy_url.map(|proxy_url| vec![proxy_url]))
        .unwrap_or_default();

    // `defaults()` supplies every field except `webhooks_file`, so the rest unwrap safely.
    Ok(Config {
        webhooks_file: merged.webhooks_file.ok_or_else(|| {
            BoxError::from(
                "webhooks_file is required (DISCORD_WEBHOOKS_FILE, --webhooks-file, or webhooks_file in TOML)",
            )
        })?,
        proxy_urls,
        bind: merged.bind.unwrap(),
        database_url: merged.database_url.unwrap(),
        frame_size: merged.frame_size.unwrap(),
        max_file_size: merged.max_file_size.unwrap(),
        target_rate: merged.target_rate.unwrap(),
        object_rate: merged.object_rate.unwrap(),
        data_ttfb_ms: merged.data_ttfb_ms.unwrap(),
        url_latency_ms: merged.url_latency_ms.unwrap(),
        frame_budget: merged.frame_budget.unwrap(),
    })
}

fn env_str(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|value| !value.is_empty())
}

fn env_list(key: &str) -> Option<Vec<String>> {
    env_str(key).and_then(|value| split_list(&value))
}

fn split_list(value: &str) -> Option<Vec<String>> {
    let values: Vec<_> = value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect();
    (!values.is_empty()).then_some(values)
}

fn env_parse<T: FromStr>(key: &str) -> Option<T> {
    env_str(key).and_then(|value| value.parse().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn partial(bind: &str, frame_size: Option<usize>) -> Partial {
        Partial {
            bind: Some(bind.to_owned()),
            frame_size,
            ..Partial::default()
        }
    }

    #[test]
    fn earlier_source_wins_and_unique_fields_survive() {
        let env = partial("env", None);
        let cli = partial("cli", None);
        // frame_size only set by the lowest layer -> it survives the merge.
        let toml = Partial {
            frame_size: Some(42),
            ..partial("toml", None)
        };
        let merged = env.or(cli).or(toml).or(Partial::defaults());
        assert_eq!(merged.bind.unwrap(), "env");
        assert_eq!(merged.frame_size.unwrap(), 42);
    }

    #[test]
    fn cli_proxy_url_is_repeatable() {
        let cli = Cli::try_parse_from(["cmd", "--proxy-url", "a", "--proxy-url", "b"]).unwrap();
        assert_eq!(cli.into_partial().proxy_urls.unwrap(), vec!["a", "b"]);
    }

    #[test]
    fn env_proxy_url_accepts_comma_separated_values() {
        assert_eq!(
            split_list("http://a, socks5h://b ,,").unwrap(),
            vec!["http://a", "socks5h://b"]
        );
    }

    #[test]
    fn toml_accepts_proxy_urls_and_legacy_proxy_url() {
        let list: Partial = toml::from_str("proxy_urls = [\"a\", \"b\"]").unwrap();
        assert_eq!(list.proxy_urls.unwrap(), vec!["a", "b"]);

        let legacy: Partial = toml::from_str("proxy_url = \"a\"").unwrap();
        let proxy_urls = legacy
            .proxy_urls
            .or_else(|| legacy.proxy_url.map(|proxy_url| vec![proxy_url]))
            .unwrap_or_default();
        assert_eq!(proxy_urls, vec!["a"]);
    }
}
