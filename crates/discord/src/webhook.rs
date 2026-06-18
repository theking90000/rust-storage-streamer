use std::sync::atomic::{AtomicBool, AtomicU64};

use frame_streamer::BoxError;

/// A Discord webhook credential. The input list is assumed very large
/// (5 000–10 000 entries); only `id` and `token` are needed to address it.
#[derive(Clone, Debug)]
pub struct Webhook {
    pub id: String,
    pub token: String,
}

/// Runtime state for one webhook in the registry.
pub(crate) struct WebhookSlot {
    pub id: String,
    pub token: String,
    /// Number of uploads dispatched through this webhook (drives uniform spread).
    pub used: AtomicU64,
    /// Set once the webhook is detected as deleted / unauthorized.
    pub dead: AtomicBool,
}

impl WebhookSlot {
    pub fn new(webhook: Webhook) -> Self {
        Self {
            id: webhook.id,
            token: webhook.token,
            used: AtomicU64::new(0),
            dead: AtomicBool::new(false),
        }
    }
}

/// Parts of a `discord://{id}/{token}/{message_id}` object URI.
pub(crate) struct ParsedUri {
    pub id: String,
    pub token: String,
    pub message_id: String,
}

/// Builds the self-describing object URI. The webhook id+token+message id are
/// everything needed to rebuild the REST message URL on download.
pub(crate) fn format_uri(id: &str, token: &str, message_id: &str) -> String {
    format!("discord://{id}/{token}/{message_id}")
}

/// Parses a `discord://{id}/{token}/{message_id}` URI. Webhook ids and message
/// ids are numeric snowflakes and tokens contain no `/`, so a plain split is safe.
pub(crate) fn parse_uri(uri: &str) -> Result<ParsedUri, BoxError> {
    let rest = uri
        .strip_prefix("discord://")
        .ok_or_else(|| BoxError::from(format!("not a discord uri: {uri}")))?;
    let parts: Vec<&str> = rest.split('/').collect();
    if parts.len() != 3 || parts.iter().any(|part| part.is_empty()) {
        return Err(BoxError::from(format!("malformed discord uri: {uri}")));
    }
    Ok(ParsedUri {
        id: parts[0].to_owned(),
        token: parts[1].to_owned(),
        message_id: parts[2].to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_a_uri() {
        let uri = format_uri("123", "tok.en-_x", "456");
        assert_eq!(uri, "discord://123/tok.en-_x/456");
        let parsed = parse_uri(&uri).unwrap();
        assert_eq!(parsed.id, "123");
        assert_eq!(parsed.token, "tok.en-_x");
        assert_eq!(parsed.message_id, "456");
    }

    #[test]
    fn rejects_malformed_uris() {
        assert!(parse_uri("https://example.com/x").is_err());
        assert!(parse_uri("discord://123/token").is_err());
        assert!(parse_uri("discord://123//456").is_err());
        assert!(parse_uri("discord://123/token/456/extra").is_err());
    }
}
