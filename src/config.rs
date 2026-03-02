//! Configuration loaded from environment variables.

use std::path::PathBuf;

/// Sidecar configuration, read from env vars with defaults.
#[derive(Debug, Clone)]
pub struct Config {
    /// HTTP API listen port.
    pub port: u16,
    /// Base directory for iroh blob store and index.
    pub data_dir: PathBuf,
    /// Gossip topic name (hashed to TopicId).
    pub gossip_topic: String,
}

impl Config {
    /// Load configuration from environment variables.
    pub fn from_env() -> Self {
        Self {
            port: std::env::var("IROH_SIDECAR_PORT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(4002),
            data_dir: std::env::var("IROH_DATA_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from("data")),
            gossip_topic: std::env::var("IROH_GOSSIP_TOPIC")
                .unwrap_or_else(|_| "wesense-archives".to_string()),
        }
    }
}
