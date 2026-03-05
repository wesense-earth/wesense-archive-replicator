//! Configuration loaded from environment variables.

use std::path::PathBuf;

/// A scope pattern like `"nz/wgn"` or `"au/*"`.
/// Matches country/subdivision pairs in archive paths.
#[derive(Debug, Clone)]
pub struct ScopePattern {
    pub country: String,
    pub subdivision: String, // "*" = any
}

impl ScopePattern {
    /// Parse a single pattern like `"nz/wgn"` or `"au/*"`.
    pub fn parse(s: &str) -> Option<Self> {
        let parts: Vec<&str> = s.trim().splitn(2, '/').collect();
        if parts.len() == 2 && !parts[0].is_empty() && !parts[1].is_empty() {
            Some(Self {
                country: parts[0].to_lowercase(),
                subdivision: parts[1].to_lowercase(),
            })
        } else {
            None
        }
    }

    /// Check if this pattern matches a given country/subdivision.
    pub fn matches(&self, country: &str, subdivision: &str) -> bool {
        let country_lower = country.to_lowercase();
        let subdiv_lower = subdivision.to_lowercase();
        self.country == country_lower
            && (self.subdivision == "*" || self.subdivision == subdiv_lower)
    }
}

/// Sidecar configuration, read from env vars with defaults.
#[derive(Debug, Clone)]
pub struct Config {
    /// HTTP API listen port.
    pub port: u16,
    /// Base directory for iroh blob store and index.
    pub data_dir: PathBuf,
    /// Gossip topic name (hashed to TopicId).
    pub gossip_topic: String,
    /// QUIC bind port for iroh P2P connections.
    pub quic_port: u16,
    /// Store scope — which country/subdivision archives to download.
    /// Empty = no downloads (announce-only mode).
    pub store_scope: Vec<ScopePattern>,
    /// OrbitDB HTTP API URL for peer discovery and reconciliation.
    pub orbitdb_url: String,
    /// Bootstrap peers to connect to on startup (comma-separated NodeId or addresses).
    pub bootstrap_peers: Vec<String>,
    /// Interval in seconds between reconciliation runs. 0 = disabled.
    pub reconcile_interval_secs: u64,
    /// Public address to announce to other peers (IP or hostname).
    pub announce_address: Option<String>,
    /// DERP relay URLs for NAT traversal fallback (e.g. `https://derp.wesense.earth`).
    pub relay_urls: Vec<String>,
}

impl Config {
    /// Load configuration from environment variables.
    pub fn from_env() -> Self {
        let store_scope = std::env::var("IROH_STORE_SCOPE")
            .ok()
            .map(|v| {
                v.split(',')
                    .filter_map(ScopePattern::parse)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        let bootstrap_peers = std::env::var("IROH_BOOTSTRAP_PEERS")
            .ok()
            .map(|v| {
                v.split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

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
            quic_port: std::env::var("IROH_QUIC_PORT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(4401),
            store_scope,
            orbitdb_url: std::env::var("ORBITDB_URL")
                .unwrap_or_else(|_| "http://wesense-orbitdb:5200".to_string()),
            bootstrap_peers,
            reconcile_interval_secs: std::env::var("IROH_RECONCILE_INTERVAL")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(900),
            announce_address: std::env::var("ANNOUNCE_ADDRESS").ok().filter(|s| !s.is_empty()),
            relay_urls: std::env::var("IROH_RELAY_URLS")
                .ok()
                .map(|v| {
                    v.split(',')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect()
                })
                .unwrap_or_default(),
        }
    }

    /// Check if a country/subdivision pair matches the store scope.
    /// Returns false if store_scope is empty (no downloads).
    pub fn matches_store_scope(&self, country: &str, subdivision: &str) -> bool {
        self.store_scope.iter().any(|p| p.matches(country, subdivision))
    }
}
