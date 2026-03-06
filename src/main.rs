//! WeSense Iroh Sidecar — blob store + gossip HTTP API.
//!
//! A small axum HTTP server wrapping iroh-blobs and iroh-gossip.
//! The Python storage gateway's IrohBackend talks to this sidecar.

mod api;
mod config;
mod discovery;
mod gossip;
mod index;
mod replicator;
mod store;

use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use iroh::address_lookup::MemoryLookup;
use iroh::protocol::Router;
use iroh::{Endpoint, RelayMode, RelayUrl, SecretKey};
use iroh_blobs::BlobsProtocol;
use iroh_gossip::net::Gossip;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::api::AppState;
use crate::config::Config;
use crate::gossip::{FetchRequest, GossipHandle};
use crate::index::PathIndex;
use crate::replicator::{ReplicationStats, Replicator};
use crate::store::BlobStore;

/// Load or generate a persistent secret key from `{data_dir}/secret_key`.
async fn load_or_generate_secret_key(data_dir: &Path) -> Result<SecretKey> {
    let key_path = data_dir.join("secret_key");

    if key_path.exists() {
        let bytes = tokio::fs::read(&key_path)
            .await
            .context("Failed to read secret key file")?;
        let key_bytes: [u8; 32] = bytes
            .try_into()
            .map_err(|_| anyhow::anyhow!("Secret key file must be exactly 32 bytes"))?;
        let key = SecretKey::from_bytes(&key_bytes);
        info!("Loaded persistent secret key");
        Ok(key)
    } else {
        let key = SecretKey::generate(&mut rand::rng());
        tokio::fs::write(&key_path, key.to_bytes())
            .await
            .context("Failed to write secret key file")?;
        info!("Generated and saved new secret key");
        Ok(key)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Tracing setup
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "wesense_iroh_sidecar=info".parse().unwrap()),
        )
        .init();

    let config = Config::from_env();

    if config.announce_address.is_none() {
        warn!("ANNOUNCE_ADDRESS not set — other stations cannot discover this node's QUIC address. \
               Set ANNOUNCE_ADDRESS to this host's reachable IP or hostname for cross-host replication.");
    }

    info!(?config, "Starting WeSense Iroh Sidecar");

    // Ensure data directory exists
    tokio::fs::create_dir_all(&config.data_dir).await?;

    // 1. Load the path index
    let index = Arc::new(
        PathIndex::load(&config.data_dir)
            .await
            .context("Failed to load path index")?,
    );

    // 2. Load or generate persistent secret key
    let secret_key = load_or_generate_secret_key(&config.data_dir).await?;

    // 3. Create the iroh endpoint — bound to configured QUIC port
    //    Uses custom DERP relays if configured, otherwise DERP-free.
    let quic_addr = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, config.quic_port);
    let relay_mode = if config.relay_urls.is_empty() {
        info!("No relay URLs configured — DERP disabled");
        RelayMode::Disabled
    } else {
        let urls: Vec<RelayUrl> = config
            .relay_urls
            .iter()
            .filter_map(|u| match u.parse::<RelayUrl>() {
                Ok(url) => Some(url),
                Err(e) => {
                    warn!(url = %u, error = %e, "Invalid relay URL, skipping");
                    None
                }
            })
            .collect();
        if urls.is_empty() {
            info!("All relay URLs invalid — DERP disabled");
            RelayMode::Disabled
        } else {
            info!(relay_count = urls.len(), "Using custom DERP relays");
            RelayMode::custom(urls)
        }
    };

    let endpoint = Endpoint::empty_builder(relay_mode)
        .secret_key(secret_key)
        .clear_address_lookup()
        .bind_addr(quic_addr)?
        .bind()
        .await
        .context("Failed to bind iroh endpoint")?;

    // Create a MemoryLookup for OrbitDB-discovered peer addresses
    let memory_lookup = MemoryLookup::default();
    endpoint.address_lookup().add(memory_lookup.clone());

    let node_id = endpoint.id();
    let node_id_str = node_id.to_string();
    info!(node_id = %node_id_str, quic_port = config.quic_port, "Iroh endpoint bound");

    // 4. Open the blob store
    let blob_store = BlobStore::open(&config.data_dir, Arc::clone(&index))
        .await
        .context("Failed to open blob store")?;

    // 5. Create the blobs network protocol
    let blobs_protocol = BlobsProtocol::new(blob_store.inner(), None);

    // 6. Create downloader from the blob store (before router takes ownership)
    let downloader = blobs_protocol.store().downloader(&endpoint);

    // 7. Create the gossip protocol
    let gossip_raw = Gossip::builder().spawn(endpoint.clone());

    // 8. Create the fetch request channel (gossip → replicator)
    let (fetch_tx, fetch_rx) = mpsc::channel::<FetchRequest>(256);

    let config = Arc::new(config);

    let gossip_handle = Arc::new(GossipHandle::new(
        gossip_raw.clone(),
        &config.gossip_topic,
        node_id_str.clone(),
        Some(fetch_tx.clone()),
    ));

    // 9. Wire protocols into the router
    let endpoint_for_discovery = endpoint.clone();
    let _router = Router::builder(endpoint)
        .accept(iroh_blobs::ALPN, blobs_protocol)
        .accept(iroh_gossip::ALPN, gossip_raw)
        .spawn();

    info!("Iroh protocol router spawned");

    // 10. Start gossip (subscribe to topic)
    gossip_handle
        .start()
        .await
        .context("Failed to start gossip")?;

    // 11. Create and spawn the replicator worker
    let blob_store = Arc::new(blob_store);
    let stats = Arc::new(ReplicationStats::new());
    let replicator = Replicator::new(
        downloader,
        Arc::clone(&blob_store),
        Arc::clone(&index),
        Arc::clone(&config),
        Arc::clone(&stats),
    );
    tokio::spawn(replicator.run(fetch_rx));

    // 12. Spawn OrbitDB discovery loop
    let discovered_peers = discovery::spawn_discovery_loop(
        Arc::clone(&config),
        endpoint_for_discovery,
        node_id_str.clone(),
        Arc::clone(&blob_store),
        memory_lookup,
        Arc::clone(&gossip_handle),
    );

    // 13. Spawn reconciliation loop
    replicator::spawn_reconciliation_loop(
        Arc::clone(&config),
        fetch_tx.clone(),
        Arc::clone(&stats),
        Arc::clone(&discovered_peers),
    );

    // 14. Build shared app state for axum
    let app_state = Arc::new(AppState {
        store: Arc::clone(&blob_store),
        index,
        gossip: gossip_handle,
        node_id: node_id_str.clone(),
        config: Arc::clone(&config),
        stats: Arc::clone(&stats),
    });

    // 15. Start axum HTTP server
    let app = api::router(app_state);
    let addr = format!("0.0.0.0:{}", config.port);
    let listener = TcpListener::bind(&addr)
        .await
        .context("Failed to bind HTTP listener")?;

    info!(addr = %addr, node_id = %node_id_str, "Sidecar ready");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("HTTP server error")?;

    info!("Sidecar shutting down");
    Ok(())
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to install Ctrl+C handler");
    info!("Shutdown signal received");
}
