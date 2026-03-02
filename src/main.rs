//! WeSense Iroh Sidecar — blob store + gossip HTTP API.
//!
//! A small axum HTTP server wrapping iroh-blobs and iroh-gossip.
//! The Python storage gateway's IrohBackend talks to this sidecar.

mod api;
mod config;
mod gossip;
mod index;
mod store;

use std::sync::Arc;

use anyhow::{Context, Result};
use iroh::protocol::Router;
use iroh::Endpoint;
use iroh_blobs::BlobsProtocol;
use iroh_gossip::net::Gossip;
use tokio::net::TcpListener;
use tracing::info;

use crate::api::AppState;
use crate::config::Config;
use crate::gossip::GossipHandle;
use crate::index::PathIndex;
use crate::store::BlobStore;

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
    info!(?config, "Starting WeSense Iroh Sidecar");

    // Ensure data directory exists
    tokio::fs::create_dir_all(&config.data_dir).await?;

    // 1. Load the path index
    let index = Arc::new(
        PathIndex::load(&config.data_dir)
            .await
            .context("Failed to load path index")?,
    );

    // 2. Create the iroh endpoint
    let endpoint = Endpoint::bind()
        .await
        .context("Failed to bind iroh endpoint")?;

    let node_id = endpoint.id().to_string();
    info!(node_id = %node_id, "Iroh endpoint bound");

    // 3. Open the blob store
    let blob_store = BlobStore::open(&config.data_dir, Arc::clone(&index))
        .await
        .context("Failed to open blob store")?;

    // 4. Create the blobs network protocol
    let blobs_protocol = BlobsProtocol::new(blob_store.inner(), None);

    // 5. Create the gossip protocol
    let gossip_raw = Gossip::builder().spawn(endpoint.clone());

    let gossip_handle = Arc::new(GossipHandle::new(
        gossip_raw.clone(),
        &config.gossip_topic,
        node_id.clone(),
    ));

    // 6. Wire protocols into the router
    let _router = Router::builder(endpoint)
        .accept(iroh_blobs::ALPN, blobs_protocol)
        .accept(iroh_gossip::ALPN, gossip_raw)
        .spawn();

    info!("Iroh protocol router spawned");

    // 7. Start gossip (subscribe to topic)
    gossip_handle
        .start()
        .await
        .context("Failed to start gossip")?;

    // 8. Build shared app state for axum
    let app_state = Arc::new(AppState {
        store: blob_store,
        index,
        gossip: gossip_handle,
        node_id: node_id.clone(),
    });

    // 9. Start axum HTTP server
    let app = api::router(app_state);
    let addr = format!("0.0.0.0:{}", config.port);
    let listener = TcpListener::bind(&addr)
        .await
        .context("Failed to bind HTTP listener")?;

    info!(addr = %addr, node_id = %node_id, "Sidecar ready");

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
