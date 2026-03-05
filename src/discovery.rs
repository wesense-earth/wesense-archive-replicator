//! OrbitDB-based peer discovery — register this node and discover peers.

use std::sync::Arc;

use anyhow::{Context, Result};
use iroh::Endpoint;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use crate::config::Config;

/// Register this node's iroh identity in OrbitDB.
async fn register_node(
    config: &Config,
    node_id: &str,
    client: &reqwest::Client,
) -> Result<()> {
    let id = format!("iroh-sidecar-{}", &node_id[..16.min(node_id.len())]);
    let url = format!("{}/nodes/{}", config.orbitdb_url, id);

    let mut body = serde_json::json!({
        "iroh_node_id": node_id,
        "iroh_quic_port": config.quic_port,
        "type": "iroh-sidecar",
    });

    if let Some(ref addr) = config.announce_address {
        body["iroh_address"] = serde_json::json!(addr);
    }

    let resp = client
        .put(&url)
        .json(&body)
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await
        .context("Failed to register node in OrbitDB")?;

    if resp.status().is_success() {
        info!(id = %id, "Registered in OrbitDB");
    } else {
        warn!(
            status = %resp.status(),
            "OrbitDB node registration returned non-success"
        );
    }

    Ok(())
}

/// Discover other iroh sidecar peers from OrbitDB.
/// Returns a list of node ID strings for peers with iroh_node_id fields.
async fn discover_peers(
    config: &Config,
    own_node_id: &str,
    client: &reqwest::Client,
) -> Result<Vec<String>> {
    let url = format!("{}/nodes", config.orbitdb_url);

    let resp = client
        .get(&url)
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await
        .context("Failed to fetch nodes from OrbitDB")?;

    if !resp.status().is_success() {
        anyhow::bail!("OrbitDB nodes returned status {}", resp.status());
    }

    let body: serde_json::Value = resp.json().await?;
    let nodes = body["nodes"]
        .as_array()
        .map(|a| a.to_vec())
        .unwrap_or_default();

    let mut peers = Vec::new();
    for node in &nodes {
        let node_id = match node["iroh_node_id"].as_str() {
            Some(id) if !id.is_empty() && id != own_node_id => id,
            _ => continue,
        };
        peers.push(node_id.to_string());

        // If the peer has an address, try to connect to it
        if let Some(addr) = node["iroh_address"].as_str() {
            if !addr.is_empty() {
                let port = node["iroh_quic_port"].as_u64().unwrap_or(4401) as u16;
                debug!(
                    peer_node_id = %&node_id[..16.min(node_id.len())],
                    address = addr,
                    port,
                    "Discovered peer with address"
                );
            }
        }
    }

    Ok(peers)
}

/// Spawn the discovery loop. Registers this node in OrbitDB and periodically
/// discovers peers. Returns a shared list of discovered peer node IDs.
pub fn spawn_discovery_loop(
    config: Arc<Config>,
    _endpoint: Endpoint,
    node_id: String,
) -> Arc<RwLock<Vec<String>>> {
    let discovered_peers: Arc<RwLock<Vec<String>>> = Arc::new(RwLock::new(Vec::new()));
    let peers_clone = Arc::clone(&discovered_peers);

    tokio::spawn(async move {
        let client = reqwest::Client::new();

        // Initial registration with retries
        for attempt in 1..=5u32 {
            match register_node(&config, &node_id, &client).await {
                Ok(()) => break,
                Err(e) => {
                    if attempt == 5 {
                        error!(error = %e, "Failed to register in OrbitDB after 5 attempts");
                    } else {
                        warn!(
                            error = %e,
                            attempt,
                            "OrbitDB registration failed, retrying..."
                        );
                        tokio::time::sleep(std::time::Duration::from_secs(5 * attempt as u64))
                            .await;
                    }
                }
            }
        }

        // Periodic re-register + discover
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        interval.tick().await; // Skip the initial tick (we just registered)

        loop {
            interval.tick().await;

            // Re-register (heartbeat)
            if let Err(e) = register_node(&config, &node_id, &client).await {
                debug!(error = %e, "OrbitDB re-registration failed");
            }

            // Discover peers
            match discover_peers(&config, &node_id, &client).await {
                Ok(new_peers) => {
                    let count = new_peers.len();

                    // Try to connect to peers with addresses
                    for peer_node_id in &new_peers {
                        if let Ok(pk) = peer_node_id.parse::<iroh::PublicKey>() {
                            // The Endpoint will cache connections established during gossip.
                            // For OrbitDB-discovered peers that we haven't seen via gossip,
                            // explicit connect would be needed, but requires EndpointAddr
                            // which needs address info. For now, just record the peer ID —
                            // the Downloader will attempt to reach them via cached connections.
                            let _pk = pk; // Validate parse
                        }
                    }

                    let mut peers = peers_clone.write().await;
                    *peers = new_peers;
                    debug!(peer_count = count, "Updated discovered peers list");
                }
                Err(e) => {
                    debug!(error = %e, "Peer discovery failed");
                }
            }

            // Try to connect to bootstrap peers if we have addresses
            for peer_addr in &config.bootstrap_peers {
                if peer_addr.is_empty() {
                    continue;
                }
                // Bootstrap peers are iroh node IDs or addresses
                // For now, just log — direct connection requires EndpointAddr
                debug!(bootstrap = %peer_addr, "Bootstrap peer configured");
            }
        }
    });

    discovered_peers
}
