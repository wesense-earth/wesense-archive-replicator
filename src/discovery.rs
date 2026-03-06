//! OrbitDB-based peer discovery — register this node and discover peers.

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use iroh::address_lookup::MemoryLookup;
use iroh::{Endpoint, EndpointAddr, PublicKey, RelayConfig, RelayUrl};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::gossip::GossipHandle;
use crate::store::BlobStore;

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

    if !config.relay_urls.is_empty() {
        body["iroh_relay_urls"] = serde_json::json!(config.relay_urls);
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
/// Also registers discovered peer addresses in the endpoint's address lookup
/// and joins them via gossip.
async fn discover_peers(
    config: &Config,
    own_node_id: &str,
    client: &reqwest::Client,
    endpoint: &Endpoint,
    memory_lookup: &MemoryLookup,
    gossip: &GossipHandle,
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
    let mut peer_ids_to_join = Vec::new();

    for node in &nodes {
        let node_id = match node["iroh_node_id"].as_str() {
            Some(id) if !id.is_empty() && id != own_node_id => id,
            _ => continue,
        };

        let pk: PublicKey = match node_id.parse() {
            Ok(pk) => pk,
            Err(_) => continue,
        };

        peers.push(node_id.to_string());

        // Build an EndpointAddr with whatever addressing info we have
        let mut endpoint_addr = EndpointAddr::new(pk);
        let mut has_address = false;

        if let Some(addr_str) = node["iroh_address"].as_str() {
            if !addr_str.is_empty() {
                let port = node["iroh_quic_port"].as_u64().unwrap_or(4401) as u16;
                if let Ok(ip) = addr_str.parse::<std::net::IpAddr>() {
                    let sock_addr = SocketAddr::new(ip, port);
                    endpoint_addr = endpoint_addr.with_ip_addr(sock_addr);
                    has_address = true;
                    debug!(
                        peer = %&node_id[..16.min(node_id.len())],
                        address = %sock_addr,
                        "Discovered peer with direct address"
                    );
                }
            }
        }

        // Add relay URLs from the peer
        if let Some(relays) = node["iroh_relay_urls"].as_array() {
            for relay in relays {
                if let Some(url_str) = relay.as_str() {
                    if let Ok(url) = url_str.parse::<RelayUrl>() {
                        endpoint_addr = endpoint_addr.with_relay_url(url.clone());
                        endpoint
                            .insert_relay(url.clone(), Arc::new(RelayConfig::from(url)))
                            .await;
                        has_address = true;
                    }
                }
            }
        }

        // Register the peer's address info in the endpoint's address lookup
        if has_address {
            memory_lookup.add_endpoint_info(endpoint_addr);
            peer_ids_to_join.push(pk);
        }
    }

    // Tell gossip to connect to all discovered peers with addresses
    if !peer_ids_to_join.is_empty() {
        let count = peer_ids_to_join.len();
        if let Err(e) = gossip.join_peers(peer_ids_to_join).await {
            warn!(error = %e, "Failed to join discovered peers via gossip");
        } else {
            info!(peer_count = count, "Joined discovered peers via gossip");
        }
    }

    Ok(peers)
}

/// Register this node's store scope in OrbitDB.
async fn register_store_scope(
    config: &Config,
    node_id: &str,
    blob_count: usize,
    client: &reqwest::Client,
) -> Result<()> {
    let short_id = &node_id[..16.min(node_id.len())];
    let id = format!("iroh-sidecar-{}", short_id);
    let url = format!("{}/stores/{}", config.orbitdb_url, id);

    let scope: Vec<String> = config
        .store_scope
        .iter()
        .map(|p| format!("{}/{}", p.country, p.subdivision))
        .collect();

    let body = serde_json::json!({
        "store_scope": scope,
        "blob_count": blob_count,
        "iroh_node_id": node_id,
        "type": "iroh-sidecar",
    });

    let resp = client
        .put(&url)
        .json(&body)
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await
        .context("Failed to register store scope in OrbitDB")?;

    if resp.status().is_success() {
        debug!(id = %id, blob_count, "Registered store scope in OrbitDB");
    } else {
        warn!(
            status = %resp.status(),
            "OrbitDB store scope registration returned non-success"
        );
    }

    Ok(())
}

/// Spawn the discovery loop. Registers this node in OrbitDB and periodically
/// discovers peers. Returns a shared list of discovered peer node IDs.
pub fn spawn_discovery_loop(
    config: Arc<Config>,
    endpoint: Endpoint,
    node_id: String,
    store: Arc<BlobStore>,
    memory_lookup: MemoryLookup,
    gossip: Arc<GossipHandle>,
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

            // Register store scope alongside node heartbeat
            let blob_count = store.blob_count().await;
            if let Err(e) = register_store_scope(&config, &node_id, blob_count, &client).await {
                debug!(error = %e, "OrbitDB store scope registration failed");
            }

            // Discover peers and wire them into endpoint + gossip
            match discover_peers(&config, &node_id, &client, &endpoint, &memory_lookup, &gossip)
                .await
            {
                Ok(new_peers) => {
                    let count = new_peers.len();
                    let mut peers = peers_clone.write().await;
                    *peers = new_peers;
                    debug!(peer_count = count, "Updated discovered peers list");
                }
                Err(e) => {
                    debug!(error = %e, "Peer discovery failed");
                }
            }
        }
    });

    discovered_peers
}
