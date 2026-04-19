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

/// Register this archive replicator node in OrbitDB.
async fn register_node(
    config: &Config,
    node_id: &str,
    client: &reqwest::Client,
) -> Result<()> {
    let id = format!("archive-replicator-{}", &node_id[..16.min(node_id.len())]);
    let url = format!("{}/nodes/{}", config.orbitdb_url, id);

    let mut body = serde_json::json!({
        "iroh_node_id": node_id,
        "iroh_quic_port": config.quic_port,
        "archive_replicator_port": config.port,
        "type": "archive-replicator",
    });

    // Proxied stations don't register their WAN address — they're not directly
    // reachable. Only register iroh_address when not proxied.
    if config.wesense_proxy.is_none() {
        if let Some(ref addr) = config.announce_address {
            body["iroh_address"] = serde_json::json!(addr);
        }
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

/// Discover other archive replicator peers from OrbitDB.
/// Returns a list of DiscoveredPeer with node IDs and HTTP URLs.
/// Also registers discovered peer addresses in the endpoint's address lookup
/// and joins them via gossip.
async fn discover_peers(
    config: &Config,
    own_node_id: &str,
    client: &reqwest::Client,
    endpoint: &Endpoint,
    memory_lookup: &MemoryLookup,
    gossip: &GossipHandle,
) -> Result<Vec<DiscoveredPeer>> {
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

        peers.push(DiscoveredPeer {
            node_id: node_id.to_string(),
        });

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

/// Register this node's guardian scope in OrbitDB.
async fn register_guardian_scope(
    config: &Config,
    node_id: &str,
    blob_count: usize,
    client: &reqwest::Client,
) -> Result<()> {
    let short_id = &node_id[..16.min(node_id.len())];
    let id = format!("archive-replicator-{}", short_id);
    let url = format!("{}/stores/{}", config.orbitdb_url, id);

    let scope: Vec<String> = config
        .guardian_scope
        .iter()
        .map(|p| format!("{}/{}", p.country, p.subdivision))
        .collect();

    let body = serde_json::json!({
        "guardian_scope": scope,
        "blob_count": blob_count,
        "iroh_node_id": node_id,
        "type": "archive-replicator",
    });

    let resp = client
        .put(&url)
        .json(&body)
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await
        .context("Failed to register guardian scope in OrbitDB")?;

    if resp.status().is_success() {
        debug!(id = %id, blob_count, "Registered guardian scope in OrbitDB");
    } else {
        warn!(
            status = %resp.status(),
            "OrbitDB guardian scope registration returned non-success"
        );
    }

    Ok(())
}

/// Info about a discovered peer.
#[derive(Debug, Clone)]
pub struct DiscoveredPeer {
    pub node_id: String,
}

/// Spawn the discovery loop. Registers this node in OrbitDB and periodically
/// discovers peers. Returns a shared list of discovered peers.
pub fn spawn_discovery_loop(
    config: Arc<Config>,
    endpoint: Endpoint,
    node_id: String,
    store: Arc<BlobStore>,
    memory_lookup: MemoryLookup,
    gossip: Arc<GossipHandle>,
) -> Arc<RwLock<Vec<DiscoveredPeer>>> {
    let discovered_peers: Arc<RwLock<Vec<DiscoveredPeer>>> = Arc::new(RwLock::new(Vec::new()));
    let peers_clone = Arc::clone(&discovered_peers);

    tokio::spawn(async move {
        // Build HTTP client. When TLS is enabled, trust the deployment CA
        // and skip hostname verification for LAN proxy connections (the proxy
        // IP won't be in the cert SANs).
        let client = if config.tls_enabled {
            let ca_path = config.tls_certfile.as_deref()
                .map(|p| std::path::Path::new(p).parent().unwrap_or(std::path::Path::new(".")))
                .map(|dir| dir.join("ca.pem"));
            let mut builder = reqwest::Client::builder()
                .danger_accept_invalid_hostnames(true);
            if let Some(ref path) = ca_path {
                if let Ok(pem) = tokio::fs::read(path).await {
                    if let Ok(cert) = reqwest::Certificate::from_pem(&pem) {
                        builder = builder.add_root_certificate(cert);
                    }
                }
            }
            builder.build().unwrap_or_else(|_| reqwest::Client::new())
        } else {
            reqwest::Client::new()
        };

        if let Some(ref proxy_ip) = config.wesense_proxy {
            info!(
                proxy_ip = %proxy_ip,
                "Proxied station — will connect to proxy peer via LAN for iroh gossip"
            );
        }

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
                warn!(error = %e, "OrbitDB re-registration failed");
            }

            // Register guardian scope alongside node heartbeat
            let blob_count = store.blob_count().await;
            if let Err(e) = register_guardian_scope(&config, &node_id, blob_count, &client).await {
                warn!(error = %e, "OrbitDB guardian scope registration failed");
            }

            // Discover peers and wire them into endpoint + gossip.
            match discover_peers(&config, &node_id, &client, &endpoint, &memory_lookup, &gossip)
                .await
            {
                Ok(new_peers) => {
                    let count = new_peers.len();
                    let mut current = peers_clone.write().await;
                    *current = new_peers.clone();
                    debug!(peer_count = count, "Updated discovered peers list");
                }
                Err(e) => {
                    warn!(error = %e, "Peer discovery failed");
                }
            }

            // Proxied station: connect to proxy peer via LAN.
            // First try to get the node ID from OrbitDB discovery. If that fails
            // (OrbitDB replication not working), query the proxy's status endpoint
            // directly — it returns the node_id in JSON.
            if let Some(ref proxy_ip) = config.wesense_proxy {
                if let Ok(ip) = proxy_ip.parse::<std::net::IpAddr>() {
                    let proxy_port = config.wesense_proxy_iroh_port.unwrap_or(config.quic_port);
                    let replicator_port = 4400u16; // archive replicator HTTP API

                    // Try to get proxy's node ID — first from OrbitDB peers, then direct HTTPS
                    let proxy_node_id: Option<PublicKey> = {
                        let peers = peers_clone.read().await;
                        let found = peers.iter()
                            .find_map(|p| p.node_id.parse::<PublicKey>().ok());
                        if found.is_some() {
                            info!(proxy_ip = %proxy_ip, "Got proxy node ID from OrbitDB peers");
                        } else {
                            info!(proxy_ip = %proxy_ip, peer_count = peers.len(), "No proxy node ID from OrbitDB peers, trying status endpoint");
                        }
                        found
                    };

                    let proxy_node_id = match proxy_node_id {
                        Some(pk) => Some(pk),
                        None => {
                            // Query proxy's archive replicator status for its node_id.
                            // Uses HTTPS when TLS enabled. Hostname verification is
                            // skipped in the client (see above) because the proxy is
                            // accessed by LAN IP which won't be in the cert SANs.
                            let scheme = if config.tls_enabled { "https" } else { "http" };
                            let status_url = format!("{}://{}:{}/status", scheme, proxy_ip, replicator_port);
                            match client.get(&status_url).timeout(std::time::Duration::from_secs(5)).send().await {
                                Ok(resp) if resp.status().is_success() => {
                                    match resp.json::<serde_json::Value>().await {
                                        Ok(body) => {
                                            let nid = body["node_id"].as_str()
                                                .and_then(|id| id.parse::<PublicKey>().ok());
                                            if nid.is_some() {
                                                info!(proxy_ip = %proxy_ip, "Got proxy node ID from status endpoint");
                                            }
                                            nid
                                        }
                                        Err(e) => {
                                            warn!(proxy_ip = %proxy_ip, error = %e, "Failed to parse proxy status response");
                                            None
                                        }
                                    }
                                }
                                Ok(resp) => {
                                    warn!(proxy_ip = %proxy_ip, status = %resp.status(), "Proxy status endpoint returned non-success");
                                    None
                                }
                                Err(e) => {
                                    warn!(proxy_ip = %proxy_ip, error = %e, "Failed to query proxy status endpoint");
                                    None
                                }
                            }
                        }
                    };

                    if let Some(pk) = proxy_node_id {
                        let proxy_addr = SocketAddr::new(ip, proxy_port);
                        let endpoint_addr = EndpointAddr::new(pk).with_ip_addr(proxy_addr);
                        memory_lookup.add_endpoint_info(endpoint_addr);
                        if let Err(e) = gossip.join_peers(vec![pk]).await {
                            debug!(error = %e, "Failed to join proxy peer via LAN");
                        } else {
                            info!(
                                proxy_ip = %proxy_ip,
                                peer = %pk.to_string()[..16],
                                "Joined proxy peer via LAN address"
                            );
                        }
                    } else {
                        warn!(proxy_ip = %proxy_ip, "Could not resolve proxy peer node ID — proxy may not be running or TLS mismatch");
                    }
                }
            }
        }
    });

    discovered_peers
}
