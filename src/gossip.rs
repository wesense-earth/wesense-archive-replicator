//! Gossip protocol — join topic, broadcast archive announcements, index-based catch-up.
//!
//! Real-time: new archives are announced individually via gossip (mechanism 1).
//! Catch-up: on peer connect, exchange path-index as a single blob, diff locally,
//! download missing blobs via iroh Downloader (mechanism 2).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use iroh_gossip::api::{Event, GossipSender};
use iroh_gossip::net::Gossip;
use iroh_gossip::TopicId;
use n0_future::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, info, warn};

use crate::api::parse_archive_path;
use crate::config::Config;
use crate::index::PathIndex;
use crate::store::BlobStore;

/// A gossip message — archive announcements and catch-up coordination.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GossipMessage {
    #[serde(rename = "type")]
    pub msg_type: String,
    #[serde(default)]
    pub country: String,
    #[serde(default)]
    pub subdivision: String,
    #[serde(default)]
    pub date: String,
    #[serde(default)]
    pub hash: String,
    pub node_id: String,
    #[serde(default)]
    pub path: String,
    #[serde(default)]
    pub size: u64,
}

/// Keep the old name as an alias for compatibility with announce_archive
pub type ArchiveAnnouncement = GossipMessage;

/// A request to fetch an archive from a peer, sent to the replicator.
#[derive(Debug, Clone)]
pub struct FetchRequest {
    /// BLAKE3 hash hex string.
    pub hash: String,
    /// Logical archive path.
    pub path: String,
    /// ISO country code.
    pub country: String,
    /// ISO subdivision code.
    pub subdivision: String,
    /// Size in bytes.
    pub size: u64,
    /// Node ID of the source peer.
    pub source_node: String,
}

/// Manages gossip topic membership and message broadcasting.
pub struct GossipHandle {
    sender: RwLock<Option<GossipSender>>,
    gossip: Gossip,
    topic_id: TopicId,
    node_id: String,
    fetch_tx: Option<mpsc::Sender<FetchRequest>>,
    connected_peers: AtomicUsize,
    index: Option<Arc<PathIndex>>,
    store: Option<Arc<BlobStore>>,
    config: Option<Arc<Config>>,
}

impl GossipHandle {
    /// Create a new gossip handle (does not join yet — call `start` after router is spawned).
    pub fn new(
        gossip: Gossip,
        topic_name: &str,
        node_id: String,
        fetch_tx: Option<mpsc::Sender<FetchRequest>>,
    ) -> Self {
        let topic_bytes: [u8; 32] = blake3::hash(topic_name.as_bytes()).into();
        let topic_id = TopicId::from_bytes(topic_bytes);

        info!(
            topic = topic_name,
            topic_id = %hex::encode(topic_bytes),
            "Gossip topic configured"
        );

        Self {
            sender: RwLock::new(None),
            gossip,
            topic_id,
            node_id,
            fetch_tx,
            connected_peers: AtomicUsize::new(0),
            index: None,
            store: None,
            config: None,
        }
    }

    /// Set the path index and store for catch-up sync.
    pub fn set_index(&mut self, index: Arc<PathIndex>) {
        self.index = Some(index);
    }

    /// Set the blob store for importing/reading index blobs during catch-up.
    pub fn set_store(&mut self, store: Arc<BlobStore>) {
        self.store = Some(store);
    }

    /// Set config for store scope checks during catch-up.
    pub fn set_config(&mut self, config: Arc<Config>) {
        self.config = Some(config);
    }

    /// Number of currently connected gossip peers.
    pub fn connected_peers(&self) -> usize {
        self.connected_peers.load(Ordering::Relaxed)
    }

    /// Add peers to the gossip mesh. Called by discovery when new peers are found.
    pub async fn join_peers(&self, peers: Vec<iroh::PublicKey>) -> Result<()> {
        let sender = self.sender.read().await;
        if let Some(ref s) = *sender {
            s.join_peers(peers).await?;
        }
        Ok(())
    }

    /// Subscribe to the gossip topic and spawn a receiver task.
    pub async fn start(self: &Arc<Self>) -> Result<()> {
        let topic_handle = self
            .gossip
            .subscribe(self.topic_id, Vec::new())
            .await?;

        let (sender, receiver) = topic_handle.split();

        {
            let mut s = self.sender.write().await;
            *s = Some(sender);
        }

        // Spawn receiver loop
        let handle = Arc::clone(self);
        tokio::spawn(async move {
            handle.receive_loop(receiver).await;
        });

        info!("Gossip topic joined, listening for announcements");
        Ok(())
    }

    /// Broadcast an archive announcement (mechanism 1 — real-time).
    pub async fn announce_archive(
        &self,
        country: &str,
        subdivision: &str,
        date: &str,
        hash: &str,
        path: &str,
        size: u64,
    ) -> Result<()> {
        let msg = GossipMessage {
            msg_type: "archive_available".to_string(),
            country: country.to_string(),
            subdivision: subdivision.to_string(),
            date: date.to_string(),
            hash: hash.to_string(),
            node_id: self.node_id.clone(),
            path: path.to_string(),
            size,
        };

        let json = serde_json::to_vec(&msg)?;

        let sender = self.sender.read().await;
        if let Some(ref s) = *sender {
            s.broadcast(Bytes::from(json)).await?;
            info!(
                country,
                subdivision,
                date,
                hash = &hash[..16.min(hash.len())],
                path,
                size,
                "Archive announced via gossip"
            );
        } else {
            debug!("Gossip sender not ready, skipping announcement");
        }

        Ok(())
    }

    /// Send a single gossip message.
    async fn send_message(&self, msg: &GossipMessage) -> Result<()> {
        let json = serde_json::to_vec(msg)?;
        let sender = self.sender.read().await;
        if let Some(ref s) = *sender {
            s.broadcast(Bytes::from(json)).await?;
        }
        Ok(())
    }

    /// Handle a catchup_request from a peer — export our index as a blob and announce it.
    async fn handle_catchup_request(&self, from_node: &str) {
        let (Some(ref index), Some(ref store)) = (&self.index, &self.store) else {
            debug!("No index/store available for catch-up response");
            return;
        };

        // Serialize path index to JSON
        let entries = index.dump().await;
        if entries.is_empty() {
            debug!("Empty index, skipping catch-up response");
            return;
        }

        let json_bytes = match serde_json::to_vec(&entries) {
            Ok(b) => b,
            Err(e) => {
                warn!(error = %e, "Failed to serialize index for catch-up");
                return;
            }
        };

        let size = json_bytes.len() as u64;

        // Import the index as a blob
        let hash = match store.import("_sync/index.json", Bytes::from(json_bytes)).await {
            Ok(h) => h,
            Err(e) => {
                warn!(error = %e, "Failed to import index blob for catch-up");
                return;
            }
        };

        info!(
            peer = from_node,
            entries = entries.len(),
            size,
            hash = &hash[..16.min(hash.len())],
            "Responding to catch-up request with index blob"
        );

        // Announce the index blob to the peer
        let msg = GossipMessage {
            msg_type: "catchup_index".to_string(),
            hash,
            node_id: self.node_id.clone(),
            size,
            country: String::new(),
            subdivision: String::new(),
            date: String::new(),
            path: "_sync/index.json".to_string(),
        };

        if let Err(e) = self.send_message(&msg).await {
            warn!(error = %e, "Failed to send catch-up index announcement");
        }
    }

    /// Handle a catchup_index from a peer — download their index, diff, queue missing blobs.
    async fn handle_catchup_index(&self, from_node: &str, hash: &str, size: u64) {
        let (Some(ref index), Some(ref store), Some(ref config), Some(ref tx)) =
            (&self.index, &self.store, &self.config, &self.fetch_tx) else {
            debug!("Missing dependencies for catch-up index processing");
            return;
        };

        info!(
            peer = &from_node[..16.min(from_node.len())],
            hash = &hash[..16.min(hash.len())],
            size,
            "Received peer index for catch-up, downloading..."
        );

        // Download the peer's index blob via iroh Downloader.
        // The Downloader uses the existing QUIC connection (bidirectional).
        // We create a FetchRequest for the index blob itself.
        let peer_id: iroh::PublicKey = match from_node.parse() {
            Ok(pk) => pk,
            Err(_) => {
                warn!("Invalid node ID in catch-up index: {}", &from_node[..16.min(from_node.len())]);
                return;
            }
        };

        let blob_hash: iroh_blobs::Hash = match hash.parse() {
            Ok(h) => h,
            Err(_) => {
                warn!("Invalid hash in catch-up index: {}", &hash[..16.min(hash.len())]);
                return;
            }
        };

        // Use the store's inner downloader-compatible interface
        // Actually, we need the Downloader from main.rs. Instead, register the
        // blob at a known path and use the FetchRequest mechanism.
        let index_req = FetchRequest {
            hash: hash.to_string(),
            path: format!("_sync/peer_{}.json", &from_node[..16.min(from_node.len())]),
            country: "_sync".to_string(),
            subdivision: "index".to_string(),
            size,
            source_node: from_node.to_string(),
        };

        // Send the index download request to the replicator
        if let Err(e) = tx.try_send(index_req) {
            warn!(error = %e, "Failed to queue catch-up index download");
            return;
        }

        // Wait for the download to complete — poll the store for the blob
        let index_path = format!("_sync/peer_{}.json", &from_node[..16.min(from_node.len())]);
        let mut attempts = 0;
        let peer_index_bytes = loop {
            attempts += 1;
            if attempts > 300 {
                // 5 minutes max wait
                warn!("Timed out waiting for catch-up index download");
                return;
            }
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;

            // Check if the blob is in the store now
            match store.get_by_hash(hash).await {
                Ok(Some(bytes)) => break bytes,
                Ok(None) => continue,
                Err(e) => {
                    warn!(error = %e, "Error reading catch-up index blob");
                    return;
                }
            }
        };

        info!(
            peer = &from_node[..16.min(from_node.len())],
            bytes = peer_index_bytes.len(),
            "Downloaded peer index, computing diff..."
        );

        // Parse the peer's index
        let peer_entries: std::collections::BTreeMap<String, crate::index::IndexEntry> =
            match serde_json::from_slice(&peer_index_bytes) {
                Ok(e) => e,
                Err(e) => {
                    warn!(error = %e, "Failed to parse peer index JSON");
                    return;
                }
            };

        // Diff against local index — find entries we're missing
        let mut queued = 0u64;
        let mut skipped_scope = 0u64;
        let mut skipped_existing = 0u64;

        for (path, entry) in &peer_entries {
            // Skip sync metadata blobs
            if path.starts_with("_sync/") {
                continue;
            }

            // Check store scope
            if let Some((country, subdivision, _)) = parse_archive_path(path) {
                if !config.matches_store_scope(&country, &subdivision) {
                    skipped_scope += 1;
                    continue;
                }

                // Check if we already have it
                if index.exists(path).await {
                    skipped_existing += 1;
                    continue;
                }

                // Queue for download
                let req = FetchRequest {
                    hash: entry.hash.clone(),
                    path: path.clone(),
                    country,
                    subdivision,
                    size: entry.size,
                    source_node: from_node.to_string(),
                };

                if let Err(e) = tx.try_send(req) {
                    debug!(error = %e, path, "Fetch channel full during catch-up diff");
                    // Channel full — the replicator is busy. The remaining items
                    // will be caught up on the next peer connect cycle.
                    break;
                }
                queued += 1;
            }
        }

        info!(
            peer = &from_node[..16.min(from_node.len())],
            peer_entries = peer_entries.len(),
            queued,
            skipped_existing,
            skipped_scope,
            "Catch-up diff complete"
        );
    }

    /// Receive loop — processes gossip messages.
    async fn receive_loop(
        &self,
        mut receiver: iroh_gossip::api::GossipReceiver,
    ) {
        while let Some(event) = receiver.next().await {
            match event {
                Ok(Event::Received(msg)) => {
                    match serde_json::from_slice::<GossipMessage>(&msg.content) {
                        Ok(gm) => {
                            // Skip messages from ourselves
                            if gm.node_id == self.node_id {
                                continue;
                            }

                            match gm.msg_type.as_str() {
                                "archive_available" => {
                                    // Real-time archive announcement (mechanism 1)
                                    info!(
                                        country = %gm.country,
                                        subdivision = %gm.subdivision,
                                        path = %gm.path,
                                        hash = %gm.hash,
                                        from_node = %gm.node_id,
                                        "Received gossip announcement"
                                    );

                                    if let Some(ref tx) = self.fetch_tx {
                                        let path = if gm.path.is_empty() {
                                            let date_parts: Vec<&str> = gm.date.split('-').collect();
                                            if date_parts.len() == 3 {
                                                format!(
                                                    "{}/{}/{}/{}/{}/archive.parquet",
                                                    gm.country, gm.subdivision,
                                                    date_parts[0], date_parts[1], date_parts[2]
                                                )
                                            } else {
                                                continue;
                                            }
                                        } else {
                                            gm.path.clone()
                                        };

                                        let req = FetchRequest {
                                            hash: gm.hash.clone(),
                                            path,
                                            country: gm.country.clone(),
                                            subdivision: gm.subdivision.clone(),
                                            size: gm.size,
                                            source_node: gm.node_id.clone(),
                                        };

                                        if let Err(e) = tx.try_send(req) {
                                            debug!(error = %e, "Fetch channel full");
                                        }
                                    }
                                }

                                "catchup_request" => {
                                    // Peer wants to sync — send our index as a blob
                                    info!(
                                        from_node = %gm.node_id,
                                        "Received catch-up request"
                                    );
                                    self.handle_catchup_request(&gm.node_id).await;
                                }

                                "catchup_index" => {
                                    // Peer sent their index — download, diff, fetch missing
                                    // Handle in a spawned task to not block the receive loop
                                    let node_id = gm.node_id.clone();
                                    let hash = gm.hash.clone();
                                    let size = gm.size;

                                    // We need to access self from the spawned task.
                                    // Clone what we need.
                                    let index = self.index.clone();
                                    let store = self.store.clone();
                                    let config = self.config.clone();
                                    let fetch_tx = self.fetch_tx.clone();
                                    let self_node_id = self.node_id.clone();

                                    tokio::spawn(async move {
                                        // Reconstruct a minimal handler inline
                                        let (Some(index), Some(store), Some(config), Some(tx)) =
                                            (index, store, config, fetch_tx) else {
                                            return;
                                        };

                                        info!(
                                            peer = &node_id[..16.min(node_id.len())],
                                            hash = &hash[..16.min(hash.len())],
                                            size,
                                            "Processing catch-up index from peer"
                                        );

                                        // Queue the index blob download
                                        let index_path = format!("_sync/peer_{}.json", &node_id[..16.min(node_id.len())]);
                                        let req = FetchRequest {
                                            hash: hash.clone(),
                                            path: index_path.clone(),
                                            country: "_sync".to_string(),
                                            subdivision: "index".to_string(),
                                            size,
                                            source_node: node_id.clone(),
                                        };

                                        if let Err(e) = tx.try_send(req) {
                                            warn!(error = %e, "Failed to queue catch-up index download");
                                            return;
                                        }

                                        // Wait for download to complete
                                        let mut attempts = 0;
                                        let peer_index_bytes = loop {
                                            attempts += 1;
                                            if attempts > 300 {
                                                warn!("Timed out waiting for catch-up index download");
                                                return;
                                            }
                                            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                                            match store.get_by_hash(&hash).await {
                                                Ok(Some(bytes)) => break bytes,
                                                Ok(None) => continue,
                                                Err(e) => {
                                                    warn!(error = %e, "Error reading catch-up index");
                                                    return;
                                                }
                                            }
                                        };

                                        info!(
                                            peer = &node_id[..16.min(node_id.len())],
                                            bytes = peer_index_bytes.len(),
                                            "Downloaded peer index, computing diff"
                                        );

                                        // Parse peer's index
                                        let peer_entries: std::collections::BTreeMap<String, crate::index::IndexEntry> =
                                            match serde_json::from_slice(&peer_index_bytes) {
                                                Ok(e) => e,
                                                Err(e) => {
                                                    warn!(error = %e, "Failed to parse peer index");
                                                    return;
                                                }
                                            };

                                        // Diff and queue missing blobs
                                        let mut queued = 0u64;
                                        let mut skipped_scope = 0u64;
                                        let mut skipped_existing = 0u64;

                                        for (path, entry) in &peer_entries {
                                            if path.starts_with("_sync/") {
                                                continue;
                                            }

                                            if let Some((country, subdivision, _)) = parse_archive_path(path) {
                                                if !config.matches_store_scope(&country, &subdivision) {
                                                    skipped_scope += 1;
                                                    continue;
                                                }

                                                if index.exists(path).await {
                                                    skipped_existing += 1;
                                                    continue;
                                                }

                                                let req = FetchRequest {
                                                    hash: entry.hash.clone(),
                                                    path: path.clone(),
                                                    country,
                                                    subdivision,
                                                    size: entry.size,
                                                    source_node: node_id.clone(),
                                                };

                                                if let Err(_) = tx.try_send(req) {
                                                    // Channel full — remaining items caught up next cycle
                                                    break;
                                                }
                                                queued += 1;
                                            }
                                        }

                                        info!(
                                            peer = &node_id[..16.min(node_id.len())],
                                            peer_entries = peer_entries.len(),
                                            queued,
                                            skipped_existing,
                                            skipped_scope,
                                            "Catch-up diff complete"
                                        );
                                    });
                                }

                                other => {
                                    debug!(msg_type = other, "Unknown gossip message type");
                                }
                            }
                        }
                        Err(e) => {
                            debug!(
                                error = %e,
                                bytes = msg.content.len(),
                                "Received non-parseable gossip message"
                            );
                        }
                    }
                }
                Ok(Event::NeighborUp(peer_id)) => {
                    let count = self.connected_peers.fetch_add(1, Ordering::Relaxed) + 1;
                    info!(peer = %peer_id, connected = count, "Gossip peer connected");

                    // Send a catch-up request — the peer will respond with their
                    // path-index as a single blob (mechanism 2).
                    let msg = GossipMessage {
                        msg_type: "catchup_request".to_string(),
                        node_id: self.node_id.clone(),
                        hash: String::new(),
                        country: String::new(),
                        subdivision: String::new(),
                        date: String::new(),
                        path: String::new(),
                        size: 0,
                    };

                    if let Err(e) = self.send_message(&msg).await {
                        warn!(error = %e, "Failed to send catch-up request");
                    } else {
                        info!(peer = %peer_id, "Sent catch-up request to peer");
                    }
                }
                Ok(Event::NeighborDown(peer_id)) => {
                    let count = self.connected_peers.fetch_sub(1, Ordering::Relaxed).saturating_sub(1);
                    info!(peer = %peer_id, connected = count, "Gossip peer disconnected");
                }
                Ok(Event::Lagged) => {
                    warn!("Gossip receiver lagged — missed messages");
                }
                Err(e) => {
                    warn!(error = %e, "Gossip receive error");
                }
            }
        }

        info!("Gossip receive loop ended");
    }
}
