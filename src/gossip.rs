//! Gossip protocol — join topic, broadcast archive announcements, forward to replicator.

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
use crate::index::PathIndex;

/// An archive announcement broadcast over gossip.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveAnnouncement {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub country: String,
    pub subdivision: String,
    pub date: String,
    pub hash: String,
    pub node_id: String,
    /// Logical path of the archive (e.g. "nz/wgn/2026/03/05/readings.parquet").
    #[serde(default)]
    pub path: String,
    /// Size in bytes.
    #[serde(default)]
    pub size: u64,
}

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
    /// Path index for re-announcing local archives on peer connect (catch-up).
    index: Option<Arc<PathIndex>>,
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
        }
    }

    /// Set the path index for peer catch-up re-announcements.
    pub fn set_index(&mut self, index: Arc<PathIndex>) {
        self.index = Some(index);
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

    /// Broadcast an archive announcement.
    pub async fn announce_archive(
        &self,
        country: &str,
        subdivision: &str,
        date: &str,
        hash: &str,
        path: &str,
        size: u64,
    ) -> Result<()> {
        let announcement = ArchiveAnnouncement {
            msg_type: "archive_available".to_string(),
            country: country.to_string(),
            subdivision: subdivision.to_string(),
            date: date.to_string(),
            hash: hash.to_string(),
            node_id: self.node_id.clone(),
            path: path.to_string(),
            size,
        };

        let json = serde_json::to_vec(&announcement)?;

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

    /// Receive loop — logs incoming gossip messages and forwards fetch requests.
    async fn receive_loop(
        &self,
        mut receiver: iroh_gossip::api::GossipReceiver,
    ) {
        while let Some(event) = receiver.next().await {
            match event {
                Ok(Event::Received(msg)) => {
                    match serde_json::from_slice::<ArchiveAnnouncement>(&msg.content) {
                        Ok(ann) => {
                            // Skip announcements from ourselves
                            if ann.node_id == self.node_id {
                                debug!("Skipping self-announcement");
                                continue;
                            }

                            info!(
                                msg_type = %ann.msg_type,
                                country = %ann.country,
                                subdivision = %ann.subdivision,
                                date = %ann.date,
                                hash = %ann.hash,
                                path = %ann.path,
                                size = ann.size,
                                from_node = %ann.node_id,
                                "Received gossip announcement"
                            );

                            // Forward to replicator if we have a channel
                            if let Some(ref tx) = self.fetch_tx {
                                // Derive path from announcement fields if empty
                                let path = if ann.path.is_empty() {
                                    // Best-effort path derivation from country/subdivision/date
                                    let date_parts: Vec<&str> = ann.date.split('-').collect();
                                    if date_parts.len() == 3 {
                                        format!(
                                            "{}/{}/{}/{}/{}/archive.parquet",
                                            ann.country, ann.subdivision,
                                            date_parts[0], date_parts[1], date_parts[2]
                                        )
                                    } else {
                                        debug!("Cannot derive path from announcement, skipping fetch");
                                        continue;
                                    }
                                } else {
                                    ann.path.clone()
                                };

                                let req = FetchRequest {
                                    hash: ann.hash.clone(),
                                    path,
                                    country: ann.country.clone(),
                                    subdivision: ann.subdivision.clone(),
                                    size: ann.size,
                                    source_node: ann.node_id.clone(),
                                };

                                if let Err(e) = tx.send(req).await {
                                    warn!(error = %e, "Failed to forward fetch request to replicator");
                                }
                            }
                        }
                        Err(e) => {
                            debug!(
                                error = %e,
                                bytes = msg.content.len(),
                                "Received non-announcement gossip message"
                            );
                        }
                    }
                }
                Ok(Event::NeighborUp(peer_id)) => {
                    let count = self.connected_peers.fetch_add(1, Ordering::Relaxed) + 1;
                    info!(peer = %peer_id, connected = count, "Gossip peer connected");

                    // Re-announce all local archives in a separate task so the
                    // receive loop isn't blocked during the broadcast. Broadcasting
                    // 80K+ messages inline would prevent receiving any gossip from
                    // the new peer until the broadcast completes.
                    if let Some(ref index) = self.index {
                        let index = Arc::clone(index);
                        let node_id = self.node_id.clone();
                        let sender = self.sender.read().await.clone();
                        let peer_id_str = peer_id.to_string();
                        if let Some(sender) = sender {
                            tokio::spawn(async move {
                                let entries = index.dump().await;
                                if entries.is_empty() {
                                    return;
                                }
                                info!(
                                    peer = %peer_id_str,
                                    archives = entries.len(),
                                    "Re-announcing local archives for peer catch-up"
                                );
                                let mut announced = 0u64;
                                for (path, entry) in &entries {
                                    if let Some((country, subdivision, date)) = parse_archive_path(path) {
                                        let ann = ArchiveAnnouncement {
                                            msg_type: "archive_available".to_string(),
                                            country,
                                            subdivision,
                                            date,
                                            hash: entry.hash.clone(),
                                            node_id: node_id.clone(),
                                            path: path.clone(),
                                            size: entry.size,
                                        };
                                        if let Ok(json) = serde_json::to_vec(&ann) {
                                            let _ = sender.broadcast(Bytes::from(json)).await;
                                            announced += 1;
                                        }
                                    }
                                    // Yield periodically to avoid starving other tasks
                                    if announced % 1000 == 0 {
                                        tokio::task::yield_now().await;
                                    }
                                }
                                info!(
                                    peer = %peer_id_str,
                                    announced,
                                    total = entries.len(),
                                    "Peer catch-up re-announcement complete"
                                );
                            });
                        }
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
