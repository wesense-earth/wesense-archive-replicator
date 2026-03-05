//! Gossip protocol — join topic, broadcast archive announcements, forward to replicator.

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
        }
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

                                if let Err(e) = tx.try_send(req) {
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
                    info!(peer = %peer_id, "Gossip peer connected");
                }
                Ok(Event::NeighborDown(peer_id)) => {
                    info!(peer = %peer_id, "Gossip peer disconnected");
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
