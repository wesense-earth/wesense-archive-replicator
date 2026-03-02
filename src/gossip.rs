//! Gossip protocol — join topic, broadcast archive announcements, log received.

use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use iroh_gossip::api::{Event, GossipSender};
use iroh_gossip::net::Gossip;
use iroh_gossip::TopicId;
use n0_future::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
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
}

/// Manages gossip topic membership and message broadcasting.
pub struct GossipHandle {
    sender: RwLock<Option<GossipSender>>,
    gossip: Gossip,
    topic_id: TopicId,
    node_id: String,
}

impl GossipHandle {
    /// Create a new gossip handle (does not join yet — call `start` after router is spawned).
    pub fn new(gossip: Gossip, topic_name: &str, node_id: String) -> Self {
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
    ) -> Result<()> {
        let announcement = ArchiveAnnouncement {
            msg_type: "archive_available".to_string(),
            country: country.to_string(),
            subdivision: subdivision.to_string(),
            date: date.to_string(),
            hash: hash.to_string(),
            node_id: self.node_id.clone(),
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
                "Archive announced via gossip"
            );
        } else {
            debug!("Gossip sender not ready, skipping announcement");
        }

        Ok(())
    }

    /// Receive loop — logs incoming gossip messages.
    async fn receive_loop(
        &self,
        mut receiver: iroh_gossip::api::GossipReceiver,
    ) {
        while let Some(event) = receiver.next().await {
            match event {
                Ok(Event::Received(msg)) => {
                    match serde_json::from_slice::<ArchiveAnnouncement>(&msg.content) {
                        Ok(ann) => {
                            info!(
                                msg_type = %ann.msg_type,
                                country = %ann.country,
                                subdivision = %ann.subdivision,
                                date = %ann.date,
                                hash = %ann.hash,
                                from_node = %ann.node_id,
                                "Received gossip announcement"
                            );
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
