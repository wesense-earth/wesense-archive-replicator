//! Replicator — downloads blobs from peers based on gossip announcements.
//!
//! Catch-up for missed gossip is handled by NeighborUp re-announcements in gossip.rs.
//! No HTTP reconciliation needed — everything flows over QUIC via gossip + blob downloads.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use iroh::PublicKey;
use iroh_blobs::api::downloader::Downloader;
use iroh_blobs::Hash;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info};

use crate::config::Config;
use crate::gossip::FetchRequest;
use crate::index::PathIndex;
use crate::store::BlobStore;

/// Atomic counters for replication statistics.
pub struct ReplicationStats {
    replicated: AtomicU64,
    skipped_existing: AtomicU64,
    skipped_scope: AtomicU64,
    failed: AtomicU64,
    last_replicated: RwLock<Option<String>>,
    last_reconciliation: RwLock<Option<String>>,
}

impl ReplicationStats {
    pub fn new() -> Self {
        Self {
            replicated: AtomicU64::new(0),
            skipped_existing: AtomicU64::new(0),
            skipped_scope: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            last_replicated: RwLock::new(None),
            last_reconciliation: RwLock::new(None),
        }
    }

    pub fn replicated(&self) -> u64 {
        self.replicated.load(Ordering::Relaxed)
    }

    pub fn skipped_existing(&self) -> u64 {
        self.skipped_existing.load(Ordering::Relaxed)
    }

    pub fn skipped_scope(&self) -> u64 {
        self.skipped_scope.load(Ordering::Relaxed)
    }

    pub fn failed(&self) -> u64 {
        self.failed.load(Ordering::Relaxed)
    }

    pub fn last_replicated(&self) -> Option<String> {
        // Use try_read to avoid blocking — return None if locked
        self.last_replicated
            .try_read()
            .ok()
            .and_then(|g| g.clone())
    }

    pub fn last_reconciliation(&self) -> Option<String> {
        self.last_reconciliation
            .try_read()
            .ok()
            .and_then(|g| g.clone())
    }

    async fn record_replicated(&self, path: &str) {
        self.replicated.fetch_add(1, Ordering::Relaxed);
        let mut last = self.last_replicated.write().await;
        *last = Some(path.to_string());
    }

    #[allow(dead_code)]
    async fn record_reconciliation(&self) {
        let mut last = self.last_reconciliation.write().await;
        *last = Some(chrono::Utc::now().to_rfc3339());
    }
}

/// Downloads blobs from peers based on FetchRequests from gossip and reconciliation.
pub struct Replicator {
    downloader: Downloader,
    store: Arc<BlobStore>,
    index: Arc<PathIndex>,
    config: Arc<Config>,
    stats: Arc<ReplicationStats>,
}

impl Replicator {
    pub fn new(
        downloader: Downloader,
        store: Arc<BlobStore>,
        index: Arc<PathIndex>,
        config: Arc<Config>,
        stats: Arc<ReplicationStats>,
    ) -> Self {
        Self {
            downloader,
            store,
            index,
            config,
            stats,
        }
    }

    /// Main loop — receives FetchRequests and processes them.
    pub async fn run(self, mut rx: mpsc::Receiver<FetchRequest>) {
        info!("Replicator started, waiting for fetch requests");

        while let Some(req) = rx.recv().await {
            if let Err(e) = self.handle_fetch_request(&req).await {
                error!(
                    error = %e,
                    path = %req.path,
                    hash = %req.hash,
                    "Failed to replicate archive"
                );
                self.stats.failed.fetch_add(1, Ordering::Relaxed);
            }
        }

        info!("Replicator shutting down (channel closed)");
    }

    async fn handle_fetch_request(&self, req: &FetchRequest) -> Result<()> {
        // 1. Check store scope (skip for internal sync blobs)
        if !req.path.starts_with("_sync/") && !self.config.matches_store_scope(&req.country, &req.subdivision) {
            debug!(
                path = %req.path,
                country = %req.country,
                subdivision = %req.subdivision,
                "Skipping archive outside store scope"
            );
            self.stats.skipped_scope.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }

        // 2. Check if already in index (skip for _sync/ blobs — they change each cycle)
        if !req.path.starts_with("_sync/") && self.index.exists(&req.path).await {
            debug!(path = %req.path, "Archive already exists, skipping");
            self.stats
                .skipped_existing
                .fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }

        // 3. Parse hash
        let hash: Hash = req
            .hash
            .parse()
            .context("Invalid BLAKE3 hash in fetch request")?;

        // 4. Parse source node ID
        let peer_id: PublicKey = req
            .source_node
            .parse()
            .context("Invalid node ID in fetch request")?;

        info!(
            path = %req.path,
            hash = %&req.hash[..16.min(req.hash.len())],
            source = %&req.source_node[..16.min(req.source_node.len())],
            size = req.size,
            "Downloading archive from peer"
        );

        // 5. Download blob via iroh Downloader
        // The Downloader reuses cached QUIC connections from gossip.
        let providers: Vec<PublicKey> = vec![peer_id];
        self.downloader
            .download(hash, providers)
            .await
            .context("Blob download failed")?;

        // 6. Register in path index (Downloader stored bytes in FsStore already)
        self.store
            .register_downloaded(&req.path, &req.hash, req.size)
            .await
            .context("Failed to register downloaded blob in index")?;

        info!(
            path = %req.path,
            hash = %&req.hash[..16.min(req.hash.len())],
            "Replicated archive successfully"
        );
        self.stats.record_replicated(&req.path).await;

        Ok(())
    }
}
