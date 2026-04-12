//! Iroh blob store wrapper — import, get, exists, tag operations.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use iroh_blobs::store::fs::FsStore;
use iroh_blobs::store::GcConfig;
use iroh_blobs::Hash;
use tracing::{debug, info};

use crate::index::PathIndex;

/// Recursively sum file sizes in a directory.
fn dir_size(path: &Path) -> u64 {
    let mut total = 0u64;
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            if let Ok(meta) = entry.metadata() {
                if meta.is_file() {
                    total += meta.len();
                } else if meta.is_dir() {
                    total += dir_size(&entry.path());
                }
            }
        }
    }
    total
}

/// Wraps an iroh-blobs `FsStore` with a logical path index.
pub struct BlobStore {
    store: FsStore,
    index: Arc<PathIndex>,
    blobs_dir: PathBuf,
}

impl BlobStore {
    /// Open or create the blob store at `data_dir/blobs`.
    ///
    /// Enables garbage collection (every 10 minutes) to clean up untagged
    /// blobs. Without GC, blobs that lose their tag (e.g. old path index
    /// copies replaced during catch-up sync) accumulate indefinitely.
    pub async fn open(data_dir: &Path, index: Arc<PathIndex>) -> Result<Self> {
        let blobs_dir = data_dir.join("blobs");
        tokio::fs::create_dir_all(&blobs_dir).await?;

        let mut opts = iroh_blobs::store::fs::Options::default();
        opts.gc = Some(GcConfig {
            interval: std::time::Duration::from_secs(600),
            add_protected: None,
        });

        let store = FsStore::load_with_opts(&blobs_dir, opts)
            .await
            .context("Failed to open iroh blob store")?;
        info!(path = %blobs_dir.display(), "Blob store opened with GC enabled (10min interval)");

        Ok(Self { store, index, blobs_dir })
    }

    /// Import bytes at a logical path. Returns the BLAKE3 hash hex string.
    ///
    /// Uses a named tag so that reimporting the same logical path (e.g.
    /// `_sync/index.json`) reassigns the tag to the new blob. The previous
    /// blob becomes untagged and eligible for garbage collection.
    pub async fn import(&self, logical_path: &str, data: Bytes) -> Result<String> {
        let size = data.len() as u64;

        // Use the logical path as the tag name for easy lookup.
        // with_named_tag reassigns the tag if it already exists,
        // making the old blob eligible for GC.
        let tag_name = path_to_tag(logical_path);

        let tag_info = self
            .store
            .add_bytes(data)
            .with_named_tag(tag_name)
            .await
            .context("Failed to import blob")?;

        let hash_hex = tag_info.hash.to_hex().to_string();

        // Update the path index
        self.index
            .insert(logical_path, hash_hex.clone(), size)
            .await?;

        debug!(
            path = logical_path,
            hash = %hash_hex,
            size,
            "Blob imported"
        );

        Ok(hash_hex)
    }

    /// Retrieve blob bytes by logical path.
    pub async fn get(&self, logical_path: &str) -> Result<Option<Bytes>> {
        let entry = match self.index.get(logical_path).await {
            Some(e) => e,
            None => return Ok(None),
        };

        let hash = entry
            .hash
            .parse::<Hash>()
            .context("Invalid hash in index")?;

        match self.store.get_bytes(hash).await {
            Ok(data) => Ok(Some(data)),
            Err(_) => {
                debug!(path = logical_path, "Blob not found in store (index stale?)");
                Ok(None)
            }
        }
    }

    /// Check if a blob exists at logical path.
    pub async fn exists(&self, logical_path: &str) -> bool {
        self.index.exists(logical_path).await
    }

    /// Get the underlying FsStore (for wiring into BlobsProtocol).
    pub fn inner(&self) -> &FsStore {
        &self.store
    }

    /// Register a blob that was downloaded by the Downloader (already in FsStore).
    /// Only updates the PathIndex — no re-import of bytes needed.
    pub async fn register_downloaded(
        &self,
        logical_path: &str,
        hash_hex: &str,
        size: u64,
    ) -> Result<()> {
        self.index
            .insert(logical_path, hash_hex.to_string(), size)
            .await?;
        debug!(
            path = logical_path,
            hash = hash_hex,
            size,
            "Registered downloaded blob in index"
        );
        Ok(())
    }

    /// Read blob bytes by BLAKE3 hash (hex string). Used for reading downloaded blobs
    /// that aren't in the path index (e.g. peer index blobs during catch-up sync).
    pub async fn get_by_hash(&self, hash_hex: &str) -> Result<Option<Bytes>> {
        let hash = hash_hex
            .parse::<Hash>()
            .context("Invalid BLAKE3 hash")?;
        match self.store.get_bytes(hash).await {
            Ok(data) => Ok(Some(data)),
            Err(_) => Ok(None),
        }
    }

    /// Total number of indexed blobs.
    pub async fn blob_count(&self) -> usize {
        self.index.len().await
    }

    /// Total bytes used by the blob store on disk.
    /// Walks the blobs directory and sums file sizes.
    pub async fn total_bytes(&self) -> u64 {
        let blobs_dir = self.blobs_dir.clone();
        tokio::task::spawn_blocking(move || {
            dir_size(&blobs_dir)
        })
        .await
        .unwrap_or(0)
    }
}

/// Convert a logical path to a tag name.
/// Replaces `/` with `:` since tags are flat strings.
fn path_to_tag(path: &str) -> String {
    format!("path:{}", path.replace('/', ":"))
}
