//! Iroh blob store wrapper — import, get, exists, tag operations.

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use iroh_blobs::store::fs::FsStore;
use iroh_blobs::Hash;
use tracing::{debug, info};

use crate::index::PathIndex;

/// Wraps an iroh-blobs `FsStore` with a logical path index.
pub struct BlobStore {
    store: FsStore,
    index: Arc<PathIndex>,
}

impl BlobStore {
    /// Open or create the blob store at `data_dir/blobs`.
    pub async fn open(data_dir: &Path, index: Arc<PathIndex>) -> Result<Self> {
        let blobs_dir = data_dir.join("blobs");
        tokio::fs::create_dir_all(&blobs_dir).await?;

        let store = FsStore::load(&blobs_dir)
            .await
            .context("Failed to open iroh blob store")?;
        info!(path = %blobs_dir.display(), "Blob store opened");

        Ok(Self { store, index })
    }

    /// Import bytes at a logical path. Returns the BLAKE3 hash hex string.
    pub async fn import(&self, logical_path: &str, data: Bytes) -> Result<String> {
        let size = data.len() as u64;

        // Use the logical path as the tag name for easy lookup
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

    /// Total number of indexed blobs.
    pub async fn blob_count(&self) -> usize {
        self.index.len().await
    }
}

/// Convert a logical path to a tag name.
/// Replaces `/` with `:` since tags are flat strings.
fn path_to_tag(path: &str) -> String {
    format!("path:{}", path.replace('/', ":"))
}
