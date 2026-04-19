//! Logical path → blob hash index with JSON persistence.
//!
//! Maps paths like `nz/wgn/2026/03/01/readings.parquet` to their
//! BLAKE3 hashes and iroh tag names, persisted as a JSON file on disk.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{debug, info};

/// A single entry in the index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexEntry {
    /// BLAKE3 hash hex string.
    pub hash: String,
    /// Size in bytes.
    pub size: u64,
}

/// Thread-safe path→hash index with JSON persistence.
pub struct PathIndex {
    entries: RwLock<BTreeMap<String, IndexEntry>>,
    path: PathBuf,
}

impl PathIndex {
    /// Load or create the index at the given directory.
    pub async fn load(data_dir: &Path) -> Result<Self> {
        let path = data_dir.join("path_index.json");

        let entries = if path.exists() {
            let data = tokio::fs::read_to_string(&path).await?;
            let map: BTreeMap<String, IndexEntry> = serde_json::from_str(&data)?;
            info!(count = map.len(), "Loaded path index");
            map
        } else {
            info!("Creating new path index");
            BTreeMap::new()
        };

        Ok(Self {
            entries: RwLock::new(entries),
            path,
        })
    }

    /// Insert or update a path mapping and persist to disk.
    pub async fn insert(&self, logical_path: &str, hash: String, size: u64) -> Result<()> {
        let entry = IndexEntry { hash, size };

        {
            let mut entries = self.entries.write().await;
            entries.insert(logical_path.to_string(), entry);
        }

        self.persist().await?;
        debug!(path = logical_path, "Index entry updated");
        Ok(())
    }

    /// Look up a hash by logical path.
    pub async fn get(&self, logical_path: &str) -> Option<IndexEntry> {
        let entries = self.entries.read().await;
        entries.get(logical_path).cloned()
    }

    /// Check if a path exists in the index.
    pub async fn exists(&self, logical_path: &str) -> bool {
        let entries = self.entries.read().await;
        entries.contains_key(logical_path)
    }

    /// List entries under a directory prefix.
    ///
    /// Returns immediate children names (like `ls`), not full paths.
    pub async fn list_dir(&self, prefix: &str) -> Vec<String> {
        let entries = self.entries.read().await;

        // Normalise prefix to end with /
        let prefix_norm = if prefix.is_empty() {
            String::new()
        } else if prefix.ends_with('/') {
            prefix.to_string()
        } else {
            format!("{}/", prefix)
        };

        let mut children = BTreeSet::new();

        for key in entries.keys() {
            if let Some(rest) = key.strip_prefix(&prefix_norm) {
                // Take the first path component
                let child = rest.split('/').next().unwrap_or(rest);
                if !child.is_empty() {
                    children.insert(child.to_string());
                }
            }
        }

        children.into_iter().collect()
    }

    /// Walk the index for `{country}/{subdivision}/{YYYY}/{MM}/{DD}/` patterns
    /// and return the set of ISO date strings.
    pub async fn archived_dates(&self, country: &str, subdivision: &str) -> BTreeSet<String> {
        let entries = self.entries.read().await;
        let prefix = format!("{}/{}/", country, subdivision);

        let mut dates = BTreeSet::new();

        for key in entries.keys() {
            if let Some(rest) = key.strip_prefix(&prefix) {
                // rest should be like "2026/03/01/readings.parquet"
                let parts: Vec<&str> = rest.split('/').collect();
                if parts.len() >= 3 {
                    let year = parts[0];
                    let month = parts[1];
                    let day = parts[2];

                    // Basic validation: 4-digit year, 2-digit month/day
                    if year.len() == 4 && month.len() == 2 && day.len() == 2 {
                        if year.chars().all(|c| c.is_ascii_digit())
                            && month.chars().all(|c| c.is_ascii_digit())
                            && day.chars().all(|c| c.is_ascii_digit())
                        {
                            dates.insert(format!("{}-{}-{}", year, month, day));
                        }
                    }
                }
            }
        }

        dates
    }

    /// Return all entries as a map (for the /path-index API).
    pub async fn dump(&self) -> BTreeMap<String, IndexEntry> {
        let entries = self.entries.read().await;
        entries.clone()
    }

    /// Total number of entries in the index.
    pub async fn len(&self) -> usize {
        let entries = self.entries.read().await;
        entries.len()
    }

    /// Persist the index to disk.
    async fn persist(&self) -> Result<()> {
        let entries = self.entries.read().await;
        let json = serde_json::to_string_pretty(&*entries)?;

        // Write to temp file then rename for atomicity
        let tmp = self.path.with_extension("json.tmp");
        tokio::fs::write(&tmp, json).await?;
        tokio::fs::rename(&tmp, &self.path).await?;

        Ok(())
    }
}
