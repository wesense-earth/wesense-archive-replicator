//! Axum HTTP route handlers for the sidecar API.

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, head, put};
use axum::Json;
use serde::Serialize;
use tracing::error;

use crate::config::Config;
use crate::gossip::GossipHandle;
use crate::index::PathIndex;
use crate::replicator::ReplicationStats;
use crate::store::BlobStore;

/// Shared application state for all route handlers.
pub struct AppState {
    pub store: Arc<BlobStore>,
    pub index: Arc<PathIndex>,
    pub gossip: Arc<GossipHandle>,
    pub node_id: String,
    pub config: Arc<Config>,
    pub stats: Arc<ReplicationStats>,
}

/// Response from PUT /blobs/{path}
#[derive(Serialize)]
struct StoreResponse {
    hash: String,
}

/// Response from GET /status
#[derive(Serialize)]
struct StatusResponse {
    node_id: String,
    blob_count: usize,
    gossip_topic: String,
    store_scope: Vec<String>,
    replication: ReplicationStatusResponse,
}

#[derive(Serialize)]
struct ReplicationStatusResponse {
    replicated: u64,
    skipped_existing: u64,
    skipped_scope: u64,
    failed: u64,
    last_replicated: Option<String>,
    last_reconciliation: Option<String>,
}

/// Build the axum router with all routes.
pub fn router(state: Arc<AppState>) -> axum::Router {
    axum::Router::new()
        .route("/blobs/{*path}", put(put_blob))
        .route("/blobs/{*path}", get(get_blob))
        .route("/blobs/{*path}", head(head_blob))
        .route("/list/{*path}", get(list_dir))
        .route(
            "/archived-dates/{country}/{subdivision}",
            get(archived_dates),
        )
        .route("/status", get(status))
        .with_state(state)
}

/// PUT /blobs/{path} — Store bytes at a logical path.
async fn put_blob(
    State(state): State<Arc<AppState>>,
    Path(path): Path<String>,
    body: Bytes,
) -> Response {
    if body.is_empty() {
        return (StatusCode::BAD_REQUEST, "Empty body").into_response();
    }

    let size = body.len() as u64;

    match state.store.import(&path, body).await {
        Ok(hash) => {
            // Try to announce via gossip (extract date from path if possible)
            if let Some((country, subdivision, date)) = parse_archive_path(&path) {
                if let Err(e) = state
                    .gossip
                    .announce_archive(&country, &subdivision, &date, &hash, &path, size)
                    .await
                {
                    error!(error = %e, "Failed to announce via gossip");
                }
            }

            Json(StoreResponse { hash }).into_response()
        }
        Err(e) => {
            error!(error = %e, path = %path, "Failed to store blob");
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}

/// GET /blobs/{path} — Retrieve blob by logical path.
async fn get_blob(
    State(state): State<Arc<AppState>>,
    Path(path): Path<String>,
) -> Response {
    match state.store.get(&path).await {
        Ok(Some(data)) => {
            let content_type = if path.ends_with(".parquet") {
                "application/octet-stream"
            } else if path.ends_with(".json") {
                "application/json"
            } else {
                "application/octet-stream"
            };

            (
                StatusCode::OK,
                [(axum::http::header::CONTENT_TYPE, content_type)],
                data,
            )
                .into_response()
        }
        Ok(None) => (StatusCode::NOT_FOUND, "Not found").into_response(),
        Err(e) => {
            error!(error = %e, path = %path, "Failed to retrieve blob");
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}

/// HEAD /blobs/{path} — Check if blob exists.
async fn head_blob(
    State(state): State<Arc<AppState>>,
    Path(path): Path<String>,
) -> StatusCode {
    if state.store.exists(&path).await {
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    }
}

/// GET /list/{path} — List entries under a directory prefix.
async fn list_dir(
    State(state): State<Arc<AppState>>,
    Path(path): Path<String>,
) -> Json<Vec<String>> {
    let entries = state.index.list_dir(&path).await;
    Json(entries)
}

/// GET /archived-dates/{country}/{subdivision} — Return set of archived ISO dates.
async fn archived_dates(
    State(state): State<Arc<AppState>>,
    Path((country, subdivision)): Path<(String, String)>,
) -> Json<Vec<String>> {
    let dates = state.index.archived_dates(&country, &subdivision).await;
    Json(dates.into_iter().collect())
}

/// GET /status — Node status information with replication stats.
async fn status(State(state): State<Arc<AppState>>) -> Json<StatusResponse> {
    let scope_strings: Vec<String> = state
        .config
        .store_scope
        .iter()
        .map(|p| format!("{}/{}", p.country, p.subdivision))
        .collect();

    let repl_stats = &state.stats;

    Json(StatusResponse {
        node_id: state.node_id.clone(),
        blob_count: state.store.blob_count().await,
        gossip_topic: state.config.gossip_topic.clone(),
        store_scope: scope_strings,
        replication: ReplicationStatusResponse {
            replicated: repl_stats.replicated(),
            skipped_existing: repl_stats.skipped_existing(),
            skipped_scope: repl_stats.skipped_scope(),
            failed: repl_stats.failed(),
            last_replicated: repl_stats.last_replicated(),
            last_reconciliation: repl_stats.last_reconciliation(),
        },
    })
}

/// Try to parse `{country}/{subdivision}/{YYYY}/{MM}/{DD}/...` from a path.
/// Returns (country, subdivision, "YYYY-MM-DD") if the pattern matches.
pub(crate) fn parse_archive_path(path: &str) -> Option<(String, String, String)> {
    let parts: Vec<&str> = path.split('/').collect();
    if parts.len() >= 5 {
        let country = parts[0];
        let subdivision = parts[1];
        let year = parts[2];
        let month = parts[3];
        let day = parts[4];

        // Basic validation
        if year.len() == 4
            && month.len() == 2
            && day.len() == 2
            && year.chars().all(|c| c.is_ascii_digit())
            && month.chars().all(|c| c.is_ascii_digit())
            && day.chars().all(|c| c.is_ascii_digit())
        {
            return Some((
                country.to_string(),
                subdivision.to_string(),
                format!("{}-{}-{}", year, month, day),
            ));
        }
    }
    None
}
