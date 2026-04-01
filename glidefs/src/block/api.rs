//! HTTP API for dynamic export management.
//!
//! Provides REST endpoints for creating, draining, promoting, and removing exports.
//! Used by orchestrators for microVM scale-to-zero and live migration.

use crate::block::metrics::prometheus_header;
use crate::block::router::{ExportInfo, ExportRouter, RouterError};
use crate::config::ExportConfig;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use url::form_urlencoded;

/// Request to create or update an export (PUT /api/exports/{name}).
/// Name comes from URL path, not body.
#[derive(Debug, Deserialize)]
pub struct PutExportRequest {
    pub size_gb: f64,
    #[serde(default)]
    pub s3_prefix: Option<String>,
    #[serde(default)]
    pub readonly: bool,
    #[serde(default)]
    pub block_size: Option<usize>,
    /// If set, fork this export from the named S3 manifest.
    #[serde(default)]
    pub manifest_name: Option<String>,
    /// Blocks per S3 pack (default: inherit from global config). 0 = manual mode.
    #[serde(default)]
    pub flush_threshold: Option<usize>,
    /// Flush mode: "auto" (default) or "manual" (drain-only).
    #[serde(default)]
    pub flush_mode: Option<String>,
    /// Block device transport: "nbd" (default) or "ublk" (Linux 6.0+).
    #[serde(default)]
    pub transport: Option<String>,
    /// If set (with manifest_name), fork from this specific snapshot sequence.
    #[serde(default)]
    pub snapshot_sequence: Option<u64>,
}

/// Optional request body for POST /api/exports/{name}/snapshot.
#[derive(Debug, Deserialize)]
pub struct SnapshotRequest {
    /// If set, also publish the manifest under this tag name.
    #[serde(default)]
    pub tag: Option<String>,
}

/// Request body for POST /api/exports/{name}/tag.
#[derive(Debug, Deserialize)]
pub struct TagRequest {
    /// Tag name to publish the manifest under.
    pub tag: String,
}

/// Response for export info.
#[derive(Debug, Serialize, Deserialize)]
pub struct ExportInfoResponse {
    pub name: String,
    pub size_bytes: u64,
    pub readonly: bool,
    pub transport: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device: Option<String>,
    /// S3 prefix for pack/manifest storage (None = export name).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub s3_prefix: Option<String>,
    /// Unflushed bytes waiting to be synced to S3.
    pub dirty_bytes: u64,
    /// Total logical bytes stored in S3.
    pub s3_bytes: u64,
    /// Filesystem used bytes from ext4 superblock (None if not ext4 or read failed).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fs_used_bytes: Option<u64>,
}

impl From<ExportInfo> for ExportInfoResponse {
    fn from(e: ExportInfo) -> Self {
        ExportInfoResponse {
            name: e.name,
            size_bytes: e.size,
            readonly: e.readonly,
            transport: e.transport,
            device: e.device.map(|p| p.to_string_lossy().into_owned()),
            s3_prefix: e.s3_prefix,
            dirty_bytes: e.dirty_bytes,
            s3_bytes: e.s3_bytes,
            fs_used_bytes: e.fs_used_bytes,
        }
    }
}

/// Response for list exports.
#[derive(Debug, Serialize, Deserialize)]
pub struct ListExportsResponse {
    pub exports: Vec<ExportInfoResponse>,
}

/// Generic API response.
#[derive(Debug, Serialize)]
pub struct ApiResponse {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl ApiResponse {
    fn success(message: impl Into<String>) -> Self {
        Self {
            success: true,
            message: Some(message.into()),
            error: None,
        }
    }

    fn error(error: impl Into<String>) -> Self {
        Self {
            success: false,
            message: None,
            error: Some(error.into()),
        }
    }
}

type BoxBody = http_body_util::Full<Bytes>;

fn json_response<T: Serialize>(status: StatusCode, body: &T) -> Response<BoxBody> {
    let json = serde_json::to_string(body).unwrap_or_else(|_| "{}".to_string());
    Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .body(Full::new(Bytes::from(json)))
        .unwrap()
}

fn empty_response(status: StatusCode) -> Response<BoxBody> {
    Response::builder()
        .status(status)
        .body(Full::new(Bytes::new()))
        .unwrap()
}

fn error_response(status: StatusCode, message: &str) -> Response<BoxBody> {
    json_response(status, &ApiResponse::error(message))
}

/// Check if an export name is valid: 1-128 chars, alphanumeric/hyphen/underscore/dot,
/// starting with an alphanumeric character.
fn is_valid_export_name(name: &str) -> bool {
    if name.is_empty() || name.len() > 128 {
        return false;
    }
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false; // unreachable: checked is_empty above
    };
    first.is_ascii_alphanumeric()
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
}

/// Public test wrapper for `handle_request`.
#[cfg(feature = "test-utils")]
#[allow(dead_code)]
pub async fn handle_request_for_test<B>(
    router: Arc<ExportRouter>,
    req: Request<B>,
) -> Result<Response<BoxBody>, Infallible>
where
    B: hyper::body::Body + Send + 'static,
    B::Data: Send,
    B::Error: std::fmt::Display,
{
    handle_request(router, req).await
}

/// Handle API requests.
async fn handle_request<B>(
    router: Arc<ExportRouter>,
    req: Request<B>,
) -> Result<Response<BoxBody>, Infallible>
where
    B: hyper::body::Body + Send + 'static,
    B::Data: Send,
    B::Error: std::fmt::Display,
{
    let start = std::time::Instant::now();
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let path_parts: Vec<&str> = path.trim_start_matches('/').split('/').collect();

    let response = match (method.clone(), path_parts.as_slice()) {
        // GET /api/exports - List all exports
        (Method::GET, ["api", "exports"]) => {
            let exports = router.list_exports().await;
            let responses: Vec<_> = exports
                .into_iter()
                .map(ExportInfoResponse::from)
                .collect();
            json_response(StatusCode::OK, &ListExportsResponse { exports: responses })
        }

        // PUT /api/exports/{name} - Create or resize export (idempotent)
        //
        // The "smart" endpoint for orchestrators:
        // - Export doesn't exist → create with specified size
        // - Export exists, requested size larger → grow it
        // - Export exists, requested size same/smaller → no-op (success)
        (Method::PUT, ["api", "exports", name]) => {
            if !is_valid_export_name(name) {
                return Ok(error_response(
                    StatusCode::BAD_REQUEST,
                    &format!(
                        "Invalid export name '{}': must be 1-128 chars, alphanumeric/hyphen/underscore/dot, starting with alphanumeric",
                        name
                    ),
                ));
            }

            let body = match req.collect().await {
                Ok(b) => {
                    let bytes = b.to_bytes();
                    if bytes.len() > 65_536 {
                        return Ok(error_response(
                            StatusCode::BAD_REQUEST,
                            "Request body too large (max 64KB)",
                        ));
                    }
                    bytes
                }
                Err(e) => return Ok(error_response(StatusCode::BAD_REQUEST, &e.to_string())),
            };

            let put_req: PutExportRequest = match serde_json::from_slice(&body) {
                Ok(r) => r,
                Err(e) => {
                    return Ok(error_response(
                        StatusCode::BAD_REQUEST,
                        &format!("Invalid JSON: {}", e),
                    ));
                }
            };

            if !put_req.size_gb.is_finite() || put_req.size_gb <= 0.0 || put_req.size_gb > 16384.0 {
                return Ok(error_response(
                    StatusCode::BAD_REQUEST,
                    &format!(
                        "Invalid size_gb {}: must be between 0 and 16384",
                        put_req.size_gb
                    ),
                ));
            }

            if let Some(bs) = put_req.block_size
                && (!bs.is_power_of_two() || !(4096..=1_048_576).contains(&bs))
            {
                return Ok(error_response(
                    StatusCode::BAD_REQUEST,
                    &format!(
                        "Invalid block_size {}: must be a power of 2 between 4096 and 1048576",
                        bs
                    ),
                ));
            }

            if let Some(ref fm) = put_req.flush_mode
                && fm != "auto"
                && fm != "manual"
            {
                return Ok(error_response(
                    StatusCode::BAD_REQUEST,
                    &format!("Invalid flush_mode '{}': must be 'auto' or 'manual'", fm),
                ));
            }

            if let Some(ref t) = put_req.transport {
                match t.as_str() {
                    "nbd" => {}
                    "ublk" => {
                        if !ExportRouter::device_available("ublk") {
                            return Ok(error_response(
                                StatusCode::BAD_REQUEST,
                                "Transport 'ublk' is not available on this platform/build",
                            ));
                        }
                    }
                    _ => {
                        return Ok(error_response(
                            StatusCode::BAD_REQUEST,
                            &format!(
                                "Invalid transport '{}': must be 'nbd' or 'ublk'",
                                t
                            ),
                        ));
                    }
                }
            }

            if let Some(ref prefix) = put_req.s3_prefix
                && (prefix.contains("..") || prefix.starts_with('/'))
            {
                return Ok(error_response(
                    StatusCode::BAD_REQUEST,
                    "Invalid s3_prefix: must not contain '..' or start with '/'",
                ));
            }

            if put_req.snapshot_sequence.is_some() && put_req.manifest_name.is_none() {
                return Ok(error_response(
                    StatusCode::BAD_REQUEST,
                    "snapshot_sequence requires manifest_name to be set",
                ));
            }

            // Check if export already exists (direct lookup, not a full scan)
            let existing = router.get_export_info(name).await;

            match existing {
                Some(export) => {
                    // Export exists - check if resize needed
                    let current_size_gb = export.size as f64 / 1_073_741_824.0;
                    if put_req.size_gb > current_size_gb {
                        // Need to grow
                        match router.resize_export(name, put_req.size_gb).await {
                            Ok(()) => {
                                // Re-register device after resize (device was removed)
                                let transport = export.transport.as_str();
                                #[cfg(target_os = "linux")]
                                if let Err(e) = router.register_device(name, transport).await {
                                    warn!(export = %name, error = %e, "device re-registration after resize failed");
                                }
                                let _ = transport; // suppress unused warning on non-Linux
                                match router.get_export_info(name).await {
                                    Some(info) => json_response(
                                        StatusCode::OK,
                                        &ExportInfoResponse::from(info),
                                    ),
                                    None => error_response(
                                        StatusCode::INTERNAL_SERVER_ERROR,
                                        "Export resized but not found in map",
                                    ),
                                }
                            }
                            Err(e) => {
                                error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string())
                            }
                        }
                    } else {
                        // Already at or above requested size - return current state
                        json_response(StatusCode::OK, &ExportInfoResponse::from(export))
                    }
                }
                None => {
                    // Export doesn't exist - create it
                    let transport = put_req.transport.as_deref().unwrap_or("nbd").to_string();
                    let config = ExportConfig {
                        name: name.to_string(),
                        size_gb: put_req.size_gb,
                        s3_prefix: put_req.s3_prefix,
                        block_size: put_req.block_size,
                        flush_threshold: put_req.flush_threshold,
                        flush_mode: put_req.flush_mode,
                        transport: put_req.transport,
                    };

                    match router
                        .create_export(
                            config.clone(),
                            put_req.readonly,
                            put_req.manifest_name.as_deref(),
                            put_req.snapshot_sequence,
                        )
                        .await
                    {
                        Ok(()) => {
                            // Run S3 persist and device registration concurrently.
                            // save_export must succeed; register_device is best-effort.
                            #[cfg(target_os = "linux")]
                            let (save_result, register_result) = tokio::join!(
                                router.save_export(&config),
                                router.register_device(name, &transport),
                            );
                            #[cfg(not(target_os = "linux"))]
                            let save_result = router.save_export(&config).await;

                            if let Err(e) = save_result {
                                // Export is functional locally but won't survive a restart.
                                // Return 503 so the orchestrator can retry (create_export is idempotent).
                                return Ok(error_response(
                                    StatusCode::SERVICE_UNAVAILABLE,
                                    &format!("Export created but definition not persisted to S3: {e}"),
                                ));
                            }

                            #[cfg(target_os = "linux")]
                            if let Err(e) = register_result {
                                warn!(export = %name, error = %e, "device registration failed");
                            }
                            let _ = transport; // suppress unused warning on non-Linux

                            match router.get_export_info(name).await {
                                Some(info) => json_response(
                                    StatusCode::CREATED,
                                    &ExportInfoResponse::from(info),
                                ),
                                None => error_response(
                                    StatusCode::INTERNAL_SERVER_ERROR,
                                    "Export created but not found in map",
                                ),
                            }
                        }
                        Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
                    }
                }
            }
        }

        // GET /api/exports/{name} - Get export info
        (Method::GET, ["api", "exports", name]) => {
            let exports = router.list_exports().await;
            match exports.into_iter().find(|e| e.name == *name) {
                Some(export) => json_response(
                    StatusCode::OK,
                    &ExportInfoResponse::from(export),
                ),
                None => error_response(
                    StatusCode::NOT_FOUND,
                    &format!("Export '{}' not found", name),
                ),
            }
        }

        // POST /api/exports/{name}/drain - Drain export to S3
        (Method::POST, ["api", "exports", name, "drain"]) => {
            match router.drain_export(name).await {
                Ok(()) => json_response(
                    StatusCode::OK,
                    &ApiResponse::success(format!("Export '{}' drained", name)),
                ),
                Err(RouterError::ExportNotFound(name)) => error_response(
                    StatusCode::NOT_FOUND,
                    &format!("Export '{}' not found", name),
                ),
                Err(RouterError::InvalidExportName(_)) => error_response(
                    StatusCode::BAD_REQUEST,
                    &format!("Invalid export name '{}'", name),
                ),
                Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
            }
        }

        // POST /api/exports/{name}/snapshot - Snapshot export to S3 manifest
        (Method::POST, ["api", "exports", name, "snapshot"]) => {
            // Parse optional body for tag
            let tag = match req.into_body().collect().await {
                Ok(b) => {
                    let bytes = b.to_bytes();
                    if bytes.is_empty() {
                        None
                    } else {
                        match serde_json::from_slice::<SnapshotRequest>(&bytes) {
                            Ok(r) => r.tag,
                            Err(e) => {
                                return Ok(error_response(
                                    StatusCode::BAD_REQUEST,
                                    &format!("Invalid JSON: {}", e),
                                ));
                            }
                        }
                    }
                }
                Err(_) => None,
            };
            match router.snapshot_export(name, tag.as_deref()).await {
                Ok(result) => json_response(StatusCode::OK, &result),
                Err(RouterError::ExportNotFound(name)) => error_response(
                    StatusCode::NOT_FOUND,
                    &format!("Export '{}' not found", name),
                ),
                Err(RouterError::InvalidExportName(_)) => error_response(
                    StatusCode::BAD_REQUEST,
                    &format!("Invalid export name '{}'", name),
                ),
                Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
            }
        }

        // POST /api/exports/{name}/tag - Tag export manifest under a name
        (Method::POST, ["api", "exports", name, "tag"]) => {
            let body = match req.into_body().collect().await {
                Ok(b) => b.to_bytes(),
                Err(e) => return Ok(error_response(StatusCode::BAD_REQUEST, &e.to_string())),
            };
            let tag_req: TagRequest = match serde_json::from_slice(&body) {
                Ok(r) => r,
                Err(e) => {
                    return Ok(error_response(
                        StatusCode::BAD_REQUEST,
                        &format!("Invalid JSON: {}", e),
                    ));
                }
            };
            match router.tag_export(name, &tag_req.tag).await {
                Ok(()) => json_response(
                    StatusCode::OK,
                    &ApiResponse::success(format!(
                        "Tagged '{}' as '{}'",
                        name, tag_req.tag
                    )),
                ),
                Err(RouterError::ExportNotFound(name)) => error_response(
                    StatusCode::NOT_FOUND,
                    &format!("Export '{}' not found", name),
                ),
                Err(RouterError::InvalidExportName(_)) => error_response(
                    StatusCode::BAD_REQUEST,
                    "Invalid export or tag name",
                ),
                Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
            }
        }

        // GET /api/exports/{name}/snapshots - List snapshot sequences
        (Method::GET, ["api", "exports", name, "snapshots"]) => {
            match router.list_export_snapshots(name).await {
                Ok(sequences) => json_response(StatusCode::OK, &sequences),
                Err(RouterError::ExportNotFound(name)) => error_response(
                    StatusCode::NOT_FOUND,
                    &format!("Export '{}' not found", name),
                ),
                Err(RouterError::InvalidExportName(_)) => error_response(
                    StatusCode::BAD_REQUEST,
                    &format!("Invalid export name '{}'", name),
                ),
                Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
            }
        }

        // DELETE /api/exports/{name}/snapshots/{sequence} - Delete a snapshot
        (Method::DELETE, ["api", "exports", name, "snapshots", seq_str]) => {
            match seq_str.parse::<u64>() {
                Err(_) => error_response(
                    StatusCode::BAD_REQUEST,
                    "snapshot sequence must be a valid u64",
                ),
                Ok(sequence) => match router.delete_export_snapshot(name, sequence).await {
                    Ok(()) => json_response(
                        StatusCode::OK,
                        &ApiResponse::success(format!(
                            "Snapshot seq={} deleted for '{}'",
                            sequence, name
                        )),
                    ),
                    Err(RouterError::ExportNotFound(name)) => error_response(
                        StatusCode::NOT_FOUND,
                        &format!("Export '{}' not found", name),
                    ),
                    Err(RouterError::InvalidExportName(_)) => error_response(
                        StatusCode::BAD_REQUEST,
                        &format!("Invalid export name '{}'", name),
                    ),
                    Err(e) => {
                        error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string())
                    }
                },
            }
        }

        // POST /api/exports/{name}/promote - Promote readonly to read-write
        (Method::POST, ["api", "exports", name, "promote"]) => {
            match router.promote_export(name).await {
                Ok(()) => json_response(
                    StatusCode::OK,
                    &ApiResponse::success(format!("Export '{}' promoted to read-write", name)),
                ),
                Err(RouterError::ExportNotFound(name)) => error_response(
                    StatusCode::NOT_FOUND,
                    &format!("Export '{}' not found", name),
                ),
                Err(RouterError::InvalidExportName(_)) => error_response(
                    StatusCode::BAD_REQUEST,
                    &format!("Invalid export name '{}'", name),
                ),
                Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
            }
        }

        // GET /api/exports/{name}/metrics - Get I/O metrics
        (Method::GET, ["api", "exports", name, "metrics"]) => {
            match router.get_export_metrics(name).await {
                Some(metrics) => json_response(StatusCode::OK, &metrics),
                None => error_response(
                    StatusCode::NOT_FOUND,
                    &format!("Export '{}' not found", name),
                ),
            }
        }

        // DELETE /api/exports/{name} - Remove export
        (Method::DELETE, ["api", "exports", name]) => {
            // Check for ?purge=true query param
            let purge = req
                .uri()
                .query()
                .map(|q| {
                    form_urlencoded::parse(q.as_bytes()).any(|(k, v)| k == "purge" && v == "true")
                })
                .unwrap_or(false);

            match router.remove_export(name, purge).await {
                Ok(()) => json_response(
                    StatusCode::OK,
                    &ApiResponse::success(format!("Export '{}' removed", name)),
                ),
                Err(RouterError::ExportNotFound(name)) => error_response(
                    StatusCode::NOT_FOUND,
                    &format!("Export '{}' not found", name),
                ),
                Err(RouterError::InvalidExportName(_)) => error_response(
                    StatusCode::BAD_REQUEST,
                    &format!("Invalid export name '{}'", name),
                ),
                Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
            }
        }

        // HEAD /api/manifests/{s3_prefix}/{name} - Check manifest existence
        (Method::HEAD, ["api", "manifests", s3_prefix, manifest_name]) => {
            // Reject path traversal
            if s3_prefix.contains("..") || manifest_name.contains("..") {
                return Ok(error_response(StatusCode::BAD_REQUEST, "Invalid path"));
            }
            match router.head_manifest(s3_prefix, manifest_name).await {
                Ok(true) => empty_response(StatusCode::OK),
                Ok(false) => empty_response(StatusCode::NOT_FOUND),
                Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
            }
        }

        // Liveness check (process alive)
        (Method::GET, ["health"]) => {
            json_response(StatusCode::OK, &ApiResponse::success("healthy"))
        }

        // Readiness check (exports serving, cache writable)
        (Method::GET, ["health", "ready"]) => {
            let status = router.readiness_check().await;
            let code = if status.ready {
                StatusCode::OK
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            };
            json_response(code, &status)
        }

        // GET /metrics - Prometheus metrics for all exports
        (Method::GET, ["metrics"]) => {
            let mut output = String::from(prometheus_header());
            for (name, snapshot) in router.all_export_metrics().await {
                output.push_str(&snapshot.to_prometheus(&name));
            }
            // Global scrubber metrics
            {
                use std::sync::atomic::Ordering;
                let sm = router.scrubber_metrics();
                let checked = sm.blocks_checked.load(Ordering::Relaxed);
                let evicted = sm.blocks_evicted.load(Ordering::Relaxed);
                // writeln! to String is infallible
                use std::fmt::Write;
                let _ = writeln!(output, "# HELP glidefs_scrubber_blocks_checked_total Blocks verified by background scrubber");
                let _ = writeln!(
                    output,
                    "# TYPE glidefs_scrubber_blocks_checked_total counter"
                );
                let _ = writeln!(output, "glidefs_scrubber_blocks_checked_total {checked}");
                let _ = writeln!(output, "# HELP glidefs_scrubber_blocks_evicted_total Corrupted blocks evicted by scrubber");
                let _ = writeln!(
                    output,
                    "# TYPE glidefs_scrubber_blocks_evicted_total counter"
                );
                let _ = writeln!(output, "glidefs_scrubber_blocks_evicted_total {evicted}");
            }
            // S3 circuit breaker state (0=closed, 1=open, 2=half-open)
            {
                use crate::circuit_breaker::CircuitState;
                use std::fmt::Write;
                let cb_value = match router.s3_circuit_state() {
                    CircuitState::Closed { .. } => 0,
                    CircuitState::Open => 1,
                    CircuitState::HalfOpen { .. } => 2,
                };
                let _ = writeln!(output, "# HELP glidefs_s3_circuit_breaker_state S3 circuit breaker state (0=closed, 1=open, 2=half-open)");
                let _ = writeln!(output, "# TYPE glidefs_s3_circuit_breaker_state gauge");
                let _ = writeln!(output, "glidefs_s3_circuit_breaker_state {cb_value}");
            }
            // (pack index metric removed — replaced by ChunkMetaCache)
            // SSD capacity
            {
                use std::fmt::Write;
                let utilization = router.ssd_utilization();
                let _ = writeln!(
                    output,
                    "# HELP glidefs_ssd_utilization_ratio Fraction of local SSD capacity used"
                );
                let _ = writeln!(output, "# TYPE glidefs_ssd_utilization_ratio gauge");
                let _ = writeln!(output, "glidefs_ssd_utilization_ratio {utilization:.6}");
            }
            Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
                .body(Full::new(Bytes::from(output)))
                .unwrap()
        }

        // 404 for everything else
        _ => error_response(StatusCode::NOT_FOUND, "Not found"),
    };

    let status = response.status().as_u16();
    let elapsed_us = start.elapsed().as_micros();
    if status >= 500 {
        warn!(method = %method, path = %path, status, elapsed_us, "API request");
    } else {
        info!(method = %method, path = %path, status, elapsed_us, "API request");
    }

    Ok(response)
}

/// HTTP API server for export management.
pub struct ApiServer {
    router: Arc<ExportRouter>,
    addr: SocketAddr,
}

impl ApiServer {
    /// Create a new API server.
    pub fn new(router: Arc<ExportRouter>, addr: SocketAddr) -> Self {
        Self { router, addr }
    }

    /// Start the API server.
    pub async fn start(self, shutdown: CancellationToken) -> std::io::Result<()> {
        let listener = TcpListener::bind(self.addr).await?;
        info!("HTTP API server listening on {}", self.addr);

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("API server shutting down");
                    break;
                }
                result = listener.accept() => {
                    match result {
                        Ok((stream, _addr)) => {
                            let router = Arc::clone(&self.router);
                            let io = TokioIo::new(stream);

                            tokio::spawn(async move {
                                let service = service_fn(move |req| {
                                    let router = Arc::clone(&router);
                                    handle_request(router, req)
                                });

                                if let Err(e) = http1::Builder::new()
                                    .serve_connection(io, service)
                                    .await
                                {
                                    error!("HTTP connection error: {}", e);
                                }
                            });
                        }
                        Err(e) => {
                            error!("Failed to accept connection: {}", e);
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::cache::SimpleBlockCache;
    use crate::block::router::RouterConfig;
    use tempfile::TempDir;

    async fn create_test_router(temp_dir: &TempDir) -> Arc<ExportRouter> {
        let s3: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        Arc::new(
            ExportRouter::new(RouterConfig {
                object_store: s3,
                db_path: "test".to_string(),
                cache_dir: temp_dir.path().to_path_buf(),
                block_size: 128 * 1024,
                clean_cache: Arc::new(SimpleBlockCache::new(64 * 1024 * 1024)),
                wal_sync: false,
                max_s3_uploads: 0,
                max_s3_downloads: 0,
                default_flush_threshold: crate::block::pack::DEFAULT_FLUSH_THRESHOLD,
                ublk_nr_queues: 1,
                nbd_dead_conn_timeout: 0,
            })
            .await
            .expect("failed to create test router"),
        )
    }

    /// Helper to make a request and get the response.
    async fn request(
        router: &Arc<ExportRouter>,
        method: Method,
        uri: &str,
        body: Option<&str>,
    ) -> Response<BoxBody> {
        let body_bytes = body.unwrap_or("").to_string();
        let req = Request::builder()
            .method(method)
            .uri(uri)
            .body(Full::new(Bytes::from(body_bytes)))
            .unwrap();
        handle_request(Arc::clone(router), req).await.unwrap()
    }

    // =========================================================================
    // is_valid_export_name tests
    // =========================================================================

    #[test]
    fn test_valid_export_names() {
        assert!(is_valid_export_name("vol1"));
        assert!(is_valid_export_name("my-vol"));
        assert!(is_valid_export_name("test.img"));
        assert!(is_valid_export_name("a"));
        assert!(is_valid_export_name("A123"));
        assert!(is_valid_export_name("vol_1"));
    }

    #[test]
    fn test_invalid_export_names() {
        assert!(!is_valid_export_name(""));
        assert!(!is_valid_export_name("-dash-start"));
        assert!(!is_valid_export_name(".dot-start"));
        assert!(!is_valid_export_name("_underscore-start"));
        assert!(!is_valid_export_name(&"x".repeat(129)));
        assert!(!is_valid_export_name("has/slash"));
        assert!(!is_valid_export_name("has space"));
        assert!(!is_valid_export_name("has@symbol"));
    }

    #[test]
    fn test_path_traversal_rejected() {
        // Dots are allowed within names but ".." sequences should be examined
        assert!(!is_valid_export_name(".."));
        assert!(!is_valid_export_name("../escape"));
        // Note: ".." starts with a dot, which is not alphanumeric, so it's rejected
        // by the first-char check. "a.." is technically valid per current rules.
        assert!(is_valid_export_name("a..b")); // dots allowed mid-name
    }

    #[test]
    fn test_max_length_boundary() {
        assert!(is_valid_export_name(&"a".repeat(128)));
        assert!(!is_valid_export_name(&"a".repeat(129)));
    }

    // =========================================================================
    // HTTP handler tests
    // =========================================================================

    #[tokio::test]
    async fn test_health_endpoint() {
        let temp = TempDir::new().unwrap();
        let router = create_test_router(&temp).await;
        let resp = request(&router, Method::GET, "/health", None).await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_readiness_endpoint() {
        let temp = TempDir::new().unwrap();
        let router = create_test_router(&temp).await;
        let resp = request(&router, Method::GET, "/health/ready", None).await;
        // May be OK or SERVICE_UNAVAILABLE depending on state
        assert!(
            resp.status() == StatusCode::OK || resp.status() == StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[tokio::test]
    async fn test_unknown_route_returns_404() {
        let temp = TempDir::new().unwrap();
        let router = create_test_router(&temp).await;
        let resp = request(&router, Method::GET, "/nonexistent", None).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_put_creates_export() {
        let temp = TempDir::new().unwrap();
        let router = create_test_router(&temp).await;
        let resp = request(
            &router,
            Method::PUT,
            "/api/exports/vol1",
            Some(r#"{"size_gb": 0.01}"#),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::CREATED);

        // Response includes transport (default "nbd") and device fields.
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let info: ExportInfoResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(info.name, "vol1");
        assert_eq!(info.transport, "nbd");
        // On macOS, device is None (no kernel device manager).
        // On Linux, would be Some("/dev/nbdN").
    }

    #[tokio::test]
    async fn test_put_existing_export_is_noop() {
        let temp = TempDir::new().unwrap();
        let router = create_test_router(&temp).await;
        // Create
        request(
            &router,
            Method::PUT,
            "/api/exports/vol1",
            Some(r#"{"size_gb": 0.01}"#),
        )
        .await;
        // Same PUT again — should be OK (idempotent)
        let resp = request(
            &router,
            Method::PUT,
            "/api/exports/vol1",
            Some(r#"{"size_gb": 0.01}"#),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_existing_export() {
        let temp = TempDir::new().unwrap();
        let router = create_test_router(&temp).await;
        request(
            &router,
            Method::PUT,
            "/api/exports/vol1",
            Some(r#"{"size_gb": 0.01}"#),
        )
        .await;

        let resp = request(&router, Method::GET, "/api/exports/vol1", None).await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_missing_export_returns_404() {
        let temp = TempDir::new().unwrap();
        let router = create_test_router(&temp).await;
        let resp = request(&router, Method::GET, "/api/exports/nope", None).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_list_exports() {
        let temp = TempDir::new().unwrap();
        let router = create_test_router(&temp).await;
        request(
            &router,
            Method::PUT,
            "/api/exports/vol1",
            Some(r#"{"size_gb": 0.01}"#),
        )
        .await;

        let resp = request(&router, Method::GET, "/api/exports", None).await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_delete_export() {
        let temp = TempDir::new().unwrap();
        let router = create_test_router(&temp).await;
        request(
            &router,
            Method::PUT,
            "/api/exports/vol1",
            Some(r#"{"size_gb": 0.01}"#),
        )
        .await;

        let resp = request(&router, Method::DELETE, "/api/exports/vol1", None).await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_delete_with_purge() {
        let temp = TempDir::new().unwrap();
        let router = create_test_router(&temp).await;
        request(
            &router,
            Method::PUT,
            "/api/exports/vol1",
            Some(r#"{"size_gb": 0.01}"#),
        )
        .await;

        let resp = request(
            &router,
            Method::DELETE,
            "/api/exports/vol1?purge=true",
            None,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_drain_missing_export_returns_404() {
        let temp = TempDir::new().unwrap();
        let router = create_test_router(&temp).await;
        let resp = request(&router, Method::POST, "/api/exports/nope/drain", None).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_invalid_export_name_returns_400() {
        let temp = TempDir::new().unwrap();
        let router = create_test_router(&temp).await;
        let resp = request(
            &router,
            Method::PUT,
            "/api/exports/-bad-name",
            Some(r#"{"size_gb": 0.01}"#),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_invalid_size_returns_400() {
        let temp = TempDir::new().unwrap();
        let router = create_test_router(&temp).await;
        let resp = request(
            &router,
            Method::PUT,
            "/api/exports/vol1",
            Some(r#"{"size_gb": -1.0}"#),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_ublk_transport_unavailable_returns_400() {
        // On macOS (test platform), ublk is never available.
        if ExportRouter::device_available("ublk") {
            return; // Skip on Linux+ublk builds where it would succeed.
        }
        let temp = TempDir::new().unwrap();
        let router = create_test_router(&temp).await;
        let resp = request(
            &router,
            Method::PUT,
            "/api/exports/vol1",
            Some(r#"{"size_gb": 0.01, "transport": "ublk"}"#),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_invalid_transport_returns_400() {
        let temp = TempDir::new().unwrap();
        let router = create_test_router(&temp).await;
        let resp = request(
            &router,
            Method::PUT,
            "/api/exports/vol1",
            Some(r#"{"size_gb": 0.01, "transport": "scsi"}"#),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_nbd_transport_accepted() {
        let temp = TempDir::new().unwrap();
        let router = create_test_router(&temp).await;
        let resp = request(
            &router,
            Method::PUT,
            "/api/exports/vol1",
            Some(r#"{"size_gb": 0.01, "transport": "nbd"}"#),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn test_invalid_json_returns_400() {
        let temp = TempDir::new().unwrap();
        let router = create_test_router(&temp).await;
        let resp = request(&router, Method::PUT, "/api/exports/vol1", Some("not json")).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_metrics_endpoint() {
        let temp = TempDir::new().unwrap();
        let router = create_test_router(&temp).await;
        let resp = request(&router, Method::GET, "/metrics", None).await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    // =========================================================================
    // Drain / Promote / Snapshot endpoint tests
    // =========================================================================

    #[tokio::test]
    async fn test_drain_export_success() {
        let temp = TempDir::new().unwrap();
        let router = create_test_router(&temp).await;
        request(
            &router,
            Method::PUT,
            "/api/exports/vol1",
            Some(r#"{"size_gb": 0.01}"#),
        )
        .await;

        let resp = request(&router, Method::POST, "/api/exports/vol1/drain", None).await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_promote_export_success() {
        let temp = TempDir::new().unwrap();
        let router = create_test_router(&temp).await;
        // Create a readonly export
        request(
            &router,
            Method::PUT,
            "/api/exports/vol1",
            Some(r#"{"size_gb": 0.01, "readonly": true}"#),
        )
        .await;

        let resp = request(&router, Method::POST, "/api/exports/vol1/promote", None).await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_promote_nonexistent_export_returns_404() {
        let temp = TempDir::new().unwrap();
        let router = create_test_router(&temp).await;
        let resp = request(&router, Method::POST, "/api/exports/nope/promote", None).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_snapshot_export_success() {
        let temp = TempDir::new().unwrap();
        let router = create_test_router(&temp).await;
        request(
            &router,
            Method::PUT,
            "/api/exports/vol1",
            Some(r#"{"size_gb": 0.01}"#),
        )
        .await;

        let resp = request(&router, Method::POST, "/api/exports/vol1/snapshot", None).await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_snapshot_nonexistent_export_returns_404() {
        let temp = TempDir::new().unwrap();
        let router = create_test_router(&temp).await;
        let resp = request(&router, Method::POST, "/api/exports/nope/snapshot", None).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_delete_snapshot_invalid_sequence_returns_400() {
        let temp = TempDir::new().unwrap();
        let router = create_test_router(&temp).await;
        request(
            &router,
            Method::PUT,
            "/api/exports/vol1",
            Some(r#"{"size_gb": 0.01}"#),
        )
        .await;

        let resp = request(
            &router,
            Method::DELETE,
            "/api/exports/vol1/snapshots/not-a-number",
            None,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_api_fork_from_snapshot_sequence() {
        let temp = TempDir::new().unwrap();
        let router = create_test_router(&temp).await;

        // Create source export
        request(
            &router,
            Method::PUT,
            "/api/exports/source",
            Some(r#"{"size_gb": 0.01}"#),
        )
        .await;

        // Snapshot source
        let resp = request(&router, Method::POST, "/api/exports/source/snapshot", None).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let snap: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let seq = snap["sequence"].as_u64().unwrap();

        // Fork from snapshot_sequence via API
        let body = format!(
            r#"{{"size_gb": 0.01, "s3_prefix": "source", "manifest_name": "source", "snapshot_sequence": {}}}"#,
            seq
        );
        let resp = request(&router, Method::PUT, "/api/exports/fork1", Some(&body)).await;
        assert_eq!(resp.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn test_api_snapshot_sequence_without_manifest_name_returns_400() {
        let temp = TempDir::new().unwrap();
        let router = create_test_router(&temp).await;

        let resp = request(
            &router,
            Method::PUT,
            "/api/exports/vol1",
            Some(r#"{"size_gb": 0.01, "snapshot_sequence": 1}"#),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    // =========================================================================
    // Transport / device path response tests
    // =========================================================================

    #[tokio::test]
    async fn test_get_export_includes_transport_and_device() {
        let temp = TempDir::new().unwrap();
        let router = create_test_router(&temp).await;
        request(
            &router,
            Method::PUT,
            "/api/exports/vol1",
            Some(r#"{"size_gb": 0.01}"#),
        )
        .await;

        let resp = request(&router, Method::GET, "/api/exports/vol1", None).await;
        assert_eq!(resp.status(), StatusCode::OK);

        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let info: ExportInfoResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(info.name, "vol1");
        assert_eq!(info.transport, "nbd");
        // On macOS: no kernel device manager, so device is None.
        // On Linux: would have a device path after registration.
    }

    #[tokio::test]
    async fn test_list_exports_includes_transport() {
        let temp = TempDir::new().unwrap();
        let router = create_test_router(&temp).await;
        request(
            &router,
            Method::PUT,
            "/api/exports/vol1",
            Some(r#"{"size_gb": 0.01}"#),
        )
        .await;

        let resp = request(&router, Method::GET, "/api/exports", None).await;
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let list: ListExportsResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(list.exports.len(), 1);
        assert_eq!(list.exports[0].transport, "nbd");
    }

    #[tokio::test]
    async fn test_put_idempotent_returns_export_info() {
        let temp = TempDir::new().unwrap();
        let router = create_test_router(&temp).await;

        // First PUT — creates
        let resp = request(
            &router,
            Method::PUT,
            "/api/exports/vol1",
            Some(r#"{"size_gb": 0.01}"#),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::CREATED);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let info: ExportInfoResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(info.transport, "nbd");

        // Second PUT — idempotent, returns ExportInfoResponse too
        let resp = request(
            &router,
            Method::PUT,
            "/api/exports/vol1",
            Some(r#"{"size_gb": 0.01}"#),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let info: ExportInfoResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(info.name, "vol1");
        assert_eq!(info.transport, "nbd");
    }
}
