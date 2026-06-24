#![allow(clippy::cast_possible_wrap, clippy::cast_sign_loss, clippy::cast_possible_truncation)]
//! HTTP API for dynamic export management.
//!
//! Provides REST endpoints for creating, draining, promoting, and removing exports.
//! Used by orchestrators for microVM scale-to-zero and live migration.

use crate::block::metrics::prometheus_header;
use crate::block::registry::FromRef;
use crate::block::router::{ExportInfo, ExportRouter, RouterError};
use crate::config::ExportConfig;
use crate::task;
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
use std::time::Instant;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use url::form_urlencoded;

/// Request to create, fork, re-attach, or resize a volume
/// (`PUT /api/exports/{name}`). Name comes from the URL path, not the body.
///
/// Fully logical: callers never supply an `s3_prefix` or `manifest_name`. To
/// fork, set `from` to a logical ref (`"image:<name>"`, `"volume:<name>"`,
/// `"snapshot:<id>"`); GlideFS resolves it to a pool + manifest internally and
/// places the new volume in the source's pool for CoW pack sharing.
#[derive(Debug, Deserialize)]
pub struct CreateVolumeRequest {
    pub size_gb: f64,
    /// Logical source to fork from. Omit (or `null`) for a fresh blank volume.
    #[serde(default)]
    pub from: Option<String>,
    #[serde(default)]
    pub readonly: bool,
    #[serde(default)]
    pub block_size: Option<usize>,
    /// Blocks per S3 pack (default: inherit from global config). 0 = manual mode.
    #[serde(default)]
    pub flush_threshold: Option<usize>,
    /// Flush mode: "auto" (default) or "manual" (drain-only).
    #[serde(default)]
    pub flush_mode: Option<String>,
    /// Block device transport: "nbd" (default) or "ublk" (Linux 6.0+).
    #[serde(default)]
    pub transport: Option<String>,
    /// Cooldown compaction window in flush cycles (0/unset = disabled). Defers
    /// dead-ratio compaction of a chunk until it has been idle this many cycles;
    /// cuts S3 PUT write-amp on overwrite-heavy DB volumes. Typical value: 8.
    #[serde(default)]
    pub compaction_cooldown: Option<u64>,
    /// Single-attach fencing token (the orchestrator's monotonic per-instance
    /// placement generation). When `> 0`, the attach is admitted only if this
    /// generation is `>=` the volume's stored generation; a strictly-newer
    /// generation fences an older holder out (see [`crate::block::fence`]).
    /// Omitted / `0` = the caller does not participate in fencing (back-compat).
    #[serde(default)]
    pub generation: Option<u64>,
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
    /// Current single-attach fencing generation owning this export (0 / omitted
    /// = un-fenced). Lets a caller confirm which generation won the attach.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generation: Option<u64>,
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
            generation: (e.generation > 0).then_some(e.generation),
        }
    }
}

/// Response for list exports.
#[derive(Debug, Serialize, Deserialize)]
pub struct ListExportsResponse {
    pub exports: Vec<ExportInfoResponse>,
}

/// Response for `GET /api/resolve/{name}`: where a volume's data physically
/// lives, resolved from the durable name-keyed index (`export.json`). Lets any
/// node locate a volume given only its stable name — no `s3_prefix` required.
#[derive(Debug, Serialize, Deserialize)]
pub struct ResolveResponse {
    pub name: String,
    /// Effective S3 prefix (pool) holding this volume's packs + manifests.
    pub s3_prefix: String,
    /// Manifest name within the pool (equals the volume name).
    pub manifest_name: String,
    pub size_gb: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_size: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transport: Option<String>,
}

impl ResolveResponse {
    fn from_config(name: &str, cfg: &ExportConfig) -> Self {
        ResolveResponse {
            name: name.to_string(),
            s3_prefix: cfg.s3_prefix().to_string(),
            manifest_name: name.to_string(),
            size_gb: cfg.size_gb,
            block_size: cfg.block_size,
            transport: cfg.transport.clone(),
        }
    }
}

/// Request to bless an OCI image (POST /api/bless/{s3_prefix}/{name}).
#[derive(Debug, Deserialize)]
pub struct BlessRequest {
    pub oci_image: String,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    /// Use plain HTTP instead of HTTPS for the registry connection.
    #[serde(default)]
    pub insecure: bool,
}

/// Request to promote an export snapshot to a base manifest
/// (POST /api/exports/{name}/promote-base).
#[derive(Debug, Deserialize)]
pub struct PromoteBaseRequest {
    /// Name to publish under `bases/` (the namespace blessed bases live in).
    pub base_name: String,
    /// Snapshot sequence to promote (must already exist — snapshot first).
    pub sequence: u64,
}

/// Request to profile a base's boot set (POST /api/profile/{s3_prefix}/{name}).
#[derive(Debug, Deserialize)]
pub struct ProfileRequest {
    /// Entrypoint override, run via `/bin/sh -c`. Falls back to the base's
    /// recorded runspec; an error if neither is present.
    #[serde(default)]
    pub cmd: Option<String>,
    /// Extra absolute in-image paths faulted under the tracer before the
    /// entrypoint (unioned with the runspec's static seed).
    #[serde(default)]
    pub seed_paths: Vec<String>,
    #[serde(default)]
    pub fs_type: Option<String>,
    #[serde(default = "default_profile_runs")]
    pub runs: u32,
    #[serde(default = "default_profile_timeout_secs")]
    pub timeout_secs: u64,
    #[serde(default)]
    pub force: bool,
    #[serde(default)]
    pub untrusted: bool,
    #[serde(default = "default_profile_max_blocks")]
    pub max_blocks: usize,
}

fn default_profile_runs() -> u32 {
    1
}
fn default_profile_timeout_secs() -> u64 {
    30
}
fn default_profile_max_blocks() -> usize {
    200_000
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
/// Proxy to the local handoff control socket. Used by the HTTP API
/// `POST /admin/handoff` endpoint to forward orchestrator-driven
/// handoff requests (Ansible, box-manager, etc.) through the same
/// pipeline the `glidefs handoff` CLI uses.
pub(crate) async fn trigger_handoff_via_ctl(
    socket_path: &std::path::Path,
    dry_run: bool,
) -> anyhow::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut stream = tokio::net::UnixStream::connect(socket_path)
        .await
        .map_err(|e| anyhow::anyhow!("connecting to handoff control socket: {e}"))?;
    let req_byte = if dry_run {
        crate::handoff::protocol::ctl_wire::REQUEST_HANDOFF_DRY_RUN
    } else {
        crate::handoff::protocol::ctl_wire::REQUEST_HANDOFF
    };
    stream.write_all(&[req_byte]).await?;
    stream.flush().await?;
    let mut response = [0u8; 1];
    if stream.read(&mut response).await? == 0 {
        anyhow::bail!("handoff control socket closed without response");
    }
    match response[0] {
        crate::handoff::protocol::ctl_wire::RESPONSE_ACCEPTED => Ok(()),
        crate::handoff::protocol::ctl_wire::RESPONSE_BUSY => {
            anyhow::bail!("handoff already in progress")
        }
        crate::handoff::protocol::ctl_wire::RESPONSE_UNSUPPORTED => {
            anyhow::bail!("daemon does not support handoff over control socket")
        }
        other => anyhow::bail!("unexpected response byte: 0x{:02x}", other),
    }
}

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

/// Create, fork, re-attach, or resize a volume — the body of
/// `PUT /api/exports/{name}`. Inputs are fully logical: `req` carries no
/// physical coordinates and `from` is the parsed logical source ref. This
/// function owns the create/fork/re-attach/resize decision and feeds the
/// existing physical machinery (`create_export`) the coordinates it resolved.
async fn create_or_attach_volume(
    router: &Arc<ExportRouter>,
    name: &str,
    req: &CreateVolumeRequest,
    from: &FromRef,
) -> Response<BoxBody> {
    // Single-attach fencing token (orchestrator placement generation). 0 = the
    // caller does not participate in fencing (back-compat).
    let my_gen = req.generation.unwrap_or(0);

    // Already attached on this node → resize-or-noop (idempotent create).
    if let Some(export) = router.get_export_info(name).await {
        // Re-PUT of an already-attached export: re-run the fence. A strictly
        // higher generation (the same instance re-forked onto this node) seizes
        // and is honored; a stale generation is rejected.
        match router.enforce_attach_fence(name, my_gen).await {
            Ok(crate::block::fence::Fence::Grant) => {}
            Ok(crate::block::fence::Fence::Reject) => {
                return error_response(
                    StatusCode::CONFLICT,
                    &format!(
                        "attach rejected: volume '{name}' is owned by a newer generation \
                         than {my_gen} (fenced)"
                    ),
                );
            }
            Err(e) => {
                return error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &format!("attach fence failed for '{name}': {e}"),
                );
            }
        }
        let current_size_gb = export.size as f64 / 1_073_741_824.0;
        if req.size_gb > current_size_gb {
            return match router.resize_export(name, req.size_gb).await {
                Ok(()) => {
                    let transport = export.transport.as_str();
                    #[cfg(target_os = "linux")]
                    if let Err(e) = router.register_device(name, transport).await {
                        warn!(export = %name, error = %e, "device re-registration after resize failed");
                    }
                    let _ = transport;
                    match router.get_export_info(name).await {
                        Some(info) => {
                            json_response(StatusCode::OK, &ExportInfoResponse::from(info))
                        }
                        None => error_response(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "Export resized but not found in map",
                        ),
                    }
                }
                Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
            };
        }
        return json_response(StatusCode::OK, &ExportInfoResponse::from(export));
    }

    // Not attached here. Resolve the durable, name-keyed index (export.json):
    // if this volume already exists in S3 (created on another node, or before
    // this node booted), recover its pool and ATTACH the real data — never
    // create a fresh empty volume at the wrong pool. Resolve-by-name.
    let resolved = match router.resolve_export(name).await {
        Ok(r) => r,
        Err(e) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("failed to resolve export '{}': {}", name, e),
            );
        }
    };

    let (config, fork_manifest, fork_seq) = if let Some(existing) = resolved {
        // RE-ATTACH: adopt the persisted pool + on-disk geometry (these MUST
        // match the stored data); honor runtime prefs; never re-fork, never
        // shrink. `from` is ignored — the volume already exists.
        let config = ExportConfig {
            name: name.to_string(),
            size_gb: req.size_gb.max(existing.size_gb),
            s3_prefix: existing.s3_prefix.clone(),
            block_size: req.block_size.or(existing.block_size),
            flush_threshold: req.flush_threshold.or(existing.flush_threshold),
            flush_mode: req.flush_mode.clone().or(existing.flush_mode.clone()),
            transport: req.transport.clone().or(existing.transport.clone()),
            compaction_cooldown: req.compaction_cooldown.or(existing.compaction_cooldown),
            source: existing.source.clone(),
        };
        info!(
            export = %name,
            s3_prefix = %config.s3_prefix(),
            "re-attaching volume from persisted index"
        );
        (config, None, None)
    } else {
        // CREATE or FORK: resolve the logical source to physical coordinates.
        // The source's pool becomes the new volume's pool so CoW pack sharing
        // works; blank volumes get their own pool (= their name).
        let src = match router.resolve_source(from).await {
            Ok(s) => s,
            Err(RouterError::SourceNotFound(s)) => {
                return error_response(StatusCode::NOT_FOUND, &format!("source not found: {s}"));
            }
            Err(e) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
        };
        let config = ExportConfig {
            name: name.to_string(),
            size_gb: req.size_gb,
            s3_prefix: src.pool,
            block_size: req.block_size,
            flush_threshold: req.flush_threshold,
            flush_mode: req.flush_mode.clone(),
            transport: req.transport.clone(),
            compaction_cooldown: req.compaction_cooldown,
            source: from.as_source(),
        };
        (config, src.manifest_name, src.snapshot_sequence)
    };

    let transport = config.transport.as_deref().unwrap_or("nbd").to_string();
    let is_fork = fork_manifest.is_some();

    let t_handler = Instant::now();
    let t_create = Instant::now();
    let create_result = router
        .create_export(config.clone(), req.readonly, fork_manifest.as_deref(), fork_seq)
        .await;
    let create_ms = t_create.elapsed().as_millis() as u64;

    match create_result {
        Ok(()) => {
            // Attach-time fence + seize, BEFORE persisting the index, registering
            // the device, or serving any I/O. A rejected attach has uploaded zero
            // data packs, so there is nothing to orphan — this is what prevents
            // the at-sync orphaned-packs data loss. Skipped for the gen-0 bypass.
            match router.enforce_attach_fence(name, my_gen).await {
                Ok(crate::block::fence::Fence::Grant) => {}
                Ok(crate::block::fence::Fence::Reject) => {
                    router.cleanup_failed_create(name, &transport).await;
                    return error_response(
                        StatusCode::CONFLICT,
                        &format!(
                            "attach rejected: volume '{name}' is owned by a newer generation \
                             than {my_gen} (fenced)"
                        ),
                    );
                }
                Err(e) => {
                    router.cleanup_failed_create(name, &transport).await;
                    return error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        &format!("attach fence failed for '{name}': {e}"),
                    );
                }
            }

            // Persist the index entry and register the device concurrently.
            // save_export MUST succeed; register_device is best-effort.
            let t_io = Instant::now();
            #[cfg(target_os = "linux")]
            let (save_result, register_result) = tokio::join!(
                router.save_export(&config),
                router.register_device(name, &transport),
            );
            #[cfg(not(target_os = "linux"))]
            let save_result = router.save_export(&config).await;

            if let Err(e) = save_result {
                // Without teardown, the in-memory entry silently shadows the
                // missing S3 export.json — retries hit create_export's
                // idempotency check and never re-attempt the S3 write.
                router.cleanup_failed_create(name, &transport).await;
                return error_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    &format!(
                        "Export definition not persisted to S3 ({e}); \
                         in-memory state cleaned up, retry the request"
                    ),
                );
            }
            #[cfg(target_os = "linux")]
            if let Err(e) = register_result {
                warn!(export = %name, error = %e, "device registration failed");
            }
            let _ = transport;

            tracing::info!(
                target: "glidefs.timing",
                export = %name,
                fork = is_fork,
                create_export_ms = create_ms,
                io_ms = t_io.elapsed().as_millis() as u64,
                total_ms = t_handler.elapsed().as_millis() as u64,
                "PUT /api/exports timing"
            );

            match router.get_export_info(name).await {
                Some(info) => json_response(StatusCode::CREATED, &ExportInfoResponse::from(info)),
                None => error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Export created but not found in map",
                ),
            }
        }
        Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
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
    let uri = req.uri().clone();
    let path = uri.path().to_string();
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

            let put_req: CreateVolumeRequest = match serde_json::from_slice(&body) {
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

            // Parse the logical source ref (`from`). GlideFS resolves it to a
            // pool + manifest internally — callers never supply physical coords.
            let from = match FromRef::parse(put_req.from.as_deref()) {
                Ok(f) => f,
                Err(e) => {
                    return Ok(error_response(StatusCode::BAD_REQUEST, &e.to_string()));
                }
            };

            create_or_attach_volume(&router, name, &put_req, &from).await
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

        // GET /api/resolve/{name} - Resolve a volume's physical location by its
        // stable logical name. Reads export.json directly from S3, so it works on
        // ANY node — even one that has never attached or discovered the volume.
        // This is the durable logical→physical mapping GlideFS owns.
        (Method::GET, ["api", "resolve", name]) => {
            if !is_valid_export_name(name) {
                return Ok(error_response(
                    StatusCode::BAD_REQUEST,
                    &format!("Invalid name '{}'", name),
                ));
            }
            match router.resolve_export(name).await {
                Ok(Some(cfg)) => {
                    json_response(StatusCode::OK, &ResolveResponse::from_config(name, &cfg))
                }
                Ok(None) => error_response(
                    StatusCode::NOT_FOUND,
                    &format!("Export '{}' not found", name),
                ),
                Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
            }
        }

        // GET /api/images/{name} - Resolve a logical image's physical location
        // from the durable image index. Lets any node locate an image (and thus
        // fork from it via `from: "image:<name>"`) by name alone.
        (Method::GET, ["api", "images", name]) => {
            if !is_valid_export_name(name) {
                return Ok(error_response(
                    StatusCode::BAD_REQUEST,
                    &format!("Invalid name '{}'", name),
                ));
            }
            match router.load_image_entry(name).await {
                Ok(Some(entry)) => json_response(StatusCode::OK, &entry),
                Ok(None) => error_response(
                    StatusCode::NOT_FOUND,
                    &format!("Image '{}' not found", name),
                ),
                Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
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

        // POST /api/exports/{name}/promote-base - Publish a snapshot's manifest
        // under bases/{base_name} (no data re-upload; forkable + profileable)
        (Method::POST, ["api", "exports", name, "promote-base"]) => {
            let body = match req.into_body().collect().await {
                Ok(b) => b.to_bytes(),
                Err(e) => return Ok(error_response(StatusCode::BAD_REQUEST, &e.to_string())),
            };
            let promote_req: PromoteBaseRequest = match serde_json::from_slice(&body) {
                Ok(r) => r,
                Err(e) => {
                    return Ok(error_response(
                        StatusCode::BAD_REQUEST,
                        &format!("Invalid JSON: {}", e),
                    ));
                }
            };
            match router
                .promote_snapshot_to_base(name, promote_req.sequence, &promote_req.base_name)
                .await
            {
                Ok(promoted) => json_response(
                    StatusCode::OK,
                    &ApiResponse::success(if promoted {
                        format!(
                            "Promoted snapshot seq={} of '{}' to bases/{}",
                            promote_req.sequence, name, promote_req.base_name
                        )
                    } else {
                        format!("bases/{} already exists", promote_req.base_name)
                    }),
                ),
                Err(RouterError::ExportNotFound(name)) => error_response(
                    StatusCode::NOT_FOUND,
                    &format!("Export '{}' not found", name),
                ),
                Err(RouterError::InvalidExportName(_)) => error_response(
                    StatusCode::BAD_REQUEST,
                    "Invalid export or base name",
                ),
                Err(RouterError::Manifest(m)) => {
                    error_response(StatusCode::NOT_FOUND, &m)
                }
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

        // HEAD /api/manifests/{s3_prefix}/bases/{name} - Check base manifest existence
        (Method::HEAD, ["api", "manifests", s3_prefix, "bases", name]) => {
            if s3_prefix.contains("..") || name.contains("..") {
                return Ok(error_response(StatusCode::BAD_REQUEST, "Invalid path"));
            }
            let manifest_name = format!("bases/{}", name);
            match router.head_manifest(s3_prefix, &manifest_name).await {
                Ok(true) => empty_response(StatusCode::OK),
                Ok(false) => empty_response(StatusCode::NOT_FOUND),
                Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
            }
        }

        // POST /api/bless/{s3_prefix}/{name} - Start OCI image bless
        (Method::POST, ["api", "bless", s3_prefix, name]) => {
            if s3_prefix.contains("..") || name.contains("..") {
                return Ok(error_response(StatusCode::BAD_REQUEST, "Invalid path"));
            }
            if !is_valid_export_name(name) {
                return Ok(error_response(
                    StatusCode::BAD_REQUEST,
                    &format!("Invalid name '{}'", name),
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

            let bless_req: BlessRequest = match serde_json::from_slice(&body) {
                Ok(r) => r,
                Err(e) => {
                    return Ok(error_response(
                        StatusCode::BAD_REQUEST,
                        &format!("Invalid JSON: {}", e),
                    ));
                }
            };

            if bless_req.oci_image.is_empty() {
                return Ok(error_response(
                    StatusCode::BAD_REQUEST,
                    "oci_image is required",
                ));
            }

            let credentials = match (bless_req.username, bless_req.password) {
                (Some(u), Some(p)) => {
                    oci_registry::Credentials::UsernamePassword {
                        username: u,
                        password: p,
                    }
                }
                _ => oci_registry::Credentials::Anonymous,
            };

            match router.bless_oci_image(s3_prefix, name, &bless_req.oci_image, credentials, bless_req.insecure).await
            {
                Ok(status) => {
                    let code = if status.state == "complete" {
                        StatusCode::OK
                    } else {
                        StatusCode::ACCEPTED
                    };
                    json_response(code, &status)
                }
                Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
            }
        }

        // GET /api/bless/{s3_prefix}/{name} - Poll bless status
        (Method::GET, ["api", "bless", s3_prefix, name]) => {
            if s3_prefix.contains("..") || name.contains("..") {
                return Ok(error_response(StatusCode::BAD_REQUEST, "Invalid path"));
            }

            let key = format!("{s3_prefix}/{name}");
            if let Some(status) = router.get_bless_status(&key).await {
                json_response(StatusCode::OK, &status)
            } else {
                // No in-flight task — check S3 for completed manifest.
                let manifest_name = format!("bases/{}", name);
                match router.head_manifest(s3_prefix, &manifest_name).await {
                    Ok(true) => json_response(
                        StatusCode::OK,
                        &crate::block::router::BlessStatus {
                            state: "complete".to_string(),
                            oci_image: String::new(),
                        },
                    ),
                    Ok(false) => empty_response(StatusCode::NOT_FOUND),
                    Err(e) => {
                        error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string())
                    }
                }
            }
        }

        // POST /api/profile/{s3_prefix}/{name} - Start a boot-set profile of bases/{name}
        (Method::POST, ["api", "profile", s3_prefix, name]) => {
            if s3_prefix.contains("..") || name.contains("..") {
                return Ok(error_response(StatusCode::BAD_REQUEST, "Invalid path"));
            }
            if !is_valid_export_name(name) {
                return Ok(error_response(
                    StatusCode::BAD_REQUEST,
                    &format!("Invalid name '{}'", name),
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
            let profile_req: ProfileRequest = if body.is_empty() {
                serde_json::from_slice(b"{}").unwrap()
            } else {
                match serde_json::from_slice(&body) {
                    Ok(r) => r,
                    Err(e) => {
                        return Ok(error_response(
                            StatusCode::BAD_REQUEST,
                            &format!("Invalid JSON: {}", e),
                        ));
                    }
                }
            };

            let params = crate::block::router::ProfileParams {
                cmd: profile_req.cmd,
                seed_paths: profile_req.seed_paths,
                fs_type: profile_req.fs_type,
                runs: profile_req.runs,
                timeout_secs: profile_req.timeout_secs,
                force: profile_req.force,
                untrusted: profile_req.untrusted,
                max_blocks: profile_req.max_blocks,
            };
            match router.start_profile(s3_prefix, name, params).await {
                Ok(status) => json_response(StatusCode::ACCEPTED, &status),
                Err(RouterError::Profile(m)) => {
                    error_response(StatusCode::SERVICE_UNAVAILABLE, &m)
                }
                Err(RouterError::InvalidExportName(_)) => error_response(
                    StatusCode::BAD_REQUEST,
                    &format!("Invalid name '{}'", name),
                ),
                Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
            }
        }

        // GET /api/profile/{s3_prefix}/{name} - Poll profile status.
        // running → in-flight; complete → .boot-set.meta exists; 404 → neither
        // (never profiled, or the last attempt failed).
        (Method::GET, ["api", "profile", s3_prefix, name]) => {
            if s3_prefix.contains("..") || name.contains("..") {
                return Ok(error_response(StatusCode::BAD_REQUEST, "Invalid path"));
            }

            let key = format!("{s3_prefix}/{name}");
            if let Some(status) = router.get_profile_status(&key).await {
                json_response(StatusCode::OK, &status)
            } else {
                match router.boot_set_meta_exists(s3_prefix, name).await {
                    Ok(true) => json_response(
                        StatusCode::OK,
                        &crate::block::router::ProfileStatus {
                            state: "complete".to_string(),
                        },
                    ),
                    Ok(false) => empty_response(StatusCode::NOT_FOUND),
                    Err(e) => {
                        error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string())
                    }
                }
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

        // GET /api/stats - aggregate stats for host pressure monitoring
        (Method::GET, ["api", "stats"]) => {
            let stats = router.aggregate_stats().await;
            json_response(StatusCode::OK, &stats)
        }

        // POST /admin/handoff - trigger a graceful zero-downtime handoff
        // Optional query string: ?dry_run=true performs WARMING then aborts.
        // Same effect as `kill -HUP $(pidof glidefs)` or `glidefs handoff`.
        (Method::POST, ["admin", "handoff"]) => {
            let dry_run = uri
                .query()
                .map(|q| q.contains("dry_run=true") || q.contains("dry_run=1"))
                .unwrap_or(false);
            // Proxy to the control socket. The local serve_with_router
            // task listens on it and routes to the handoff dispatcher.
            let socket_path = std::path::PathBuf::from(
                crate::handoff::DEFAULT_HANDOFF_CTL_SOCKET,
            );
            match crate::block::api::trigger_handoff_via_ctl(&socket_path, dry_run).await {
                Ok(()) => json_response(
                    StatusCode::ACCEPTED,
                    &ApiResponse::success(format!(
                        "handoff request accepted (dry_run={dry_run})"
                    )),
                ),
                Err(e) => error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &format!("handoff trigger failed: {e}"),
                ),
            }
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
            // ublk worker pool capacity — per-worker slot occupancy and
            // hosted queue counts. Lets ops see hot-spotting (one worker
            // approaching capacity while others idle) before an AddQueue
            // returns AtCapacity to a user-facing API call. Cheap (a few
            // relaxed atomic loads per worker, no message roundtrip).
            #[cfg(all(target_os = "linux", feature = "ublk"))]
            {
                use std::fmt::Write;
                let snap = router.ublk_worker_capacity().await;
                let _ = writeln!(
                    output,
                    "# HELP glidefs_ublk_worker_slots_used Currently-occupied executor task slots per ublk worker"
                );
                let _ = writeln!(output, "# TYPE glidefs_ublk_worker_slots_used gauge");
                for (idx, used, _cap, _hq) in &snap {
                    let _ = writeln!(
                        output,
                        "glidefs_ublk_worker_slots_used{{worker=\"{idx}\"}} {used}"
                    );
                }
                let _ = writeln!(
                    output,
                    "# HELP glidefs_ublk_worker_slots_capacity Maximum executor task slots per ublk worker"
                );
                let _ = writeln!(output, "# TYPE glidefs_ublk_worker_slots_capacity gauge");
                for (idx, _used, cap, _hq) in &snap {
                    let _ = writeln!(
                        output,
                        "glidefs_ublk_worker_slots_capacity{{worker=\"{idx}\"}} {cap}"
                    );
                }
                let _ = writeln!(
                    output,
                    "# HELP glidefs_ublk_worker_hosted_queues Number of ublk queues hosted on each worker"
                );
                let _ = writeln!(output, "# TYPE glidefs_ublk_worker_hosted_queues gauge");
                for (idx, _used, _cap, hq) in &snap {
                    let _ = writeln!(
                        output,
                        "glidefs_ublk_worker_hosted_queues{{worker=\"{idx}\"}} {hq}"
                    );
                }

                // USER_COPY bounce pool diagnostics: total acquires, backpressure
                // waits (transient exhaustion — pool full, futures park, RSS stays
                // bounded), heap fallbacks (init OOM — pool couldn't be mmap'd, so
                // the worker serves from elastic heap buffers and the RSS bound is
                // broken until it recovers), and how many worker pools are live.
                use std::sync::atomic::Ordering;
                use crate::block::ublk::buffer_pool::{
                    GLOBAL_ACQUIRES, GLOBAL_BACKPRESSURE_WAITS, GLOBAL_HEAP_ALLOC_FAILURES,
                    GLOBAL_HEAP_FALLBACKS, GLOBAL_POOLS_INITIALIZED,
                };
                let acq = GLOBAL_ACQUIRES.load(Ordering::Relaxed);
                let waits = GLOBAL_BACKPRESSURE_WAITS.load(Ordering::Relaxed);
                let pools = GLOBAL_POOLS_INITIALIZED.load(Ordering::Relaxed);
                let heap_fallbacks = GLOBAL_HEAP_FALLBACKS.load(Ordering::Relaxed);
                let heap_alloc_failures = GLOBAL_HEAP_ALLOC_FAILURES.load(Ordering::Relaxed);
                let _ = writeln!(
                    output,
                    "# HELP glidefs_ublk_buffer_pool_acquires_total USER_COPY bounce buffers acquired from per-worker pool"
                );
                let _ = writeln!(output, "# TYPE glidefs_ublk_buffer_pool_acquires_total counter");
                let _ = writeln!(output, "glidefs_ublk_buffer_pool_acquires_total {acq}");
                let _ = writeln!(
                    output,
                    "# HELP glidefs_ublk_buffer_pool_backpressure_waits_total USER_COPY bounce-buffer acquires that had to park because the per-worker pool was full. Throughput drops gracefully; RSS stays bounded. Sustained non-zero means the pool is undersized for the workload — raise POOL_SLOTS."
                );
                let _ = writeln!(
                    output,
                    "# TYPE glidefs_ublk_buffer_pool_backpressure_waits_total counter"
                );
                let _ = writeln!(output, "glidefs_ublk_buffer_pool_backpressure_waits_total {waits}");
                let _ = writeln!(
                    output,
                    "# HELP glidefs_ublk_buffer_pool_workers_initialized Number of ublk worker threads that have allocated their per-thread buffer pool"
                );
                let _ = writeln!(output, "# TYPE glidefs_ublk_buffer_pool_workers_initialized gauge");
                let _ = writeln!(output, "glidefs_ublk_buffer_pool_workers_initialized {pools}");
                let _ = writeln!(
                    output,
                    "# HELP glidefs_ublk_buffer_pool_heap_fallbacks_total USER_COPY I/Os served from a heap buffer because the worker's pool could not be mmap'd (host OOM at worker init). Daemon stays up but RSS is unbounded for that worker. Sustained growth means a worker is stuck degraded — investigate host memory."
                );
                let _ = writeln!(output, "# TYPE glidefs_ublk_buffer_pool_heap_fallbacks_total counter");
                let _ = writeln!(output, "glidefs_ublk_buffer_pool_heap_fallbacks_total {heap_fallbacks}");
                let _ = writeln!(
                    output,
                    "# HELP glidefs_ublk_buffer_pool_heap_alloc_failures_total USER_COPY I/Os failed with EIO because neither the per-worker pool nor the heap fallback could be allocated (host critically OOM). The daemon stays up and fails only the single starved I/O instead of aborting. Any non-zero value means the host is out of memory — page hard."
                );
                let _ = writeln!(output, "# TYPE glidefs_ublk_buffer_pool_heap_alloc_failures_total counter");
                let _ = writeln!(output, "glidefs_ublk_buffer_pool_heap_alloc_failures_total {heap_alloc_failures}");
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
    /// Cap on concurrent HTTP connections. Each accepted connection holds
    /// an `OwnedSemaphorePermit` until its task ends. Past the cap, new
    /// connections are dropped immediately to prevent OOM from connection
    /// flooding.
    connection_limiter: Arc<Semaphore>,
    /// Listener-fd registry for handoff (see `NBDServer::listener_registry`).
    listener_registry: Option<crate::handoff::listener_registry::ListenerRegistry>,
    /// Inherited listener fd from a handoff predecessor (see
    /// `NBDServer::inherited_listener`).
    inherited_listener: parking_lot::Mutex<Option<std::os::fd::OwnedFd>>,
}

impl ApiServer {
    /// Create a new API server with an unlimited connection budget. Test
    /// only — production callers use `new_with_limiter`. Available under
    /// `cfg(test)` or the `test-utils` feature.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn new(router: Arc<ExportRouter>, addr: SocketAddr) -> Self {
        Self::new_with_limiter(router, addr, Arc::new(Semaphore::new(Semaphore::MAX_PERMITS)))
    }

    /// Create a new API server bounded by `connection_limiter`.
    pub fn new_with_limiter(
        router: Arc<ExportRouter>,
        addr: SocketAddr,
        connection_limiter: Arc<Semaphore>,
    ) -> Self {
        Self {
            router,
            addr,
            connection_limiter,
            listener_registry: None,
            inherited_listener: parking_lot::Mutex::new(None),
        }
    }

    /// Attach a `ListenerRegistry` so that `start` records the bound
    /// listener fd for later handoff via SCM_RIGHTS.
    pub fn with_listener_registry(
        mut self,
        registry: crate::handoff::listener_registry::ListenerRegistry,
    ) -> Self {
        self.listener_registry = Some(registry);
        self
    }

    /// Attach an inherited listener fd (received via SCM_RIGHTS from a
    /// handoff predecessor). When set, `start` reuses it instead of
    /// calling `bind`.
    pub fn with_inherited_listener(self, fd: std::os::fd::OwnedFd) -> Self {
        *self.inherited_listener.lock() = Some(fd);
        self
    }

    /// Start the API server.
    pub async fn start(self, shutdown: CancellationToken) -> std::io::Result<()> {
        use std::os::fd::AsRawFd;
        let listener = if let Some(fd) = self.inherited_listener.lock().take() {
            info!("HTTP API server inheriting listener fd from handoff predecessor (target {})", self.addr);
            let std_listener = std::net::TcpListener::from(fd);
            std_listener.set_nonblocking(true)?;
            TcpListener::from_std(std_listener)?
        } else {
            let l = TcpListener::bind(self.addr).await?;
            info!("HTTP API server listening on {}", self.addr);
            l
        };
        if let Some(reg) = &self.listener_registry {
            reg.register(
                crate::handoff::listener_registry::ListenerKind::HttpApi,
                listener.as_raw_fd(),
            );
        }

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("API server shutting down");
                    break;
                }
                result = listener.accept() => {
                    match result {
                        Ok((stream, _addr)) => {
                            let permit = match Arc::clone(&self.connection_limiter).try_acquire_owned() {
                                Ok(p) => p,
                                Err(_) => {
                                    warn!(
                                        cap = self.connection_limiter.available_permits(),
                                        "HTTP API connection budget exhausted; rejecting client",
                                    );
                                    drop(stream);
                                    continue;
                                }
                            };
                            let router = Arc::clone(&self.router);
                            let io = TokioIo::new(stream);

                            // Supervised: a panic in one HTTP handler must
                            // not bring down the API for other clients. The
                            // connection's hyper service drops on unwind.
                            // The permit is moved into the task and drops
                            // on completion (or panic), releasing budget.
                            let _handle = task::spawn_supervised("http-conn", async move {
                                let _permit = permit;
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
                pack_index_cache: None,
                wal_sync: false,
                max_s3_uploads: 0,
                max_s3_downloads: 0,
                default_flush_threshold: crate::block::pack::DEFAULT_FLUSH_THRESHOLD,
                ublk_nr_queues: 1,
                nbd_dead_conn_timeout: 0,
                max_exports: 10_000,
                manifest_cache_bytes: crate::block::router::DEFAULT_MANIFEST_CACHE_BYTES,
                profile: None,
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

    // =========================================================================
    // Phase 1: resolve-by-name + re-attach across nodes (shared S3, fresh node)
    // =========================================================================

    async fn body_json(resp: Response<BoxBody>) -> serde_json::Value {
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    /// Seed a persisted volume binding directly (simulating a forked volume
    /// whose CoW pool differs from its name), bypassing the device layer.
    fn custom_pool_config(name: &str, pool: &str) -> ExportConfig {
        ExportConfig {
            name: name.to_string(),
            size_gb: 0.01,
            s3_prefix: Some(pool.to_string()),
            block_size: None,
            flush_threshold: None,
            flush_mode: None,
            transport: None,
            compaction_cooldown: None,
            source: Some(format!("volume:{pool}")),
        }
    }

    /// A volume living in a CUSTOM pool (name != pool, as forks do) must be
    /// resolvable by name alone on a fresh node that never attached or
    /// discovered it.
    #[tokio::test]
    async fn test_resolve_by_name_custom_pool_fresh_node() {
        let shared: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());

        // Node A persists the binding (name=vol1 lives in pool "custompool").
        let temp_a = TempDir::new().unwrap();
        let node_a = create_test_router_with_store(&temp_a, Arc::clone(&shared)).await;
        node_a
            .save_export(&custom_pool_config("vol1", "custompool"))
            .await
            .unwrap();

        // Node B: fresh node, same bucket, never saw vol1.
        let temp_b = TempDir::new().unwrap();
        let node_b = create_test_router_with_store(&temp_b, Arc::clone(&shared)).await;
        let resp = request(&node_b, Method::GET, "/api/resolve/vol1", None).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let v = body_json(resp).await;
        assert_eq!(v["s3_prefix"], "custompool");
        assert_eq!(v["manifest_name"], "vol1");
    }

    /// Re-attaching by name on a fresh node must adopt the persisted custom
    /// pool — NOT create a fresh empty volume at the wrong pool and clobber
    /// export.json.
    #[tokio::test]
    async fn test_reattach_by_name_adopts_persisted_pool() {
        let shared: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());

        let temp_a = TempDir::new().unwrap();
        let node_a = create_test_router_with_store(&temp_a, Arc::clone(&shared)).await;
        node_a
            .save_export(&custom_pool_config("vol1", "custompool"))
            .await
            .unwrap();

        // Node B re-attaches by name alone (no physical coords exist anymore).
        let temp_b = TempDir::new().unwrap();
        let node_b = create_test_router_with_store(&temp_b, Arc::clone(&shared)).await;
        let resp = request(
            &node_b,
            Method::PUT,
            "/api/exports/vol1",
            Some(r#"{"size_gb": 0.01}"#),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::CREATED);

        // The binding survived: a THIRD fresh node still resolves to custompool,
        // proving node B adopted the pool instead of overwriting export.json.
        let temp_c = TempDir::new().unwrap();
        let node_c = create_test_router_with_store(&temp_c, Arc::clone(&shared)).await;
        let resp = request(&node_c, Method::GET, "/api/resolve/vol1", None).await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(body_json(resp).await["s3_prefix"], "custompool");
    }

    /// Single-attach generation fence: two nodes sharing one object store cannot
    /// both own a volume. A stale generation is rejected (409); a newer one wins
    /// and fences the old owner's stale re-attach.
    #[tokio::test]
    async fn test_attach_generation_fence_prevents_split_brain() {
        let shared: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());

        // Node A attaches vol1 at generation 5 → wins and seizes the manifest.
        let temp_a = TempDir::new().unwrap();
        let node_a = create_test_router_with_store(&temp_a, Arc::clone(&shared)).await;
        let resp = request(
            &node_a,
            Method::PUT,
            "/api/exports/vol1",
            Some(r#"{"size_gb": 0.01, "generation": 5}"#),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::CREATED);
        let info: ExportInfoResponse = serde_json::from_slice(
            &resp.into_body().collect().await.unwrap().to_bytes(),
        )
        .unwrap();
        assert_eq!(info.generation, Some(5), "node A owns generation 5");

        // Node B attaches with a STALE generation 4 → fenced (409), no clobber.
        let temp_b = TempDir::new().unwrap();
        let node_b = create_test_router_with_store(&temp_b, Arc::clone(&shared)).await;
        let resp = request(
            &node_b,
            Method::PUT,
            "/api/exports/vol1",
            Some(r#"{"size_gb": 0.01, "generation": 4}"#),
        )
        .await;
        assert_eq!(
            resp.status(),
            StatusCode::CONFLICT,
            "a stale generation must be fenced out"
        );

        // Node B attaches with a NEWER generation 6 → wins, bumps the manifest.
        let resp = request(
            &node_b,
            Method::PUT,
            "/api/exports/vol1",
            Some(r#"{"size_gb": 0.01, "generation": 6}"#),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::CREATED);
        let info: ExportInfoResponse = serde_json::from_slice(
            &resp.into_body().collect().await.unwrap().to_bytes(),
        )
        .unwrap();
        assert_eq!(info.generation, Some(6), "node B took ownership at generation 6");

        // The old owner (gen 5) tries to re-attach on a fresh node → now fenced,
        // because the volume is owned by the strictly-newer generation 6.
        let temp_a2 = TempDir::new().unwrap();
        let node_a2 = create_test_router_with_store(&temp_a2, Arc::clone(&shared)).await;
        let resp = request(
            &node_a2,
            Method::PUT,
            "/api/exports/vol1",
            Some(r#"{"size_gb": 0.01, "generation": 5}"#),
        )
        .await;
        assert_eq!(
            resp.status(),
            StatusCode::CONFLICT,
            "the superseded owner must not be able to re-attach"
        );

        // Re-attach at the SAME winning generation 6 is idempotent (>= rule).
        let resp = request(
            &node_b,
            Method::PUT,
            "/api/exports/vol1",
            Some(r#"{"size_gb": 0.01, "generation": 6}"#),
        )
        .await;
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "re-attaching the owning generation must be admitted"
        );
    }

    /// Back-compat: a caller that does not send a generation (the gen-0 bypass)
    /// is never fenced, even against a volume already owned at a high generation.
    #[tokio::test]
    async fn test_attach_without_generation_bypasses_fence() {
        let shared: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());

        let temp_a = TempDir::new().unwrap();
        let node_a = create_test_router_with_store(&temp_a, Arc::clone(&shared)).await;
        request(
            &node_a,
            Method::PUT,
            "/api/exports/vol1",
            Some(r#"{"size_gb": 0.01, "generation": 9}"#),
        )
        .await;

        // A legacy caller (no generation field) attaches on a fresh node → granted.
        let temp_b = TempDir::new().unwrap();
        let node_b = create_test_router_with_store(&temp_b, Arc::clone(&shared)).await;
        let resp = request(
            &node_b,
            Method::PUT,
            "/api/exports/vol1",
            Some(r#"{"size_gb": 0.01}"#),
        )
        .await;
        assert_eq!(
            resp.status(),
            StatusCode::CREATED,
            "the gen-0 bypass must not be fenced"
        );
    }

    /// A blank volume (no `from`) gets its own pool (= its name).
    #[tokio::test]
    async fn test_create_blank_volume_owns_pool() {
        let temp = TempDir::new().unwrap();
        let router = create_test_router(&temp).await;
        let resp = request(
            &router,
            Method::PUT,
            "/api/exports/v2",
            Some(r#"{"size_gb": 0.01}"#),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::CREATED);
        let resp = request(&router, Method::GET, "/api/resolve/v2", None).await;
        assert_eq!(body_json(resp).await["s3_prefix"], "v2");
    }

    /// Forking from a logical source that doesn't exist → 404.
    #[tokio::test]
    async fn test_create_from_unknown_image_returns_404() {
        let temp = TempDir::new().unwrap();
        let router = create_test_router(&temp).await;
        let resp = request(
            &router,
            Method::PUT,
            "/api/exports/v3",
            Some(r#"{"size_gb": 0.01, "from": "image:ghost"}"#),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    /// A malformed `from` ref → 400.
    #[tokio::test]
    async fn test_create_from_invalid_ref_returns_400() {
        let temp = TempDir::new().unwrap();
        let router = create_test_router(&temp).await;
        let resp = request(
            &router,
            Method::PUT,
            "/api/exports/v4",
            Some(r#"{"size_gb": 0.01, "from": "bogus:x"}"#),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_resolve_missing_returns_404() {
        let temp = TempDir::new().unwrap();
        let router = create_test_router(&temp).await;
        let resp = request(&router, Method::GET, "/api/resolve/ghost", None).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_resolve_invalid_name_returns_400() {
        let temp = TempDir::new().unwrap();
        let router = create_test_router(&temp).await;
        let resp = request(&router, Method::GET, "/api/resolve/-bad", None).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    /// Wraps `InMemory` and fails `put_opts` when armed. Used to simulate
    /// the S3 outage that makes `save_export` return `Err` mid-PUT.
    struct PutFailingStore {
        inner: object_store::memory::InMemory,
        fail_puts: std::sync::atomic::AtomicBool,
    }

    impl PutFailingStore {
        fn new() -> Self {
            Self {
                inner: object_store::memory::InMemory::new(),
                fail_puts: std::sync::atomic::AtomicBool::new(false),
            }
        }

        fn set_fail(&self, fail: bool) {
            self.fail_puts
                .store(fail, std::sync::atomic::Ordering::SeqCst);
        }
    }

    impl std::fmt::Display for PutFailingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "PutFailingStore")
        }
    }

    impl std::fmt::Debug for PutFailingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "PutFailingStore")
        }
    }

    #[async_trait::async_trait]
    impl object_store::ObjectStore for PutFailingStore {
        async fn put_opts(
            &self,
            location: &object_store::path::Path,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            if self.fail_puts.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(object_store::Error::Generic {
                    store: "PutFailingStore",
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::ConnectionRefused,
                        "simulated S3 PUT outage",
                    )),
                });
            }
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &object_store::path::Path,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &object_store::path::Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }

        async fn delete(&self, location: &object_store::path::Path) -> object_store::Result<()> {
            self.inner.delete(location).await
        }

        fn list(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
        ) -> object_store::Result<()> {
            self.inner.copy(from, to).await
        }

        async fn copy_if_not_exists(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
        ) -> object_store::Result<()> {
            self.inner.copy_if_not_exists(from, to).await
        }
    }

    async fn create_test_router_with_store(
        temp_dir: &TempDir,
        store: Arc<dyn object_store::ObjectStore>,
    ) -> Arc<ExportRouter> {
        Arc::new(
            ExportRouter::new(RouterConfig {
                object_store: store,
                db_path: "test".to_string(),
                cache_dir: temp_dir.path().to_path_buf(),
                block_size: 128 * 1024,
                clean_cache: Arc::new(SimpleBlockCache::new(64 * 1024 * 1024)),
                pack_index_cache: None,
                wal_sync: false,
                max_s3_uploads: 0,
                max_s3_downloads: 0,
                default_flush_threshold: crate::block::pack::DEFAULT_FLUSH_THRESHOLD,
                ublk_nr_queues: 1,
                nbd_dead_conn_timeout: 0,
                max_exports: 10_000,
                manifest_cache_bytes: crate::block::router::DEFAULT_MANIFEST_CACHE_BYTES,
                profile: None,
            })
            .await
            .expect("failed to create test router"),
        )
    }

    /// Full HTTP-stack verification of Stage 2b: when `save_export` fails
    /// after `create_export` succeeded, the PUT must respond 503 AND clear
    /// the in-memory entry so a subsequent retry re-runs from scratch.
    ///
    /// Without the fix, the second PUT would hit `create_export`'s
    /// idempotency check, return 200 immediately, and S3 would still have
    /// no export.json — silent data loss on the next daemon restart.
    #[tokio::test]
    async fn test_put_503_on_save_failure_cleans_up_and_retry_succeeds() {
        let temp = TempDir::new().unwrap();
        let store = Arc::new(PutFailingStore::new());
        let router = create_test_router_with_store(&temp, store.clone()).await;

        // Arm the failure: any PUT to S3 (including save_export) returns Err.
        store.set_fail(true);
        let resp = request(
            &router,
            Method::PUT,
            "/api/exports/vol1",
            Some(r#"{"size_gb": 0.01}"#),
        )
        .await;
        assert_eq!(
            resp.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "save_export failure must surface as 503"
        );

        // In-memory state must be gone. The bug: without cleanup_failed_create
        // the export sits in `self.exports` and GET would return 200.
        let resp = request(&router, Method::GET, "/api/exports/vol1", None).await;
        assert_eq!(
            resp.status(),
            StatusCode::NOT_FOUND,
            "in-memory state must be torn down after save_export failure"
        );

        // S3 recovers — retry succeeds. This is the "idempotent retry actually
        // works" assertion: without cleanup, this PUT would hit the idempotency
        // check and 200 with no S3 write.
        store.set_fail(false);
        let resp = request(
            &router,
            Method::PUT,
            "/api/exports/vol1",
            Some(r#"{"size_gb": 0.01}"#),
        )
        .await;
        assert_eq!(
            resp.status(),
            StatusCode::CREATED,
            "retry after S3 recovery must re-run create_export and save_export"
        );

        // Confirm the export now exists in-memory.
        let resp = request(&router, Method::GET, "/api/exports/vol1", None).await;
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
    async fn test_promote_base_lifecycle() {
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
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let snap: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let sequence = snap["sequence"].as_u64().unwrap();

        // Promote the snapshot to a base manifest.
        let promote_body = format!(r#"{{"base_name": "rootfs-test", "sequence": {sequence}}}"#);
        let resp = request(
            &router,
            Method::POST,
            "/api/exports/vol1/promote-base",
            Some(&promote_body),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);

        // The promoted base is visible to the manifest HEAD endpoint.
        let resp = request(
            &router,
            Method::HEAD,
            "/api/manifests/vol1/bases/rootfs-test",
            None,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);

        // Idempotent re-promote.
        let resp = request(
            &router,
            Method::POST,
            "/api/exports/vol1/promote-base",
            Some(&promote_body),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);

        // Missing snapshot sequence → 404, nothing published.
        let resp = request(
            &router,
            Method::POST,
            "/api/exports/vol1/promote-base",
            Some(r#"{"base_name": "rootfs-bad", "sequence": 999}"#),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_promote_base_invalid_body_returns_400() {
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
            Method::POST,
            "/api/exports/vol1/promote-base",
            Some("not json"),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_profile_disabled_returns_503() {
        // Test routers have no [profile] config — the POST must be rejected
        // with an explanatory 503, not spawn anything.
        let temp = TempDir::new().unwrap();
        let router = create_test_router(&temp).await;
        let resp = request(&router, Method::POST, "/api/profile/pfx/base1", Some("{}")).await;
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn test_profile_status_unknown_returns_404() {
        let temp = TempDir::new().unwrap();
        let router = create_test_router(&temp).await;
        // No in-flight task and no .boot-set.meta sidecar → 404.
        let resp = request(&router, Method::GET, "/api/profile/pfx/base1", None).await;
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
    async fn test_api_fork_from_snapshot() {
        let temp = TempDir::new().unwrap();
        let router = create_test_router(&temp).await;

        // Create source volume.
        request(
            &router,
            Method::PUT,
            "/api/exports/source",
            Some(r#"{"size_gb": 0.01}"#),
        )
        .await;

        // Snapshot it → GlideFS assigns a stable snapshot id.
        let resp = request(&router, Method::POST, "/api/exports/source/snapshot", None).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let snap = body_json(resp).await;
        let id = snap["snapshot_id"]
            .as_str()
            .expect("snapshot response carries a stable snapshot_id")
            .to_string();

        // Fork a new volume from the snapshot by logical id alone — no pool,
        // no manifest, no sequence.
        let body = format!(r#"{{"size_gb": 0.01, "from": "snapshot:{}"}}"#, id);
        let resp = request(&router, Method::PUT, "/api/exports/fork1", Some(&body)).await;
        assert_eq!(resp.status(), StatusCode::CREATED);

        // fork1 was placed in source's pool for CoW sharing.
        let resp = request(&router, Method::GET, "/api/resolve/fork1", None).await;
        assert_eq!(body_json(resp).await["s3_prefix"], "source");
    }

    #[tokio::test]
    async fn test_api_fork_from_image() {
        let temp = TempDir::new().unwrap();
        let router = create_test_router(&temp).await;

        // Create + snapshot a source volume, then promote the snapshot to a
        // named image — this registers the logical image index entry.
        request(
            &router,
            Method::PUT,
            "/api/exports/golden",
            Some(r#"{"size_gb": 0.01}"#),
        )
        .await;
        let resp = request(&router, Method::POST, "/api/exports/golden/snapshot", None).await;
        let seq = body_json(resp).await["sequence"].as_u64().unwrap();
        let resp = request(
            &router,
            Method::POST,
            "/api/exports/golden/promote-base",
            Some(&format!(
                r#"{{"sequence": {}, "base_name": "ubuntu"}}"#,
                seq
            )),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);

        // The image is now resolvable by logical name.
        let resp = request(&router, Method::GET, "/api/images/ubuntu", None).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let img = body_json(resp).await;
        assert_eq!(img["pool"], "golden");
        assert_eq!(img["manifest"], "bases/ubuntu");

        // Fork a new volume from the image by logical name alone.
        let resp = request(
            &router,
            Method::PUT,
            "/api/exports/vm1",
            Some(r#"{"size_gb": 0.01, "from": "image:ubuntu"}"#),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::CREATED);

        // vm1 was placed in the image's pool (CoW sharing) and records lineage.
        let resp = request(&router, Method::GET, "/api/resolve/vm1", None).await;
        assert_eq!(body_json(resp).await["s3_prefix"], "golden");
    }

    #[tokio::test]
    async fn test_tag_is_forkable_as_image() {
        let temp = TempDir::new().unwrap();
        let router = create_test_router(&temp).await;

        // Create a volume and tag its current manifest under a name.
        request(
            &router,
            Method::PUT,
            "/api/exports/work",
            Some(r#"{"size_gb": 0.01}"#),
        )
        .await;
        let resp = request(
            &router,
            Method::POST,
            "/api/exports/work/tag",
            Some(r#"{"tag": "setup-v1"}"#),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);

        // The tag is registered in the image index and resolvable by name.
        let resp = request(&router, Method::GET, "/api/images/setup-v1", None).await;
        assert_eq!(resp.status(), StatusCode::OK);

        // And forkable via `from: "image:<tag>"`.
        let resp = request(
            &router,
            Method::PUT,
            "/api/exports/deploy",
            Some(r#"{"size_gb": 0.01, "from": "image:setup-v1"}"#),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::CREATED);
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
