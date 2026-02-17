//! HTTP API for dynamic export management.
//!
//! Provides REST endpoints for creating, draining, promoting, and removing exports.
//! Used by orchestrators for microVM scale-to-zero and live migration.

use crate::config::ExportConfig;
use crate::nbd::flush_scheduler::FlushMode;
use crate::nbd::metrics::prometheus_header;
use crate::nbd::router::{ExportRouter, RouterError};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
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
use tracing::{error, info};

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
    /// Flush mode for this export.
    #[serde(default)]
    pub flush_mode: Option<FlushMode>,
    /// Dirty budget in GB for this export.
    #[serde(default)]
    pub dirty_budget_gb: Option<f64>,
}

/// Response for export info.
#[derive(Debug, Serialize)]
pub struct ExportInfoResponse {
    pub name: String,
    pub size_bytes: u64,
    pub readonly: bool,
    pub flush_mode: Option<FlushMode>,
}

/// Response for list exports.
#[derive(Debug, Serialize)]
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

fn error_response(status: StatusCode, message: &str) -> Response<BoxBody> {
    json_response(status, &ApiResponse::error(message))
}

/// Handle API requests.
async fn handle_request(
    router: Arc<ExportRouter>,
    req: Request<Incoming>,
) -> Result<Response<BoxBody>, Infallible> {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let path_parts: Vec<&str> = path.trim_start_matches('/').split('/').collect();

    let response = match (method, path_parts.as_slice()) {
        // GET /api/exports - List all exports
        (Method::GET, ["api", "exports"]) => {
            let exports = router.list_exports().await;
            let mut responses = Vec::new();
            for e in exports {
                let flush_mode = router.get_flush_mode(&e.name).await;
                responses.push(ExportInfoResponse {
                    name: e.name,
                    size_bytes: e.size,
                    readonly: e.readonly,
                    flush_mode,
                });
            }
            json_response(StatusCode::OK, &ListExportsResponse { exports: responses })
        }

        // PUT /api/exports/{name} - Create or resize export (idempotent)
        //
        // The "smart" endpoint for orchestrators:
        // - Export doesn't exist → create with specified size
        // - Export exists, requested size larger → grow it
        // - Export exists, requested size same/smaller → no-op (success)
        (Method::PUT, ["api", "exports", name]) => {
            let body = match req.collect().await {
                Ok(b) => b.to_bytes(),
                Err(e) => return Ok(error_response(StatusCode::BAD_REQUEST, &e.to_string())),
            };

            let put_req: PutExportRequest = match serde_json::from_slice(&body) {
                Ok(r) => r,
                Err(e) => {
                    return Ok(error_response(
                        StatusCode::BAD_REQUEST,
                        &format!("Invalid JSON: {}", e),
                    ))
                }
            };

            // Check if export already exists
            let existing = router
                .list_exports()
                .await
                .into_iter()
                .find(|e| e.name == *name);

            match existing {
                Some(export) => {
                    // Export exists - check if resize needed
                    let current_size_gb = export.size as f64 / 1_000_000_000.0;
                    if put_req.size_gb > current_size_gb {
                        // Need to grow
                        match router.resize_export(name, put_req.size_gb).await {
                            Ok(()) => json_response(
                                StatusCode::OK,
                                &ApiResponse::success(format!(
                                    "Export '{}' resized to {:.1}GB",
                                    name, put_req.size_gb
                                )),
                            ),
                            Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
                        }
                    } else {
                        // Already at or above requested size - no-op
                        json_response(
                            StatusCode::OK,
                            &ApiResponse::success(format!("Export '{}' ready", name)),
                        )
                    }
                }
                None => {
                    // Export doesn't exist - create it
                    let config = ExportConfig {
                        name: name.to_string(),
                        size_gb: put_req.size_gb,
                        s3_prefix: put_req.s3_prefix,
                        block_size: put_req.block_size,
                        flush_mode: put_req.flush_mode,
                        dirty_budget_gb: put_req.dirty_budget_gb,
                    };

                    match router.create_export(config, put_req.readonly, put_req.manifest_name.as_deref()).await {
                        Ok(()) => json_response(
                            StatusCode::CREATED,
                            &ApiResponse::success(format!("Export '{}' created", name)),
                        ),
                        Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
                    }
                }
            }
        }

        // GET /api/exports/{name} - Get export info
        (Method::GET, ["api", "exports", name]) => {
            let flush_mode = router.get_flush_mode(name).await;
            let exports = router.list_exports().await;
            match exports.into_iter().find(|e| e.name == *name) {
                Some(export) => json_response(
                    StatusCode::OK,
                    &ExportInfoResponse {
                        name: export.name,
                        size_bytes: export.size,
                        readonly: export.readonly,
                        flush_mode,
                    },
                ),
                None => error_response(StatusCode::NOT_FOUND, &format!("Export '{}' not found", name)),
            }
        }

        // POST /api/exports/{name}/flush-mode - Set flush mode
        (Method::POST, ["api", "exports", name, "flush-mode"]) => {
            let body = match req.collect().await {
                Ok(b) => b.to_bytes(),
                Err(e) => return Ok(error_response(StatusCode::BAD_REQUEST, &e.to_string())),
            };

            let mode: FlushMode = match serde_json::from_slice(&body) {
                Ok(m) => m,
                Err(e) => {
                    return Ok(error_response(
                        StatusCode::BAD_REQUEST,
                        &format!("Invalid JSON: {}", e),
                    ))
                }
            };

            match router.set_flush_mode(name, mode).await {
                Ok(()) => json_response(
                    StatusCode::OK,
                    &ApiResponse::success(format!("Flush mode updated for '{}'", name)),
                ),
                Err(RouterError::ExportNotFound(name)) => {
                    error_response(StatusCode::NOT_FOUND, &format!("Export '{}' not found", name))
                }
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
                Err(RouterError::ExportNotFound(name)) => {
                    error_response(StatusCode::NOT_FOUND, &format!("Export '{}' not found", name))
                }
                Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
            }
        }

        // POST /api/exports/{name}/snapshot - Snapshot export to S3 manifest
        (Method::POST, ["api", "exports", name, "snapshot"]) => {
            match router.snapshot_export(name).await {
                Ok(result) => json_response(StatusCode::OK, &result),
                Err(RouterError::ExportNotFound(name)) => {
                    error_response(StatusCode::NOT_FOUND, &format!("Export '{}' not found", name))
                }
                Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
            }
        }

        // POST /api/exports/{name}/promote - Promote readonly to read-write
        (Method::POST, ["api", "exports", name, "promote"]) => {
            match router.promote_export(name).await {
                Ok(()) => json_response(
                    StatusCode::OK,
                    &ApiResponse::success(format!("Export '{}' promoted to read-write", name)),
                ),
                Err(RouterError::ExportNotFound(name)) => {
                    error_response(StatusCode::NOT_FOUND, &format!("Export '{}' not found", name))
                }
                Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
            }
        }

        // GET /api/exports/{name}/metrics - Get I/O metrics
        (Method::GET, ["api", "exports", name, "metrics"]) => {
            match router.get_export_metrics(name).await {
                Some(metrics) => json_response(StatusCode::OK, &metrics),
                None => error_response(StatusCode::NOT_FOUND, &format!("Export '{}' not found", name)),
            }
        }

        // DELETE /api/exports/{name} - Remove export
        (Method::DELETE, ["api", "exports", name]) => {
            // Check for ?purge=true query param
            let purge = req
                .uri()
                .query()
                .map(|q| q.contains("purge=true"))
                .unwrap_or(false);

            match router.remove_export(name, purge).await {
                Ok(()) => json_response(
                    StatusCode::OK,
                    &ApiResponse::success(format!("Export '{}' removed", name)),
                ),
                Err(RouterError::ExportNotFound(name)) => {
                    error_response(StatusCode::NOT_FOUND, &format!("Export '{}' not found", name))
                }
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
            let names = router.list_export_names().await;
            for name in names {
                if let Some(snapshot) = router.get_export_metrics(&name).await {
                    output.push_str(&snapshot.to_prometheus(&name));
                }
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

// Note: API endpoint tests are covered via the router tests and integration tests.
// The handle_request function is a thin routing layer over ExportRouter methods.
// Testing the full HTTP stack with hyper's Incoming type requires complex setup.
// See tests/integration/ for end-to-end HTTP API tests.
//
// The core API functionality is tested through:
// - router.rs unit tests (create_export, drain_export, promote_export, etc.)
// - integration tests (multi-node scenarios)
// - GitHub Actions workflows (full HTTP API with real server)
