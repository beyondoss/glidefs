use std::convert::Infallible;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::body::Frame;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use object_store::ObjectStore;
use object_store::path::Path as ObjectPath;
use tokio::net::TcpListener;
use tracing::{error, info};

use glidefs::config::Settings;
use glidefs::oci::hex_sha256;

type ResponseBody = http_body_util::combinators::UnsyncBoxBody<Bytes, io::Error>;

/// Create a small buffered response body.
fn full(data: impl Into<Bytes>) -> ResponseBody {
    Full::new(data.into())
        .map_err(|never| match never {})
        .boxed_unsync()
}

/// Create an empty response body.
fn empty() -> ResponseBody {
    full(Bytes::new())
}

/// Stream an S3 object as the response body.
fn stream_from_s3(result: object_store::GetResult) -> ResponseBody {
    let stream = result.into_stream().map(|r| {
        r.map(Frame::data)
            .map_err(io::Error::other)
    });
    BodyExt::boxed_unsync(StreamBody::new(stream))
}

/// Returns true if `s` is a valid lowercase sha256 hex digest (64 chars).
fn is_valid_hex_sha256(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}

/// Build an OCI error response.
fn oci_error(status: StatusCode, code: &str, message: &str) -> Response<ResponseBody> {
    let body = serde_json::json!({
        "errors": [{
            "code": code,
            "message": message
        }]
    });
    Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .body(full(serde_json::to_vec(&body).unwrap()))
        .unwrap()
}

struct RegistryState {
    object_store: Arc<dyn ObjectStore>,
    /// S3 base path for OCI artifacts (e.g., "db/exports/myexport/oci").
    oci_base: String,
    /// The {name} used in /v2/{name}/... URLs.
    name: String,
}

pub async fn run_serve(
    listen: SocketAddr,
    s3_prefix: String,
    config_path: PathBuf,
) -> anyhow::Result<()> {
    let settings = Settings::from_file(&config_path)?;
    let (object_store, db_path) = crate::config::setup_object_store(&settings)?;

    let base = format!("{}/exports/{}", db_path, s3_prefix);
    let oci_base = format!("{}/oci", base);

    let state = Arc::new(RegistryState {
        object_store,
        oci_base,
        name: s3_prefix,
    });

    let listener = TcpListener::bind(listen).await?;
    info!(addr = %listen, "OCI distribution server listening");

    loop {
        let (stream, _) = listener.accept().await?;
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let svc = service_fn(move |req| {
                let state = Arc::clone(&state);
                async move { handle_request(state, req).await }
            });
            if let Err(e) = http1::Builder::new().serve_connection(io, svc).await {
                error!(error = %e, "HTTP connection error");
            }
        });
    }
}

async fn handle_request<B>(
    state: Arc<RegistryState>,
    req: Request<B>,
) -> Result<Response<ResponseBody>, Infallible>
where
    B: hyper::body::Body + Send + 'static,
{
    let path = req.uri().path().to_string();
    let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    let method = req.method().clone();

    let response = match (method, parts.as_slice()) {
        // GET /v2/ — API version check
        (Method::GET, ["v2"]) => Response::builder()
            .status(StatusCode::OK)
            .header("Docker-Distribution-API-Version", "registry/2.0")
            .header("Content-Type", "application/json")
            .body(full("{}"))
            .unwrap(),

        // GET/HEAD /v2/{name}/manifests/{ref}
        (ref m, ["v2", name, "manifests", reference])
            if *name == state.name && (m == Method::GET || m == Method::HEAD) =>
        {
            handle_manifest(&state, reference, m == Method::HEAD).await
        }

        // GET/HEAD /v2/{name}/blobs/sha256:{hex}
        (ref m, ["v2", name, "blobs", digest])
            if *name == state.name && (m == Method::GET || m == Method::HEAD) =>
        {
            handle_blob(&state, digest, m == Method::HEAD).await
        }

        // GET /v2/{name}/tags/list
        (Method::GET, ["v2", name, "tags", "list"]) if *name == state.name => {
            handle_tags_list(&state).await
        }

        _ => oci_error(StatusCode::NOT_FOUND, "NAME_UNKNOWN", "not found"),
    };

    Ok(response)
}

/// Serve a manifest by tag or digest reference.
async fn handle_manifest(
    state: &RegistryState,
    reference: &str,
    head_only: bool,
) -> Response<ResponseBody> {
    // Resolve reference: sha256:hex → by digest, otherwise by tag.
    let s3_path = if let Some(hex) = reference.strip_prefix("sha256:") {
        ObjectPath::from(format!("{}/manifests/sha256/{}", state.oci_base, hex))
    } else {
        ObjectPath::from(format!("{}/manifests/{}", state.oci_base, reference))
    };

    let data = match state.object_store.get(&s3_path).await {
        Ok(result) => match result.bytes().await {
            Ok(bytes) => bytes,
            Err(e) => {
                error!(error = %e, path = %s3_path, "failed to read manifest bytes");
                return oci_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "MANIFEST_UNKNOWN",
                    "failed to read manifest",
                );
            }
        },
        Err(object_store::Error::NotFound { .. }) => {
            return oci_error(
                StatusCode::NOT_FOUND,
                "MANIFEST_UNKNOWN",
                "manifest not found",
            );
        }
        Err(e) => {
            error!(error = %e, path = %s3_path, "S3 error reading manifest");
            return oci_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "MANIFEST_UNKNOWN",
                "internal error",
            );
        }
    };

    let digest = format!("sha256:{}", hex_sha256(&data));

    let builder = Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
        .header("Docker-Content-Digest", &digest)
        .header("Content-Length", data.len().to_string());

    if head_only {
        builder.body(empty()).unwrap()
    } else {
        builder.body(full(data)).unwrap()
    }
}

/// Serve a blob by digest, streaming from S3.
async fn handle_blob(
    state: &RegistryState,
    digest_ref: &str,
    head_only: bool,
) -> Response<ResponseBody> {
    let Some(hex) = digest_ref.strip_prefix("sha256:") else {
        return oci_error(
            StatusCode::BAD_REQUEST,
            "DIGEST_INVALID",
            "digest must start with sha256:",
        );
    };

    if !is_valid_hex_sha256(hex) {
        return oci_error(
            StatusCode::BAD_REQUEST,
            "DIGEST_INVALID",
            "invalid sha256 digest",
        );
    }

    let s3_path = ObjectPath::from(format!("{}/blobs/sha256/{}", state.oci_base, hex));

    if head_only {
        // HEAD: just get metadata for Content-Length.
        match state.object_store.head(&s3_path).await {
            Ok(meta) => Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "application/octet-stream")
                .header("Docker-Content-Digest", digest_ref)
                .header("Content-Length", meta.size.to_string())
                .body(empty())
                .unwrap(),
            Err(object_store::Error::NotFound { .. }) => {
                oci_error(StatusCode::NOT_FOUND, "BLOB_UNKNOWN", "blob not found")
            }
            Err(e) => {
                error!(error = %e, path = %s3_path, "S3 error reading blob");
                oci_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "BLOB_UNKNOWN",
                    "internal error",
                )
            }
        }
    } else {
        // GET: stream the blob from S3.
        match state.object_store.get(&s3_path).await {
            Ok(result) => {
                let meta = result.meta.clone();
                Response::builder()
                    .status(StatusCode::OK)
                    .header("Content-Type", "application/octet-stream")
                    .header("Docker-Content-Digest", digest_ref)
                    .header("Content-Length", meta.size.to_string())
                    .body(stream_from_s3(result))
                    .unwrap()
            }
            Err(object_store::Error::NotFound { .. }) => {
                oci_error(StatusCode::NOT_FOUND, "BLOB_UNKNOWN", "blob not found")
            }
            Err(e) => {
                error!(error = %e, path = %s3_path, "S3 error reading blob");
                oci_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "BLOB_UNKNOWN",
                    "internal error",
                )
            }
        }
    }
}

/// List all tags for the image.
async fn handle_tags_list(state: &RegistryState) -> Response<ResponseBody> {
    let prefix = ObjectPath::from(format!("{}/manifests/", state.oci_base));
    let mut tags = Vec::new();

    let mut stream = state.object_store.list(Some(&prefix));
    while let Some(result) = stream.next().await {
        match result {
            Ok(meta) => {
                let path_str = meta.location.to_string();
                // Skip digest-keyed entries under sha256/ subdirectory.
                let relative = path_str
                    .strip_prefix(&format!("{}/manifests/", state.oci_base))
                    .unwrap_or("");
                if !relative.is_empty() && !relative.starts_with("sha256/") {
                    tags.push(relative.to_string());
                }
            }
            Err(e) => {
                error!(error = %e, "S3 error listing tags");
                return oci_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "INTERNAL_ERROR",
                    "failed to list tags",
                );
            }
        }
    }

    tags.sort();

    let body = serde_json::json!({
        "name": state.name,
        "tags": tags
    });
    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/json")
        .body(full(serde_json::to_vec(&body).unwrap()))
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    /// Create test state with an InMemory object store.
    fn test_state() -> (Arc<RegistryState>, Arc<InMemory>) {
        let store = Arc::new(InMemory::new());
        let state = Arc::new(RegistryState {
            object_store: Arc::clone(&store) as Arc<dyn ObjectStore>,
            oci_base: "test/exports/myapp/oci".to_string(),
            name: "myapp".to_string(),
        });
        (state, store)
    }

    /// Populate the store with a minimal OCI image (config + 1 layer + manifest).
    async fn populate_store(store: &InMemory) -> (String, String, String) {
        let layer_data = b"fake-compressed-layer-data";
        let layer_digest = hex_sha256(layer_data);

        let config = serde_json::json!({
            "architecture": "amd64",
            "os": "linux",
            "rootfs": {
                "type": "layers",
                "diff_ids": [format!("sha256:{}", hex_sha256(b"uncompressed"))]
            },
            "config": {}
        });
        let config_bytes = serde_json::to_vec(&config).unwrap();
        let config_digest = hex_sha256(&config_bytes);

        let manifest = serde_json::json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": format!("sha256:{}", config_digest),
                "size": config_bytes.len()
            },
            "layers": [{
                "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                "digest": format!("sha256:{}", layer_digest),
                "size": layer_data.len()
            }]
        });
        let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
        let manifest_digest = hex_sha256(&manifest_bytes);

        // Upload all artifacts.
        let base = "test/exports/myapp/oci";
        store
            .put(
                &ObjectPath::from(format!("{}/blobs/sha256/{}", base, layer_digest)),
                Bytes::from_static(layer_data).into(),
            )
            .await
            .unwrap();
        store
            .put(
                &ObjectPath::from(format!("{}/blobs/sha256/{}", base, config_digest)),
                config_bytes.into(),
            )
            .await
            .unwrap();
        store
            .put(
                &ObjectPath::from(format!("{}/manifests/v1", base)),
                Bytes::copy_from_slice(&manifest_bytes).into(),
            )
            .await
            .unwrap();
        store
            .put(
                &ObjectPath::from(format!("{}/manifests/sha256/{}", base, manifest_digest)),
                manifest_bytes.into(),
            )
            .await
            .unwrap();

        (layer_digest, config_digest, manifest_digest)
    }

    /// Helper to send a request and get a response.
    async fn send(state: &Arc<RegistryState>, method: Method, uri: &str) -> Response<ResponseBody> {
        let req = Request::builder()
            .method(method)
            .uri(uri)
            .body(Full::new(Bytes::new()))
            .unwrap();
        handle_request(Arc::clone(state), req).await.unwrap()
    }

    /// Collect body bytes from a response.
    async fn body_bytes(resp: Response<ResponseBody>) -> Vec<u8> {
        use http_body_util::BodyExt;
        let collected = resp.into_body().collect().await.unwrap();
        collected.to_bytes().to_vec()
    }

    #[tokio::test]
    async fn test_v2_endpoint() {
        let (state, _store) = test_state();
        let resp = send(&state, Method::GET, "/v2/").await;

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers()
                .get("Docker-Distribution-API-Version")
                .unwrap(),
            "registry/2.0"
        );
    }

    #[tokio::test]
    async fn test_get_manifest_by_tag() {
        let (state, store) = test_state();
        let (_, _, manifest_digest) = populate_store(&store).await;

        let resp = send(&state, Method::GET, "/v2/myapp/manifests/v1").await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get("Content-Type").unwrap(),
            "application/vnd.oci.image.manifest.v1+json"
        );
        assert_eq!(
            resp.headers()
                .get("Docker-Content-Digest")
                .unwrap()
                .to_str()
                .unwrap(),
            format!("sha256:{}", manifest_digest)
        );

        let body = body_bytes(resp).await;
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["schemaVersion"], 2);
    }

    #[tokio::test]
    async fn test_head_manifest() {
        let (state, store) = test_state();
        let (_, _, manifest_digest) = populate_store(&store).await;

        let resp = send(&state, Method::HEAD, "/v2/myapp/manifests/v1").await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers()
                .get("Docker-Content-Digest")
                .unwrap()
                .to_str()
                .unwrap(),
            format!("sha256:{}", manifest_digest)
        );
        // HEAD should have Content-Length but empty body.
        let body = body_bytes(resp).await;
        assert!(body.is_empty());
    }

    #[tokio::test]
    async fn test_get_manifest_by_digest() {
        let (state, store) = test_state();
        let (_, _, manifest_digest) = populate_store(&store).await;

        let uri = format!("/v2/myapp/manifests/sha256:{}", manifest_digest);
        let resp = send(&state, Method::GET, &uri).await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_blob() {
        let (state, store) = test_state();
        let (layer_digest, _, _) = populate_store(&store).await;

        let uri = format!("/v2/myapp/blobs/sha256:{}", layer_digest);
        let resp = send(&state, Method::GET, &uri).await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers()
                .get("Docker-Content-Digest")
                .unwrap()
                .to_str()
                .unwrap(),
            format!("sha256:{}", layer_digest)
        );
        let body = body_bytes(resp).await;
        assert_eq!(body, b"fake-compressed-layer-data");
    }

    #[tokio::test]
    async fn test_head_blob() {
        let (state, store) = test_state();
        let (layer_digest, _, _) = populate_store(&store).await;

        let uri = format!("/v2/myapp/blobs/sha256:{}", layer_digest);
        let resp = send(&state, Method::HEAD, &uri).await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers()
                .get("Content-Length")
                .unwrap()
                .to_str()
                .unwrap(),
            "26"
        );
        let body = body_bytes(resp).await;
        assert!(body.is_empty());
    }

    #[tokio::test]
    async fn test_tags_list() {
        let (state, store) = test_state();
        populate_store(&store).await;

        // Add a second tag.
        let manifest_bytes = b"{}";
        store
            .put(
                &ObjectPath::from("test/exports/myapp/oci/manifests/v2"),
                Bytes::from_static(manifest_bytes).into(),
            )
            .await
            .unwrap();

        let resp = send(&state, Method::GET, "/v2/myapp/tags/list").await;
        assert_eq!(resp.status(), StatusCode::OK);

        let body = body_bytes(resp).await;
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["name"], "myapp");
        let tags = parsed["tags"].as_array().unwrap();
        assert!(tags.contains(&serde_json::json!("v1")));
        assert!(tags.contains(&serde_json::json!("v2")));
        // sha256/ entries should be filtered out.
        for tag in tags {
            assert!(
                !tag.as_str().unwrap().starts_with("sha256/"),
                "sha256/ entries should be filtered"
            );
        }
    }

    #[tokio::test]
    async fn test_manifest_not_found() {
        let (state, _store) = test_state();
        let resp = send(&state, Method::GET, "/v2/myapp/manifests/nonexistent").await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        let body = body_bytes(resp).await;
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["errors"][0]["code"], "MANIFEST_UNKNOWN");
    }

    #[tokio::test]
    async fn test_blob_not_found() {
        let (state, _store) = test_state();
        let resp = send(
            &state,
            Method::GET,
            "/v2/myapp/blobs/sha256:0000000000000000000000000000000000000000000000000000000000000000",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        let body = body_bytes(resp).await;
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["errors"][0]["code"], "BLOB_UNKNOWN");
    }

    #[tokio::test]
    async fn test_unknown_name() {
        let (state, _store) = test_state();
        let resp = send(&state, Method::GET, "/v2/unknown/manifests/v1").await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
}
