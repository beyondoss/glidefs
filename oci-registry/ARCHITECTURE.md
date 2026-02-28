# oci-registry Architecture

A thin, ergonomic wrapper around `oci-client` that gives GlideFS a typed, streaming interface to OCI-compatible container registries.

## Data Flow

### Pull (registry → caller)

```
resolve(image, auth)
  │
  ├─► GET /v2/{repo}/manifests/{ref}
  │         │
  │         ├── OciManifest::Image ──────────────────────────────────┐
  │         │                                                        │
  │         └── OciManifest::ImageIndex                              │
  │                   │                                              │
  │                   ├─► find linux/amd64 entry                     │
  │                   └─► GET /v2/{repo}/manifests/{platform-digest} ┘
  │
  ├─► GET /v2/{repo}/blobs/{config-digest}  (config JSON, buffered)
  │
  └─► ResolvedImage { manifest, manifest_digest, config, layers }

pull_layer(image, layer, auth)
  │
  ├─► auth(image, Pull)
  └─► GET /v2/{repo}/blobs/{layer-digest}  → Stream<Bytes>  (never buffered)
```

### Push (caller → registry)

```
push_blob(image, stream, digest, auth)
  │
  ├─► auth(image, Push)
  ├─► HEAD /v2/{repo}/blobs/{digest}  → if exists, return early (idempotent)
  ├─► POST  /v2/{repo}/blobs/uploads/           (initiate chunked upload)
  ├─► PATCH /v2/{repo}/blobs/uploads/{uuid}     (stream data, chunked)
  └─► PUT   /v2/{repo}/blobs/uploads/{uuid}?digest=  (commit)

push_manifest(image, manifest, auth)
  │
  ├─► auth(image, Push)
  └─► PUT /v2/{repo}/manifests/{ref}
```

## Concepts & Terminology

| Term | Definition | NOT |
|------|-----------|-----|
| `Reference` | `registry/repository:tag` or `registry/repository@sha256:hex` | Not just a tag; includes registry host |
| `OciDescriptor` | Pointer to a blob: `{digest, size, mediaType}` | Not the blob itself |
| `OciImageManifest` | JSON listing config descriptor + ordered layer descriptors | Not the config blob |
| `OciImageIndex` | Multi-arch manifest list mapping platform → image manifest | Not an image manifest |
| manifest digest | sha256 of the serialized manifest JSON bytes | Not the layer digest |
| diff_id | sha256 of the *uncompressed* layer tar (used in config) | Not the blob digest |
| blob digest | sha256 of the *compressed* layer tar (used as S3/registry key) | Not the diff_id |
| layer | gzip-compressed tar archive; the actual filesystem delta | Not a raw disk image |

## Core Mechanism

### Resolve

`resolve()` handles two manifest shapes transparently (`lib.rs:46`):

1. **Single-arch image**: the manifest is fetched directly; config blob is pulled and buffered.
2. **Multi-arch image index**: the index lists manifests keyed by platform. The client finds the first entry matching `linux/amd64` (`Os::Linux` + `Arch::Amd64`) and fetches that nested manifest. Nested image indexes are rejected.

The returned `ResolvedImage::layers` is a bottom-to-top ordered list of `OciDescriptor`s ready for streaming. Callers iterate this list and call `pull_layer()` per entry.

### Idempotent Blob Push

Before any upload, `push_blob()` issues a HEAD request (`blob_exists()`). If the registry already has that digest, the upload is skipped entirely (`lib.rs:164`). This makes re-running a push safe — content-addressed storage guarantees that same digest = same bytes.

### Streaming

Both pulling and pushing avoid holding full blobs in memory:

- **Pull**: `pull_blob_stream()` returns a `Stream<Item = Result<Bytes>>` that the caller can pipe through decompression into the block store without ever materializing the full layer.
- **Push**: `push_blob()` accepts a `Stream<Item = Result<Bytes, io::Error>>` and forwards it via the OCI chunked upload protocol. The caller (typically `glidefs::oci::push`) spools compressed data to a temp file first (to compute digest), then streams from disk.

## Design Decisions

### Why wrap `oci-client` instead of raw HTTP?

The OCI Distribution Spec is deceptively complex: auth token flows, chunked blob upload, multi-arch resolution, content negotiation. `oci-client` handles all of this. Our wrapper adds:

1. **Streaming pull** — `oci-client`'s `pull_blob` buffers into `Vec<u8>`; `pull_blob_stream` gives us the byte stream we need
2. **Idempotent push** — pre-flight `blob_exists` check not in upstream ergonomics
3. **Typed credentials** — a clean `Credentials` enum instead of scattered auth structs
4. **Platform selection** — automatic `linux/amd64` selection from image indexes

Without this wrapper, callers would deal with `oci_client::secrets::RegistryAuth`, `OciManifest` enum matching, and per-request auth token management directly.

### Why hardcode `linux/amd64`?

GlideFS is a hypervisor storage system for x86-64 VMs. The images it pulls are always Linux amd64 root filesystems. Parameterizing platform selection adds complexity with no current use case. If multi-arch support is needed in the future, `resolve()` can accept a `Platform` argument.

### Why buffer the config blob but stream layers?

The config blob is small (typically < 4 KB) and must be fully parsed before the caller can enumerate layers. Buffering it is harmless and simplifies the API. Layers can be gigabytes — streaming is mandatory.

### Why 30s connect / 300s read timeouts?

- **30s connect**: a long connect timeout usually means the registry is unreachable or misconfigured; fail fast
- **300s read**: large layer blobs (multi-GB) must transfer within this window; too short causes spurious failures on slow links

Both are defaults; callers can override via `RegistryClient::with_config(ClientConfig { ... })`.

## Package Structure

| File | Purpose |
|------|---------|
| `src/lib.rs` | `RegistryClient`: `resolve`, `pull_layer`, `push_blob`, `push_manifest`, `blob_exists` |
| `src/types.rs` | `Credentials` enum, `ResolvedImage` struct; re-exports from `oci-client` and `oci-spec` |
| `src/error.rs` | `Error` enum wrapping `OciDistributionError`, invalid-reference, no-platform-match, io |

## Configuration

`RegistryClient::new()` uses built-in defaults. Custom config via `RegistryClient::with_config(ClientConfig { ... })`:

| Setting | Default | Why |
|---------|---------|-----|
| `connect_timeout` | 30s | Fail fast on unreachable registries |
| `read_timeout` | 300s | Allow large layer transfers without spurious timeout |
| `protocol` | HTTPS (via rustls) | `oci-client` compiled with `rustls-tls`, no OpenSSL dependency |

The crate has no environment variables or config files of its own. Credentials are passed explicitly to each method.

## Error Handling

```
Error::Registry(OciDistributionError)   — HTTP/auth failures, 4xx/5xx from registry
Error::InvalidReference(String)          — Malformed image reference, nested image index
Error::NoPlatformMatch                   — Image index has no linux/amd64 entry
Error::Io(io::Error)                     — Stream I/O errors during push
```

All errors implement `std::error::Error` via `thiserror`. Callers in `glidefs::oci` convert to `PullError` or `PushError` for their own error types.

## Failure Modes

| Failure | Behavior | Recovery |
|---------|----------|---------|
| Registry unreachable | `Error::Registry` with connection error | Retry after checking network/credentials |
| Auth failure (403) | `Error::Registry` | Check credentials and registry permissions |
| Blob not found (404) during pull | `Error::Registry` | Image reference or digest is wrong |
| No linux/amd64 in image index | `Error::NoPlatformMatch` | Choose a different image |
| Stream interrupted during push | `Error::Registry` or `Error::Io` | Re-run push; idempotent blob check skips already-uploaded blobs |
| Manifest push conflict (409) | `Error::Registry` | Usually idempotent; same tag can be overwritten |

## Integration Points

This crate is a library consumed by `glidefs::oci`:

| Consumer | Uses |
|---------|------|
| `glidefs::oci::pull` | `resolve()` + `pull_layer()` to download image layers into GlideFS blocks |
| `glidefs::oci::push` | `blob_exists()` + `push_blob()` + `push_manifest()` to upload GlideFS snapshots as OCI images |
