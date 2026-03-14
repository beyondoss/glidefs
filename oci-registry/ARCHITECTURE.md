# oci-registry Architecture

Takes an image reference and credentials, makes outbound HTTPS calls to an OCI-compatible registry, and returns either a streaming byte source (pull) or writes blobs/manifests to the registry (push) — with no local disk I/O and no persistent state.

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
  │                   │     └── not found ──► Error::NoPlatformMatch │
  │                   └─► GET /v2/{repo}/manifests/{platform-digest} ┘
  │                             └── nested ImageIndex ──► Error::InvalidReference
  │
  ├─► GET /v2/{repo}/blobs/{config-digest}  (config JSON, buffered in Vec<u8>)
  │         └── 404 / error ──► Error::Registry
  │
  └─► Ok(ResolvedImage { manifest, manifest_digest, config, layers })

pull_layer(image, layer, auth)
  │
  ├─► auth(image, Pull)
  │         └── 401/403 ──► Error::Registry
  └─► GET /v2/{repo}/blobs/{layer-digest}  → Stream<Bytes>  (never buffered)
            └── error ──► Error::Registry or Error::Io (from stream consumer)
```

### Push (caller → registry)

```
push_blob(image, stream, digest, auth)
  │
  ├─► auth(image, Push)
  │         └── 401/403 ──► Error::Registry
  ├─► HEAD /v2/{repo}/blobs/{digest}
  │         └── 200 ──► return Ok(digest) immediately  [idempotent skip]
  ├─► POST  /v2/{repo}/blobs/uploads/           (initiate chunked upload)
  ├─► PATCH /v2/{repo}/blobs/uploads/{uuid}     (stream chunks from caller)
  │         └── stream error ──► Error::Io
  └─► PUT   /v2/{repo}/blobs/uploads/{uuid}?digest=  (commit; registry verifies digest)
            └── digest mismatch or error ──► Error::Registry

push_manifest(image, manifest, auth)
  │
  ├─► auth(image, Push)
  │         └── 401/403 ──► Error::Registry
  └─► PUT /v2/{repo}/manifests/{ref}
            └── error ──► Error::Registry
```

## Concepts & Terminology

| Term | What It Controls | NOT |
|------|-----------------|-----|
| `Reference` | Which registry, repo, and tag/digest are targeted for all HTTP calls | Not just a tag; the registry host is part of it |
| `OciDescriptor` | The URL key for fetching or pushing a blob (`/v2/{repo}/blobs/{digest}`) | Not the blob data itself |
| `OciImageManifest` | Which config blob and which layers (ordered bottom-to-top) are fetched | Not the config blob or layer contents |
| `OciImageIndex` | Which platform-specific manifest digest is used for a given OS/arch | Not an image manifest; not directly pullable |
| manifest digest | The SHA-256 used to address a specific manifest version; deterministic from JSON bytes | Not a layer digest; not a config digest |
| diff_id | SHA-256 of the *uncompressed* layer tar; referenced in the config blob | Not the blob digest used for download |
| blob digest | SHA-256 of the *compressed* layer tar; used as the registry key for GET/PUT | Not the diff_id used in config |
| layer | gzip-compressed tar archive containing a filesystem delta | Not a raw disk image |

## Core Mechanism

### Resolve

`resolve()` normalizes two manifest shapes into one result (`lib.rs:46`):

1. **Single-arch image**: pulls the manifest directly; config blob is fetched and buffered (always < 4 KB).
2. **Multi-arch image index**: scans the index for the first entry with `Os::Linux` + `Arch::Amd64`, then fetches that platform's nested manifest. Nested image indexes are rejected with `Error::InvalidReference`.

The returned `ResolvedImage::layers` is an ordered `Vec<OciDescriptor>` (bottom layer first). Callers iterate this list and call `pull_layer()` per entry.

### Idempotent Blob Push

`push_blob()` issues a HEAD request before any upload (`lib.rs:164`). If the registry already has that digest, the upload is skipped and `Ok(digest)` is returned immediately. Content-addressed storage guarantees that same digest = same bytes, so skipping is always safe.

### Streaming

Blobs are never fully buffered:

- **Pull**: `pull_blob_stream()` returns a lazy `Stream<Item = Result<Bytes>>`. Bytes arrive from the registry as the caller consumes the stream — no full-layer materialization.
- **Push**: `push_blob()` accepts a `Stream<Item = Result<Bytes, io::Error>>` and forwards chunks via the OCI chunked upload protocol. The caller (typically `glidefs::oci::push`) spools compressed data to a temp file first to compute the digest, then streams from disk.

## Trust Boundaries

**What this crate verifies (rejects if invalid):**

- Authentication succeeds before any pull or push (`auth()` call returns error on 401/403)
- Image index has a `linux/amd64` entry before proceeding (returns `Error::NoPlatformMatch` if absent)
- Nested image indexes are rejected (returns `Error::InvalidReference`)

**What passes through unchecked:**

- **Digest integrity on pull**: the crate does not re-hash received bytes against the descriptor digest; the registry is trusted to serve correct content
- **TLS certificate verification**: delegated entirely to `rustls` via `oci-client`; the crate applies no additional cert pinning or verification
- **Manifest signature verification**: OCI image signing (cosign, Notary v2) is out of scope; any valid manifest is accepted
- **Layer content**: tar archives are forwarded as-is; no scanning or validation of archive contents
- **Reference format**: basic parsing delegated to `oci-client`; malformed references surface as `Error::Registry` or `Error::InvalidReference` from the upstream crate

**Why these boundaries are here:**

- Digest re-verification would require buffering GB-scale layers — incompatible with the streaming model
- Signature verification belongs to the OCI policy enforcement layer in the caller, not the transport layer
- The registry is the authoritative source of truth for content addressing

## Why It Behaves This Way

### Why the system wraps `oci-client` instead of speaking HTTP directly

`oci-client` handles auth token exchange, chunked upload sequencing, and content negotiation — all of which are complex enough that building them from scratch would introduce bugs. The wrapper adds the four behaviors `oci-client` doesn't expose ergonomically:

1. **Streaming pull** — `oci-client`'s `pull_blob` buffers into `Vec<u8>`; `pull_blob_stream` gives a lazy stream
2. **Idempotent push** — the pre-flight `blob_exists` HEAD check isn't part of `oci-client`'s push API
3. **Typed credentials** — a clean `Credentials` enum instead of `oci_client::secrets::RegistryAuth` in callers
4. **Automatic platform selection** — `linux/amd64` chosen from image indexes without caller involvement

### Why the system hardcodes `linux/amd64`

GlideFS provisions x86-64 Linux VMs. Every image it pulls is a Linux amd64 root filesystem. Parameterizing platform selection adds complexity with no current use case; `resolve()` can accept a `Platform` argument when that changes.

### Why the system buffers the config blob but streams layers

The config blob is small (typically < 4 KB) and must be fully parsed before the caller can enumerate layer descriptors. Buffering it simplifies the API at negligible cost. Layers can be multi-GB — streaming is mandatory to avoid OOM.

### Why connect timeout is 30s and read timeout is 300s

- **30s connect**: registries that don't respond in 30s are either unreachable or misconfigured; fail fast rather than hold a connection open
- **300s read**: multi-GB layers on slow links need this window; too-short read timeouts cause spurious failures and wasted re-upload attempts

Both are defaults; callers can override via `RegistryClient::with_config(ClientConfig { ... })`.

## Package Structure

| File | What It Does |
|------|-------------|
| `src/lib.rs` | `RegistryClient`: implements `resolve`, `pull_layer`, `push_blob`, `push_manifest`, `blob_exists`; all outbound HTTP calls originate here |
| `src/types.rs` | `Credentials` enum (Anonymous / UsernamePassword); `ResolvedImage` struct; re-exports `Reference`, `OciImageManifest`, `OciDescriptor`, `OciImageIndex` from `oci-client`/`oci-spec` |
| `src/error.rs` | `Error` enum: `Registry(OciDistributionError)`, `InvalidReference(String)`, `NoPlatformMatch`, `Io(io::Error)` |

## Configuration

`RegistryClient::new()` uses built-in defaults. Pass `RegistryClient::with_config(ClientConfig { ... })` to override:

| Setting | Default | What It Controls |
|---------|---------|-----------------|
| `connect_timeout` | 30s | TCP handshake deadline; requests that don't connect within this window get `Error::Registry` |
| `read_timeout` | 300s | Per-read deadline during blob transfer; streams that stall for this long produce `Error::Registry` or `Error::Io` |
| `protocol` | HTTPS via rustls | TLS stack used for all registry connections |

No environment variables or config files. Credentials are passed explicitly to each method and never stored in `RegistryClient`.

## Failure Modes

| Failure | What Actually Happens | Recovery |
|---------|-----------------------|---------|
| Registry unreachable | `Error::Registry(OciDistributionError::ConnectionError(...))` returned immediately | Retry after network or credential check |
| Auth failure (401/403) | `Error::Registry` returned from `auth()` before any data transfer begins | Check credentials and registry ACLs |
| Blob not found (404) during pull | `Error::Registry` from `pull_blob_stream` or `pull_blob` | Verify image reference or digest is correct |
| No `linux/amd64` in image index | `Error::NoPlatformMatch` returned by `resolve()` after fetching the index | Choose a different image |
| Nested image index | `Error::InvalidReference("nested image index not supported")` | No recovery; this image structure is not supported |
| Stream error mid-push | `Error::Io` from PATCH phase; partial upload is abandoned | Re-run push; `blob_exists` check skips any already-uploaded content |
| Stream error mid-pull | `Error::Io` surfaces to the stream consumer | Caller must restart `pull_layer()` from scratch |
| Manifest push conflict (409) | `Error::Registry` | Retry usually succeeds; same-digest pushes are idempotent at the registry |
| Connect timeout exceeded | `Error::Registry` after 30s | Check registry reachability |
| Read timeout exceeded | `Error::Registry` or `Error::Io` after 300s of stalled transfer | Check link speed; consider increasing `read_timeout` |

No retries are built into this crate. Callers must implement retry logic if needed.

## Integration Points

| Consumer | What It Calls |
|---------|--------------|
| `glidefs::oci::pull` | `resolve()` to enumerate layers, then `pull_layer()` per layer to stream compressed tar into the block store |
| `glidefs::oci::push` | `blob_exists()` + `push_blob()` per layer, then `push_manifest()` to publish the image |
