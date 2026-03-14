# OCI Registry Architecture

Takes a GlideFS volume manifest name and S3 config, exports the volume's filesystem as gzip-compressed tar, uploads OCI blobs and manifests to S3, and serves them over a read-only HTTP server that streams blobs directly from S3 to any OCI-compatible client (Docker, containerd, skopeo).

## Data Flow

### Publish (GlideFS snapshot → S3 OCI artifacts)

```
CLI: --manifest {name} --s3-prefix {export} --tag {tag} -c {config}
      │
      ├─ parse config TOML → object_store connection to S3
      ├─ load_readonly_handler() → load GlideFS manifest from S3, build BlockHandler
      │         └── manifest not found ──► anyhow::Error, process exits non-zero
      │
      │  spawn_blocking (ext4 reading + gzip is sync I/O)
      ▼
 export_full_layer(handler)
   tar bytes
     → DigestWriter<uncompressed>   (accumulates diff_id = sha256 of raw tar)
     → GzEncoder
     → DigestWriter<compressed>     (accumulates blob_digest = sha256 of gzip)
     → BufWriter<NamedTempFile>
      │
      ▼
 ExportedLayer { temp_file, diff_id, digest, size }
      │
      │  (async multipart upload, 8 MB chunks, streamed from temp file)
      ├─► S3: {oci_base}/blobs/sha256/{blob_digest}     ← compressed layer
      │         └── upload error ──► anyhow::Error, process exits non-zero
      │                               (partial artifacts remain; retry is safe)
      ├─► S3: {oci_base}/blobs/sha256/{config_digest}   ← OCI image config JSON
      ├─► S3: {oci_base}/manifests/{tag}                ← OCI manifest (by tag)
      └─► S3: {oci_base}/manifests/sha256/{manifest_digest} ← (by digest)
```

### Incremental Publish (delta between two snapshots)

```
--base-manifest {base} --manifest {target}
      │
      ├─ load_readonly_handler(base)   ─── both loaded before export starts
      └─ load_readonly_handler(target) ─┘
                │
                │ tokio::task::spawn_blocking (two threads run in parallel)
                ├─────────────────────────────────────────────┐
                ▼                                             ▼
   export_full_layer(base)                    export_delta_layer(base, target)
   base layer (temp file)                     delta layer (temp file)
                │                                             │
                └─────────────────┬───────────────────────────┘
                                  ▼ (both awaited before upload)
                     S3: 2 blobs + config + manifest
                     layer 0 = full base snapshot
                     layer 1 = delta (adds/mods/whiteouts only)
```

### Serve (HTTP request → S3 stream → response)

```
Docker / containerd / skopeo / any OCI client
      │
      │  TCP connect to 0.0.0.0:5000 (default)
      ▼
 handle_request() ── route by method + path segments
      │
      ├── GET /v2/
      │         └──► 200 + Docker-Distribution-API-Version header + {} body
      │
      ├── GET|HEAD /v2/{name}/manifests/{ref}
      │         ├── name != configured name ──► 404 NAME_UNKNOWN
      │         ├── object_store.get({oci_base}/manifests/{ref})
      │         │         └── not found ──► 404 MANIFEST_UNKNOWN
      │         ├── buffer bytes (manifest JSON is small)
      │         ├── compute sha256 → Docker-Content-Digest header
      │         └──► 200 + manifest JSON body (HEAD: headers only)
      │
      ├── GET|HEAD /v2/{name}/blobs/sha256:{hex}
      │         ├── digest format invalid ──► 400 DIGEST_INVALID
      │         ├── object_store.get({oci_base}/blobs/sha256/{hex})
      │         │         └── not found ──► 404 BLOB_UNKNOWN
      │         │         └── S3 error during stream ──► 500 BLOB_UNKNOWN
      │         └──► 200 + stream bytes from S3 (never buffered to memory)
      │
      ├── GET /v2/{name}/tags/list
      │         ├── object_store.list({oci_base}/manifests/)
      │         ├── filter out sha256/* sub-prefixes
      │         ├── sort alphabetically
      │         └──► 200 + {"name": "...", "tags": [...]}
      │
      └── anything else ──► 404 NAME_UNKNOWN
```

## Concepts & Terminology

| Term         | What It Controls                                                       | NOT                                                   |
| ------------ | ---------------------------------------------------------------------- | ----------------------------------------------------- |
| Publish      | Whether OCI artifacts exist in S3 and can be served or pulled          | Not a running service; exits when done                |
| Serve        | Which OCI clients can pull the image and from where                    | Not a push-capable registry; read-only                |
| OCI manifest | Which blobs a client fetches and in what order (controls the pull)     | Not the GlideFS volume manifest                       |
| OCI config   | What `docker inspect` reports (architecture, OS, layer chain)          | Not container entrypoint/env — `config` block is empty|
| Layer blob   | The bytes streamed to the container runtime for filesystem extraction  | Not a GlideFS pack file                               |
| diff_id      | The layer identity in the image config; computed from uncompressed tar | Not the blob digest (sha256 of compressed data)       |
| Blob digest  | The S3 key and URL path for fetching a blob; computed from gzip output | Not the diff_id used in the image config              |
| Full layer   | A layer that contains the entire volume filesystem                     | Not a Docker base layer in general                    |
| Delta layer  | A layer containing only changed/added/deleted files since base         | Not a binary diff; still valid OCI tar+whiteout format|
| oci_base     | The S3 prefix where all OCI artifacts for this export live             | Not the GlideFS ContentStore prefix                   |
| Reference    | The tag or `sha256:{hex}` that maps to a manifest on this server       | Not a git ref                                         |
| name         | The URL path segment that gates which requests this server accepts     | Not a human-readable label; must match `--s3-prefix`  |

## Core Mechanism

### S3 Storage Layout

All OCI artifacts for one export live under a single prefix:

```
{db_path}/exports/{s3_prefix}/oci/
  blobs/
    sha256/
      {hex}          ← compressed layer blobs + config JSON
  manifests/
    {tag}            ← manifest JSON addressed by tag (e.g., "latest", "v1")
    sha256/
      {hex}          ← manifest JSON addressed by digest
```

The server maps OCI Distribution Spec URL paths to this layout directly in `serve.rs:handle_manifest()` and `serve.rs:handle_blob()`. No database or index — the object store *is* the index.

### Digest-First Upload

The OCI spec requires the blob digest to be known before uploading. The publish pipeline resolves this by computing both digests in a single streaming pass through a layered writer chain:

```
tar bytes
  → DigestWriter<uncompressed>   (accumulates diff_id as bytes flow through)
  → GzEncoder
  → DigestWriter<compressed>     (accumulates blob_digest as bytes flow through)
  → BufWriter<NamedTempFile>
```

After `finish_layer()`, both digests are known and the temp file holds the exact bytes to upload. Memory usage is bounded to gzip buffers, never proportional to volume size. The writer chain lives in `glidefs::oci::export_full_layer` and `export_delta_layer`.

### Idempotent Blob Upload

`object_store::put()` is unconditional — publishing the same volume twice overwrites the same S3 keys with identical bytes. Because layer content is content-addressed by sha256, this is safe. Tags (`manifests/latest`) are overwritten to point to the new manifest on each publish.

### Single-Image-Per-Server

`RegistryState` holds a single `name` string. Any request where the URL `{name}` doesn't match returns `NAME_UNKNOWN 404` immediately, before any S3 access. One server instance serves one exported volume.

### Tag Listing

Tags are discovered by listing `{oci_base}/manifests/` in S3 and filtering out entries under `sha256/`. No separate tag index is maintained; tag objects are the flat files directly under `manifests/`, returned sorted alphabetically (`serve.rs:handle_tags_list()`).

## Trust Boundaries

**What the server verifies (rejects if invalid):**

- Request `{name}` matches the configured `--s3-prefix` (all others → 404)
- Blob digest format is `sha256:` + 64 lowercase hex chars (others → 400)
- S3 object exists for manifests and blobs (missing → 404, not empty response)

**What passes through unchecked:**

- **Client identity**: no authentication, no authorization; any client that can reach the port gets the image
- **Blob integrity on serve**: the server streams bytes from S3 without re-hashing; the `Docker-Content-Digest` header reflects what was uploaded, not what was just served
- **TLS**: runs plain HTTP; there is no TLS termination in this binary
- **Manifest content**: the server serves whatever JSON was stored at publish time without schema validation

**Why these boundaries are here:**

- The server is designed for private networks or behind an authenticated reverse proxy
- GlideFS volumes contain VM rootfs images; production deployments must not expose this server publicly without a proxy providing auth and TLS
- Digest re-verification would require buffering multi-GB layer streams; the container runtime (docker/containerd) re-verifies the digest client-side against the `Docker-Content-Digest` header

## Why It Behaves This Way

### Why the server streams blobs directly from S3 without buffering

The layer blobs can be multi-GB. Buffering them in memory would exhaust RAM on any concurrent pull. `object_store::get()` returns a `Stream<Bytes>` that drives bytes from S3 through the TCP socket with no intermediate storage (`serve.rs:handle_blob()`).

### Why publish spools to a temp file instead of streaming uploads directly

The OCI spec requires the digest in the upload URL, which isn't known until all content has been hashed. Three options were considered:

1. **Two-pass**: read data twice — once for hashing, once for upload. Doubles I/O cost.
2. **Buffer in memory**: hold the entire layer in RAM. Prohibitive for large volumes.
3. **Spool to temp file**: single pass computing digest, then upload from disk. ← chosen

The temp file is held in `ExportedLayer::temp_file` (a `NamedTempFile`) which auto-deletes on drop.

### Why base and delta export run in parallel

`export_full_layer` and `export_delta_layer` are CPU- and I/O-bound on different block ranges. Running them in parallel with `tokio::task::spawn_blocking` halves wall-clock time for incremental publishes with no added complexity (`publish.rs:45-61`).

### Why the server accepts only one image name per instance

Serving multiple volumes via URL dispatch would require either listing all available volumes at startup or dynamic volume registration — both require state that isn't in scope. Container runtimes use `registry-host:port/name:tag`; the host:port already disambiguates instances. Multiple volumes = multiple server instances on different ports, trivially managed by a process supervisor.

### Why the server uses a custom HTTP implementation instead of a registry library

The OCI Distribution Spec subset needed here is three read-only routes. A full registry library (distribution/distribution, Harbor) would add hundreds of thousands of lines, require a database, auth subsystem, and blob upload protocol — none of which apply when S3 is the storage layer and publishing is a separate CLI step. Hyper + object_store adds no new dependencies (both are already in the glidefs crate) and gives direct S3 streaming.

### Why OCI artifacts are stored in S3 instead of pushed to an external registry

Publishing to an external OCI registry requires chunked upload (POST → PATCH × N → PUT) with no advantage over S3 multipart upload. Storing artifacts in S3 means the serve path streams them unchanged — no double-buffering — and OCI artifacts are colocated with the GlideFS packs they were derived from. External OCI registries remain usable via `push_image()` / `push_delta_image()` in `glidefs/src/oci/push.rs`.

## Package Structure

| File             | What It Does                                                                                     |
| ---------------- | ------------------------------------------------------------------------------------------------ |
| `src/main.rs`    | Parses CLI (`publish` vs `serve` subcommand via clap); dispatches to `run_publish` or `run_serve` |
| `src/publish.rs` | Loads volume handlers, runs export(s) on blocking threads, uploads blobs + config + manifest to S3 |
| `src/serve.rs`   | Binds TCP, routes HTTP requests, streams manifests/blobs from S3, handles graceful shutdown on ctrl-c |
| `src/config.rs`  | Parses GlideFS config TOML into an `object_store` handle; loads a GlideFS volume manifest into a read-only `BlockHandler` |

The publish and serve paths share no state beyond the `object_store` connection and can run as separate processes.

## Configuration

`glidefs-registry publish`:

| Flag              | Default  | What It Controls                                                          |
| ----------------- | -------- | ------------------------------------------------------------------------- |
| `--manifest`      | required | Which GlideFS volume snapshot is exported                                 |
| `--s3-prefix`     | required | S3 path component and OCI image name (must match what `serve` is given)   |
| `--tag`           | `latest` | Which S3 manifest key is overwritten; determines the tag clients pull     |
| `--base-manifest` | none     | If set, triggers two-layer delta publish using this as layer 0            |
| `-c`/`--config`   | required | S3 credentials, bucket URL, and local cache config                        |

`glidefs-registry serve`:

| Flag            | Default        | What It Controls                                                           |
| --------------- | -------------- | -------------------------------------------------------------------------- |
| `--listen`      | `0.0.0.0:5000` | TCP address the HTTP server binds; all clients connect here                |
| `--s3-prefix`   | required       | The only `{name}` that returns non-404; must match a published export      |
| `-c`/`--config` | required       | S3 credentials and bucket URL for all object_store reads                   |

`RUST_LOG` (env var): controls tracing level (default: `info`); logs go to stderr.

## Failure Modes

| Failure | What Actually Happens | Recovery |
|---------|----------------------|---------|
| S3 read error during blob serve | 500 with `BLOB_UNKNOWN` JSON; TCP connection closed after partial response | Client's digest check fails; docker/containerd retry automatically |
| Manifest not found in S3 | 404 `MANIFEST_UNKNOWN` JSON | Run `glidefs-registry publish` first |
| Unknown `{name}` in URL | 404 `NAME_UNKNOWN` JSON immediately, before any S3 access | Verify `--s3-prefix` matches both publish and serve invocations |
| S3 upload failure during publish | `anyhow::Error` printed to stderr; process exits non-zero; partial artifacts remain in S3 | Retry publish; idempotent overwrites make retries safe |
| Layer export panic in spawn_blocking | `JoinError` surfaces as `anyhow::Error`; publish aborts before any upload | Check logs; likely a bug in the ext4 reader |
| Serve process crash mid-stream | Client sees truncated response; digest mismatch → container runtime retries | Restart server; no persistent state to recover |
| Invalid blob digest format in URL | 400 `DIGEST_INVALID` JSON | Use correct format: `sha256:` + 64 lowercase hex chars |
| S3 credentials missing/invalid | `anyhow::Error` from `setup_object_store()` at startup; process exits before serving | Check config file and AWS credential chain |
