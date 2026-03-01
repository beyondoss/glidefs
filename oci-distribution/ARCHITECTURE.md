# OCI Registry Architecture

Read-only OCI Distribution Spec server and publish CLI that expose GlideFS volume snapshots as container-pullable images stored directly in S3.

## Data Flow

### Publish (GlideFS snapshot → S3 OCI artifacts)

```
GlideFS volume manifest (in S3)
      │
      ▼
 load_readonly_handler()    ← loads manifest, creates in-memory BlockHandler
      │
      │  spawn_blocking (ext4 + gzip work is sync I/O)
      ▼
 export_full_layer()        ← BlockHandler → ext4::Reader → to_tar() → DigestWriter
      │                        or export_delta_layer() for incremental
      │  writer chain:
      │  tar  →  DigestWriter(uncompressed)  →  GzEncoder  →  DigestWriter(compressed)  →  temp file
      │
      ▼
 ExportedLayer { temp_file, diff_id, digest, size }
      │
      │  (async upload from temp file)
      ▼
 S3: .../oci/blobs/sha256/{digest}        ← compressed layer blob
 S3: .../oci/blobs/sha256/{config_digest} ← OCI image config JSON
 S3: .../oci/manifests/{tag}              ← OCI manifest (by tag)
 S3: .../oci/manifests/sha256/{digest}    ← OCI manifest (by digest)
```

### Incremental Publish (delta between two snapshots)

```
BlockHandler (base)        BlockHandler (target)
      │                          │
      ▼ (parallel)               ▼ (parallel)
 export_full_layer()       export_delta_layer()
      │                          │
      ▼                          ▼
 base layer (temp file)    delta layer (temp file)
      │                          │
      └──────────┬───────────────┘
                 ▼
      S3: two layer blobs + config + manifest
      layer 0 = full base snapshot
      layer 1 = delta (adds/mods/whiteouts only)
```

### Serve (HTTP request → S3 object → response)

```
Docker / containerd / skopeo
      │
      │  GET /v2/{name}/manifests/{ref}
      │  GET /v2/{name}/blobs/sha256:{hex}
      │  GET /v2/{name}/tags/list
      ▼
 handle_request()          ← routes by method + path segments
      │
      ▼
 object_store::get()       ← maps OCI path to S3 object path
      │
      │  stream from S3
      ▼
 HTTP response with Docker-Content-Digest header
```

## Concepts & Terminology

| Term              | Definition                                                                  | NOT                                                |
| ----------------- | --------------------------------------------------------------------------- | -------------------------------------------------- |
| Publish           | One-shot CLI command: export GlideFS snapshot to S3 as OCI artifacts       | Not a running service; exits when done             |
| Serve             | Long-running HTTP server serving previously published OCI artifacts         | Not a push-capable registry; read-only             |
| OCI manifest      | JSON document listing config + layer blobs with digests and sizes           | Not the GlideFS volume manifest                    |
| OCI config        | JSON metadata blob: architecture, OS, layer diff_ids                        | Not container entrypoint/env config                |
| Layer blob        | gzip-compressed tar archive of a filesystem snapshot or delta               | Not a GlideFS pack file                            |
| diff_id           | sha256 of the _uncompressed_ tar (required by OCI spec)                    | Not the blob digest (sha256 of compressed data)    |
| Blob digest       | sha256 of the _compressed_ layer blob; used as the S3 key and OCI ref     | Not diff_id                                        |
| Full layer        | Complete ext4 → tar → gzip export of an entire volume                      | Not a docker image base layer in general           |
| Delta layer       | ext4 diff between two snapshots exported as tar + gzip (changes only)      | Not a binary diff; still valid OCI tar format      |
| oci_base          | S3 path prefix for OCI artifacts: `{db_path}/exports/{name}/oci`          | Not the GlideFS ContentStore base path             |
| Reference         | Tag (e.g., `latest`, `v1`) or `sha256:{hex}` digest addressing a manifest | Not a git ref                                      |

## Core Mechanism

### S3 Storage Layout

All OCI artifacts for an export live under a single prefix:

```
{db_path}/exports/{s3_prefix}/oci/
  blobs/
    sha256/
      {hex}          ← compressed layer blobs + config JSON
  manifests/
    {tag}            ← manifest JSON addressed by tag (e.g., "latest", "v1")
    sha256/
      {hex}          ← manifest JSON addressed by digest (for `docker pull @sha256:...`)
```

The server maps OCI Distribution Spec URL paths to this layout directly in `serve.rs:handle_manifest()` and `serve.rs:handle_blob()`. No database or index — the object store _is_ the index.

### Digest-First Upload

The OCI spec requires the layer digest to be known before the blob can be committed. The publish pipeline resolves this by spooling the gzip output to a temp file while simultaneously computing both digests through a layered `DigestWriter` chain:

```
tar bytes
  → DigestWriter<uncompressed>   (computes diff_id as bytes flow through)
  → GzEncoder
  → DigestWriter<compressed>     (computes blob digest as bytes flow through)
  → BufWriter<NamedTempFile>
```

After `finish_layer()` unwraps the chain, both digests are known and the temp file on disk holds the exact bytes to upload. Memory usage is bounded to gzip buffers, never proportional to volume size. The writer chain lives in `glidefs::oci::export_full_layer` and `export_delta_layer`.

### Idempotent Blob Upload

`object_store::put()` is unconditional — publishing the same volume twice overwrites the same S3 keys with identical bytes. Because layer content is content-addressed by sha256, this is safe. Tags (`manifests/latest`) are overwritten to point to the new manifest on each publish.

### Read-Only Registry Protocol

The server implements only the read side of the [OCI Distribution Spec](https://github.com/opencontainers/distribution-spec/blob/main/spec.md):

| Route                                | Method     | Description                                  |
| ------------------------------------ | ---------- | -------------------------------------------- |
| `/v2/`                               | GET        | API version check (required by clients)      |
| `/v2/{name}/manifests/{ref}`         | GET, HEAD  | Manifest by tag or `sha256:` digest          |
| `/v2/{name}/blobs/sha256:{hex}`      | GET, HEAD  | Blob by digest; GET streams from S3          |
| `/v2/{name}/tags/list`               | GET        | Lists tag names (excludes `sha256/` entries) |

Push endpoints (`PUT /v2/{name}/blobs/uploads/`, `PUT /v2/{name}/manifests/{ref}`) are intentionally absent. Publishing is a separate offline CLI operation (`glidefs-registry publish`), not an HTTP endpoint.

### Single-Image-Per-Server

`RegistryState` holds a single `name` string corresponding to one S3 prefix. Requests for any other `{name}` receive `NAME_UNKNOWN 404`. One server instance = one exported volume.

This is intentional. Multiple volumes can be served by running multiple server instances on different ports. It removes the need for multi-tenant routing, authentication, or namespace isolation.

### Tag Listing

Tags are discovered by listing `{oci_base}/manifests/` in S3 and filtering out `sha256/` sub-prefixes. This means tags are the flat objects directly under `manifests/`. No separate tag index is maintained. Tags are returned sorted alphabetically (`serve.rs:handle_tags_list()`).

## Design Decisions

### Why a custom HTTP server instead of a registry library?

The OCI Distribution Spec subset needed here is tiny: three read-only routes. A full registry library (Harbor, distribution/distribution) would add hundreds of thousands of lines and require a database, auth subsystem, and blob upload protocol — all irrelevant when the storage layer is already S3 and publishing is a separate step.

Hyper + object_store gives:
1. **Zero extra dependencies** beyond what glidefs already uses
2. **Direct S3 streaming** — blobs flow from S3 to the client without buffering
3. **Trivial deployment** — single binary, no sidecar database

### Why publish to S3 instead of directly to a registry?

Publishing large layer blobs to an OCI registry requires a multi-step chunked upload (POST → PATCH × N → PUT). S3 multipart upload is equally complex. By targeting S3 directly and serving with this thin server, we:

1. **Avoid double-buffering** — layer data goes to S3 once; the serve path streams it unchanged
2. **Get colocation** — OCI artifacts live beside the GlideFS packs they were derived from
3. **Decouple publish from serve** — publish can run as a batch job; the server is stateless

External OCI registries (ECR, GHCR, Docker Hub) remain usable via the existing `push_image()` / `push_delta_image()` functions in `glidefs/src/oci/push.rs`.

### Why run base and delta export in parallel?

`export_full_layer` and `export_delta_layer` are CPU- and I/O-bound on different block ranges. Running them in parallel with `tokio::task::spawn_blocking` on separate threads halves the wall-clock time for incremental publishes with no added complexity (`publish.rs:45-61`).

### Why spool to a temp file instead of streaming the upload?

OCI blob PUT requires the digest in the URL, which isn't known until the full content has been hashed. Options considered:

1. **Two-pass**: read all data twice — once for hashing, once for upload. Doubles I/O.
2. **Buffer in memory**: hold the entire layer in RAM. Prohibitive for large volumes.
3. **Spool to temp file**: single pass computing digest, then upload from disk. Chosen.

The temp file path is held in `ExportedLayer::temp_file` (a `NamedTempFile`) which auto-deletes on drop.

### Why one name per server instance?

Considered: a single server instance serving multiple volumes via URL-based dispatch (`/v2/vol-a/...`, `/v2/vol-b/...`). Rejected because:

- It requires listing all available volumes at startup or dynamic volume registration
- It complicates routing and makes misconfiguration harder to detect
- Container runtimes pull from `registry-host:port/name:tag`; the host:port already disambiguates instances

Running multiple instances is trivial with a process supervisor and port allocation.

## Package Structure

| File             | Purpose                                                                              |
| ---------------- | ------------------------------------------------------------------------------------ |
| `src/main.rs`    | CLI entry point: `publish` and `serve` subcommands via `clap`                       |
| `src/publish.rs` | `run_publish()`: export GlideFS snapshot, upload blobs + config + manifest to S3   |
| `src/serve.rs`   | `run_serve()`: OCI Distribution HTTP server; routes to manifest/blob/tags handlers |
| `src/config.rs`  | `setup_object_store()`: parse GlideFS config → object_store; `load_readonly_handler()`: load a volume manifest as a read-only BlockHandler |

The publish and serve paths share no state beyond the `object_store` connection. They can be run as entirely separate processes.

## Configuration

`glidefs-registry publish` flags:

| Flag              | Default    | Purpose                                                             |
| ----------------- | ---------- | ------------------------------------------------------------------- |
| `--manifest`      | required   | Volume manifest name in S3 to export                               |
| `--s3-prefix`     | required   | Export name (used as both S3 path component and OCI image name)    |
| `--tag`           | `latest`   | OCI image tag to publish under                                      |
| `--base-manifest` | none       | If set, produce a 2-layer delta image against this base manifest    |
| `-c`/`--config`   | required   | Path to GlideFS config YAML (S3 credentials, bucket URL)           |

`glidefs-registry serve` flags:

| Flag              | Default         | Purpose                                                              |
| ----------------- | --------------- | -------------------------------------------------------------------- |
| `--listen`        | `0.0.0.0:5000`  | TCP address to bind the HTTP server                                  |
| `--s3-prefix`     | required        | Export name; must match a published export                          |
| `-c`/`--config`   | required        | Path to GlideFS config YAML                                          |

The `RUST_LOG` environment variable controls log level (default: `info`); logs go to stderr.

## Failure Modes

| Failure                             | Behavior                                                                                    | Recovery                                      |
| ----------------------------------- | ------------------------------------------------------------------------------------------- | --------------------------------------------- |
| S3 read error during blob serve     | 500 with `BLOB_UNKNOWN` JSON; connection closed                                             | Client retries; transient S3 errors self-heal |
| Manifest not found in S3            | 404 with `MANIFEST_UNKNOWN`                                                                 | Run `glidefs-registry publish` first          |
| Unknown `{name}` in URL             | 404 with `NAME_UNKNOWN`                                                                     | Verify `--s3-prefix` matches published export |
| S3 upload failure during publish    | `anyhow::Error` printed; process exits non-zero; partial artifacts left in S3              | Retry publish; idempotent overwrites are safe |
| Layer export panic in spawn_blocking | `JoinError` converted to `anyhow::Error`; publish aborts                                   | Check logs; usually a bug in the ext4 reader  |
| Serve process crash mid-stream      | Client sees truncated response; `Docker-Content-Digest` mismatch → client retries          | Restart server; no persistent state to recover |

## Trust Model

**What the server verifies:**
- Request path matches `/v2/{expected-name}/...` — all other names get 404
- Digest format starts with `sha256:` — other algorithms get 400
- S3 object exists — missing objects get 404, not empty responses

**What the server does NOT verify:**
- Client identity — no authentication, no authorization
- Digest integrity of served blobs — S3 is trusted to return correct bytes
- TLS — runs plain HTTP; deploy behind a TLS-terminating proxy for production

**Why this is acceptable:**

The server is designed to run on a private network or behind an authenticated proxy. GlideFS volumes contain VM rootfs images — production deployments should not expose this server publicly without a reverse proxy providing auth and TLS.
