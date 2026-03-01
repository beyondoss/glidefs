# glidefs-registry

Publishes GlideFS volume snapshots as OCI images and serves them over the OCI Distribution Spec.

## Why

GlideFS stores block devices as content-addressed packs in S3. Container runtimes (Docker, containerd, Kubernetes) don't speak GlideFS — they speak OCI. This tool bridges the two: it reads the blocks from a GlideFS volume, builds an ext4 filesystem, compresses it as a tar+gzip layer, and writes it to S3 in the OCI image format. A built-in HTTP server makes those artifacts pullable with `docker pull`.

Incremental publish compares two snapshots and produces a two-layer image where the delta layer contains only changes. A 50 GB volume with 10 MB of changes produces a ~10 MB delta layer.

## How It Fits Together

```
GlideFS volume (NBD block device backed by S3 packs)
      │
      │  glidefs-registry publish
      ▼
OCI image artifacts in S3 (manifests + blobs alongside the GlideFS packs)
      │
      │  glidefs-registry serve
      ▼
HTTP endpoint speaking OCI Distribution Spec
      │
      │  docker pull / containerd / skopeo / crane
      ▼
Container runtime has the filesystem
```

### Key Concepts

**Volume manifest** — A GlideFS volume manifest is a serialized block map + pack index stored at `{s3_prefix}/manifests/{name}` in S3. It's created by GlideFS when you snapshot a volume (e.g., via `save_export()`). The `--manifest` flag refers to this name. Example: if your export is named `myapp` and you saved a snapshot called `myapp-2024-01-15`, that's your manifest name.

**S3 prefix** — The `--s3-prefix` flag identifies which GlideFS export to read from. It maps to the directory structure in S3: `{db_path}/exports/{s3_prefix}/`. This same value becomes the OCI image `{name}` in `/v2/{name}/...` URLs.

**Config file** — A TOML file with GlideFS settings. The registry only needs the `[cache]` and `[storage]` sections for S3 access:

```toml
[cache]
dir = "/tmp/glidefs-cache"
disk_size_gb = 10.0

[storage]
url = "s3://my-bucket/glidefs-data"

[servers]
```

Cloud credentials go in provider-specific sections:

```toml
[aws]
access_key_id = "${AWS_ACCESS_KEY_ID}"
secret_access_key = "${AWS_SECRET_ACCESS_KEY}"
```

## Quick Start

```bash
# Build
cargo build --release -p oci-distribution

# Publish a volume snapshot as an OCI image
glidefs-registry publish \
  --manifest myapp-2024-01-15 \
  --s3-prefix myapp \
  --tag v1 \
  -c config.toml

# Start the registry server
glidefs-registry serve \
  --s3-prefix myapp \
  -c config.toml

# Pull it from anywhere
docker pull localhost:5000/myapp:v1
```

Next day, publish an incremental update — only the delta gets uploaded:

```bash
glidefs-registry publish \
  --manifest myapp-2024-01-16 \
  --s3-prefix myapp \
  --tag v2 \
  --base-manifest myapp-2024-01-15 \
  -c config.toml

# Clients see the new tag immediately
docker pull localhost:5000/myapp:v2
```

## Reference

### publish

Exports a GlideFS volume snapshot to S3 as a pullable OCI image.

```bash
glidefs-registry publish \
  --manifest myapp-2024-01-15 \
  --s3-prefix myapp \
  --tag v1.2.3 \
  -c config.toml
```

Output:

```
Published 'myapp-2024-01-15' as tag 'v1.2.3' successfully:
  Manifest digest: sha256:a3f2...
  Layer digest:    sha256:c1b9...
  Layer size:      842.3 MB
  Elapsed:         18.4s
```

For incremental updates, pass `--base-manifest` to produce a two-layer image (base + delta). Base and delta are exported in parallel.

```bash
glidefs-registry publish \
  --manifest myapp-2024-01-16 \
  --s3-prefix myapp \
  --tag v1.2.4 \
  --base-manifest myapp-2024-01-15 \
  -c config.toml
```

Output:

```
Published 'myapp-2024-01-16' as tag 'v1.2.4' successfully:
  Mode:            incremental (delta)
  Layers:          2
  Manifest digest: sha256:d7e1...
  Layer digest:    sha256:8f3a...
  Layer size:      12.1 MB
  Elapsed:         4.2s
```

| Flag | Default | Description |
| --- | --- | --- |
| `--manifest` | required | Volume manifest name in S3 (created by `save_export()`) |
| `--s3-prefix` | required | GlideFS export name — becomes the OCI image `{name}` |
| `--tag` | `latest` | OCI image tag |
| `--base-manifest` | — | Base snapshot for two-layer delta publish |
| `-c`, `--config` | required | GlideFS config TOML (needs `[cache]`, `[storage]`, and cloud credentials) |

Publishing is idempotent. Re-publishing the same manifest + tag overwrites with identical bytes.

### serve

Starts a read-only OCI Distribution Spec HTTP server backed by S3.

```bash
glidefs-registry serve \
  --s3-prefix myapp \
  -c config.toml
```

Pull with any OCI client:

```bash
docker pull localhost:5000/myapp:v1.2.3
skopeo copy docker://localhost:5000/myapp:v1.2.3 oci:./local-copy
```

| Flag | Default | Description |
| --- | --- | --- |
| `--listen` | `0.0.0.0:5000` | TCP address to bind |
| `--s3-prefix` | required | Export name; must match a published export |
| `-c`, `--config` | required | GlideFS config TOML |

One server instance serves one image name. Run multiple instances on separate ports for multiple volumes.

No auth, no TLS. Deploy behind a reverse proxy for production.

## S3 Layout

OCI artifacts are written alongside the GlideFS packs under `{db_path}/exports/{s3_prefix}/oci/`:

```
{db_path}/exports/{s3_prefix}/
  manifests/              ← GlideFS volume manifests (input)
  packs/                  ← GlideFS content-addressed packs (input)
  oci/                    ← OCI artifacts (output of publish)
    blobs/sha256/{hex}    ← compressed layer blobs + config JSON
    manifests/{tag}       ← OCI manifest by tag
    manifests/sha256/{hex} ← OCI manifest by digest
```

## Implemented Routes

| Route | Methods | Notes |
| --- | --- | --- |
| `/v2/` | GET | Version check |
| `/v2/{name}/manifests/{ref}` | GET, HEAD | `{ref}` = tag or `sha256:{hex}` |
| `/v2/{name}/blobs/sha256:{hex}` | GET, HEAD | Blob streamed from S3 |
| `/v2/{name}/tags/list` | GET | Lists published tags |

Push endpoints are not implemented. Publish via the CLI.

## Logging

```bash
RUST_LOG=debug glidefs-registry serve ...
```

Logs go to stderr. Default level: `info`.
