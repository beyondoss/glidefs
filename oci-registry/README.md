# oci-registry

Async OCI registry client. Resolves image references, streams layers, and pushes blobs and manifests.

## Pull an image

```rust
use oci_registry::{Credentials, RegistryClient};

let client = RegistryClient::new();
let image = "docker.io/library/alpine:3.19".parse().unwrap();

let resolved = client.resolve(&image, &Credentials::Anonymous).await?;
// resolved.manifest_digest — sha256:...
// resolved.layers          — Vec<OciDescriptor>, bottom-to-top
// resolved.config          — raw config JSON bytes
```

Multi-arch image indexes are resolved automatically to `linux/amd64`.

## Stream a layer

Layers are gzip-compressed tar archives. The stream yields raw compressed bytes — decompress on your end.

```rust
use futures::StreamExt;

let layer = &resolved.layers[0];
let mut stream = std::pin::pin!(
    client.pull_layer(&image, layer, &Credentials::Anonymous).await?
);

while let Some(chunk) = stream.next().await {
    let bytes = chunk?;
}
```

## Push a blob and manifest

`push_blob` is idempotent — skips the upload if the blob already exists.

```rust
let auth = Credentials::UsernamePassword {
    username: "user".into(),
    password: "token".into(),
};

let digest = client.push_blob(&image, &data, "sha256:abc...", &auth).await?;
let manifest_digest = client.push_manifest(&image, &manifest, &auth).await?;
```

## Authentication

```rust
Credentials::Anonymous
Credentials::UsernamePassword { username, password }
```

## API

| Method | Description |
|---|---|
| `resolve(image, auth)` | Manifest + config + layer list. Handles multi-arch indexes. |
| `pull_layer(image, layer, auth)` | Stream raw layer bytes (gzip-compressed). |
| `push_blob(image, data, digest, auth)` | Push blob. No-op if digest already exists. |
| `push_manifest(image, manifest, auth)` | Push image manifest. Returns digest. |
| `blob_exists(image, digest, auth)` | HEAD check for a blob. |
