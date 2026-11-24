// Vendored and slightly modified from: https://github.com/apache/arrow-rs-object-store/blob/c0e241eb95a61d52964f3d7741673b91f86db58b/src/parse.rs
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use object_store::ClientOptions;
use object_store::client::{HttpClient, HttpConnector, HttpError, HttpRequest, HttpResponse, HttpService, ReqwestConnector};

/// The object-store client's request timeout when none is configured. `glidefs_kernels::snapshot_owner::STORE_WRITE_SLACK_SECS` is at least
/// the slack's request-timeout terms (`slack_tests`).
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(300);

/// A store config refused at startup: its request timeout is longer than the store write slack was derived from.
#[derive(Debug)]
pub struct StoreConfigRefused {
    pub request_timeout: Duration,
}

impl std::fmt::Display for StoreConfigRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "request_timeout {}s exceeds the {}s the store write slack allows",
            self.request_timeout.as_secs(),
            glidefs_kernels::snapshot_owner::REQUEST_TIMEOUT_SECS
        )
    }
}

impl std::error::Error for StoreConfigRefused {}
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::Duration;
use url::Url;

/// The store's own clock, read from the `Date` header of its responses (S3 stamps `last_modified` and `Date` from one clock). The newest
/// `Date` any response carried, in unix seconds, or 0 before the first. Every `Date` is at or before the store's clock now, so an age
/// measured against it is never longer than the true age: the closed-marker removal errs toward keeping
/// (`snapshot_owner::closed_marker_removable`).
static STORE_DATE_SECS: AtomicI64 = AtomicI64::new(0);
/// The store is on this machine (`file:`, `memory:`), so its clock is the local one.
static STORE_IS_LOCAL: AtomicBool = AtomicBool::new(false);

/// The store's clock now, in unix seconds: the local clock for a local store, else the newest response `Date`. `None` before any response
/// carried one: the caller must not remove anything on an age it cannot measure.
pub fn store_now_secs() -> Option<u64> {
    if STORE_IS_LOCAL.load(Ordering::Acquire) {
        return u64::try_from(chrono::Utc::now().timestamp()).ok();
    }
    u64::try_from(STORE_DATE_SECS.load(Ordering::Acquire)).ok().filter(|&d| d > 0)
}

/// Mark the store as local (tests that run the router on an in-memory store).
pub fn set_store_is_local() {
    STORE_IS_LOCAL.store(true, Ordering::Release);
}

fn record_store_date(resp: &HttpResponse) {
    if let Some(date) = resp
        .headers()
        .get("date")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| chrono::DateTime::parse_from_rfc2822(v).ok())
    {
        STORE_DATE_SECS.fetch_max(date.timestamp(), Ordering::AcqRel);
    }
}

/// The reqwest client, recording the `Date` of every response.
#[derive(Debug)]
struct DateRecording(HttpClient);

#[async_trait::async_trait]
impl HttpService for DateRecording {
    async fn call(&self, req: HttpRequest) -> Result<HttpResponse, HttpError> {
        let resp = self.0.execute(req).await?;
        record_store_date(&resp);
        Ok(resp)
    }
}

#[derive(Debug, Default)]
struct DateRecordingConnector(ReqwestConnector);

impl HttpConnector for DateRecordingConnector {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        Ok(HttpClient::new(DateRecording(self.0.connect(options)?)))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Unable to recognise URL \"{}\"", url)]
    Unrecognised { url: Url },

    #[error(transparent)]
    Path {
        #[from]
        source: object_store::path::Error,
    },
}

impl From<Error> for object_store::Error {
    fn from(e: Error) -> Self {
        Self::Generic {
            store: "URL",
            source: Box::new(e),
        }
    }
}

/// Recognizes various URL formats, identifying the relevant [`ObjectStore`]
///
/// See [`ObjectStoreScheme::parse`] for more details
///
/// # Supported formats:
/// - `file:///path/to/my/file` -> [`LocalFileSystem`]
/// - `memory:///` -> [`InMemory`]
/// - `s3://bucket/path` -> [`AmazonS3`](crate::aws::AmazonS3) (also supports `s3a`)
/// - `gs://bucket/path` -> [`GoogleCloudStorage`](crate::gcp::GoogleCloudStorage)
/// - `az://account/container/path` -> [`MicrosoftAzure`](crate::azure::MicrosoftAzure) (also supports `adl`, `azure`, `abfs`, `abfss`)
/// - `http://mydomain/path` -> [`HttpStore`](crate::http::HttpStore)
/// - `https://mydomain/path` -> [`HttpStore`](crate::http::HttpStore)
///
/// There are also special cases for AWS and Azure for `https://{host?}/path` paths:
/// - `dfs.core.windows.net`, `blob.core.windows.net`, `dfs.fabric.microsoft.com`, `blob.fabric.microsoft.com` -> [`MicrosoftAzure`](crate::azure::MicrosoftAzure)
/// - `amazonaws.com` -> [`AmazonS3`](crate::aws::AmazonS3)
/// - `r2.cloudflarestorage.com` -> [`AmazonS3`](crate::aws::AmazonS3)
///
#[non_exhaustive] // permit new variants
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum ObjectStoreScheme {
    /// Url corresponding to [`LocalFileSystem`]
    Local,
    /// Url corresponding to [`InMemory`]
    Memory,
    /// Url corresponding to [`AmazonS3`](crate::aws::AmazonS3)
    AmazonS3,
    /// Url corresponding to [`GoogleCloudStorage`](crate::gcp::GoogleCloudStorage)
    GoogleCloudStorage,
    /// Url corresponding to [`MicrosoftAzure`](crate::azure::MicrosoftAzure)
    MicrosoftAzure,
    /// Url corresponding to [`HttpStore`](crate::http::HttpStore)
    Http,
}

impl ObjectStoreScheme {
    /// Create an [`ObjectStoreScheme`] from the provided [`Url`]
    ///
    /// Returns the [`ObjectStoreScheme`] and the remaining [`Path`]
    ///
    /// # Example
    /// ```
    /// # use url::Url;
    /// # use object_store::ObjectStoreScheme;
    /// let url: Url = "file:///path/to/my/file".parse().unwrap();
    /// let (scheme, path) = ObjectStoreScheme::parse(&url).unwrap();
    /// assert_eq!(scheme, ObjectStoreScheme::Local);
    /// assert_eq!(path.as_ref(), "path/to/my/file");
    ///
    /// let url: Url = "https://blob.core.windows.net/container/path/to/my/file".parse().unwrap();
    /// let (scheme, path) = ObjectStoreScheme::parse(&url).unwrap();
    /// assert_eq!(scheme, ObjectStoreScheme::MicrosoftAzure);
    /// assert_eq!(path.as_ref(), "path/to/my/file");
    ///
    /// let url: Url = "https://example.com/path/to/my/file".parse().unwrap();
    /// let (scheme, path) = ObjectStoreScheme::parse(&url).unwrap();
    /// assert_eq!(scheme, ObjectStoreScheme::Http);
    /// assert_eq!(path.as_ref(), "path/to/my/file");
    /// ```
    pub fn parse(url: &Url) -> Result<(Self, Path), Error> {
        let strip_bucket = || Some(url.path().strip_prefix('/')?.split_once('/')?.1);

        let (scheme, path) = match (url.scheme(), url.host_str()) {
            ("file", None) => (Self::Local, url.path()),
            ("memory", None) => (Self::Memory, url.path()),
            ("s3" | "s3a", Some(_)) => (Self::AmazonS3, url.path()),
            ("gs", Some(_)) => (Self::GoogleCloudStorage, url.path()),
            ("az", Some(_)) => (Self::MicrosoftAzure, strip_bucket().unwrap_or_default()),
            ("adl" | "azure" | "abfs" | "abfss", Some(_)) => (Self::MicrosoftAzure, url.path()),
            ("http", Some(_)) => (Self::Http, url.path()),
            ("https", Some(host)) => {
                if host.ends_with("dfs.core.windows.net")
                    || host.ends_with("blob.core.windows.net")
                    || host.ends_with("dfs.fabric.microsoft.com")
                    || host.ends_with("blob.fabric.microsoft.com")
                {
                    (Self::MicrosoftAzure, strip_bucket().unwrap_or_default())
                } else if host.ends_with("amazonaws.com") {
                    match host.starts_with("s3") {
                        true => (Self::AmazonS3, strip_bucket().unwrap_or_default()),
                        false => (Self::AmazonS3, url.path()),
                    }
                } else if host.ends_with("r2.cloudflarestorage.com") {
                    (Self::AmazonS3, strip_bucket().unwrap_or_default())
                } else {
                    (Self::Http, url.path())
                }
            }
            _ => return Err(Error::Unrecognised { url: url.clone() }),
        };

        Ok((scheme, Path::from_url_path(path)?))
    }
}

/// Create an [`ObjectStore`] based on the provided `url` and options
///
/// This method can be used to create an instance of one of the provided
/// `ObjectStore` implementations based on the URL scheme (see
/// [`ObjectStoreScheme`] for more details).
///
/// For example
/// * `file:///path/to/my/file` will return a [`LocalFileSystem`] instance
/// * `s3://bucket/path` will return an [`AmazonS3`] instance if the `aws` feature is enabled.
///
/// Arguments:
/// * `url`: The URL to parse
/// * `options`: A list of key-value pairs to pass to the [`ObjectStore`] builder.
///   Note different object stores accept different configuration options, so
///   the options that are read depends on the `url` value. One common pattern
///   is to pass configuration information via process variables using
///   [`std::env::vars`].
///
/// Returns
/// - An [`ObjectStore`] of the corresponding type
/// - The [`Path`] into the [`ObjectStore`] of the addressed resource
///
/// [`AmazonS3`]: https://docs.rs/object_store/0.12.0/object_store/aws/struct.AmazonS3.html
pub fn parse_url_opts<I, K, V>(
    url: &Url,
    options: I,
    connect_timeout: Option<Duration>,
    request_timeout: Option<Duration>,
) -> Result<(Box<dyn ObjectStore>, Path), object_store::Error>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: Into<String>,
{
    let (scheme, path) = ObjectStoreScheme::parse(url)?;
    let path = Path::parse(path)?;

    // The store write slack (`glidefs_kernels::snapshot_owner::STORE_WRITE_SLACK_SECS`) holds only for a request timeout it was derived from: a
    // config with a longer one is refused here, so no config can break the proof's premise.
    let request_timeout = request_timeout.unwrap_or(DEFAULT_REQUEST_TIMEOUT);
    if !glidefs_kernels::snapshot_owner::store_config_admitted(request_timeout.as_secs()) {
        return Err(object_store::Error::Generic {
            store: "object store config",
            source: Box::new(StoreConfigRefused { request_timeout }),
        });
    }
    let client_options = ClientOptions::default()
        .with_connect_timeout(connect_timeout.unwrap_or(Duration::from_secs(10)))
        .with_timeout(request_timeout);

    let store: Box<dyn ObjectStore> = match scheme {
        ObjectStoreScheme::Local => {
            set_store_is_local();
            Box::new(LocalFileSystem::new())
        },
        ObjectStoreScheme::Memory => {
            set_store_is_local();
            Box::new(InMemory::new())
        },
        ObjectStoreScheme::AmazonS3 => {
            let builder = options.into_iter().fold(
                object_store::aws::AmazonS3Builder::new()
                    .with_url(url.to_string())
                    .with_client_options(client_options)
                    .with_http_connector(DateRecordingConnector::default()),
                |builder, (key, value)| match key.as_ref().parse() {
                    Ok(k) => builder.with_config(k, value),
                    Err(_) => builder,
                },
            );
            Box::new(builder.build()?)
        }
        ObjectStoreScheme::GoogleCloudStorage => {
            let builder = options.into_iter().fold(
                object_store::gcp::GoogleCloudStorageBuilder::new()
                    .with_url(url.to_string())
                    .with_client_options(client_options)
                    .with_http_connector(DateRecordingConnector::default()),
                |builder, (key, value)| match key.as_ref().parse() {
                    Ok(k) => builder.with_config(k, value),
                    Err(_) => builder,
                },
            );
            Box::new(builder.build()?)
        }
        ObjectStoreScheme::MicrosoftAzure => {
            let builder = options.into_iter().fold(
                object_store::azure::MicrosoftAzureBuilder::new()
                    .with_url(url.to_string())
                    .with_client_options(client_options)
                    .with_http_connector(DateRecordingConnector::default()),
                |builder, (key, value)| match key.as_ref().parse() {
                    Ok(k) => builder.with_config(k, value),
                    Err(_) => builder,
                },
            );
            Box::new(builder.build()?)
        }
        s => {
            return Err(object_store::Error::Generic {
                store: "parse_url",
                source: format!("feature for {s:?} not enabled").into(),
            });
        }
    };

    Ok((store, path))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse() {
        let cases = [
            ("file:/path", (ObjectStoreScheme::Local, "path")),
            ("file:///path", (ObjectStoreScheme::Local, "path")),
            ("memory:/path", (ObjectStoreScheme::Memory, "path")),
            ("memory:///", (ObjectStoreScheme::Memory, "")),
            ("s3://bucket/path", (ObjectStoreScheme::AmazonS3, "path")),
            ("s3a://bucket/path", (ObjectStoreScheme::AmazonS3, "path")),
            (
                "https://s3.region.amazonaws.com/bucket",
                (ObjectStoreScheme::AmazonS3, ""),
            ),
            (
                "https://s3.region.amazonaws.com/bucket/path",
                (ObjectStoreScheme::AmazonS3, "path"),
            ),
            (
                "https://bucket.s3.region.amazonaws.com",
                (ObjectStoreScheme::AmazonS3, ""),
            ),
            (
                "https://ACCOUNT_ID.r2.cloudflarestorage.com/bucket",
                (ObjectStoreScheme::AmazonS3, ""),
            ),
            (
                "https://ACCOUNT_ID.r2.cloudflarestorage.com/bucket/path",
                (ObjectStoreScheme::AmazonS3, "path"),
            ),
            (
                "abfs://container/path",
                (ObjectStoreScheme::MicrosoftAzure, "path"),
            ),
            (
                "abfs://file_system@account_name.dfs.core.windows.net/path",
                (ObjectStoreScheme::MicrosoftAzure, "path"),
            ),
            (
                "abfss://file_system@account_name.dfs.core.windows.net/path",
                (ObjectStoreScheme::MicrosoftAzure, "path"),
            ),
            (
                "https://account.dfs.core.windows.net",
                (ObjectStoreScheme::MicrosoftAzure, ""),
            ),
            (
                "https://account.dfs.core.windows.net/container/path",
                (ObjectStoreScheme::MicrosoftAzure, "path"),
            ),
            (
                "https://account.blob.core.windows.net",
                (ObjectStoreScheme::MicrosoftAzure, ""),
            ),
            (
                "https://account.blob.core.windows.net/container/path",
                (ObjectStoreScheme::MicrosoftAzure, "path"),
            ),
            (
                "az://account/container",
                (ObjectStoreScheme::MicrosoftAzure, ""),
            ),
            (
                "az://account/container/path",
                (ObjectStoreScheme::MicrosoftAzure, "path"),
            ),
            (
                "gs://bucket/path",
                (ObjectStoreScheme::GoogleCloudStorage, "path"),
            ),
            (
                "gs://test.example.com/path",
                (ObjectStoreScheme::GoogleCloudStorage, "path"),
            ),
            ("http://mydomain/path", (ObjectStoreScheme::Http, "path")),
            ("https://mydomain/path", (ObjectStoreScheme::Http, "path")),
            (
                "s3://bucket/foo%20bar",
                (ObjectStoreScheme::AmazonS3, "foo bar"),
            ),
            (
                "s3://bucket/foo bar",
                (ObjectStoreScheme::AmazonS3, "foo bar"),
            ),
            ("s3://bucket/😀", (ObjectStoreScheme::AmazonS3, "😀")),
            (
                "s3://bucket/%F0%9F%98%80",
                (ObjectStoreScheme::AmazonS3, "😀"),
            ),
            (
                "https://foo/bar%20baz",
                (ObjectStoreScheme::Http, "bar baz"),
            ),
            (
                "file:///bar%252Efoo",
                (ObjectStoreScheme::Local, "bar%2Efoo"),
            ),
            (
                "abfss://file_system@account.dfs.fabric.microsoft.com/",
                (ObjectStoreScheme::MicrosoftAzure, ""),
            ),
            (
                "abfss://file_system@account.dfs.fabric.microsoft.com/",
                (ObjectStoreScheme::MicrosoftAzure, ""),
            ),
            (
                "https://account.dfs.fabric.microsoft.com/",
                (ObjectStoreScheme::MicrosoftAzure, ""),
            ),
            (
                "https://account.dfs.fabric.microsoft.com/container",
                (ObjectStoreScheme::MicrosoftAzure, ""),
            ),
            (
                "https://account.dfs.fabric.microsoft.com/container/path",
                (ObjectStoreScheme::MicrosoftAzure, "path"),
            ),
            (
                "https://account.blob.fabric.microsoft.com/",
                (ObjectStoreScheme::MicrosoftAzure, ""),
            ),
            (
                "https://account.blob.fabric.microsoft.com/container",
                (ObjectStoreScheme::MicrosoftAzure, ""),
            ),
            (
                "https://account.blob.fabric.microsoft.com/container/path",
                (ObjectStoreScheme::MicrosoftAzure, "path"),
            ),
        ];

        for (s, (expected_scheme, expected_path)) in cases {
            let url = Url::parse(s).unwrap();
            let (scheme, path) = ObjectStoreScheme::parse(&url).unwrap();

            assert_eq!(scheme, expected_scheme, "{s}");
            assert_eq!(path, Path::parse(expected_path).unwrap(), "{s}");
        }

        let neg_cases = [
            "unix:/run/foo.socket",
            "file://remote/path",
            "memory://remote/",
        ];
        for s in neg_cases {
            let url = Url::parse(s).unwrap();
            assert!(ObjectStoreScheme::parse(&url).is_err());
        }
    }
}

#[cfg(test)]
mod slack_tests {
    use super::*;

    fn parse(request_timeout_secs: u64) -> Result<(), object_store::Error> {
        let url = Url::parse("memory:///x").unwrap();
        parse_url_opts(&url, Vec::<(String, String)>::new(), None, Some(Duration::from_secs(request_timeout_secs))).map(|_| ())
    }

    /// The store write slack is built from the client's real timeout: the default fits, and a config with a longer request timeout is refused
    /// at startup with a typed error (`snapshot_owner::store_config_admitted`; `mut_store_config_unchecked` admits it).
    #[test]
    fn a_store_config_that_breaks_the_slack_is_refused() {
        assert!(DEFAULT_REQUEST_TIMEOUT.as_secs() <= glidefs_kernels::snapshot_owner::REQUEST_TIMEOUT_SECS);
        parse(300).expect("the default request timeout is admitted");
        match parse(301) {
            Err(object_store::Error::Generic { source, .. }) => {
                assert!(source.downcast_ref::<StoreConfigRefused>().is_some(), "{source}");
            }
            other => panic!("a 301 s request timeout must be refused, got {other:?}"),
        }
    }

    /// The signature's date is fixed when a request is signed, and object_store 0.12.5 signs once, before its retry loop (`aws/client.rs`
    /// `with_aws_sigv4` before `.retryable()`; `client/retry.rs` re-sends the same signed request). The slack's 15-minute term starts at that
    /// signing. A different object_store version turns this red: re-read those two places, then update the version here.
    #[test]
    fn object_store_is_the_version_whose_signing_was_read() {
        let lock = include_str!("../../Cargo.lock");
        let at = lock.find("name = \"object_store\"").expect("object_store in Cargo.lock");
        assert!(lock[at..].lines().nth(1).unwrap().contains("version = \"0.12.5\""), "object_store moved: re-read its signing (see this test's doc)");
    }

    /// A write is signed after the gate: `snapshot_owned` checks `may_write` before each store write (the marker, the manifest and each manifest
    /// retry), and the signing happens inside the store call that follows (`a_removed_closed_marker_has_no_late_cut_to_stop`: `write_at <=
    /// signed_at`). Moving a write above its gate turns this red.
    #[test]
    fn every_owned_write_is_gated_before_it_is_signed() {
        let flush = include_str!("block/write_cache/flush.rs");
        let start = flush.find("pub async fn snapshot_owned").expect("snapshot_owned");
        let body = &flush[start..];
        let body = &body[..body.find("pub(crate) async fn owned_cut_answer").expect("end")];
        for write in ["put_cut_marker(", "put_cut_manifest("] {
            let w = body.find(write).unwrap_or_else(|| panic!("no {write}"));
            let gate = body[..w].rfind("may_write(started)").unwrap_or_else(|| panic!("no gate before {write}"));
            assert!(body[gate..w].lines().count() < 12, "the gate must sit right before {write}");
        }
    }

    /// (b) A PUT the client times out stops sending its body: against a store that stops reading mid-body, the client gives up at its timeout,
    /// closes the connection, and the store sees the end of the stream with less than the declared Content-Length received, so the store has no
    /// whole body to apply (AX-s3-whole-body). The router's own cancel of an owned cut drops the same request future.
    #[tokio::test]
    async fn an_abandoned_put_stops_sending_its_body() {
        use object_store::{ObjectStore, PutPayload, aws::AmazonS3Builder};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut conn, _) = listener.accept().await.unwrap();
            // Read the headers, then stop reading while the client is mid-body.
            let mut head = Vec::new();
            let mut buf = [0u8; 4096];
            let split = loop {
                let n = conn.read(&mut buf).await.unwrap();
                assert!(n > 0, "closed before the headers");
                head.extend_from_slice(&buf[..n]);
                if let Some(i) = head.windows(4).position(|w| w == b"\r\n\r\n") {
                    break i + 4;
                }
            };
            let headers = String::from_utf8_lossy(&head[..split]).to_ascii_lowercase();
            let declared: usize = headers
                .lines()
                .find_map(|l| l.strip_prefix("content-length:"))
                .expect("a PUT declares its length")
                .trim()
                .parse()
                .unwrap();
            let mut received = head.len() - split;
            tokio::time::sleep(Duration::from_secs(3)).await; // the client times out (1 s) while this sleeps
            // Drain what was already sent; the end of the stream arrives when the client closed.
            let end = tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    match conn.read(&mut buf).await {
                        Ok(0) | Err(_) => break true,
                        Ok(n) => received += n,
                    }
                }
            })
            .await
            .unwrap_or(false);
            let _ = conn.shutdown().await;
            (declared, received, end)
        });
        let store = AmazonS3Builder::new()
            .with_endpoint(format!("http://127.0.0.1:{port}"))
            .with_bucket_name("b")
            .with_region("us-east-1")
            .with_access_key_id("k")
            .with_secret_access_key("s")
            .with_retry(object_store::RetryConfig { max_retries: 0, ..Default::default() })
            .with_client_options(ClientOptions::default().with_allow_http(true).with_timeout(Duration::from_secs(1)))
            .build()
            .unwrap();
        let payload = PutPayload::from(vec![7u8; 64 * 1024 * 1024]);
        let put = store.put(&Path::from("x"), payload).await;
        assert!(put.is_err(), "the stalled PUT timed out");
        let (declared, received, end) = tokio::time::timeout(Duration::from_secs(30), server).await.expect("the store saw the connection").unwrap();
        assert!(end, "the client closed the connection");
        assert!(received < declared, "the client stopped sending: {received} of {declared} bytes arrived");
    }
}
