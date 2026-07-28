use std::{
    collections::BTreeMap,
    fs::File,
    io::{Read, Write},
    path::Path,
};

use chrono::Utc;
use hmac::{Hmac, Mac};
use reqwest::{Method, StatusCode, Url};
use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;
use uuid::Uuid;

use crate::resource::{CancellationToken, ResourceError};

const SIGV4_ALGORITHM: &str = "AWS4-HMAC-SHA256";
const SIGV4_SERVICE: &str = "s3";
const SIGV4_TERMINATOR: &str = "aws4_request";
const MULTIPART_UPLOAD_THRESHOLD_BYTES: u64 = 8 * 1024 * 1024;
const MULTIPART_UPLOAD_PART_SIZE_BYTES: usize = 8 * 1024 * 1024;
const XML_CONTENT_TYPE: &str = "application/xml";
const DEFAULT_OBJECT_TRANSFER_LIMIT_BYTES: u64 = 5 * 1024 * 1024 * 1024 * 1024;
const FILE_HASH_BUFFER_BYTES: usize = 1024 * 1024;

type HmacSha256 = Hmac<Sha256>;
const EMPTY_PAYLOAD_SHA256: &str =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// S3-compatible object storage client using path-style URL uploads.
#[derive(Debug, Clone)]
pub struct ObjectStoreClient {
    endpoint: String,
    region: String,
    bucket: String,
    prefix: String,
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
    max_upload_bytes: Option<u64>,
    client: reqwest::Client,
}

#[derive(Debug, Clone)]
pub struct ObjectUploadResult {
    pub object_url: String,
    pub upload_mode: &'static str,
    pub part_count: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct ObjectTransferOptions {
    pub max_bytes: u64,
    pub expected_sha256: Option<String>,
    pub cancellation: CancellationToken,
}

impl ObjectTransferOptions {
    #[must_use]
    pub fn new(max_bytes: u64) -> Self {
        Self {
            max_bytes,
            expected_sha256: None,
            cancellation: CancellationToken::default(),
        }
    }

    #[must_use]
    pub fn with_expected_sha256(mut self, expected_sha256: impl Into<String>) -> Self {
        self.expected_sha256 = Some(expected_sha256.into());
        self
    }

    #[must_use]
    pub fn with_cancellation(mut self, cancellation: CancellationToken) -> Self {
        self.cancellation = cancellation;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectTransferResult {
    pub object_url: String,
    pub byte_size: u64,
    pub sha256: String,
}

#[derive(Debug, Clone)]
pub struct ObjectFileUploadResult {
    pub upload: ObjectUploadResult,
    pub byte_size: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectDeleteOutcome {
    Deleted,
    Missing,
}

#[derive(Debug, Clone)]
pub struct ObjectStoreUploadError {
    pub stage: &'static str,
    pub upload_mode: &'static str,
    pub status_code: Option<u16>,
    pub s3_error_code: Option<String>,
    pub object_byte_size: Option<u64>,
    pub max_upload_bytes: Option<u64>,
    pub part_number: Option<usize>,
    pub part_count: Option<usize>,
    pub message: String,
}

impl ObjectStoreUploadError {
    #[must_use]
    pub fn error_code(&self) -> &'static str {
        if self.is_oversize() {
            "artifact_too_large"
        } else {
            "object_store_upload_failed"
        }
    }

    #[must_use]
    pub fn is_oversize(&self) -> bool {
        self.status_code == Some(StatusCode::PAYLOAD_TOO_LARGE.as_u16())
            || self.s3_error_code.as_deref() == Some("EntityTooLarge")
    }
}

impl std::fmt::Display for ObjectStoreUploadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message.as_str())
    }
}

impl std::error::Error for ObjectStoreUploadError {}

impl ObjectStoreClient {
    /// Creates storage client from config.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        endpoint: &str,
        region: &str,
        bucket: &str,
        prefix: &str,
        access_key_id: &str,
        secret_access_key: &str,
        session_token: Option<String>,
    ) -> anyhow::Result<Self> {
        Self::new_with_upload_limit(
            endpoint,
            region,
            bucket,
            prefix,
            access_key_id,
            secret_access_key,
            session_token,
            None,
        )
    }

    /// Creates storage client from config with an optional local upload-size limit.
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_upload_limit(
        endpoint: &str,
        region: &str,
        bucket: &str,
        prefix: &str,
        access_key_id: &str,
        secret_access_key: &str,
        session_token: Option<String>,
        max_upload_bytes: Option<u64>,
    ) -> anyhow::Result<Self> {
        let endpoint = endpoint.trim_end_matches('/').to_owned();
        let region = region.trim().to_owned();
        let bucket = bucket.trim().to_owned();
        let access_key_id = access_key_id.trim().to_owned();
        let secret_access_key = secret_access_key.trim().to_owned();

        if endpoint.is_empty() {
            return Err(anyhow::anyhow!("S3 endpoint must not be empty"));
        }
        if region.is_empty() {
            return Err(anyhow::anyhow!("S3 region must not be empty"));
        }
        if bucket.is_empty() {
            return Err(anyhow::anyhow!("S3 bucket must not be empty"));
        }
        if access_key_id.is_empty() {
            return Err(anyhow::anyhow!("S3 access key id must not be empty"));
        }
        if secret_access_key.is_empty() {
            return Err(anyhow::anyhow!("S3 secret access key must not be empty"));
        }
        if max_upload_bytes == Some(0) {
            return Err(anyhow::anyhow!(
                "S3 max upload bytes must be greater than 0"
            ));
        }

        Ok(Self {
            endpoint,
            region,
            bucket,
            prefix: prefix.trim_matches('/').to_owned(),
            access_key_id,
            secret_access_key,
            session_token,
            max_upload_bytes,
            client: reqwest::Client::new(),
        })
    }

    /// Uploads bytes to object storage and returns object URL.
    pub async fn upload_result(
        &self,
        snapshot_id: Uuid,
        job_id: Uuid,
        suffix: &str,
        extension: &str,
        content_type: &str,
        bytes: Vec<u8>,
    ) -> anyhow::Result<String> {
        let key = self.object_key(snapshot_id, job_id, suffix, extension);
        self.upload_object(&key, content_type, bytes, None)
            .await
            .map(|result| result.object_url)
    }

    /// Uploads one snapshot artifact object and returns object URL.
    pub async fn upload_snapshot_artifact(
        &self,
        snapshot_id: Uuid,
        extension: &str,
        content_type: &str,
        bytes: Vec<u8>,
    ) -> anyhow::Result<String> {
        let key = if self.prefix.is_empty() {
            format!("snapshots/{snapshot_id}/snapshot/sparse.{extension}")
        } else {
            format!(
                "{}/snapshots/{snapshot_id}/snapshot/sparse.{extension}",
                self.prefix
            )
        };
        self.upload_object(&key, content_type, bytes, None)
            .await
            .map(|result| result.object_url)
    }

    /// Uploads snapshot index sidecar and returns object URL.
    pub async fn upload_snapshot_index(
        &self,
        snapshot_id: Uuid,
        bytes: Vec<u8>,
    ) -> anyhow::Result<String> {
        let key = if self.prefix.is_empty() {
            format!("snapshots/{snapshot_id}/snapshot/snapshot-index-v1.json")
        } else {
            format!(
                "{}/snapshots/{snapshot_id}/snapshot/snapshot-index-v1.json",
                self.prefix
            )
        };
        self.upload_object(&key, "application/json", bytes, None)
            .await
            .map(|result| result.object_url)
    }

    /// Uploads deterministic LCIA method/exchange gap evidence from a bounded local spool file.
    pub async fn upload_snapshot_lcia_uncharacterized_evidence_file(
        &self,
        snapshot_id: Uuid,
        file_path: &Path,
        artifact_byte_size: u64,
    ) -> anyhow::Result<String> {
        let key = if self.prefix.is_empty() {
            format!("snapshots/{snapshot_id}/snapshot/lcia-uncharacterized-v2.jsonl")
        } else {
            format!(
                "{}/snapshots/{snapshot_id}/snapshot/lcia-uncharacterized-v2.jsonl",
                self.prefix
            )
        };
        let upload_mode = if artifact_byte_size < MULTIPART_UPLOAD_THRESHOLD_BYTES {
            "single_put"
        } else {
            "multipart"
        };
        self.ensure_upload_size_allowed(upload_mode, Some(artifact_byte_size))?;
        if artifact_byte_size < MULTIPART_UPLOAD_THRESHOLD_BYTES {
            return self
                .upload_object(
                    &key,
                    "application/x-ndjson",
                    std::fs::read(file_path)?,
                    Some(artifact_byte_size),
                )
                .await
                .map(|result| result.object_url);
        }
        self.upload_object_multipart(&key, "application/x-ndjson", file_path, artifact_byte_size)
            .await
            .map(|result| result.object_url)
    }

    /// Uploads one package artifact object and returns object URL.
    pub async fn upload_package_artifact(
        &self,
        job_id: Uuid,
        suffix: &str,
        extension: &str,
        content_type: &str,
        bytes: Vec<u8>,
    ) -> anyhow::Result<String> {
        let key = self.package_object_key(job_id, suffix, extension);
        self.upload_object(&key, content_type, bytes, None)
            .await
            .map(|result| result.object_url)
    }

    /// Uploads bytes to an explicit bucket-relative object key.
    pub async fn upload_object_key(
        &self,
        key: &str,
        content_type: &str,
        bytes: Vec<u8>,
    ) -> anyhow::Result<ObjectUploadResult> {
        self.upload_object(key, content_type, bytes, None).await
    }

    /// Uploads a local file to an explicit bucket-relative object key with bounded multipart use.
    pub async fn upload_object_key_file(
        &self,
        key: &str,
        content_type: &str,
        file_path: &Path,
        artifact_byte_size: u64,
    ) -> anyhow::Result<ObjectUploadResult> {
        let actual_byte_size = std::fs::metadata(file_path)?.len();
        if actual_byte_size != artifact_byte_size {
            return Err(anyhow::anyhow!(
                "declared artifact byte size does not match file metadata declared={artifact_byte_size} actual={actual_byte_size}"
            ));
        }
        let max_bytes = self
            .max_upload_bytes
            .unwrap_or(DEFAULT_OBJECT_TRANSFER_LIMIT_BYTES);
        self.upload_object_key_file_bounded(
            key,
            content_type,
            file_path,
            ObjectTransferOptions::new(max_bytes),
        )
        .await
        .map(|result| result.upload)
    }

    /// Uploads a local file with byte admission, SHA-256 verification, and cancellation.
    pub async fn upload_object_key_file_bounded(
        &self,
        key: &str,
        content_type: &str,
        file_path: &Path,
        options: ObjectTransferOptions,
    ) -> anyhow::Result<ObjectFileUploadResult> {
        let key = key.trim_start_matches('/');
        if key.is_empty()
            || key
                .split('/')
                .any(|segment| segment.is_empty() || segment == "..")
        {
            return Err(anyhow::anyhow!(
                "object key must be a normalized relative path"
            ));
        }
        validate_transfer_options(&options)?;
        options.cancellation.check("upload_preflight")?;
        let artifact_byte_size = std::fs::metadata(file_path)?.len();
        let max_bytes = self
            .max_upload_bytes
            .map_or(options.max_bytes, |configured| {
                configured.min(options.max_bytes)
            });
        ensure_artifact_limit("objectUploadBytes", artifact_byte_size, max_bytes)?;
        let sha256 = hash_file(file_path, &options.cancellation)?;
        verify_expected_sha256(options.expected_sha256.as_deref(), &sha256)?;

        let upload_mode = if artifact_byte_size < MULTIPART_UPLOAD_THRESHOLD_BYTES {
            "single_put"
        } else {
            "multipart"
        };
        self.ensure_upload_size_allowed(upload_mode, Some(artifact_byte_size))?;
        let upload = if artifact_byte_size < MULTIPART_UPLOAD_THRESHOLD_BYTES {
            options.cancellation.check("upload_single_put")?;
            self.upload_object(
                key,
                content_type,
                std::fs::read(file_path)?,
                Some(artifact_byte_size),
            )
            .await?
        } else {
            self.upload_object_multipart_with_cancellation(
                key,
                content_type,
                file_path,
                artifact_byte_size,
                &options.cancellation,
            )
            .await?
        };
        Ok(ObjectFileUploadResult {
            upload,
            byte_size: artifact_byte_size,
            sha256,
        })
    }

    /// Returns the configured object-store prefix joined to a normalized relative key.
    pub fn prefixed_object_key(&self, relative_key: &str) -> anyhow::Result<String> {
        let relative_key = relative_key.trim_matches('/');
        if relative_key.is_empty()
            || relative_key
                .split('/')
                .any(|segment| segment.is_empty() || segment == "..")
        {
            return Err(anyhow::anyhow!("relative object key must be normalized"));
        }
        if self.prefix.is_empty() {
            Ok(relative_key.to_owned())
        } else {
            Ok(format!("{}/{relative_key}", self.prefix))
        }
    }

    /// Uploads one package artifact from a local file and returns upload metadata.
    pub async fn upload_package_artifact_file(
        &self,
        job_id: Uuid,
        suffix: &str,
        extension: &str,
        content_type: &str,
        file_path: &Path,
        artifact_byte_size: u64,
    ) -> anyhow::Result<ObjectUploadResult> {
        let key = self.package_object_key(job_id, suffix, extension);
        let upload_mode = if artifact_byte_size < MULTIPART_UPLOAD_THRESHOLD_BYTES {
            "single_put"
        } else {
            "multipart"
        };
        self.ensure_upload_size_allowed(upload_mode, Some(artifact_byte_size))?;
        if artifact_byte_size < MULTIPART_UPLOAD_THRESHOLD_BYTES {
            let bytes = std::fs::read(file_path)?;
            return self
                .upload_object(&key, content_type, bytes, Some(artifact_byte_size))
                .await;
        }

        self.upload_object_multipart(&key, content_type, file_path, artifact_byte_size)
            .await
    }

    /// Deletes an object by full object URL.
    pub async fn delete_object_url(&self, object_url: &str) -> anyhow::Result<()> {
        self.delete_object_url_with_outcome(object_url)
            .await
            .map(|_| ())
    }

    /// Deletes an object by bucket-relative object key.
    pub async fn delete_object_key(&self, object_key: &str) -> anyhow::Result<ObjectDeleteOutcome> {
        let key = object_key.trim_start_matches('/');
        if key.trim().is_empty() {
            return Err(anyhow::anyhow!("object key must not be empty"));
        }
        let object_url = format!("{}/{}/{}", self.endpoint, self.bucket, key);
        self.delete_object_url_with_outcome(&object_url).await
    }

    async fn delete_object_url_with_outcome(
        &self,
        object_url: &str,
    ) -> anyhow::Result<ObjectDeleteOutcome> {
        let url = Url::parse(object_url)
            .map_err(|err| anyhow::anyhow!("invalid object URL {object_url}: {err}"))?;
        let host = canonical_host(&url)?;

        let payload_hash = EMPTY_PAYLOAD_SHA256;
        let (amz_date, date_stamp) = sigv4_timestamps();
        let signed = self.sign_request(
            &Method::DELETE,
            SigV4Input {
                canonical_uri: url.path(),
                canonical_query: url.query().unwrap_or_default(),
                host: &host,
                content_type: None,
                payload_hash,
                amz_date: &amz_date,
                date_stamp: &date_stamp,
            },
        )?;

        let mut request = self
            .client
            .delete(url)
            .header("host", host)
            .header("x-amz-content-sha256", payload_hash)
            .header("x-amz-date", amz_date)
            .header("authorization", signed.authorization);
        if let Some(token) = &self.session_token {
            request = request.header("x-amz-security-token", token);
        }

        let response = request.send().await?;
        if let Some(outcome) = object_delete_outcome(response.status()) {
            return Ok(outcome);
        }

        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        let body_preview = body.chars().take(400).collect::<String>();
        Err(anyhow::anyhow!(
            "object delete failed status={status} body={body_preview}"
        ))
    }

    /// Downloads bytes from object URL.
    pub async fn download_object_url(&self, object_url: &str) -> anyhow::Result<Vec<u8>> {
        let temp_dir = tempfile::tempdir()?;
        let target = temp_dir.path().join("object.download");
        self.download_object_url_to_file(
            object_url,
            &target,
            ObjectTransferOptions::new(DEFAULT_OBJECT_TRANSFER_LIMIT_BYTES),
        )
        .await?;
        Ok(std::fs::read(target)?)
    }

    /// Downloads an object to a file through a fixed-memory stream.
    ///
    /// The destination is atomically persisted only after byte-limit and hash checks pass.
    pub async fn download_object_url_to_file(
        &self,
        object_url: &str,
        destination: &Path,
        options: ObjectTransferOptions,
    ) -> anyhow::Result<ObjectTransferResult> {
        validate_transfer_options(&options)?;
        options.cancellation.check("download_preflight")?;
        let mut response = self.download_response(object_url).await?;
        if let Some(content_length) = response.content_length() {
            ensure_artifact_limit("objectDownloadBytes", content_length, options.max_bytes)?;
        }
        let parent = destination.parent().ok_or_else(|| {
            anyhow::anyhow!(
                "download destination must have a parent: {}",
                destination.display()
            )
        })?;
        std::fs::create_dir_all(parent)?;
        let mut partial = NamedTempFile::new_in(parent)?;
        let mut hasher = Sha256::new();
        let mut byte_size = 0_u64;
        while let Some(chunk) = response.chunk().await? {
            options.cancellation.check("download_stream")?;
            byte_size = byte_size
                .checked_add(u64::try_from(chunk.len())?)
                .ok_or_else(|| anyhow::anyhow!("object download byte counter overflow"))?;
            ensure_artifact_limit("objectDownloadBytes", byte_size, options.max_bytes)?;
            partial.write_all(&chunk)?;
            hasher.update(&chunk);
        }
        options.cancellation.check("download_finalize")?;
        let sha256 = hex::encode(hasher.finalize());
        verify_expected_sha256(options.expected_sha256.as_deref(), &sha256)?;
        partial.flush()?;
        partial
            .persist(destination)
            .map_err(|err| anyhow::Error::new(err.error))?;
        Ok(ObjectTransferResult {
            object_url: object_url.to_owned(),
            byte_size,
            sha256,
        })
    }

    async fn download_response(&self, object_url: &str) -> anyhow::Result<reqwest::Response> {
        let url = Url::parse(object_url)
            .map_err(|err| anyhow::anyhow!("invalid object URL {object_url}: {err}"))?;
        let host = canonical_host(&url)?;
        let unsigned_response = self.client.get(url.clone()).send().await?;
        if unsigned_response.status().is_success() {
            return Ok(unsigned_response);
        }

        // Retry with SigV4 for private buckets.
        let payload_hash = EMPTY_PAYLOAD_SHA256;
        let (amz_date, date_stamp) = sigv4_timestamps();
        let signed = self.sign_request(
            &Method::GET,
            SigV4Input {
                canonical_uri: url.path(),
                canonical_query: url.query().unwrap_or_default(),
                host: &host,
                content_type: None,
                payload_hash,
                amz_date: &amz_date,
                date_stamp: &date_stamp,
            },
        )?;

        let mut request = self
            .client
            .get(url)
            .header("host", host)
            .header("x-amz-content-sha256", payload_hash)
            .header("x-amz-date", amz_date)
            .header("authorization", signed.authorization);
        if let Some(token) = &self.session_token {
            request = request.header("x-amz-security-token", token);
        }

        let response = request.send().await?;
        if response.status().is_success() {
            return Ok(response);
        }

        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        let body_preview = body.chars().take(400).collect::<String>();
        Err(anyhow::anyhow!(
            "object download failed status={status} body={body_preview}"
        ))
    }

    /// Downloads bytes from an explicit bucket-relative object key.
    pub async fn download_object_key(&self, object_key: &str) -> anyhow::Result<Vec<u8>> {
        let key = object_key.trim_start_matches('/');
        if key.trim().is_empty() {
            return Err(anyhow::anyhow!("object key must not be empty"));
        }
        let object_url = format!("{}/{}/{}", self.endpoint, self.bucket, key);
        self.download_object_url(&object_url).await
    }

    fn package_object_key(&self, job_id: Uuid, suffix: &str, extension: &str) -> String {
        if self.prefix.is_empty() {
            format!("packages/jobs/{job_id}/{suffix}.{extension}")
        } else {
            format!(
                "{}/packages/jobs/{job_id}/{suffix}.{extension}",
                self.prefix
            )
        }
    }

    fn ensure_upload_size_allowed(
        &self,
        upload_mode: &'static str,
        object_byte_size: Option<u64>,
    ) -> anyhow::Result<()> {
        let Some(max_upload_bytes) = self.max_upload_bytes else {
            return Ok(());
        };
        let Some(object_byte_size) = object_byte_size else {
            return Ok(());
        };
        if object_byte_size <= max_upload_bytes {
            return Ok(());
        }

        Err(object_upload_size_limit_error(
            "preflight_upload_size",
            upload_mode,
            object_byte_size,
            max_upload_bytes,
        ))
    }

    async fn upload_object(
        &self,
        key: &str,
        content_type: &str,
        bytes: Vec<u8>,
        object_byte_size: Option<u64>,
    ) -> anyhow::Result<ObjectUploadResult> {
        let object_url = format!("{}/{}/{}", self.endpoint, self.bucket, key);
        let url = Url::parse(&object_url)
            .map_err(|err| anyhow::anyhow!("invalid S3 URL {object_url}: {err}"))?;
        let host = canonical_host(&url)?;

        let payload_hash = sha256_hex(bytes.as_slice());
        let payload_size = object_byte_size.or_else(|| u64::try_from(bytes.len()).ok());
        self.ensure_upload_size_allowed("single_put", payload_size)?;
        let (amz_date, date_stamp) = sigv4_timestamps();
        let signed = self.sign_request(
            &Method::PUT,
            SigV4Input {
                canonical_uri: url.path(),
                canonical_query: url.query().unwrap_or_default(),
                host: &host,
                content_type: Some(content_type),
                payload_hash: &payload_hash,
                amz_date: &amz_date,
                date_stamp: &date_stamp,
            },
        )?;

        let mut request = self
            .client
            .put(url)
            .header("host", host)
            .header("content-type", content_type)
            .header("x-amz-content-sha256", payload_hash)
            .header("x-amz-date", amz_date)
            .header("authorization", signed.authorization)
            .body(bytes);

        if let Some(token) = &self.session_token {
            request = request.header("x-amz-security-token", token);
        }

        let response = request.send().await?;
        if response.status().is_success() {
            return Ok(ObjectUploadResult {
                object_url,
                upload_mode: "single_put",
                part_count: None,
            });
        }

        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        Err(object_upload_error(
            "upload_object",
            "single_put",
            status,
            &body,
            payload_size,
            None,
            None,
        ))
    }

    async fn upload_object_multipart(
        &self,
        key: &str,
        content_type: &str,
        file_path: &Path,
        object_byte_size: u64,
    ) -> anyhow::Result<ObjectUploadResult> {
        self.upload_object_multipart_with_cancellation(
            key,
            content_type,
            file_path,
            object_byte_size,
            &CancellationToken::default(),
        )
        .await
    }

    async fn upload_object_multipart_with_cancellation(
        &self,
        key: &str,
        content_type: &str,
        file_path: &Path,
        object_byte_size: u64,
        cancellation: &CancellationToken,
    ) -> anyhow::Result<ObjectUploadResult> {
        self.ensure_upload_size_allowed("multipart", Some(object_byte_size))?;
        cancellation.check("upload_create_multipart")?;
        let upload_id = self
            .create_multipart_upload(key, content_type, object_byte_size)
            .await?;
        let mut file = File::open(file_path)?;
        let mut parts = Vec::new();
        let mut part_number = 1usize;
        let mut buffer = vec![0_u8; MULTIPART_UPLOAD_PART_SIZE_BYTES];

        loop {
            if let Err(err) = cancellation.check("upload_read_part") {
                let _ = self.abort_multipart_upload(key, upload_id.as_str()).await;
                return Err(err.into());
            }
            let read_len = file.read(buffer.as_mut_slice())?;
            if read_len == 0 {
                break;
            }

            let etag = match self
                .upload_multipart_part(
                    key,
                    upload_id.as_str(),
                    part_number,
                    buffer[..read_len].to_vec(),
                    object_byte_size,
                )
                .await
            {
                Ok(etag) => etag,
                Err(err) => {
                    let _ = self.abort_multipart_upload(key, upload_id.as_str()).await;
                    return Err(err);
                }
            };
            parts.push((part_number, etag));
            part_number += 1;
        }

        if parts.is_empty() {
            let _ = self.abort_multipart_upload(key, upload_id.as_str()).await;
            return Err(anyhow::anyhow!(
                "multipart upload cannot start with an empty file"
            ));
        }

        if let Err(err) = cancellation.check("upload_complete_multipart") {
            let _ = self.abort_multipart_upload(key, upload_id.as_str()).await;
            return Err(err.into());
        }
        if let Err(err) = self
            .complete_multipart_upload(key, upload_id.as_str(), parts.as_slice(), object_byte_size)
            .await
        {
            let _ = self.abort_multipart_upload(key, upload_id.as_str()).await;
            return Err(err);
        }

        Ok(ObjectUploadResult {
            object_url: format!("{}/{}/{}", self.endpoint, self.bucket, key),
            upload_mode: "multipart",
            part_count: Some(parts.len()),
        })
    }

    async fn create_multipart_upload(
        &self,
        key: &str,
        content_type: &str,
        object_byte_size: u64,
    ) -> anyhow::Result<String> {
        let object_url = format!("{}/{}/{}", self.endpoint, self.bucket, key);
        let mut url = Url::parse(&object_url)
            .map_err(|err| anyhow::anyhow!("invalid S3 URL {object_url}: {err}"))?;
        url.query_pairs_mut().append_pair("uploads", "");
        let host = canonical_host(&url)?;
        let payload_hash = EMPTY_PAYLOAD_SHA256;
        let (amz_date, date_stamp) = sigv4_timestamps();
        let signed = self.sign_request(
            &Method::POST,
            SigV4Input {
                canonical_uri: url.path(),
                canonical_query: url.query().unwrap_or_default(),
                host: &host,
                content_type: Some(content_type),
                payload_hash,
                amz_date: &amz_date,
                date_stamp: &date_stamp,
            },
        )?;

        let mut request = self
            .client
            .post(url)
            .header("host", host)
            .header("content-type", content_type)
            .header("x-amz-content-sha256", payload_hash)
            .header("x-amz-date", amz_date)
            .header("authorization", signed.authorization);
        if let Some(token) = &self.session_token {
            request = request.header("x-amz-security-token", token);
        }

        let response = request.send().await?;
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(object_upload_error(
                "create_multipart_upload",
                "multipart",
                status,
                &body,
                Some(object_byte_size),
                None,
                None,
            ));
        }

        extract_xml_tag(body.as_str(), "UploadId").ok_or_else(|| {
            anyhow::anyhow!("multipart upload succeeded but response did not include an UploadId")
        })
    }

    async fn upload_multipart_part(
        &self,
        key: &str,
        upload_id: &str,
        part_number: usize,
        bytes: Vec<u8>,
        object_byte_size: u64,
    ) -> anyhow::Result<String> {
        let object_url = format!("{}/{}/{}", self.endpoint, self.bucket, key);
        let mut url = Url::parse(&object_url)
            .map_err(|err| anyhow::anyhow!("invalid S3 URL {object_url}: {err}"))?;
        url.query_pairs_mut()
            .append_pair("partNumber", &part_number.to_string())
            .append_pair("uploadId", upload_id);
        let host = canonical_host(&url)?;
        let payload_hash = sha256_hex(bytes.as_slice());
        let (amz_date, date_stamp) = sigv4_timestamps();
        let signed = self.sign_request(
            &Method::PUT,
            SigV4Input {
                canonical_uri: url.path(),
                canonical_query: url.query().unwrap_or_default(),
                host: &host,
                content_type: None,
                payload_hash: &payload_hash,
                amz_date: &amz_date,
                date_stamp: &date_stamp,
            },
        )?;

        let mut request = self
            .client
            .put(url)
            .header("host", host)
            .header("x-amz-content-sha256", payload_hash)
            .header("x-amz-date", amz_date)
            .header("authorization", signed.authorization)
            .body(bytes);
        if let Some(token) = &self.session_token {
            request = request.header("x-amz-security-token", token);
        }

        let response = request.send().await?;
        if response.status().is_success() {
            return response
                .headers()
                .get("etag")
                .and_then(|etag| etag.to_str().ok())
                .map(str::to_owned)
                .ok_or_else(|| anyhow::anyhow!("multipart upload part response missing ETag"));
        }

        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        Err(object_upload_error(
            "upload_multipart_part",
            "multipart",
            status,
            &body,
            Some(object_byte_size),
            Some(part_number),
            None,
        ))
    }

    async fn complete_multipart_upload(
        &self,
        key: &str,
        upload_id: &str,
        parts: &[(usize, String)],
        object_byte_size: u64,
    ) -> anyhow::Result<()> {
        let object_url = format!("{}/{}/{}", self.endpoint, self.bucket, key);
        let mut url = Url::parse(&object_url)
            .map_err(|err| anyhow::anyhow!("invalid S3 URL {object_url}: {err}"))?;
        url.query_pairs_mut().append_pair("uploadId", upload_id);
        let host = canonical_host(&url)?;
        let mut part_entries = String::new();
        for (part_number, etag) in parts {
            part_entries.push_str(
                format!("<Part><PartNumber>{part_number}</PartNumber><ETag>{etag}</ETag></Part>")
                    .as_str(),
            );
        }
        let body = format!("<CompleteMultipartUpload>{part_entries}</CompleteMultipartUpload>");
        let payload_hash = sha256_hex(body.as_bytes());
        let (amz_date, date_stamp) = sigv4_timestamps();
        let signed = self.sign_request(
            &Method::POST,
            SigV4Input {
                canonical_uri: url.path(),
                canonical_query: url.query().unwrap_or_default(),
                host: &host,
                content_type: Some(XML_CONTENT_TYPE),
                payload_hash: &payload_hash,
                amz_date: &amz_date,
                date_stamp: &date_stamp,
            },
        )?;

        let mut request = self
            .client
            .post(url)
            .header("host", host)
            .header("content-type", XML_CONTENT_TYPE)
            .header("x-amz-content-sha256", payload_hash)
            .header("x-amz-date", amz_date)
            .header("authorization", signed.authorization)
            .body(body);
        if let Some(token) = &self.session_token {
            request = request.header("x-amz-security-token", token);
        }

        let response = request.send().await?;
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        if status.is_success() && !body.contains("<Error>") {
            return Ok(());
        }

        Err(object_upload_error(
            "complete_multipart_upload",
            "multipart",
            status,
            &body,
            Some(object_byte_size),
            None,
            Some(parts.len()),
        ))
    }

    async fn abort_multipart_upload(&self, key: &str, upload_id: &str) -> anyhow::Result<()> {
        let object_url = format!("{}/{}/{}", self.endpoint, self.bucket, key);
        let mut url = Url::parse(&object_url)
            .map_err(|err| anyhow::anyhow!("invalid S3 URL {object_url}: {err}"))?;
        url.query_pairs_mut().append_pair("uploadId", upload_id);
        let host = canonical_host(&url)?;
        let payload_hash = EMPTY_PAYLOAD_SHA256;
        let (amz_date, date_stamp) = sigv4_timestamps();
        let signed = self.sign_request(
            &Method::DELETE,
            SigV4Input {
                canonical_uri: url.path(),
                canonical_query: url.query().unwrap_or_default(),
                host: &host,
                content_type: None,
                payload_hash,
                amz_date: &amz_date,
                date_stamp: &date_stamp,
            },
        )?;

        let mut request = self
            .client
            .delete(url)
            .header("host", host)
            .header("x-amz-content-sha256", payload_hash)
            .header("x-amz-date", amz_date)
            .header("authorization", signed.authorization);
        if let Some(token) = &self.session_token {
            request = request.header("x-amz-security-token", token);
        }

        let response = request.send().await?;
        if response.status().is_success() || response.status() == StatusCode::NOT_FOUND {
            return Ok(());
        }

        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        Err(object_upload_error(
            "abort_multipart_upload",
            "multipart",
            status,
            &body,
            None,
            None,
            None,
        ))
    }

    fn object_key(&self, snapshot_id: Uuid, job_id: Uuid, suffix: &str, extension: &str) -> String {
        if self.prefix.is_empty() {
            return format!("snapshots/{snapshot_id}/jobs/{job_id}/{suffix}.{extension}");
        }

        format!(
            "{}/snapshots/{snapshot_id}/jobs/{job_id}/{suffix}.{extension}",
            self.prefix
        )
    }

    fn sign_request(
        &self,
        method: &Method,
        input: SigV4Input<'_>,
    ) -> anyhow::Result<SignedRequest> {
        let mut headers = BTreeMap::<&str, String>::new();
        headers.insert("host", input.host.to_owned());
        headers.insert("x-amz-content-sha256", input.payload_hash.to_owned());
        headers.insert("x-amz-date", input.amz_date.to_owned());
        if let Some(content_type) = input.content_type {
            headers.insert("content-type", content_type.trim().to_owned());
        }
        if let Some(token) = &self.session_token {
            headers.insert("x-amz-security-token", token.trim().to_owned());
        }

        let canonical_headers = headers
            .iter()
            .map(|(name, value)| format!("{name}:{}", value.trim()))
            .collect::<Vec<_>>()
            .join("\n");
        let signed_headers = headers.keys().copied().collect::<Vec<_>>().join(";");

        let canonical_request = format!(
            "{}\n{}\n{}\n{canonical_headers}\n\n{signed_headers}\n{}",
            method.as_str(),
            input.canonical_uri,
            input.canonical_query,
            input.payload_hash
        );
        let canonical_request_hash = sha256_hex(canonical_request.as_bytes());
        let credential_scope = format!(
            "{}/{}/{SIGV4_SERVICE}/{SIGV4_TERMINATOR}",
            input.date_stamp, self.region
        );
        let string_to_sign = format!(
            "{SIGV4_ALGORITHM}\n{}\n{credential_scope}\n{canonical_request_hash}",
            input.amz_date
        );

        let signing_key = self.signing_key(input.date_stamp)?;
        let signature = hmac_sha256_hex(signing_key.as_slice(), &string_to_sign)?;
        let authorization = format!(
            "{SIGV4_ALGORITHM} Credential={}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}",
            self.access_key_id
        );

        Ok(SignedRequest { authorization })
    }

    fn signing_key(&self, date_stamp: &str) -> anyhow::Result<Vec<u8>> {
        let date_key = hmac_sha256_bytes(
            format!("AWS4{}", self.secret_access_key).as_bytes(),
            date_stamp,
        )?;
        let region_key = hmac_sha256_bytes(date_key.as_slice(), &self.region)?;
        let service_key = hmac_sha256_bytes(region_key.as_slice(), SIGV4_SERVICE)?;
        hmac_sha256_bytes(service_key.as_slice(), SIGV4_TERMINATOR)
    }
}

#[derive(Debug)]
struct SignedRequest {
    authorization: String,
}

#[derive(Debug, Clone, Copy)]
struct SigV4Input<'a> {
    canonical_uri: &'a str,
    canonical_query: &'a str,
    host: &'a str,
    content_type: Option<&'a str>,
    payload_hash: &'a str,
    amz_date: &'a str,
    date_stamp: &'a str,
}

fn sigv4_timestamps() -> (String, String) {
    let now = Utc::now();
    (
        now.format("%Y%m%dT%H%M%SZ").to_string(),
        now.format("%Y%m%d").to_string(),
    )
}

fn object_delete_outcome(status: StatusCode) -> Option<ObjectDeleteOutcome> {
    if status.is_success() {
        Some(ObjectDeleteOutcome::Deleted)
    } else if status == StatusCode::NOT_FOUND {
        Some(ObjectDeleteOutcome::Missing)
    } else {
        None
    }
}

fn canonical_host(url: &Url) -> anyhow::Result<String> {
    let host = url
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("S3 endpoint URL is missing host"))?;
    match url.port() {
        Some(port) => Ok(format!("{host}:{port}")),
        None => Ok(host.to_owned()),
    }
}

fn extract_xml_tag(body: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = body.find(open.as_str())?;
    let content_start = start + open.len();
    let end = body[content_start..].find(close.as_str())?;
    Some(body[content_start..content_start + end].trim().to_owned())
}

fn validate_transfer_options(options: &ObjectTransferOptions) -> anyhow::Result<()> {
    if options.max_bytes == 0 {
        return Err(ResourceError::InvalidProfile(
            "object transfer max_bytes must be greater than zero".to_owned(),
        )
        .into());
    }
    if let Some(expected) = options.expected_sha256.as_deref() {
        let normalized = expected.trim().to_ascii_lowercase();
        if normalized.len() != 64 || !normalized.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(anyhow::anyhow!(
                "expected SHA-256 must be 64 hexadecimal characters"
            ));
        }
    }
    Ok(())
}

fn ensure_artifact_limit(resource: &'static str, observed: u64, limit: u64) -> anyhow::Result<()> {
    if observed > limit {
        return Err(ResourceError::ArtifactLimitExceeded {
            resource,
            observed,
            limit,
        }
        .into());
    }
    Ok(())
}

fn hash_file(path: &Path, cancellation: &CancellationToken) -> anyhow::Result<String> {
    let mut file = File::open(path)?;
    let mut buffer = vec![0_u8; FILE_HASH_BUFFER_BYTES];
    let mut hasher = Sha256::new();
    loop {
        cancellation.check("upload_hash")?;
        let read_len = file.read(&mut buffer)?;
        if read_len == 0 {
            break;
        }
        hasher.update(&buffer[..read_len]);
    }
    Ok(hex::encode(hasher.finalize()))
}

fn verify_expected_sha256(expected: Option<&str>, observed: &str) -> anyhow::Result<()> {
    if let Some(expected) = expected
        && !expected.trim().eq_ignore_ascii_case(observed)
    {
        return Err(anyhow::anyhow!(
            "artifact SHA-256 mismatch expected={} observed={observed}",
            expected.trim()
        ));
    }
    Ok(())
}

fn object_upload_error(
    stage: &'static str,
    upload_mode: &'static str,
    status: StatusCode,
    body: &str,
    object_byte_size: Option<u64>,
    part_number: Option<usize>,
    part_count: Option<usize>,
) -> anyhow::Error {
    let body_preview = body.chars().take(400).collect::<String>();
    let s3_error_code = extract_xml_tag(body, "Code");
    let auth_hint = if status == StatusCode::FORBIDDEN || status == StatusCode::UNAUTHORIZED {
        " (check S3 key/secret/region, bucket policy, and endpoint)"
    } else {
        ""
    };
    let size_hint = object_byte_size
        .map(|size| format!(" object_byte_size={size}"))
        .unwrap_or_default();
    let part_hint = match (part_number, part_count) {
        (Some(number), Some(count)) => format!(" part_number={number} part_count={count}"),
        (Some(number), None) => format!(" part_number={number}"),
        (None, Some(count)) => format!(" part_count={count}"),
        (None, None) => String::new(),
    };
    let code_hint = s3_error_code
        .as_deref()
        .map(|code| format!(" s3_code={code}"))
        .unwrap_or_default();

    anyhow::Error::new(ObjectStoreUploadError {
        stage,
        upload_mode,
        status_code: Some(status.as_u16()),
        s3_error_code,
        object_byte_size,
        max_upload_bytes: None,
        part_number,
        part_count,
        message: format!(
            "object upload failed stage={stage} upload_mode={upload_mode} status={status}{code_hint}{size_hint}{part_hint}{auth_hint} body={body_preview}"
        ),
    })
}

fn object_upload_size_limit_error(
    stage: &'static str,
    upload_mode: &'static str,
    object_byte_size: u64,
    max_upload_bytes: u64,
) -> anyhow::Error {
    anyhow::Error::new(ObjectStoreUploadError {
        stage,
        upload_mode,
        status_code: None,
        s3_error_code: Some("EntityTooLarge".to_owned()),
        object_byte_size: Some(object_byte_size),
        max_upload_bytes: Some(max_upload_bytes),
        part_number: None,
        part_count: None,
        message: format!(
            "object upload rejected before network upload stage={stage} upload_mode={upload_mode} object_byte_size={object_byte_size} max_upload_bytes={max_upload_bytes}; raise the storage max-file-limit or set S3_MAX_UPLOAD_BYTES above the expected artifact size"
        ),
    })
}

fn sha256_hex(input: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input);
    hex::encode(hasher.finalize())
}

fn hmac_sha256_bytes(key: &[u8], data: &str) -> anyhow::Result<Vec<u8>> {
    let mut mac = HmacSha256::new_from_slice(key)
        .map_err(|_| anyhow::anyhow!("failed to initialize HMAC-SHA256"))?;
    mac.update(data.as_bytes());
    Ok(mac.finalize().into_bytes().to_vec())
}

fn hmac_sha256_hex(key: &[u8], data: &str) -> anyhow::Result<String> {
    let bytes = hmac_sha256_bytes(key, data)?;
    Ok(hex::encode(bytes))
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Write},
        net::TcpListener,
        thread,
        time::Duration,
    };

    use reqwest::StatusCode;
    use tempfile::{NamedTempFile, tempdir};
    use uuid::Uuid;

    use super::{
        MULTIPART_UPLOAD_THRESHOLD_BYTES, ObjectDeleteOutcome, ObjectStoreClient,
        ObjectStoreUploadError, ObjectTransferOptions, extract_xml_tag, object_delete_outcome,
        object_upload_error,
    };
    use crate::resource::{CancellationToken, ResourceError};

    #[test]
    fn delete_treats_success_statuses_as_deleted() {
        assert_eq!(
            object_delete_outcome(StatusCode::NO_CONTENT),
            Some(ObjectDeleteOutcome::Deleted)
        );
        assert_eq!(
            object_delete_outcome(StatusCode::OK),
            Some(ObjectDeleteOutcome::Deleted)
        );
    }

    #[test]
    fn delete_treats_404_as_successful_missing_object() {
        assert_eq!(
            object_delete_outcome(StatusCode::NOT_FOUND),
            Some(ObjectDeleteOutcome::Missing)
        );
    }

    #[test]
    fn delete_rejects_non_success_statuses() {
        assert_eq!(object_delete_outcome(StatusCode::FORBIDDEN), None);
    }

    #[test]
    fn extract_xml_tag_reads_s3_error_code() {
        let body = r"<Error><Code>EntityTooLarge</Code><Message>too large</Message></Error>";
        assert_eq!(
            extract_xml_tag(body, "Code").as_deref(),
            Some("EntityTooLarge")
        );
    }

    #[test]
    fn upload_error_classifies_oversize_failures() {
        let err = object_upload_error(
            "upload_object",
            "single_put",
            StatusCode::PAYLOAD_TOO_LARGE,
            r"<Error><Code>EntityTooLarge</Code></Error>",
            Some(42),
            None,
            None,
        );
        let upload_err = err
            .downcast_ref::<ObjectStoreUploadError>()
            .expect("downcast upload error");
        assert!(upload_err.is_oversize());
        assert_eq!(upload_err.error_code(), "artifact_too_large");
    }

    #[tokio::test]
    async fn package_file_upload_rejects_configured_oversize_before_network_upload() {
        let mut file = NamedTempFile::new().expect("temp file");
        file.write_all(b"header").expect("write file header");
        let artifact_byte_size = MULTIPART_UPLOAD_THRESHOLD_BYTES + 1;
        file.as_file()
            .set_len(artifact_byte_size)
            .expect("set sparse file length");

        let client = ObjectStoreClient::new_with_upload_limit(
            "https://storage.example.test",
            "auto",
            "lca-results",
            "lca-results",
            "access-key",
            "secret-key",
            None,
            Some(MULTIPART_UPLOAD_THRESHOLD_BYTES),
        )
        .expect("client");

        let err = client
            .upload_package_artifact_file(
                Uuid::nil(),
                "export-package",
                "zip",
                "application/zip",
                file.path(),
                artifact_byte_size,
            )
            .await
            .expect_err("configured upload limit should reject oversized package artifact");
        let upload_err = err
            .downcast_ref::<ObjectStoreUploadError>()
            .expect("downcast upload error");

        assert_eq!(upload_err.stage, "preflight_upload_size");
        assert_eq!(upload_err.upload_mode, "multipart");
        assert_eq!(upload_err.object_byte_size, Some(artifact_byte_size));
        assert_eq!(
            upload_err.max_upload_bytes,
            Some(MULTIPART_UPLOAD_THRESHOLD_BYTES)
        );
        assert!(upload_err.is_oversize());
        assert_eq!(upload_err.error_code(), "artifact_too_large");
    }

    fn test_client(endpoint: &str) -> ObjectStoreClient {
        ObjectStoreClient::new(endpoint, "test", "bucket", "", "key", "secret", None)
            .expect("test client")
    }

    fn serve_without_content_length(chunks: Vec<Vec<u8>>, delay: Duration) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind local server");
        let address = listener.local_addr().expect("local address");
        thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept request");
            let mut request = [0_u8; 2048];
            let _ = stream.read(&mut request);
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n")
                .expect("write response header");
            for chunk in chunks {
                stream.write_all(&chunk).expect("write response chunk");
                stream.flush().expect("flush response chunk");
                thread::sleep(delay);
            }
        });
        format!("http://{address}/bucket/object")
    }

    fn serve_with_content_length(content_length: u64) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind local server");
        let address = listener.local_addr().expect("local address");
        thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept request");
            let mut request = [0_u8; 2048];
            let _ = stream.read(&mut request);
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Length: {content_length}\r\nConnection: close\r\n\r\n"
            )
            .expect("write response header");
            stream.flush().expect("flush response header");
        });
        format!("http://{address}/bucket/object")
    }

    #[tokio::test]
    async fn download_content_length_rejects_cap_before_body_transfer() {
        let url = serve_with_content_length(11);
        let temp = tempdir().expect("temp directory");
        let destination = temp.path().join("download.bin");
        let endpoint = url.split("/bucket/").next().expect("endpoint");
        let err = test_client(endpoint)
            .download_object_url_to_file(&url, &destination, ObjectTransferOptions::new(10))
            .await
            .expect_err("content length must reject cap before body transfer");
        assert_eq!(
            err.downcast_ref::<ResourceError>()
                .expect("resource limit error")
                .error_code(),
            "artifact_limit_exceeded"
        );
        assert!(!destination.exists());
        assert_eq!(
            std::fs::read_dir(temp.path())
                .expect("read temp directory")
                .count(),
            0
        );
    }

    #[tokio::test]
    async fn download_without_content_length_enforces_cap_and_cleans_partial_file() {
        let url = serve_without_content_length(
            vec![vec![b'a'; 6], vec![b'b'; 6]],
            Duration::from_millis(5),
        );
        let temp = tempdir().expect("temp directory");
        let destination = temp.path().join("download.bin");
        let endpoint = url.split("/bucket/").next().expect("endpoint");
        let err = test_client(endpoint)
            .download_object_url_to_file(&url, &destination, ObjectTransferOptions::new(10))
            .await
            .expect_err("stream must exceed cap");
        let resource = err
            .downcast_ref::<ResourceError>()
            .expect("resource limit error");
        assert_eq!(resource.error_code(), "artifact_limit_exceeded");
        assert!(!destination.exists());
        assert_eq!(
            std::fs::read_dir(temp.path())
                .expect("read temp directory")
                .count(),
            0
        );
    }

    #[tokio::test]
    async fn cancelled_download_never_persists_destination() {
        let url = serve_without_content_length(
            vec![vec![b'a'; 1024], vec![b'b'; 1024]],
            Duration::from_millis(100),
        );
        let temp = tempdir().expect("temp directory");
        let destination = temp.path().join("download.bin");
        let cancellation = CancellationToken::default();
        let canceller = cancellation.clone();
        let cancel_task = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            canceller.cancel();
        });
        let endpoint = url.split("/bucket/").next().expect("endpoint");
        let err = test_client(endpoint)
            .download_object_url_to_file(
                &url,
                &destination,
                ObjectTransferOptions::new(4096).with_cancellation(cancellation),
            )
            .await
            .expect_err("cancelled download");
        cancel_task.await.expect("cancel task");
        assert_eq!(
            err.downcast_ref::<ResourceError>()
                .expect("cancellation error")
                .error_code(),
            "operation_cancelled"
        );
        assert!(!destination.exists());
        assert_eq!(
            std::fs::read_dir(temp.path())
                .expect("read temp directory")
                .count(),
            0
        );
    }

    #[tokio::test]
    async fn download_hash_mismatch_cleans_partial_file() {
        let url = serve_without_content_length(vec![b"payload".to_vec()], Duration::from_millis(0));
        let temp = tempdir().expect("temp directory");
        let destination = temp.path().join("download.bin");
        let endpoint = url.split("/bucket/").next().expect("endpoint");
        let err = test_client(endpoint)
            .download_object_url_to_file(
                &url,
                &destination,
                ObjectTransferOptions::new(100).with_expected_sha256("0".repeat(64)),
            )
            .await
            .expect_err("hash mismatch");
        assert!(err.to_string().contains("SHA-256 mismatch"));
        assert!(!destination.exists());
        assert_eq!(
            std::fs::read_dir(temp.path())
                .expect("read temp directory")
                .count(),
            0
        );
    }

    #[tokio::test]
    async fn upload_file_rejects_byte_cap_before_network_transfer() {
        let temp = tempdir().expect("temp directory");
        let source = temp.path().join("upload.bin");
        std::fs::write(&source, [7_u8; 11]).expect("write upload fixture");
        let err = test_client("http://127.0.0.1:9")
            .upload_object_key_file_bounded(
                "bounded/upload.bin",
                "application/octet-stream",
                &source,
                ObjectTransferOptions::new(10),
            )
            .await
            .expect_err("upload preflight must reject cap");
        assert_eq!(
            err.downcast_ref::<ResourceError>()
                .expect("resource limit error")
                .error_code(),
            "artifact_limit_exceeded"
        );
    }

    #[tokio::test]
    async fn cancelled_upload_file_stops_before_network_transfer() {
        let temp = tempdir().expect("temp directory");
        let source = temp.path().join("upload.bin");
        std::fs::write(&source, [7_u8; 11]).expect("write upload fixture");
        let cancellation = CancellationToken::default();
        cancellation.cancel();
        let err = test_client("http://127.0.0.1:9")
            .upload_object_key_file_bounded(
                "bounded/upload.bin",
                "application/octet-stream",
                &source,
                ObjectTransferOptions::new(100).with_cancellation(cancellation),
            )
            .await
            .expect_err("cancelled upload");
        assert_eq!(
            err.downcast_ref::<ResourceError>()
                .expect("cancellation error")
                .error_code(),
            "operation_cancelled"
        );
    }
}
