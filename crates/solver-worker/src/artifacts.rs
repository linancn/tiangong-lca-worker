use std::path::Path;

use hdf5::File;
use serde::Serialize;
use sha2::{Digest, Sha256};
use solver_core::{SolveBatchResult, SolveResult};
use tempfile::Builder;
use uuid::Uuid;

use crate::calculation_bundle::{CalculationBundleArtifactRef, CalculationBundleManifest};
use crate::contribution_path::ContributionPathArtifact;

const SCHEMA_VERSION: u8 = 1;
const DATASET_SCHEMA_VERSION: &str = "schema_version";
const DATASET_FORMAT: &str = "format";
const DATASET_ENVELOPE_JSON: &str = "envelope_json";
const HDF5_DEFLATE_LEVEL: u8 = 4;
const HDF5_CHUNK_TARGET_BYTES: usize = 256 * 1024;

/// `HDF5` result artifact format persisted in object storage.
pub const ARTIFACT_FORMAT: &str = "hdf5:v1";
/// File extension for `HDF5` artifacts.
pub const ARTIFACT_EXTENSION: &str = "h5";
/// MIME type used for uploads.
pub const ARTIFACT_CONTENT_TYPE: &str = "application/x-hdf5";
/// Query-friendly sidecar format indexing canonical Calculation Bundle partitions.
pub const ALL_UNIT_QUERY_ARTIFACT_FORMAT: &str = "all-unit-query:v2";
/// File extension for query sidecar artifacts.
pub const ALL_UNIT_QUERY_ARTIFACT_EXTENSION: &str = "json";
/// MIME type for query sidecar artifacts.
pub const ALL_UNIT_QUERY_ARTIFACT_CONTENT_TYPE: &str = "application/json";
/// Query-friendly JSON artifact for contribution path analysis.
pub const CONTRIBUTION_PATH_ARTIFACT_FORMAT: &str = "contribution-path:v1";
/// File extension for contribution path artifacts.
pub const CONTRIBUTION_PATH_ARTIFACT_EXTENSION: &str = "json";
/// MIME type for contribution path artifacts.
pub const CONTRIBUTION_PATH_ARTIFACT_CONTENT_TYPE: &str = "application/json";

#[derive(Debug, Clone, Serialize)]
struct ArtifactEnvelope<T>
where
    T: Serialize,
{
    version: u8,
    format: &'static str,
    snapshot_id: Uuid,
    job_id: Uuid,
    payload: T,
}

/// Encoded artifact bytes and checksums.
#[derive(Debug, Clone)]
pub struct EncodedArtifact {
    /// Artifact bytes in `HDF5` container format.
    pub bytes: Vec<u8>,
    /// `SHA-256` checksum in hex.
    pub sha256: String,
    /// Format identifier.
    pub format: &'static str,
    /// Content type for upload.
    pub content_type: &'static str,
    /// Recommended file extension.
    pub extension: &'static str,
}

/// Encodes one solve result as `HDF5`.
pub fn encode_solve_one_artifact(
    snapshot_id: Uuid,
    job_id: Uuid,
    result: &SolveResult,
) -> anyhow::Result<EncodedArtifact> {
    let envelope = ArtifactEnvelope {
        version: SCHEMA_VERSION,
        format: ARTIFACT_FORMAT,
        snapshot_id,
        job_id,
        payload: result,
    };
    encode(&envelope)
}

/// Encodes batch solve result as `HDF5`.
pub fn encode_solve_batch_artifact(
    snapshot_id: Uuid,
    job_id: Uuid,
    result: &SolveBatchResult,
) -> anyhow::Result<EncodedArtifact> {
    let envelope = ArtifactEnvelope {
        version: SCHEMA_VERSION,
        format: ARTIFACT_FORMAT,
        snapshot_id,
        job_id,
        payload: result,
    };
    encode(&envelope)
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct AllUnitQueryChunk {
    path: String,
    schema_version: String,
    compression: String,
    sha256: String,
    byte_size: u64,
    record_count: u64,
    first_process_index: usize,
    last_process_index: usize,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct AllUnitQueryEnvelope {
    version: u8,
    format: &'static str,
    snapshot_id: Uuid,
    job_id: Uuid,
    process_count: usize,
    impact_count: usize,
    calculation_bundle: CalculationBundleArtifactRef,
    lcia_chunks: Vec<AllUnitQueryChunk>,
}

/// Encodes a deterministic query index over canonical Calculation Bundle LCIA chunks.
pub fn encode_solve_all_unit_query_artifact(
    snapshot_id: Uuid,
    job_id: Uuid,
    manifest: &CalculationBundleManifest,
    bundle_ref: &CalculationBundleArtifactRef,
) -> anyhow::Result<EncodedArtifact> {
    if manifest.bundle_content_hash != bundle_ref.bundle_content_hash
        || manifest.calculation_id != bundle_ref.calculation_id
    {
        return Err(anyhow::anyhow!(
            "Calculation Bundle manifest/reference identity mismatch"
        ));
    }
    let mut lcia_chunks = manifest
        .artifacts
        .iter()
        .filter(|artifact| artifact.kind == "lcia")
        .map(|artifact| {
            Ok(AllUnitQueryChunk {
                path: artifact.path.clone(),
                schema_version: artifact.schema_version.clone(),
                compression: artifact.compression.clone(),
                sha256: artifact.sha256.clone(),
                byte_size: artifact.byte_size,
                record_count: artifact.record_count,
                first_process_index: artifact.first_process_index.ok_or_else(|| {
                    anyhow::anyhow!("LCIA chunk {} missing first process index", artifact.path)
                })?,
                last_process_index: artifact.last_process_index.ok_or_else(|| {
                    anyhow::anyhow!("LCIA chunk {} missing last process index", artifact.path)
                })?,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    lcia_chunks.sort_by(|left, right| {
        left.first_process_index
            .cmp(&right.first_process_index)
            .then_with(|| left.path.cmp(&right.path))
    });
    let covered_processes = lcia_chunks.iter().try_fold(0_usize, |count, chunk| {
        let chunk_count = chunk
            .last_process_index
            .checked_sub(chunk.first_process_index)
            .and_then(|span| span.checked_add(1))
            .ok_or_else(|| anyhow::anyhow!("invalid LCIA chunk range {}", chunk.path))?;
        count
            .checked_add(chunk_count)
            .ok_or_else(|| anyhow::anyhow!("LCIA chunk process count overflow"))
    })?;
    if covered_processes != manifest.scope.process_count {
        return Err(anyhow::anyhow!(
            "LCIA query index coverage mismatch: expected={} got={covered_processes}",
            manifest.scope.process_count
        ));
    }

    let envelope = AllUnitQueryEnvelope {
        version: 2,
        format: ALL_UNIT_QUERY_ARTIFACT_FORMAT,
        snapshot_id,
        job_id,
        process_count: manifest.scope.process_count,
        impact_count: manifest.snapshot.impact_count,
        calculation_bundle: bundle_ref.clone(),
        lcia_chunks,
    };

    let bytes = serde_json::to_vec(&envelope)?;
    let mut hasher = Sha256::new();
    hasher.update(bytes.as_slice());
    let digest = format!("{:x}", hasher.finalize());

    Ok(EncodedArtifact {
        bytes,
        sha256: digest,
        format: ALL_UNIT_QUERY_ARTIFACT_FORMAT,
        content_type: ALL_UNIT_QUERY_ARTIFACT_CONTENT_TYPE,
        extension: ALL_UNIT_QUERY_ARTIFACT_EXTENSION,
    })
}

#[derive(Debug, Clone, Serialize)]
struct SolveAllUnitDescriptor<'a> {
    result_kind: &'static str,
    canonical_result: &'a CalculationBundleArtifactRef,
    query_index: serde_json::Value,
}

/// Encodes the bounded legacy `lca_results` compatibility descriptor.
///
/// The descriptor preserves the existing HDF5 result row contract while
/// pointing consumers at the complete Calculation Bundle and query index. It
/// intentionally never embeds the full H matrix.
pub fn encode_solve_all_unit_result_descriptor(
    snapshot_id: Uuid,
    job_id: Uuid,
    bundle_ref: &CalculationBundleArtifactRef,
    query_index: serde_json::Value,
) -> anyhow::Result<EncodedArtifact> {
    let envelope = ArtifactEnvelope {
        version: 2,
        format: ARTIFACT_FORMAT,
        snapshot_id,
        job_id,
        payload: SolveAllUnitDescriptor {
            result_kind: "calculation_bundle",
            canonical_result: bundle_ref,
            query_index,
        },
    };
    encode(&envelope)
}

/// Encodes one contribution path result into a query-friendly JSON artifact.
pub fn encode_contribution_path_artifact(
    result: &ContributionPathArtifact,
) -> anyhow::Result<EncodedArtifact> {
    encode_json_artifact(
        result,
        CONTRIBUTION_PATH_ARTIFACT_FORMAT,
        CONTRIBUTION_PATH_ARTIFACT_CONTENT_TYPE,
        CONTRIBUTION_PATH_ARTIFACT_EXTENSION,
    )
}

fn encode<T>(value: &T) -> anyhow::Result<EncodedArtifact>
where
    T: Serialize,
{
    let json = serde_json::to_vec(value)?;
    let temp = Builder::new()
        .prefix("lca-artifact-")
        .suffix(".h5")
        .tempfile()?;

    write_hdf5_file(temp.path(), json.as_slice())?;
    let bytes = std::fs::read(temp.path())?;

    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    let digest = format!("{:x}", hasher.finalize());

    Ok(EncodedArtifact {
        bytes,
        sha256: digest,
        format: ARTIFACT_FORMAT,
        content_type: ARTIFACT_CONTENT_TYPE,
        extension: ARTIFACT_EXTENSION,
    })
}

fn encode_json_artifact<T>(
    value: &T,
    format: &'static str,
    content_type: &'static str,
    extension: &'static str,
) -> anyhow::Result<EncodedArtifact>
where
    T: Serialize,
{
    let bytes = serde_json::to_vec(value)?;
    let mut hasher = Sha256::new();
    hasher.update(bytes.as_slice());
    let digest = format!("{:x}", hasher.finalize());

    Ok(EncodedArtifact {
        bytes,
        sha256: digest,
        format,
        content_type,
        extension,
    })
}

fn write_hdf5_file(path: &Path, envelope_json: &[u8]) -> anyhow::Result<()> {
    let file = File::create(path)?;

    file.new_dataset_builder()
        .with_data(&[SCHEMA_VERSION])
        .create(DATASET_SCHEMA_VERSION)?;
    file.new_dataset_builder()
        .with_data(ARTIFACT_FORMAT.as_bytes())
        .create(DATASET_FORMAT)?;
    if !hdf5::filters::deflate_available() {
        return Err(anyhow::anyhow!(
            "HDF5 deflate filter is unavailable; zlib-enabled HDF5 is required"
        ));
    }
    let chunk_len = envelope_json.len().clamp(1, HDF5_CHUNK_TARGET_BYTES);
    file.new_dataset_builder()
        .chunk((chunk_len,))
        .deflate(HDF5_DEFLATE_LEVEL)
        .with_data(envelope_json)
        .create(DATASET_ENVELOPE_JSON)?;

    file.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use hdf5::File;
    use hdf5::filters::Filter;
    use serde::Deserialize;
    use serde_json::json;
    use solver_core::{FactorizationState, SolveBatchResult, SolveResult};
    use tempfile::Builder;
    use uuid::Uuid;

    use crate::calculation_bundle::{CalculationBundleArtifactRef, CalculationBundleManifest};
    use crate::contribution_path::{
        ContributionPathArtifact, ContributionPathImpact, ContributionPathMeta,
        ContributionPathOptions, ContributionPathRoot, ContributionPathSummary,
    };

    use super::{
        ALL_UNIT_QUERY_ARTIFACT_FORMAT, ARTIFACT_CONTENT_TYPE, ARTIFACT_EXTENSION, ARTIFACT_FORMAT,
        CONTRIBUTION_PATH_ARTIFACT_CONTENT_TYPE, CONTRIBUTION_PATH_ARTIFACT_EXTENSION,
        CONTRIBUTION_PATH_ARTIFACT_FORMAT, DATASET_ENVELOPE_JSON, DATASET_FORMAT,
        DATASET_SCHEMA_VERSION, HDF5_DEFLATE_LEVEL, encode_contribution_path_artifact,
        encode_solve_all_unit_query_artifact, encode_solve_batch_artifact,
        encode_solve_one_artifact,
    };

    #[derive(Debug, Deserialize)]
    struct EnvelopeOne {
        version: u8,
        format: String,
        snapshot_id: Uuid,
        job_id: Uuid,
        payload: SolveResult,
    }

    #[derive(Debug, Deserialize)]
    struct EnvelopeBatch {
        version: u8,
        format: String,
        snapshot_id: Uuid,
        job_id: Uuid,
        payload: SolveBatchResult,
    }

    #[test]
    fn encode_solve_one_artifact_metadata_and_payload_roundtrip() {
        let solved = SolveResult {
            x: Some(vec![1.0, 2.0]),
            g: Some(vec![3.0]),
            h: None,
            factorization_state: FactorizationState::Ready,
        };

        let encoded =
            encode_solve_one_artifact(Uuid::nil(), Uuid::nil(), &solved).expect("encode artifact");

        assert_eq!(encoded.format, ARTIFACT_FORMAT);
        assert_eq!(encoded.content_type, ARTIFACT_CONTENT_TYPE);
        assert_eq!(encoded.extension, ARTIFACT_EXTENSION);
        assert_eq!(encoded.sha256.len(), 64);
        assert!(encoded.sha256.chars().all(|ch| ch.is_ascii_hexdigit()));

        let file = write_and_open_hdf5(&encoded.bytes);
        let format_bytes = file
            .dataset(DATASET_FORMAT)
            .expect("format dataset")
            .read_1d::<u8>()
            .expect("read format bytes")
            .to_vec();
        assert_eq!(
            String::from_utf8(format_bytes).expect("utf8 format"),
            ARTIFACT_FORMAT
        );

        let schema_version = file
            .dataset(DATASET_SCHEMA_VERSION)
            .expect("schema_version dataset")
            .read_1d::<u8>()
            .expect("read schema version")
            .to_vec();
        assert_eq!(schema_version, vec![1_u8]);

        let envelope_bytes = file
            .dataset(DATASET_ENVELOPE_JSON)
            .expect("envelope_json dataset")
            .read_1d::<u8>()
            .expect("read envelope_json")
            .to_vec();
        let envelope: EnvelopeOne =
            serde_json::from_slice(&envelope_bytes).expect("parse envelope");
        let envelope_ds = file
            .dataset(DATASET_ENVELOPE_JSON)
            .expect("envelope_json dataset");
        assert!(envelope_ds.is_chunked());
        let filters = envelope_ds.filters();
        assert!(filters.iter().any(
            |filter| matches!(filter, Filter::Deflate(level) if *level == HDF5_DEFLATE_LEVEL)
        ));

        assert_eq!(envelope.version, 1);
        assert_eq!(envelope.format, ARTIFACT_FORMAT);
        assert_eq!(envelope.snapshot_id, Uuid::nil());
        assert_eq!(envelope.job_id, Uuid::nil());
        assert!((envelope.payload.x.as_ref().expect("x exists")[0] - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn encode_solve_batch_artifact_metadata_and_payload_roundtrip() {
        let solved = SolveBatchResult {
            items: vec![SolveResult {
                x: Some(vec![1.0]),
                g: None,
                h: Some(vec![2.0]),
                factorization_state: FactorizationState::Ready,
            }],
        };

        let encoded =
            encode_solve_batch_artifact(Uuid::nil(), Uuid::nil(), &solved).expect("encode batch");

        assert_eq!(encoded.format, ARTIFACT_FORMAT);
        assert_eq!(encoded.content_type, ARTIFACT_CONTENT_TYPE);
        assert_eq!(encoded.extension, ARTIFACT_EXTENSION);
        assert_eq!(encoded.sha256.len(), 64);

        let file = write_and_open_hdf5(&encoded.bytes);
        let envelope_bytes = file
            .dataset(DATASET_ENVELOPE_JSON)
            .expect("envelope_json dataset")
            .read_1d::<u8>()
            .expect("read envelope_json")
            .to_vec();
        let envelope: EnvelopeBatch =
            serde_json::from_slice(&envelope_bytes).expect("parse envelope");

        assert_eq!(envelope.version, 1);
        assert_eq!(envelope.format, ARTIFACT_FORMAT);
        assert_eq!(envelope.snapshot_id, Uuid::nil());
        assert_eq!(envelope.job_id, Uuid::nil());
        assert!(
            (envelope.payload.items[0].h.as_ref().expect("h exists")[0] - 2.0).abs() < f64::EPSILON
        );
    }

    #[test]
    fn all_unit_query_v2_indexes_chunks_without_materializing_h_matrix() {
        let manifest: CalculationBundleManifest = serde_json::from_value(json!({
            "schemaVersion": "tiangong.calculation-bundle.v2",
            "calculationContractVersion": "1.0.0",
            "calculationId": Uuid::nil(),
            "bundleContentHash": "a".repeat(64),
            "scope": {
                "coverageMode": "global_eligible",
                "processCount": 2,
                "selectionManifestHash": "b".repeat(64)
            },
            "snapshot": {
                "id": Uuid::nil(),
                "sha256": "c".repeat(64),
                "processCount": 2,
                "flowCount": 1,
                "impactCount": 1
            },
            "solver": {},
            "methodSet": {},
            "artifacts": [{
                "kind": "lcia",
                "path": "results/lcia-000000.ndjson.gz",
                "schemaVersion": "tiangong.calculation-bundle.lcia.v1",
                "mediaType": "application/x-ndjson",
                "compression": "gzip",
                "sha256": "d".repeat(64),
                "byteSize": 123,
                "recordCount": 2,
                "firstProcessIndex": 0,
                "lastProcessIndex": 1
            }],
            "calculationEvidence": {},
            "hashes": {}
        }))
        .unwrap();
        let bundle_ref = CalculationBundleArtifactRef {
            schema_version: "tiangong.calculation-bundle.v2".to_owned(),
            calculation_id: Uuid::nil(),
            bundle_content_hash: "a".repeat(64),
            manifest_url: "s3://bucket/calculation-bundle.json".to_owned(),
            manifest_sha256: "e".repeat(64),
            manifest_byte_size: 456,
            artifact_count: 1,
            downloads: Vec::new(),
        };

        let encoded =
            encode_solve_all_unit_query_artifact(Uuid::nil(), Uuid::nil(), &manifest, &bundle_ref)
                .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&encoded.bytes).unwrap();

        assert_eq!(encoded.format, ALL_UNIT_QUERY_ARTIFACT_FORMAT);
        assert_eq!(body["format"], ALL_UNIT_QUERY_ARTIFACT_FORMAT);
        assert_eq!(body["processCount"], 2);
        assert_eq!(body["impactCount"], 1);
        assert_eq!(body["lciaChunks"][0]["firstProcessIndex"], 0);
        assert!(body.get("hMatrix").is_none());
    }

    #[test]
    fn encode_contribution_path_artifact_as_json() {
        let artifact = ContributionPathArtifact {
            version: 1,
            format: CONTRIBUTION_PATH_ARTIFACT_FORMAT.to_owned(),
            snapshot_id: Uuid::nil(),
            job_id: Uuid::nil(),
            process_id: Uuid::nil(),
            impact_id: Uuid::nil(),
            amount: 1.0,
            options: ContributionPathOptions::default(),
            summary: ContributionPathSummary {
                total_impact: 1.23,
                unit: "kg CO2-eq".to_owned(),
                coverage_ratio: 0.9,
                expanded_node_count: 3,
                truncated_node_count: 1,
                computed_at: "2026-03-13T00:00:00Z".to_owned(),
            },
            root: ContributionPathRoot {
                process_id: Uuid::nil(),
                label: "Root".to_owned(),
            },
            impact: ContributionPathImpact {
                impact_id: Uuid::nil(),
                label: "GWP".to_owned(),
                unit: "kg CO2-eq".to_owned(),
            },
            process_contributions: Vec::new(),
            branches: Vec::new(),
            links: Vec::new(),
            meta: ContributionPathMeta {
                source: "solve_one_path_analysis".to_owned(),
                snapshot_index_version: 1,
            },
        };

        let encoded = encode_contribution_path_artifact(&artifact).expect("encode json artifact");
        assert_eq!(encoded.format, CONTRIBUTION_PATH_ARTIFACT_FORMAT);
        assert_eq!(
            encoded.content_type,
            CONTRIBUTION_PATH_ARTIFACT_CONTENT_TYPE
        );
        assert_eq!(encoded.extension, CONTRIBUTION_PATH_ARTIFACT_EXTENSION);

        let decoded: ContributionPathArtifact =
            serde_json::from_slice(encoded.bytes.as_slice()).expect("decode json artifact");
        assert!((decoded.summary.total_impact - 1.23).abs() < 1e-12);
        assert_eq!(decoded.root.label, "Root");
    }

    fn write_and_open_hdf5(bytes: &[u8]) -> File {
        let temp = Builder::new()
            .prefix("lca-artifact-test-")
            .suffix(".h5")
            .tempfile()
            .expect("create tempfile");
        std::fs::write(temp.path(), bytes).expect("write hdf5 bytes");
        File::open(temp.path()).expect("open hdf5 file")
    }
}
