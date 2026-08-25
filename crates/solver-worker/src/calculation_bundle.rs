use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_array::{ArrayRef, Float64Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use flate2::{Compression, GzBuilder, read::GzDecoder, write::GzEncoder};
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression as ParquetCompression, ZstdLevel},
    file::properties::WriterProperties,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use solver_core::SolveResult;
use tempfile::TempDir;
use uuid::Uuid;
use zip::{CompressionMethod, ZipWriter, write::SimpleFileOptions};

use crate::{
    calculation_evidence::{
        RELEASE_BUNDLE_MANIFEST_SHA256, RELEASE_BUNDLE_VERSION, RELEASE_FACTOR_MANIFEST_SHA256,
        RELEASE_METHOD_COUNT, RELEASE_METHOD_IDENTITIES, RELEASE_METHOD_IDENTITY_MANIFEST_SHA256,
        RELEASE_METHOD_MANIFEST_SHA256, RELEASE_SOURCE_SNAPSHOT_SHA256,
        canonical_json_bytes as canonical_value_json_bytes,
    },
    compiled_graph::{
        CompiledExchangeDirection, CompiledReleaseEvidence, CompiledReleaseInventoryExchange,
        CompiledReleaseSourceDataset, CompiledReleaseSourceDatasetRole,
        CompiledReleaseSourceDatasetType,
    },
    portal_lcia_projection::{
        PortalImpactSource, PortalLciaShard, PortalLocalizedText, PortalProcessSource,
        PreparedPortalLciaProjection, prepare_portal_lcia_projection,
    },
    snapshot_artifacts::{SnapshotBuildConfig, SnapshotCoverageReport},
    snapshot_index::{SnapshotImpactMapEntry, SnapshotIndexDocument},
    storage::ObjectStoreClient,
};

pub const CALCULATION_BUNDLE_FORMAT: &str = "tiangong.calculation-bundle.v2";
pub const CALCULATION_BUNDLE_MANIFEST_CONTENT_TYPE: &str = "application/json";
pub const CALCULATION_BUNDLE_CHUNK_PROCESS_COUNT: usize = 256;
const CALCULATION_BUNDLE_GZIP_CONTENT_TYPE: &str = "application/gzip";
const CALCULATION_CONTRACT_VERSION: &str = "1.0.0";
const GZIP_LEVEL: u32 = 6;
const XLSX_MAX_DATA_ROWS: u64 = 1_048_575;
const SEMANTIC_DOWNLOAD_SCHEMA: &str = "tiangong.calculation-download.v1";

fn calculation_solver_contract(config: &SnapshotBuildConfig) -> Value {
    json!({
        "engineVersion": env!("CARGO_PKG_VERSION"),
        "numericalPolicy": {
            "equation": "M=I-A; Mx=y",
            "backend": "umfpack",
            "unitDemandAmount": 1,
        },
        "providerPolicy": { "rule": config.provider_rule },
        "allocationPolicy": {
            "semanticsVersion": config.allocation_semantics_version,
            "mode": config.allocation_fraction_mode,
        },
        "linkPolicy": {
            "semanticsVersion": config.link_semantics_version,
            "candidateEligibility": config.provider_candidate_eligibility_mode,
            "technosphereBoundary": config.technosphere_boundary_policy,
            "flowIdentity": config.flow_identity_policy,
        },
        "sourceClosurePolicy": config.source_closure_policy,
        "sourceReferencePolicy": config.source_reference_policy,
        "zeroPolicy": {
            "directionalLci": "retain_finite_nonzero",
            "lcia": "retain_finite_including_zero",
        },
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CalculationBundleArtifact {
    pub kind: String,
    pub path: String,
    pub schema_version: String,
    pub media_type: String,
    pub compression: String,
    pub sha256: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uncompressed_sha256: Option<String>,
    pub byte_size: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uncompressed_byte_size: Option<u64>,
    pub record_count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_process_index: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_process_index: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub derived: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CalculationBundleManifest {
    pub schema_version: String,
    pub calculation_contract_version: String,
    pub calculation_id: Uuid,
    pub bundle_content_hash: String,
    pub scope: CalculationBundleScope,
    pub snapshot: CalculationBundleSnapshot,
    pub solver: Value,
    pub method_set: Value,
    pub artifacts: Vec<CalculationBundleArtifact>,
    pub calculation_evidence: Value,
    pub hashes: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CalculationBundleScope {
    pub coverage_mode: String,
    pub process_count: usize,
    pub selection_manifest_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CalculationBundleSnapshot {
    pub id: Uuid,
    pub sha256: String,
    pub process_count: usize,
    pub flow_count: usize,
    pub impact_count: usize,
}

#[derive(Debug)]
pub struct LocalCalculationBundleArtifact {
    pub metadata: CalculationBundleArtifact,
    pub local_path: PathBuf,
}

#[derive(Debug)]
pub struct BuiltCalculationBundle {
    _directory: TempDir,
    pub calculation_id: Uuid,
    pub bundle_content_hash: String,
    pub manifest_sha256: String,
    pub manifest_byte_size: u64,
    pub manifest_path: PathBuf,
    pub manifest: CalculationBundleManifest,
    pub artifacts: Vec<LocalCalculationBundleArtifact>,
    pub downloads: Vec<LocalCalculationDownloadArtifact>,
    pub portal_lcia_projection: Option<PreparedPortalLciaProjection>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CalculationDownloadArtifact {
    pub role: String,
    pub group: String,
    pub file_name: String,
    pub schema_version: String,
    pub media_type: String,
    pub sha256: String,
    pub byte_size: u64,
    pub record_count: u64,
}

#[derive(Debug)]
pub struct LocalCalculationDownloadArtifact {
    pub metadata: CalculationDownloadArtifact,
    pub local_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CalculationDownloadArtifactRef {
    #[serde(flatten)]
    pub metadata: CalculationDownloadArtifact,
    pub artifact_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CalculationBundleArtifactRef {
    pub schema_version: String,
    pub calculation_id: Uuid,
    pub bundle_content_hash: String,
    pub manifest_url: String,
    pub manifest_sha256: String,
    pub manifest_byte_size: u64,
    pub artifact_count: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub downloads: Vec<CalculationDownloadArtifactRef>,
}

pub async fn upload_built_calculation_bundle(
    store: &ObjectStoreClient,
    bundle: &BuiltCalculationBundle,
) -> anyhow::Result<CalculationBundleArtifactRef> {
    let relative_prefix = format!(
        "calculation-bundles/{}/{}",
        bundle.calculation_id, bundle.bundle_content_hash
    );
    for artifact in &bundle.artifacts {
        let relative_key = format!("{relative_prefix}/{}", artifact.metadata.path);
        let key = store.prefixed_object_key(&relative_key)?;
        let storage_content_type = calculation_bundle_storage_content_type(&artifact.metadata)?;
        store
            .upload_object_key_file(
                &key,
                storage_content_type,
                &artifact.local_path,
                artifact.metadata.byte_size,
            )
            .await?;
    }

    let mut downloads = Vec::with_capacity(bundle.downloads.len());
    for artifact in &bundle.downloads {
        let relative_key = format!(
            "{relative_prefix}/downloads/{}",
            artifact.metadata.file_name
        );
        let key = store.prefixed_object_key(&relative_key)?;
        let uploaded = store
            .upload_object_key_file(
                &key,
                artifact.metadata.media_type.as_str(),
                &artifact.local_path,
                artifact.metadata.byte_size,
            )
            .await?;
        downloads.push(CalculationDownloadArtifactRef {
            metadata: artifact.metadata.clone(),
            artifact_url: uploaded.object_url,
        });
    }

    let manifest_relative_key = format!("{relative_prefix}/calculation-bundle.json");
    let manifest_key = store.prefixed_object_key(&manifest_relative_key)?;
    let uploaded = store
        .upload_object_key_file(
            &manifest_key,
            CALCULATION_BUNDLE_MANIFEST_CONTENT_TYPE,
            &bundle.manifest_path,
            bundle.manifest_byte_size,
        )
        .await?;
    Ok(CalculationBundleArtifactRef {
        schema_version: CALCULATION_BUNDLE_FORMAT.to_owned(),
        calculation_id: bundle.calculation_id,
        bundle_content_hash: bundle.bundle_content_hash.clone(),
        manifest_url: uploaded.object_url,
        manifest_sha256: bundle.manifest_sha256.clone(),
        manifest_byte_size: bundle.manifest_byte_size,
        artifact_count: bundle.artifacts.len(),
        downloads,
    })
}

fn calculation_bundle_storage_content_type(
    artifact: &CalculationBundleArtifact,
) -> anyhow::Result<&str> {
    match artifact.compression.as_str() {
        "gzip" => Ok(CALCULATION_BUNDLE_GZIP_CONTENT_TYPE),
        "none" => Ok(artifact.media_type.as_str()),
        compression => Err(anyhow::anyhow!(
            "unsupported Calculation Bundle artifact compression: {compression}"
        )),
    }
}

#[derive(Debug)]
pub struct CalculationBundleWriter {
    directory: TempDir,
    calculation_id: Uuid,
    snapshot_id: Uuid,
    snapshot_sha256: String,
    snapshot_flow_count: usize,
    config: SnapshotBuildConfig,
    coverage: SnapshotCoverageReport,
    calculation_evidence: Value,
    processes: Vec<ReleaseProcessRecord>,
    impacts: Vec<ReleaseImpact>,
    biosphere_by_process: Vec<Vec<CompiledReleaseInventoryExchange>>,
    process_axis_schema_version: &'static str,
    artifacts: Vec<LocalCalculationBundleArtifact>,
    completed_result_chunks: BTreeSet<usize>,
    portal_projection_axes: Option<(Vec<PortalProcessSource>, Vec<PortalImpactSource>)>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ReleaseProcessRecord {
    process_index: usize,
    root_process: GlobalReference,
    quantitative_reference: QuantitativeReference,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GlobalReference {
    id: Uuid,
    version: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct QuantitativeReference {
    exchange_internal_id: String,
    flow: GlobalReference,
    direction: &'static str,
    reference_unit: String,
    mean_amount: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pivot: Option<QuantitativeReferencePivot>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct QuantitativeReferencePivot {
    raw_direction: CompiledExchangeDirection,
    raw_mean_amount: f64,
    signed_raw_coefficient: f64,
    normalization_scale: f64,
    normalized_coefficient: f64,
}

#[derive(Debug, Clone)]
struct ReleaseImpact {
    index: usize,
    id: Uuid,
    version: String,
    key: String,
    name: String,
    unit: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct InventoryKey {
    flow_id: Uuid,
    flow_version: String,
    direction: CompiledExchangeDirection,
    unit: String,
    location: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct InventoryRecord<'a> {
    process_index: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    exchange_internal_id: Option<&'a str>,
    flow: GlobalReference,
    direction: CompiledExchangeDirection,
    unit: &'a str,
    location: Option<&'a str>,
    mean_amount: f64,
    allocation_target_internal_id: &'a str,
    allocation_fraction: f64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OwnedInventoryRecord {
    process_index: usize,
    flow: GlobalReference,
    direction: CompiledExchangeDirection,
    unit: String,
    location: Option<String>,
    mean_amount: f64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LciaRecord {
    process_index: usize,
    method: GlobalReference,
    mean_amount: f64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TechnosphereRecord<'a> {
    dependent_process_index: usize,
    residual_exchange_internal_id: &'a str,
    balancing_process_index: usize,
    balancing_reference_exchange_internal_id: &'a str,
    residual_coefficient: f64,
    reference_coefficient: f64,
    routing_weight: f64,
    activity_requirement: f64,
    flow: GlobalReference,
    location: Option<&'a str>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SourceClosureRecord<'a> {
    schema_version: &'static str,
    dataset_type: &'static str,
    role: &'static str,
    uuid: Uuid,
    version: &'a str,
    path: String,
    sha256: &'a str,
    document: &'a Value,
}

struct DeterministicGzipNdjsonWriter {
    encoder: GzEncoder<File>,
    plain_hasher: Sha256,
    plain_byte_size: u64,
    record_count: u64,
    path: PathBuf,
}

impl DeterministicGzipNdjsonWriter {
    fn create(path: &Path) -> anyhow::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = File::create(path)?;
        let encoder = GzBuilder::new()
            .mtime(0)
            .write(file, Compression::new(GZIP_LEVEL));
        Ok(Self {
            encoder,
            plain_hasher: Sha256::new(),
            plain_byte_size: 0,
            record_count: 0,
            path: path.to_owned(),
        })
    }

    fn write<T: Serialize>(&mut self, value: &T) -> anyhow::Result<()> {
        let bytes = canonical_json_bytes(value)?;
        self.encoder.write_all(bytes.as_slice())?;
        self.encoder.write_all(b"\n")?;
        self.plain_hasher.update(bytes.as_slice());
        self.plain_hasher.update(b"\n");
        self.plain_byte_size = self
            .plain_byte_size
            .checked_add(u64::try_from(bytes.len() + 1)?)
            .ok_or_else(|| anyhow::anyhow!("Calculation Bundle uncompressed byte size overflow"))?;
        self.record_count = self
            .record_count
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("Calculation Bundle record count overflow"))?;
        Ok(())
    }

    fn finish(self) -> anyhow::Result<FinishedNdjson> {
        let Self {
            encoder,
            plain_hasher,
            plain_byte_size,
            record_count,
            path,
        } = self;
        let file = encoder.finish()?;
        file.sync_all()?;
        let byte_size = file.metadata()?.len();
        Ok(FinishedNdjson {
            sha256: sha256_file(&path)?,
            path,
            uncompressed_sha256: hex::encode(plain_hasher.finalize()),
            byte_size,
            uncompressed_byte_size: plain_byte_size,
            record_count,
        })
    }
}

struct FinishedNdjson {
    path: PathBuf,
    sha256: String,
    uncompressed_sha256: String,
    byte_size: u64,
    uncompressed_byte_size: u64,
    record_count: u64,
}

impl CalculationBundleWriter {
    #[allow(clippy::too_many_lines)]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        calculation_id: Uuid,
        snapshot_id: Uuid,
        snapshot_sha256: String,
        snapshot_flow_count: usize,
        config: SnapshotBuildConfig,
        coverage: SnapshotCoverageReport,
        snapshot_index: &SnapshotIndexDocument,
        release_evidence: &CompiledReleaseEvidence,
    ) -> anyhow::Result<Self> {
        validate_sha256(&snapshot_sha256, "snapshot.sha256")?;
        let process_count = usize::try_from(snapshot_index.process_count)
            .map_err(|_| anyhow::anyhow!("negative snapshot process count"))?;
        if process_count == 0 || release_evidence.processes.len() != process_count {
            return Err(anyhow::anyhow!(
                "Calculation Bundle process evidence mismatch: snapshot={process_count} evidence={}",
                release_evidence.processes.len()
            ));
        }
        validate_source_datasets(release_evidence)?;

        let mut processes = release_evidence
            .processes
            .iter()
            .map(|process| {
                let process_index = usize::try_from(process.process_idx)
                    .map_err(|_| anyhow::anyhow!("negative release process index"))?;
                validate_version(&process.process_version, "process.version")?;
                validate_version(
                    &process.quantitative_reference_flow_version,
                    "quantitativeReference.flow.version",
                )?;
                require_nonempty(
                    &process.quantitative_reference_exchange_internal_id,
                    "quantitativeReference.exchangeInternalId",
                )?;
                require_nonempty(
                    &process.reference_unit,
                    "quantitativeReference.referenceUnit",
                )?;
                ensure_finite_nonzero(
                    process.normalized_mean_amount,
                    "quantitativeReference.meanAmount",
                )?;
                let pivot = match (
                    process.reference_direction,
                    process.raw_reference_amount,
                    process.signed_raw_reference_coefficient,
                    process.normalized_reference_coefficient,
                ) {
                    (None, None, None, None) => None,
                    (
                        Some(raw_direction),
                        Some(raw_mean_amount),
                        Some(signed_raw_coefficient),
                        Some(normalized_coefficient),
                    ) => {
                        ensure_finite_nonzero(
                            raw_mean_amount,
                            "quantitativeReference.pivot.rawMeanAmount",
                        )?;
                        ensure_finite_nonzero(
                            signed_raw_coefficient,
                            "quantitativeReference.pivot.signedRawCoefficient",
                        )?;
                        ensure_finite_nonzero(
                            normalized_coefficient,
                            "quantitativeReference.pivot.normalizedCoefficient",
                        )?;
                        let direction_sign = match raw_direction {
                            CompiledExchangeDirection::Input => -1.0,
                            CompiledExchangeDirection::Output => 1.0,
                        };
                        ensure_nearly_equal(
                            signed_raw_coefficient,
                            direction_sign * raw_mean_amount,
                            "quantitativeReference.pivot.signedRawCoefficient",
                        )?;
                        let normalization_scale = 1.0 / signed_raw_coefficient.abs();
                        ensure_nearly_equal(
                            process.normalized_mean_amount,
                            raw_mean_amount * normalization_scale,
                            "quantitativeReference.meanAmount",
                        )?;
                        ensure_nearly_equal(
                            normalized_coefficient,
                            signed_raw_coefficient.signum(),
                            "quantitativeReference.pivot.normalizedCoefficient",
                        )?;
                        Some(QuantitativeReferencePivot {
                            raw_direction,
                            raw_mean_amount,
                            signed_raw_coefficient,
                            normalization_scale,
                            normalized_coefficient,
                        })
                    }
                    _ => {
                        return Err(anyhow::anyhow!(
                            "quantitativeReference.pivot is partially populated for processIndex={process_index}"
                        ));
                    }
                };
                Ok(ReleaseProcessRecord {
                    process_index,
                    root_process: GlobalReference {
                        id: process.process_id,
                        version: process.process_version.clone(),
                    },
                    quantitative_reference: QuantitativeReference {
                        exchange_internal_id: process
                            .quantitative_reference_exchange_internal_id
                            .clone(),
                        flow: GlobalReference {
                            id: process.quantitative_reference_flow_id,
                            version: process.quantitative_reference_flow_version.clone(),
                        },
                        direction: "Output",
                        reference_unit: process.reference_unit.clone(),
                        mean_amount: process.normalized_mean_amount,
                        pivot,
                    },
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        processes.sort_unstable_by_key(|process| process.process_index);
        for (expected, process) in processes.iter().enumerate() {
            if process.process_index != expected {
                return Err(anyhow::anyhow!(
                    "Calculation Bundle process index gap: expected={expected} got={}",
                    process.process_index
                ));
            }
        }
        let pivot_count = processes
            .iter()
            .filter(|process| process.quantitative_reference.pivot.is_some())
            .count();
        if pivot_count != 0 && pivot_count != processes.len() {
            return Err(anyhow::anyhow!(
                "Calculation Bundle process-axis pivot evidence is mixed: withPivot={pivot_count} total={}",
                processes.len()
            ));
        }
        let process_axis_schema_version = if pivot_count == processes.len() {
            "tiangong.calculation-bundle.process-axis.v2"
        } else {
            "tiangong.calculation-bundle.process-axis.v1"
        };

        let impacts = validate_impacts(&snapshot_index.impact_map)?;
        let calculation_evidence = snapshot_index
            .calculation_evidence
            .as_ref()
            .map(serde_json::to_value)
            .transpose()?
            .unwrap_or_else(|| {
                json!({
                    "schemaVersion": "lca.calculation-evidence.legacy-snapshot.v1",
                    "snapshotCoverage": coverage,
                })
            });

        let mut biosphere_by_process = vec![Vec::new(); process_count];
        for exchange in &release_evidence.biosphere_edges {
            validate_inventory_exchange(exchange, process_count)?;
            let process_index = usize::try_from(exchange.process_idx)?;
            biosphere_by_process[process_index].push(exchange.clone());
        }
        for exchanges in &mut biosphere_by_process {
            exchanges.sort_by(inventory_exchange_order);
        }

        let mut writer = Self {
            directory: tempfile::Builder::new()
                .prefix("tiangong-calculation-bundle-")
                .tempdir()?,
            calculation_id,
            snapshot_id,
            snapshot_sha256,
            snapshot_flow_count,
            config,
            coverage,
            calculation_evidence,
            processes,
            impacts,
            biosphere_by_process,
            process_axis_schema_version,
            artifacts: Vec::new(),
            completed_result_chunks: BTreeSet::new(),
            portal_projection_axes: None,
        };
        writer.write_static_artifacts(release_evidence)?;
        Ok(writer)
    }

    pub fn enable_portal_lcia_projection(
        &mut self,
        release_evidence: &CompiledReleaseEvidence,
    ) -> anyhow::Result<()> {
        if self.portal_projection_axes.is_some() {
            return Err(anyhow::anyhow!(
                "Portal LCIA projection preparation was enabled more than once"
            ));
        }
        self.portal_projection_axes = Some(portal_projection_axes(
            &self.processes,
            &self.impacts,
            release_evidence,
        )?);
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    pub fn write_result_chunk(
        &mut self,
        first_process_index: usize,
        items: &[SolveResult],
    ) -> anyhow::Result<()> {
        if !first_process_index.is_multiple_of(CALCULATION_BUNDLE_CHUNK_PROCESS_COUNT) {
            return Err(anyhow::anyhow!(
                "Calculation Bundle result chunk must start on a 256-process boundary"
            ));
        }
        if items.is_empty() || items.len() > CALCULATION_BUNDLE_CHUNK_PROCESS_COUNT {
            return Err(anyhow::anyhow!(
                "Calculation Bundle result chunk size is invalid"
            ));
        }
        let end = first_process_index
            .checked_add(items.len())
            .ok_or_else(|| anyhow::anyhow!("Calculation Bundle result chunk index overflow"))?;
        if end > self.processes.len() {
            return Err(anyhow::anyhow!(
                "Calculation Bundle result chunk exceeds process axis"
            ));
        }
        if !self.completed_result_chunks.insert(first_process_index) {
            return Err(anyhow::anyhow!(
                "Calculation Bundle result chunk already written"
            ));
        }

        let chunk_number = first_process_index / CALCULATION_BUNDLE_CHUNK_PROCESS_COUNT;
        let last_process_index = end - 1;
        let lci_path = format!("results/lci-{chunk_number:06}.ndjson.gz");
        let lcia_path = format!("results/lcia-{chunk_number:06}.ndjson.gz");
        let mut lci_writer =
            DeterministicGzipNdjsonWriter::create(&self.directory.path().join(lci_path.as_str()))?;
        let mut lcia_writer =
            DeterministicGzipNdjsonWriter::create(&self.directory.path().join(lcia_path.as_str()))?;

        for (offset, item) in items.iter().enumerate() {
            let process_index = first_process_index + offset;
            let x = item.x.as_ref().ok_or_else(|| {
                anyhow::anyhow!("Calculation Bundle solve item[{process_index}] is missing x")
            })?;
            if x.len() != self.processes.len() {
                return Err(anyhow::anyhow!(
                    "Calculation Bundle x axis mismatch for process {process_index}: expected={} got={}",
                    self.processes.len(),
                    x.len()
                ));
            }
            let h = item.h.as_ref().ok_or_else(|| {
                anyhow::anyhow!("Calculation Bundle solve item[{process_index}] is missing h")
            })?;
            if h.len() != self.impacts.len() {
                return Err(anyhow::anyhow!(
                    "Calculation Bundle h axis mismatch for process {process_index}: expected={} got={}",
                    self.impacts.len(),
                    h.len()
                ));
            }

            let mut inventory = BTreeMap::<InventoryKey, f64>::new();
            for (source_process_index, scale) in x.iter().copied().enumerate() {
                ensure_finite(scale, "x")?;
                if scale == 0.0 {
                    continue;
                }
                for exchange in &self.biosphere_by_process[source_process_index] {
                    let contribution = exchange.normalized_mean_amount * scale;
                    ensure_finite(contribution, "directional LCI contribution")?;
                    let key = InventoryKey {
                        flow_id: exchange.flow_id,
                        flow_version: exchange.flow_version.clone(),
                        direction: exchange.direction,
                        unit: exchange.unit.clone(),
                        location: exchange.location.clone(),
                    };
                    let value = inventory.entry(key).or_insert(0.0);
                    *value += contribution;
                    ensure_finite(*value, "directional LCI aggregate")?;
                }
            }
            for (key, mean_amount) in inventory {
                if mean_amount == 0.0 {
                    continue;
                }
                lci_writer.write(&OwnedInventoryRecord {
                    process_index,
                    flow: GlobalReference {
                        id: key.flow_id,
                        version: key.flow_version,
                    },
                    direction: key.direction,
                    unit: key.unit,
                    location: key.location,
                    mean_amount,
                })?;
            }

            for impact in &self.impacts {
                let mean_amount = h[impact.index];
                ensure_finite(mean_amount, "LCIA result")?;
                lcia_writer.write(&LciaRecord {
                    process_index,
                    method: GlobalReference {
                        id: impact.id,
                        version: impact.version.clone(),
                    },
                    mean_amount,
                })?;
            }
        }

        self.push_finished_ndjson(
            "lci",
            "tiangong.calculation-bundle.lci.v1",
            lci_path,
            first_process_index,
            last_process_index,
            lci_writer.finish()?,
        );
        self.push_finished_ndjson(
            "lcia",
            "tiangong.calculation-bundle.lcia.v1",
            lcia_path,
            first_process_index,
            last_process_index,
            lcia_writer.finish()?,
        );
        Ok(())
    }

    pub fn finish(mut self) -> anyhow::Result<BuiltCalculationBundle> {
        let expected_chunk_starts = (0..self.processes.len())
            .step_by(CALCULATION_BUNDLE_CHUNK_PROCESS_COUNT)
            .collect::<BTreeSet<_>>();
        if self.completed_result_chunks != expected_chunk_starts {
            return Err(anyhow::anyhow!(
                "Calculation Bundle result chunks incomplete: expected={expected_chunk_starts:?} got={:?}",
                self.completed_result_chunks
            ));
        }

        self.write_coverage_artifact()?;
        self.artifacts
            .sort_by(|left, right| left.metadata.path.cmp(&right.metadata.path));
        let selection_manifest_hash = canonical_sha256(&json!({
            "schemaVersion": "tiangong.calculation-bundle.selection.v1",
            "processes": self.processes,
        }))?;
        let coverage_mode = if self.config.request_roots.is_empty() {
            "global_eligible"
        } else {
            "subset"
        };
        let process_count = self.processes.len();
        let impact_count = self.impacts.len();
        let mut manifest = CalculationBundleManifest {
            schema_version: CALCULATION_BUNDLE_FORMAT.to_owned(),
            calculation_contract_version: CALCULATION_CONTRACT_VERSION.to_owned(),
            calculation_id: self.calculation_id,
            bundle_content_hash: "0".repeat(64),
            scope: CalculationBundleScope {
                coverage_mode: coverage_mode.to_owned(),
                process_count,
                selection_manifest_hash,
            },
            snapshot: CalculationBundleSnapshot {
                id: self.snapshot_id,
                sha256: self.snapshot_sha256,
                process_count,
                flow_count: self.snapshot_flow_count,
                impact_count,
            },
            solver: calculation_solver_contract(&self.config),
            method_set: json!({
                "schemaVersion": "lcia.static_cache_bundle.v1",
                "bundleVersion": RELEASE_BUNDLE_VERSION,
                "methodCount": RELEASE_METHOD_COUNT,
                "rawManifestSha256": RELEASE_BUNDLE_MANIFEST_SHA256,
                "sourceSnapshotSha256": RELEASE_SOURCE_SNAPSHOT_SHA256,
                "methodManifestSha256": RELEASE_METHOD_MANIFEST_SHA256,
                "methodIdentityManifestSha256": RELEASE_METHOD_IDENTITY_MANIFEST_SHA256,
                "factorManifestSha256": RELEASE_FACTOR_MANIFEST_SHA256,
            }),
            artifacts: self
                .artifacts
                .iter()
                .map(|artifact| artifact.metadata.clone())
                .collect(),
            calculation_evidence: self.calculation_evidence,
            hashes: json!({
                "algorithm": "sha256",
                "canonicalJson": "RFC8785/JCS",
                "gzip": { "level": GZIP_LEVEL, "mtime": 0 },
            }),
        };
        manifest.bundle_content_hash = bundle_content_hash(&manifest)?;
        let manifest_bytes = canonical_json_bytes(&manifest)?;
        let manifest_sha256 = sha256_bytes(manifest_bytes.as_slice());
        let manifest_byte_size = u64::try_from(manifest_bytes.len())?;
        let manifest_path = self.directory.path().join("calculation-bundle.json");
        std::fs::write(&manifest_path, manifest_bytes)?;
        let downloads = build_calculation_downloads(
            self.directory.path(),
            &manifest_path,
            &self.artifacts,
            &self.processes,
            &self.impacts,
        )?;
        let portal_lcia_projection = self
            .portal_projection_axes
            .take()
            .map(|(processes, impacts)| {
                let shards = portal_lcia_shards(&self.artifacts)?;
                prepare_portal_lcia_projection(&processes, &impacts, &shards)
            })
            .transpose()?;

        Ok(BuiltCalculationBundle {
            _directory: self.directory,
            calculation_id: self.calculation_id,
            bundle_content_hash: manifest.bundle_content_hash.clone(),
            manifest_sha256,
            manifest_byte_size,
            manifest_path,
            manifest,
            artifacts: self.artifacts,
            downloads,
            portal_lcia_projection,
        })
    }

    #[allow(clippy::too_many_lines)]
    fn write_static_artifacts(
        &mut self,
        release_evidence: &CompiledReleaseEvidence,
    ) -> anyhow::Result<()> {
        self.write_source_closure_artifact(&release_evidence.source_datasets)?;
        for first_process_index in
            (0..self.processes.len()).step_by(CALCULATION_BUNDLE_CHUNK_PROCESS_COUNT)
        {
            let end = (first_process_index + CALCULATION_BUNDLE_CHUNK_PROCESS_COUNT)
                .min(self.processes.len());
            let last_process_index = end - 1;
            let chunk_number = first_process_index / CALCULATION_BUNDLE_CHUNK_PROCESS_COUNT;

            let process_path = format!("axes/processes-{chunk_number:06}.ndjson.gz");
            let mut process_writer = DeterministicGzipNdjsonWriter::create(
                &self.directory.path().join(process_path.as_str()),
            )?;
            for process in &self.processes[first_process_index..end] {
                process_writer.write(process)?;
            }
            self.push_finished_ndjson(
                "process_axis",
                self.process_axis_schema_version,
                process_path,
                first_process_index,
                last_process_index,
                process_writer.finish()?,
            );

            let inventory_path = format!("axes/inventory-{chunk_number:06}.ndjson.gz");
            let mut inventory_writer = DeterministicGzipNdjsonWriter::create(
                &self.directory.path().join(inventory_path.as_str()),
            )?;
            let mut inventory = release_evidence
                .inventory_exchanges
                .iter()
                .filter(|exchange| {
                    usize::try_from(exchange.process_idx)
                        .is_ok_and(|index| index >= first_process_index && index < end)
                })
                .collect::<Vec<_>>();
            inventory.sort_by(|left, right| inventory_exchange_order(left, right));
            for exchange in inventory {
                validate_inventory_exchange(exchange, self.processes.len())?;
                inventory_writer.write(&inventory_record(exchange)?)?;
            }
            self.push_finished_ndjson(
                "inventory_axis",
                "tiangong.calculation-bundle.inventory-axis.v1",
                inventory_path,
                first_process_index,
                last_process_index,
                inventory_writer.finish()?,
            );

            let biosphere_path = format!("graph/biosphere-{chunk_number:06}.ndjson.gz");
            let mut biosphere_writer = DeterministicGzipNdjsonWriter::create(
                &self.directory.path().join(biosphere_path.as_str()),
            )?;
            let mut biosphere = release_evidence
                .biosphere_edges
                .iter()
                .filter(|exchange| {
                    usize::try_from(exchange.process_idx)
                        .is_ok_and(|index| index >= first_process_index && index < end)
                })
                .collect::<Vec<_>>();
            biosphere.sort_by(|left, right| inventory_exchange_order(left, right));
            for exchange in biosphere {
                validate_inventory_exchange(exchange, self.processes.len())?;
                biosphere_writer.write(&inventory_record(exchange)?)?;
            }
            self.push_finished_ndjson(
                "biosphere_edges",
                "tiangong.calculation-bundle.biosphere-edges.v1",
                biosphere_path,
                first_process_index,
                last_process_index,
                biosphere_writer.finish()?,
            );

            let technosphere_path = format!("graph/technosphere-{chunk_number:06}.ndjson.gz");
            let mut technosphere_writer = DeterministicGzipNdjsonWriter::create(
                &self.directory.path().join(technosphere_path.as_str()),
            )?;
            let mut technosphere = release_evidence
                .technosphere_edges
                .iter()
                .filter(|edge| {
                    usize::try_from(edge.dependent_process_idx)
                        .is_ok_and(|index| index >= first_process_index && index < end)
                })
                .collect::<Vec<_>>();
            technosphere.sort_by(|left, right| {
                left.dependent_process_idx
                    .cmp(&right.dependent_process_idx)
                    .then_with(|| {
                        left.residual_exchange_internal_id
                            .cmp(&right.residual_exchange_internal_id)
                    })
                    .then_with(|| left.balancing_process_idx.cmp(&right.balancing_process_idx))
            });
            for edge in technosphere {
                validate_version(&edge.flow_version, "technosphere.flow.version")?;
                require_nonempty(
                    &edge.residual_exchange_internal_id,
                    "technosphere.residualExchangeInternalId",
                )?;
                require_nonempty(
                    &edge.balancing_reference_exchange_internal_id,
                    "technosphere.balancingReferenceExchangeInternalId",
                )?;
                ensure_finite(
                    edge.residual_coefficient,
                    "technosphere.residualCoefficient",
                )?;
                ensure_finite(
                    edge.reference_coefficient,
                    "technosphere.referenceCoefficient",
                )?;
                ensure_finite(edge.routing_weight, "technosphere.routingWeight")?;
                ensure_finite(
                    edge.activity_requirement,
                    "technosphere.activityRequirement",
                )?;
                let dependent_process_index = usize::try_from(edge.dependent_process_idx)?;
                let balancing_process_index = usize::try_from(edge.balancing_process_idx)?;
                if dependent_process_index >= self.processes.len()
                    || balancing_process_index >= self.processes.len()
                {
                    return Err(anyhow::anyhow!(
                        "technosphere edge process index is outside process axis"
                    ));
                }
                technosphere_writer.write(&TechnosphereRecord {
                    dependent_process_index,
                    residual_exchange_internal_id: &edge.residual_exchange_internal_id,
                    balancing_process_index,
                    balancing_reference_exchange_internal_id: &edge
                        .balancing_reference_exchange_internal_id,
                    residual_coefficient: edge.residual_coefficient,
                    reference_coefficient: edge.reference_coefficient,
                    routing_weight: edge.routing_weight,
                    activity_requirement: edge.activity_requirement,
                    flow: GlobalReference {
                        id: edge.flow_id,
                        version: edge.flow_version.clone(),
                    },
                    location: edge.location.as_deref(),
                })?;
            }
            self.push_finished_ndjson(
                "technosphere_edges",
                "tiangong.calculation-bundle.technosphere-edges.v2",
                technosphere_path,
                first_process_index,
                last_process_index,
                technosphere_writer.finish()?,
            );
        }
        Ok(())
    }

    fn write_source_closure_artifact(
        &mut self,
        source_datasets: &[CompiledReleaseSourceDataset],
    ) -> anyhow::Result<()> {
        let relative_path = "source/source-closure.ndjson.gz";
        let mut writer =
            DeterministicGzipNdjsonWriter::create(&self.directory.path().join(relative_path))?;
        let mut datasets = source_datasets.iter().collect::<Vec<_>>();
        datasets.sort_by(|left, right| source_dataset_order(left, right));
        for dataset in datasets {
            validate_version(&dataset.dataset_version, "sourceClosure.dataset.version")?;
            validate_sha256(&dataset.document_sha256, "sourceClosure.dataset.sha256")?;
            let canonical_document = canonical_value_json_bytes(&dataset.document)?;
            if sha256_bytes(canonical_document.as_slice()) != dataset.document_sha256 {
                return Err(anyhow::anyhow!(
                    "Calculation Bundle source closure document hash drift for {}:{}@{}",
                    dataset.dataset_type.as_str(),
                    dataset.dataset_id,
                    dataset.dataset_version
                ));
            }
            writer.write(&SourceClosureRecord {
                schema_version: "tiangong.source-closure.dataset.v1",
                dataset_type: dataset.dataset_type.as_str(),
                role: dataset.role.as_str(),
                uuid: dataset.dataset_id,
                version: &dataset.dataset_version,
                path: source_dataset_path(dataset),
                sha256: &dataset.document_sha256,
                document: &dataset.document,
            })?;
        }
        let finished = writer.finish()?;
        self.artifacts.push(LocalCalculationBundleArtifact {
            metadata: CalculationBundleArtifact {
                kind: "source_closure".to_owned(),
                path: relative_path.to_owned(),
                schema_version: "tiangong.source-closure.bundle.v1".to_owned(),
                media_type: "application/x-ndjson".to_owned(),
                compression: "gzip".to_owned(),
                sha256: finished.sha256,
                uncompressed_sha256: Some(finished.uncompressed_sha256),
                byte_size: finished.byte_size,
                uncompressed_byte_size: Some(finished.uncompressed_byte_size),
                record_count: finished.record_count,
                first_process_index: None,
                last_process_index: None,
                derived: Some(false),
            },
            local_path: finished.path,
        });
        Ok(())
    }

    fn write_coverage_artifact(&mut self) -> anyhow::Result<()> {
        let relative_path = "evidence/coverage.json";
        let local_path = self.directory.path().join(relative_path);
        if let Some(parent) = local_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let complete = self.coverage.matching.unmatched_no_provider == 0
            && self.coverage.reference.missing_reference_count == 0
            && self.coverage.reference.invalid_reference_count == 0;
        let body = canonical_json_bytes(&json!({
            "schemaVersion": "tiangong.calculation-bundle.coverage.v1",
            "complete": complete,
            "processCount": self.processes.len(),
            "snapshotCoverage": self.coverage,
            "calculationEvidence": self.calculation_evidence,
        }))?;
        std::fs::write(&local_path, &body)?;
        self.artifacts.push(LocalCalculationBundleArtifact {
            metadata: CalculationBundleArtifact {
                kind: "coverage".to_owned(),
                path: relative_path.to_owned(),
                schema_version: "tiangong.calculation-bundle.coverage.v1".to_owned(),
                media_type: "application/json".to_owned(),
                compression: "none".to_owned(),
                sha256: sha256_bytes(body.as_slice()),
                uncompressed_sha256: None,
                byte_size: u64::try_from(body.len())?,
                uncompressed_byte_size: None,
                record_count: 1,
                first_process_index: None,
                last_process_index: None,
                derived: None,
            },
            local_path,
        });
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn push_finished_ndjson(
        &mut self,
        kind: &str,
        schema_version: &str,
        relative_path: String,
        first_process_index: usize,
        last_process_index: usize,
        finished: FinishedNdjson,
    ) {
        self.artifacts.push(LocalCalculationBundleArtifact {
            metadata: CalculationBundleArtifact {
                kind: kind.to_owned(),
                path: relative_path,
                schema_version: schema_version.to_owned(),
                media_type: "application/x-ndjson".to_owned(),
                compression: "gzip".to_owned(),
                sha256: finished.sha256,
                uncompressed_sha256: Some(finished.uncompressed_sha256),
                byte_size: finished.byte_size,
                uncompressed_byte_size: Some(finished.uncompressed_byte_size),
                record_count: finished.record_count,
                first_process_index: Some(first_process_index),
                last_process_index: Some(last_process_index),
                derived: None,
            },
            local_path: finished.path,
        });
    }
}

#[allow(clippy::similar_names)]
fn build_calculation_downloads(
    directory: &Path,
    manifest_path: &Path,
    artifacts: &[LocalCalculationBundleArtifact],
    processes: &[ReleaseProcessRecord],
    impacts: &[ReleaseImpact],
) -> anyhow::Result<Vec<LocalCalculationDownloadArtifact>> {
    let download_directory = directory.join("downloads");
    std::fs::create_dir_all(&download_directory)?;
    let lci_artifacts = artifacts_of_kind(artifacts, "lci");
    let lcia_artifacts = artifacts_of_kind(artifacts, "lcia");

    let lcia_xlsx = download_directory.join("lcia-results.xlsx");
    let lcia_record_count = write_lcia_xlsx(&lcia_xlsx, &lcia_artifacts, processes, impacts)?;
    let lcia_csv = download_directory.join("lcia-results.csv.zip");
    let lcia_csv_count = write_lcia_csv_zip(&lcia_csv, &lcia_artifacts, processes, impacts)?;
    if lcia_csv_count != lcia_record_count {
        return Err(anyhow::anyhow!(
            "LCIA semantic download row count drift: xlsx={lcia_record_count} csv={lcia_csv_count}"
        ));
    }

    let lci_parquet = download_directory.join("lci-inventory.parquet");
    let lci_record_count = write_lci_parquet(&lci_parquet, &lci_artifacts, processes)?;
    let lci_csv = download_directory.join("lci-inventory-csv.zip");
    let lci_csv_count = write_lci_csv_zip(&lci_csv, &lci_artifacts, processes)?;
    if lci_csv_count != lci_record_count {
        return Err(anyhow::anyhow!(
            "LCI semantic download row count drift: parquet={lci_record_count} csv={lci_csv_count}"
        ));
    }

    let audit = download_directory.join("calculation-evidence-bundle.zip");
    write_audit_archive(&audit, manifest_path, artifacts)?;

    Ok(vec![
        local_download(
            "lcia_results_xlsx",
            "results",
            "lcia-results.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            lcia_record_count,
            lcia_xlsx,
        )?,
        local_download(
            "lcia_results_csv_zip",
            "results",
            "lcia-results.csv.zip",
            "application/zip",
            lcia_record_count,
            lcia_csv,
        )?,
        local_download(
            "lci_inventory_parquet",
            "advanced_data",
            "lci-inventory.parquet",
            "application/vnd.apache.parquet",
            lci_record_count,
            lci_parquet,
        )?,
        local_download(
            "lci_inventory_csv_zip",
            "advanced_data",
            "lci-inventory-csv.zip",
            "application/zip",
            lci_record_count,
            lci_csv,
        )?,
        local_download(
            "calculation_evidence_bundle",
            "audit_evidence",
            "calculation-evidence-bundle.zip",
            "application/zip",
            u64::try_from(artifacts.len())?.saturating_add(1),
            audit,
        )?,
    ])
}

fn local_download(
    role: &str,
    group: &str,
    file_name: &str,
    media_type: &str,
    record_count: u64,
    local_path: PathBuf,
) -> anyhow::Result<LocalCalculationDownloadArtifact> {
    let byte_size = std::fs::metadata(&local_path)?.len();
    Ok(LocalCalculationDownloadArtifact {
        metadata: CalculationDownloadArtifact {
            role: role.to_owned(),
            group: group.to_owned(),
            file_name: file_name.to_owned(),
            schema_version: SEMANTIC_DOWNLOAD_SCHEMA.to_owned(),
            media_type: media_type.to_owned(),
            sha256: sha256_file(&local_path)?,
            byte_size,
            record_count,
        },
        local_path,
    })
}

fn artifacts_of_kind<'a>(
    artifacts: &'a [LocalCalculationBundleArtifact],
    kind: &str,
) -> Vec<&'a LocalCalculationBundleArtifact> {
    artifacts
        .iter()
        .filter(|artifact| artifact.metadata.kind == kind)
        .collect()
}

fn portal_lcia_shards(
    artifacts: &[LocalCalculationBundleArtifact],
) -> anyhow::Result<Vec<PortalLciaShard>> {
    artifacts
        .iter()
        .filter(|artifact| artifact.metadata.kind == "lcia")
        .enumerate()
        .map(|(chunk_ordinal, artifact)| {
            Ok(PortalLciaShard {
                chunk_ordinal: u64::try_from(chunk_ordinal)?,
                first_process_ordinal: u64::try_from(
                    artifact.metadata.first_process_index.ok_or_else(|| {
                        anyhow::anyhow!("Calculation Bundle LCIA shard omitted first process index")
                    })?,
                )?,
                last_process_ordinal: u64::try_from(
                    artifact.metadata.last_process_index.ok_or_else(|| {
                        anyhow::anyhow!("Calculation Bundle LCIA shard omitted last process index")
                    })?,
                )?,
                sha256: artifact.metadata.sha256.clone(),
                uncompressed_sha256: artifact.metadata.uncompressed_sha256.clone().ok_or_else(
                    || anyhow::anyhow!("Calculation Bundle LCIA shard omitted plain SHA-256"),
                )?,
                byte_size: artifact.metadata.byte_size,
                uncompressed_byte_size: artifact.metadata.uncompressed_byte_size.ok_or_else(
                    || anyhow::anyhow!("Calculation Bundle LCIA shard omitted plain byte size"),
                )?,
                record_count: artifact.metadata.record_count,
                local_path: artifact.local_path.clone(),
            })
        })
        .collect()
}

#[allow(clippy::too_many_lines)]
fn portal_projection_axes(
    processes: &[ReleaseProcessRecord],
    impacts: &[ReleaseImpact],
    release_evidence: &CompiledReleaseEvidence,
) -> anyhow::Result<(Vec<PortalProcessSource>, Vec<PortalImpactSource>)> {
    let process_axis = processes
        .iter()
        .map(|process| {
            let source = portal_source_dataset(
                release_evidence,
                CompiledReleaseSourceDatasetType::Process,
                process.root_process.id,
                process.root_process.version.as_str(),
            )?;
            let reference_exchange = portal_reference_exchange(
                &source.document,
                process.quantitative_reference.exchange_internal_id.as_str(),
            )?;
            let description = reference_exchange
                .get("referenceToFlowDataSet")
                .and_then(|value| {
                    value
                        .get("common:shortDescription")
                        .or_else(|| value.get("shortDescription"))
                })
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "Portal LCIA functional-unit description is missing for process {}@{}",
                        process.root_process.id,
                        process.root_process.version
                    )
                })?;
            let pivot = process
                .quantitative_reference
                .pivot
                .as_ref()
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "Portal LCIA quantitative-reference pivot is missing for process {}@{}",
                        process.root_process.id,
                        process.root_process.version
                    )
                })?;
            let geography_code = portal_process_geography(&source.document).ok_or_else(|| {
                anyhow::anyhow!(
                    "Portal LCIA geography is missing for process {}@{}",
                    process.root_process.id,
                    process.root_process.version
                )
            })?;
            let reference_year =
                portal_process_reference_year(&source.document).ok_or_else(|| {
                    anyhow::anyhow!(
                        "Portal LCIA reference year is missing for process {}@{}",
                        process.root_process.id,
                        process.root_process.version
                    )
                })?;
            Ok(PortalProcessSource {
                process_index: u64::try_from(process.process_index)?,
                process_id: process.root_process.id,
                process_version: process.root_process.version.clone(),
                process_document_sha256: source.document_sha256.clone(),
                reference_flow_id: process.quantitative_reference.flow.id,
                reference_flow_version: process.quantitative_reference.flow.version.clone(),
                reference_exchange_internal_id: process
                    .quantitative_reference
                    .exchange_internal_id
                    .clone(),
                reference_flow_amount: pivot.raw_mean_amount,
                reference_flow_direction: match pivot.raw_direction {
                    CompiledExchangeDirection::Input => "input",
                    CompiledExchangeDirection::Output => "output",
                }
                .to_owned(),
                functional_unit_amount: process.quantitative_reference.mean_amount,
                functional_unit_unit: process.quantitative_reference.reference_unit.clone(),
                functional_unit_description: portal_localized_text(description)?,
                geography_precision: portal_geography_precision(geography_code.as_str()).to_owned(),
                geography_code,
                reference_year,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    let impact_axis = impacts
        .iter()
        .map(|impact| {
            let source = portal_source_dataset(
                release_evidence,
                CompiledReleaseSourceDatasetType::LciaMethod,
                impact.id,
                impact.version.as_str(),
            )?;
            let name = portal_lcia_method_name(&source.document).ok_or_else(|| {
                anyhow::anyhow!(
                    "Portal LCIA method name is missing for {}@{}",
                    impact.id,
                    impact.version
                )
            })?;
            let impact_name = portal_localized_text(name)?;
            if !impact_name
                .iter()
                .any(|value| value.value.trim() == impact.name.trim())
            {
                return Err(anyhow::anyhow!(
                    "Portal LCIA method name drift for {}@{}: certified={} frozen={:?}",
                    impact.id,
                    impact.version,
                    impact.name,
                    impact_name
                ));
            }
            Ok(PortalImpactSource {
                impact_index: u64::try_from(impact.index)?,
                method_id: impact.id,
                method_version: impact.version.clone(),
                method_document_sha256: source.document_sha256.clone(),
                impact_category_id: impact.key.clone(),
                impact_name,
                result_unit: impact.unit.clone(),
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok((process_axis, impact_axis))
}

fn portal_source_dataset<'a>(
    release_evidence: &'a CompiledReleaseEvidence,
    dataset_type: CompiledReleaseSourceDatasetType,
    id: Uuid,
    version: &str,
) -> anyhow::Result<&'a CompiledReleaseSourceDataset> {
    let mut matches = release_evidence.source_datasets.iter().filter(|source| {
        source.dataset_type == dataset_type
            && source.dataset_id == id
            && source.dataset_version == version
    });
    let source = matches.next().ok_or_else(|| {
        anyhow::anyhow!(
            "Portal LCIA frozen source document is missing for {}:{id}@{version}",
            dataset_type.as_str()
        )
    })?;
    if matches.next().is_some() {
        return Err(anyhow::anyhow!(
            "Portal LCIA frozen source identity is duplicated for {}:{id}@{version}",
            dataset_type.as_str()
        ));
    }
    Ok(source)
}

fn portal_reference_exchange<'a>(
    process_document: &'a Value,
    internal_id: &str,
) -> anyhow::Result<&'a Value> {
    let exchanges = process_document
        .get("processDataSet")
        .and_then(|value| value.get("exchanges"))
        .and_then(|value| value.get("exchange"))
        .ok_or_else(|| anyhow::anyhow!("Portal LCIA process exchanges are missing"))?;
    let mut matches = match exchanges {
        Value::Array(items) => items
            .iter()
            .filter(|item| {
                item.get("@dataSetInternalID").and_then(Value::as_str) == Some(internal_id)
            })
            .collect::<Vec<_>>(),
        Value::Object(_)
            if exchanges.get("@dataSetInternalID").and_then(Value::as_str) == Some(internal_id) =>
        {
            vec![exchanges]
        }
        _ => Vec::new(),
    };
    if matches.len() != 1 {
        return Err(anyhow::anyhow!(
            "Portal LCIA reference exchange must resolve exactly once: internalId={internal_id} matches={}",
            matches.len()
        ));
    }
    Ok(matches.remove(0))
}

fn portal_process_geography(document: &Value) -> Option<String> {
    document
        .get("processDataSet")?
        .get("processInformation")?
        .get("geography")?
        .get("locationOfOperationSupplyOrProduction")?
        .get("@location")?
        .as_str()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn portal_process_reference_year(document: &Value) -> Option<i32> {
    let value = document
        .get("processDataSet")?
        .get("processInformation")?
        .get("time")?
        .get("common:referenceYear")?;
    match value {
        Value::Number(number) => number.as_i64().and_then(|year| i32::try_from(year).ok()),
        Value::String(text) => text.trim().parse().ok(),
        _ => None,
    }
}

fn portal_geography_precision(code: &str) -> &'static str {
    if code.len() == 2 && code.bytes().all(|byte| byte.is_ascii_alphabetic()) {
        "country"
    } else if code.contains('-') {
        "province"
    } else {
        "other"
    }
}

fn portal_lcia_method_name(document: &Value) -> Option<&Value> {
    [
        ("LCIAMethodInformation", "dataSetInformation"),
        ("methodInformation", "dataSetInformation"),
        ("methodInfo", "dataSetInformation"),
        ("methodInfo", "dataSetInfo"),
    ]
    .into_iter()
    .find_map(|(information, data_set_information)| {
        let data = document
            .get("LCIAMethodDataSet")?
            .get(information)?
            .get(data_set_information)?;
        data.get("name")
            .and_then(|name| name.get("baseName").or(Some(name)))
    })
}

fn portal_localized_text(value: &Value) -> anyhow::Result<Vec<PortalLocalizedText>> {
    let values = match value {
        Value::Array(values) => values.iter().collect::<Vec<_>>(),
        _ => vec![value],
    };
    values
        .into_iter()
        .map(|value| {
            let (language, text) = match value {
                Value::String(text) => ("und", text.as_str()),
                Value::Object(object) => {
                    let language = object
                        .get("@xml:lang")
                        .or_else(|| object.get("@lang"))
                        .and_then(Value::as_str)
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .unwrap_or("und");
                    let text = object
                        .get("#text")
                        .or_else(|| object.get("value"))
                        .and_then(Value::as_str)
                        .ok_or_else(|| {
                            anyhow::anyhow!("Portal LCIA localized text omitted value")
                        })?;
                    (language, text)
                }
                _ => {
                    return Err(anyhow::anyhow!(
                        "Portal LCIA localized text must be text or a language-tagged object"
                    ));
                }
            };
            let text = text.trim();
            if text.is_empty() {
                return Err(anyhow::anyhow!("Portal LCIA localized text omitted value"));
            }
            Ok(PortalLocalizedText {
                language: language.to_owned(),
                value: text.to_owned(),
            })
        })
        .collect()
}

fn visit_gzip_ndjson<T, F>(path: &Path, mut visitor: F) -> anyhow::Result<u64>
where
    T: DeserializeOwned,
    F: FnMut(T) -> anyhow::Result<()>,
{
    let decoder = GzDecoder::new(File::open(path)?);
    let mut count = 0_u64;
    for line in BufReader::new(decoder).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        visitor(serde_json::from_str(&line)?)?;
        count = count.saturating_add(1);
    }
    Ok(count)
}

fn lcia_row(
    record: &LciaRecord,
    processes: &[ReleaseProcessRecord],
    impacts: &BTreeMap<(Uuid, String), &ReleaseImpact>,
) -> anyhow::Result<Vec<String>> {
    let process = processes.get(record.process_index).ok_or_else(|| {
        anyhow::anyhow!("LCIA export process index is outside the certified axis")
    })?;
    let impact = impacts
        .get(&(record.method.id, record.method.version.clone()))
        .ok_or_else(|| anyhow::anyhow!("LCIA export method is outside the certified axis"))?;
    Ok(vec![
        record.process_index.to_string(),
        process.root_process.id.to_string(),
        process.root_process.version.clone(),
        process.quantitative_reference.flow.id.to_string(),
        process.quantitative_reference.flow.version.clone(),
        process.quantitative_reference.reference_unit.clone(),
        record.method.id.to_string(),
        record.method.version.clone(),
        impact.key.clone(),
        impact.name.clone(),
        impact.unit.clone(),
        record.mean_amount.to_string(),
    ])
}

const LCIA_EXPORT_HEADERS: [&str; 12] = [
    "process_index",
    "process_uuid",
    "process_version",
    "reference_flow_uuid",
    "reference_flow_version",
    "reference_unit",
    "lcia_method_uuid",
    "lcia_method_version",
    "lcia_method_key",
    "lcia_method_name",
    "result_unit",
    "mean_amount",
];

const LCI_EXPORT_HEADERS: [&str; 9] = [
    "process_index",
    "process_uuid",
    "process_version",
    "flow_uuid",
    "flow_version",
    "direction",
    "unit",
    "location",
    "mean_amount",
];

fn impact_lookup(impacts: &[ReleaseImpact]) -> BTreeMap<(Uuid, String), &ReleaseImpact> {
    impacts
        .iter()
        .map(|impact| ((impact.id, impact.version.clone()), impact))
        .collect()
}

fn write_lcia_csv_zip(
    path: &Path,
    artifacts: &[&LocalCalculationBundleArtifact],
    processes: &[ReleaseProcessRecord],
    impacts: &[ReleaseImpact],
) -> anyhow::Result<u64> {
    let mut zip = ZipWriter::new(File::create(path)?);
    zip.start_file(
        "lcia-results.csv",
        SimpleFileOptions::default().compression_method(CompressionMethod::Deflated),
    )?;
    write_csv_row(&mut zip, LCIA_EXPORT_HEADERS)?;
    let impact_lookup = impact_lookup(impacts);
    let mut total = 0_u64;
    for artifact in artifacts {
        let observed = visit_gzip_ndjson::<LciaRecord, _>(&artifact.local_path, |record| {
            write_csv_row(&mut zip, lcia_row(&record, processes, &impact_lookup)?)
        })?;
        require_record_count(artifact, observed)?;
        total = total.saturating_add(observed);
    }
    zip.finish()?;
    Ok(total)
}

fn write_lci_csv_zip(
    path: &Path,
    artifacts: &[&LocalCalculationBundleArtifact],
    processes: &[ReleaseProcessRecord],
) -> anyhow::Result<u64> {
    let mut zip = ZipWriter::new(File::create(path)?);
    let options = SimpleFileOptions::default().compression_method(CompressionMethod::Deflated);
    let mut total = 0_u64;
    for (index, artifact) in artifacts.iter().enumerate() {
        zip.start_file(format!("lci-inventory-{index:06}.csv"), options)?;
        write_csv_row(&mut zip, LCI_EXPORT_HEADERS)?;
        let observed =
            visit_gzip_ndjson::<OwnedInventoryRecord, _>(&artifact.local_path, |record| {
                write_csv_row(&mut zip, lci_row(&record, processes)?)
            })?;
        require_record_count(artifact, observed)?;
        total = total.saturating_add(observed);
    }
    zip.finish()?;
    Ok(total)
}

fn lci_row(
    record: &OwnedInventoryRecord,
    processes: &[ReleaseProcessRecord],
) -> anyhow::Result<Vec<String>> {
    let process = processes
        .get(record.process_index)
        .ok_or_else(|| anyhow::anyhow!("LCI export process index is outside the certified axis"))?;
    Ok(vec![
        record.process_index.to_string(),
        process.root_process.id.to_string(),
        process.root_process.version.clone(),
        record.flow.id.to_string(),
        record.flow.version.clone(),
        direction_name(record.direction).to_owned(),
        record.unit.clone(),
        record.location.clone().unwrap_or_default(),
        record.mean_amount.to_string(),
    ])
}

fn direction_name(direction: CompiledExchangeDirection) -> &'static str {
    match direction {
        CompiledExchangeDirection::Input => "Input",
        CompiledExchangeDirection::Output => "Output",
    }
}

fn write_csv_row<W, I, S>(writer: &mut W, values: I) -> anyhow::Result<()>
where
    W: Write,
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut first = true;
    for value in values {
        if !first {
            writer.write_all(b",")?;
        }
        first = false;
        let value = value.as_ref();
        if value
            .bytes()
            .any(|byte| matches!(byte, b',' | b'"' | b'\n' | b'\r'))
        {
            writer.write_all(b"\"")?;
            writer.write_all(value.replace('"', "\"\"").as_bytes())?;
            writer.write_all(b"\"")?;
        } else {
            writer.write_all(value.as_bytes())?;
        }
    }
    writer.write_all(b"\n")?;
    Ok(())
}

fn require_record_count(
    artifact: &LocalCalculationBundleArtifact,
    observed: u64,
) -> anyhow::Result<()> {
    if artifact.metadata.record_count != observed {
        return Err(anyhow::anyhow!(
            "Calculation Bundle {} record count mismatch: expected={} observed={observed}",
            artifact.metadata.path,
            artifact.metadata.record_count
        ));
    }
    Ok(())
}

fn write_lcia_xlsx(
    path: &Path,
    artifacts: &[&LocalCalculationBundleArtifact],
    processes: &[ReleaseProcessRecord],
    impacts: &[ReleaseImpact],
) -> anyhow::Result<u64> {
    let mut zip = ZipWriter::new(File::create(path)?);
    let options = SimpleFileOptions::default().compression_method(CompressionMethod::Deflated);
    zip.start_file("[Content_Types].xml", options)?;
    zip.write_all(br#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/></Types>"#)?;
    zip.start_file("_rels/.rels", options)?;
    zip.write_all(br#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>"#)?;
    zip.start_file("xl/workbook.xml", options)?;
    zip.write_all(br#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="LCIA Results" sheetId="1" r:id="rId1"/></sheets></workbook>"#)?;
    zip.start_file("xl/_rels/workbook.xml.rels", options)?;
    zip.write_all(br#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/></Relationships>"#)?;
    zip.start_file("xl/worksheets/sheet1.xml", options)?;
    zip.write_all(br#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>"#)?;
    write_xlsx_text_row(&mut zip, 1, LCIA_EXPORT_HEADERS)?;
    let impact_lookup = impact_lookup(impacts);
    let mut total = 0_u64;
    for artifact in artifacts {
        let observed = visit_gzip_ndjson::<LciaRecord, _>(&artifact.local_path, |record| {
            total = total.saturating_add(1);
            if total > XLSX_MAX_DATA_ROWS {
                return Err(anyhow::anyhow!(
                    "LCIA results exceed the Excel worksheet row limit"
                ));
            }
            let row = lcia_row(&record, processes, &impact_lookup)?;
            write_xlsx_lcia_row(&mut zip, usize::try_from(total)?.saturating_add(1), &row)
        })?;
        require_record_count(artifact, observed)?;
    }
    zip.write_all(b"</sheetData></worksheet>")?;
    zip.finish()?;
    Ok(total)
}

fn write_xlsx_text_row<W, I, S>(writer: &mut W, row: usize, values: I) -> anyhow::Result<()>
where
    W: Write,
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    write!(writer, "<row r=\"{row}\">")?;
    for (column, value) in values.into_iter().enumerate() {
        let reference = format!("{}{}", xlsx_column_name(column), row);
        write!(
            writer,
            "<c r=\"{reference}\" t=\"inlineStr\"><is><t>{}</t></is></c>",
            xml_escape(value.as_ref())
        )?;
    }
    writer.write_all(b"</row>")?;
    Ok(())
}

fn write_xlsx_lcia_row<W: Write>(
    writer: &mut W,
    row: usize,
    values: &[String],
) -> anyhow::Result<()> {
    write!(writer, "<row r=\"{row}\">")?;
    for (column, value) in values.iter().enumerate() {
        let reference = format!("{}{}", xlsx_column_name(column), row);
        if column == 0 || column == values.len() - 1 {
            write!(writer, "<c r=\"{reference}\"><v>{value}</v></c>")?;
        } else {
            write!(
                writer,
                "<c r=\"{reference}\" t=\"inlineStr\"><is><t>{}</t></is></c>",
                xml_escape(value)
            )?;
        }
    }
    writer.write_all(b"</row>")?;
    Ok(())
}

fn xlsx_column_name(mut index: usize) -> String {
    let mut output = String::new();
    loop {
        output.insert(
            0,
            char::from(b'A' + u8::try_from(index % 26).unwrap_or_default()),
        );
        if index < 26 {
            break;
        }
        index = index / 26 - 1;
    }
    output
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

struct LciParquetBatch {
    process_index: Vec<u64>,
    process_uuid: Vec<String>,
    process_version: Vec<String>,
    flow_uuid: Vec<String>,
    flow_version: Vec<String>,
    direction: Vec<String>,
    unit: Vec<String>,
    location: Vec<String>,
    mean_amount: Vec<f64>,
}

impl LciParquetBatch {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            process_index: Vec::with_capacity(capacity),
            process_uuid: Vec::with_capacity(capacity),
            process_version: Vec::with_capacity(capacity),
            flow_uuid: Vec::with_capacity(capacity),
            flow_version: Vec::with_capacity(capacity),
            direction: Vec::with_capacity(capacity),
            unit: Vec::with_capacity(capacity),
            location: Vec::with_capacity(capacity),
            mean_amount: Vec::with_capacity(capacity),
        }
    }

    fn len(&self) -> usize {
        self.process_index.len()
    }

    fn push(&mut self, record: OwnedInventoryRecord, process: &ReleaseProcessRecord) {
        self.process_index.push(record.process_index as u64);
        self.process_uuid.push(process.root_process.id.to_string());
        self.process_version
            .push(process.root_process.version.clone());
        self.flow_uuid.push(record.flow.id.to_string());
        self.flow_version.push(record.flow.version);
        self.direction
            .push(direction_name(record.direction).to_owned());
        self.unit.push(record.unit);
        self.location.push(record.location.unwrap_or_default());
        self.mean_amount.push(record.mean_amount);
    }

    fn take_record_batch(&mut self, schema: Arc<Schema>) -> anyhow::Result<RecordBatch> {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(UInt64Array::from(std::mem::take(&mut self.process_index))),
            Arc::new(StringArray::from(std::mem::take(&mut self.process_uuid))),
            Arc::new(StringArray::from(std::mem::take(&mut self.process_version))),
            Arc::new(StringArray::from(std::mem::take(&mut self.flow_uuid))),
            Arc::new(StringArray::from(std::mem::take(&mut self.flow_version))),
            Arc::new(StringArray::from(std::mem::take(&mut self.direction))),
            Arc::new(StringArray::from(std::mem::take(&mut self.unit))),
            Arc::new(StringArray::from(std::mem::take(&mut self.location))),
            Arc::new(Float64Array::from(std::mem::take(&mut self.mean_amount))),
        ];
        Ok(RecordBatch::try_new(schema, arrays)?)
    }
}

fn write_lci_parquet(
    path: &Path,
    artifacts: &[&LocalCalculationBundleArtifact],
    processes: &[ReleaseProcessRecord],
) -> anyhow::Result<u64> {
    let schema = Arc::new(Schema::new(
        LCI_EXPORT_HEADERS
            .iter()
            .enumerate()
            .map(|(index, name)| {
                Field::new(
                    *name,
                    if index == 0 {
                        DataType::UInt64
                    } else if index == LCI_EXPORT_HEADERS.len() - 1 {
                        DataType::Float64
                    } else {
                        DataType::Utf8
                    },
                    false,
                )
            })
            .collect::<Vec<_>>(),
    ));
    let properties = WriterProperties::builder()
        .set_compression(ParquetCompression::ZSTD(ZstdLevel::default()))
        .build();
    let mut writer =
        ArrowWriter::try_new(File::create(path)?, Arc::clone(&schema), Some(properties))?;
    let mut batch = LciParquetBatch::with_capacity(50_000);
    let mut total = 0_u64;
    for artifact in artifacts {
        let observed =
            visit_gzip_ndjson::<OwnedInventoryRecord, _>(&artifact.local_path, |record| {
                let process = processes.get(record.process_index).ok_or_else(|| {
                    anyhow::anyhow!("LCI Parquet process index is outside the certified axis")
                })?;
                batch.push(record, process);
                total = total.saturating_add(1);
                if batch.len() >= 50_000 {
                    writer.write(&batch.take_record_batch(Arc::clone(&schema))?)?;
                }
                Ok(())
            })?;
        require_record_count(artifact, observed)?;
    }
    if batch.len() > 0 {
        writer.write(&batch.take_record_batch(schema)?)?;
    }
    writer.close()?;
    Ok(total)
}

fn write_audit_archive(
    path: &Path,
    manifest_path: &Path,
    artifacts: &[LocalCalculationBundleArtifact],
) -> anyhow::Result<()> {
    let mut zip = ZipWriter::new(File::create(path)?);
    let options = SimpleFileOptions::default().compression_method(CompressionMethod::Stored);
    zip.start_file("calculation-bundle.json", options)?;
    std::io::copy(&mut File::open(manifest_path)?, &mut zip)?;
    for artifact in artifacts {
        zip.start_file(artifact.metadata.path.as_str(), options)?;
        std::io::copy(&mut File::open(&artifact.local_path)?, &mut zip)?;
    }
    zip.finish()?;
    Ok(())
}

fn validate_impacts(items: &[SnapshotImpactMapEntry]) -> anyhow::Result<Vec<ReleaseImpact>> {
    if items.is_empty() {
        return Err(anyhow::anyhow!(
            "Calculation Bundle requires a non-empty certified impact axis"
        ));
    }
    let mut impacts = items
        .iter()
        .map(|impact| {
            let index = usize::try_from(impact.impact_index)
                .map_err(|_| anyhow::anyhow!("negative impact index"))?;
            let version = impact
                .impact_version
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("Calculation Bundle impact version is missing"))?;
            validate_version(version, "impact.version")?;
            Ok(ReleaseImpact {
                index,
                id: impact.impact_id,
                version: version.to_owned(),
                key: impact.impact_key.clone(),
                name: impact.impact_name.clone(),
                unit: impact.unit.clone(),
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    impacts.sort_unstable_by_key(|impact| impact.index);
    for (expected, impact) in impacts.iter().enumerate() {
        if impact.index != expected {
            return Err(anyhow::anyhow!(
                "Calculation Bundle impact index gap: expected={expected} got={}",
                impact.index
            ));
        }
    }
    let reviewed = RELEASE_METHOD_IDENTITIES
        .iter()
        .map(|(method_id, version, _)| ((*method_id).to_owned(), (*version).to_owned()))
        .collect::<BTreeSet<_>>();
    let mut selected = BTreeSet::new();
    for impact in &impacts {
        let identity = (impact.id.to_string(), impact.version.clone());
        if !reviewed.contains(&identity) {
            return Err(anyhow::anyhow!(
                "Calculation Bundle impact identity is not in the reviewed method catalog: {}@{}",
                impact.id,
                impact.version
            ));
        }
        if !selected.insert(identity) {
            return Err(anyhow::anyhow!(
                "Calculation Bundle impact identity is duplicated: {}@{}",
                impact.id,
                impact.version
            ));
        }
    }
    Ok(impacts)
}

fn validate_inventory_exchange(
    exchange: &CompiledReleaseInventoryExchange,
    process_count: usize,
) -> anyhow::Result<()> {
    let process_index = usize::try_from(exchange.process_idx)
        .map_err(|_| anyhow::anyhow!("negative inventory process index"))?;
    if process_index >= process_count {
        return Err(anyhow::anyhow!(
            "inventory process index is outside process axis"
        ));
    }
    validate_version(&exchange.flow_version, "inventory.flow.version")?;
    require_nonempty(&exchange.unit, "inventory.unit")?;
    require_nonempty(
        &exchange.allocation_target_internal_id,
        "inventory.allocationTargetInternalId",
    )?;
    ensure_finite(exchange.allocation_fraction, "inventory.allocationFraction")?;
    ensure_finite(exchange.normalized_mean_amount, "inventory.meanAmount")
}

fn inventory_record(
    exchange: &CompiledReleaseInventoryExchange,
) -> anyhow::Result<InventoryRecord<'_>> {
    Ok(InventoryRecord {
        process_index: usize::try_from(exchange.process_idx)?,
        exchange_internal_id: exchange.exchange_internal_id.as_deref(),
        flow: GlobalReference {
            id: exchange.flow_id,
            version: exchange.flow_version.clone(),
        },
        direction: exchange.direction,
        unit: exchange.unit.as_str(),
        location: exchange.location.as_deref(),
        mean_amount: exchange.normalized_mean_amount,
        allocation_target_internal_id: exchange.allocation_target_internal_id.as_str(),
        allocation_fraction: exchange.allocation_fraction,
    })
}

fn inventory_exchange_order(
    left: &CompiledReleaseInventoryExchange,
    right: &CompiledReleaseInventoryExchange,
) -> std::cmp::Ordering {
    left.process_idx
        .cmp(&right.process_idx)
        .then_with(|| left.direction.cmp(&right.direction))
        .then_with(|| left.flow_id.cmp(&right.flow_id))
        .then_with(|| left.flow_version.cmp(&right.flow_version))
        .then_with(|| left.unit.cmp(&right.unit))
        .then_with(|| left.location.cmp(&right.location))
        .then_with(|| left.exchange_internal_id.cmp(&right.exchange_internal_id))
}

fn source_dataset_order(
    left: &CompiledReleaseSourceDataset,
    right: &CompiledReleaseSourceDataset,
) -> std::cmp::Ordering {
    left.dataset_type
        .cmp(&right.dataset_type)
        .then_with(|| left.dataset_id.cmp(&right.dataset_id))
        .then_with(|| left.dataset_version.cmp(&right.dataset_version))
}

fn source_dataset_path(dataset: &CompiledReleaseSourceDataset) -> String {
    format!(
        "{}/{}_{}.json",
        dataset.dataset_type.directory(),
        dataset.dataset_id,
        dataset.dataset_version
    )
}

fn validate_source_datasets(release_evidence: &CompiledReleaseEvidence) -> anyhow::Result<()> {
    if release_evidence.source_datasets.is_empty() {
        return Err(anyhow::anyhow!(
            "snapshot lacks frozen source closure evidence; rebuild the snapshot"
        ));
    }
    let expected_processes = release_evidence
        .processes
        .iter()
        .map(|process| (process.process_id, process.process_version.clone()))
        .collect::<BTreeSet<_>>();
    let mut observed_processes = BTreeSet::new();
    let mut keys = BTreeSet::new();
    for dataset in &release_evidence.source_datasets {
        validate_version(&dataset.dataset_version, "sourceClosure.dataset.version")?;
        validate_sha256(&dataset.document_sha256, "sourceClosure.dataset.sha256")?;
        if !dataset.document.is_object() {
            return Err(anyhow::anyhow!(
                "source closure document must be an object for {}:{}@{}",
                dataset.dataset_type.as_str(),
                dataset.dataset_id,
                dataset.dataset_version
            ));
        }
        let document_id = dataset
            .dataset_type
            .document_uuid(&dataset.document)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "source closure document is missing canonical common:UUID for {}:{}@{}",
                    dataset.dataset_type.as_str(),
                    dataset.dataset_id,
                    dataset.dataset_version
                )
            })?;
        let document_id = Uuid::parse_str(document_id).map_err(|_| {
            anyhow::anyhow!(
                "source closure document has invalid common:UUID for {}:{}@{}",
                dataset.dataset_type.as_str(),
                dataset.dataset_id,
                dataset.dataset_version
            )
        })?;
        if document_id != dataset.dataset_id {
            return Err(anyhow::anyhow!(
                "source closure document identity mismatch for {}:{}@{}: document={document_id}",
                dataset.dataset_type.as_str(),
                dataset.dataset_id,
                dataset.dataset_version
            ));
        }
        if !keys.insert((
            dataset.dataset_type,
            dataset.dataset_id,
            dataset.dataset_version.clone(),
        )) {
            return Err(anyhow::anyhow!(
                "duplicate source closure dataset {}:{}@{}",
                dataset.dataset_type.as_str(),
                dataset.dataset_id,
                dataset.dataset_version
            ));
        }
        match (dataset.dataset_type, dataset.role) {
            (
                CompiledReleaseSourceDatasetType::Process,
                CompiledReleaseSourceDatasetRole::UnitProcess,
            ) => {
                observed_processes.insert((dataset.dataset_id, dataset.dataset_version.clone()));
            }
            (CompiledReleaseSourceDatasetType::Process, _) => {
                return Err(anyhow::anyhow!(
                    "source closure Process must have unit_process role"
                ));
            }
            (_, CompiledReleaseSourceDatasetRole::Support) => {}
            (_, CompiledReleaseSourceDatasetRole::UnitProcess) => {
                return Err(anyhow::anyhow!(
                    "only source closure Process documents may have unit_process role"
                ));
            }
        }
    }
    if observed_processes != expected_processes {
        return Err(anyhow::anyhow!(
            "source closure Process identities differ from the Calculation Bundle process axis"
        ));
    }
    Ok(())
}

fn bundle_content_hash(manifest: &CalculationBundleManifest) -> anyhow::Result<String> {
    let mut value = serde_json::to_value(manifest)?;
    value
        .as_object_mut()
        .ok_or_else(|| anyhow::anyhow!("Calculation Bundle manifest must be an object"))?
        .remove("bundleContentHash");
    canonical_sha256(&value)
}

fn canonical_sha256<T: Serialize>(value: &T) -> anyhow::Result<String> {
    Ok(sha256_bytes(canonical_json_bytes(value)?.as_slice()))
}

fn canonical_json_bytes<T: Serialize>(value: &T) -> anyhow::Result<Vec<u8>> {
    let bytes = serde_json::to_vec(value)?;
    let parsed: Value = serde_json::from_slice(bytes.as_slice())?;
    reject_non_finite_json(&parsed)?;
    Ok(serde_json::to_vec(&parsed)?)
}

fn reject_non_finite_json(value: &Value) -> anyhow::Result<()> {
    match value {
        Value::Array(items) => {
            for item in items {
                reject_non_finite_json(item)?;
            }
        }
        Value::Object(items) => {
            for item in items.values() {
                reject_non_finite_json(item)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn sha256_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn sha256_file(path: &Path) -> anyhow::Result<String> {
    let mut reader = BufReader::new(File::open(path)?);
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex::encode(hasher.finalize()))
}

fn validate_sha256(value: &str, field: &str) -> anyhow::Result<()> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "{field} must be a lowercase SHA-256 digest"
        ))
    }
}

fn validate_version(value: &str, field: &str) -> anyhow::Result<()> {
    let bytes = value.as_bytes();
    if bytes.len() == 9
        && bytes[2] == b'.'
        && bytes[5] == b'.'
        && bytes
            .iter()
            .enumerate()
            .all(|(index, byte)| index == 2 || index == 5 || byte.is_ascii_digit())
    {
        Ok(())
    } else {
        Err(anyhow::anyhow!("{field} is not an ILCD version: {value}"))
    }
}

fn require_nonempty(value: &str, field: &str) -> anyhow::Result<()> {
    if value.trim().is_empty() {
        Err(anyhow::anyhow!("{field} must not be empty"))
    } else {
        Ok(())
    }
}

fn ensure_finite(value: f64, field: &str) -> anyhow::Result<()> {
    if value.is_finite() {
        Ok(())
    } else {
        Err(anyhow::anyhow!("{field} must be finite"))
    }
}

fn ensure_finite_nonzero(value: f64, field: &str) -> anyhow::Result<()> {
    ensure_finite(value, field)?;
    if value == 0.0 {
        Err(anyhow::anyhow!("{field} must be non-zero"))
    } else {
        Ok(())
    }
}

fn ensure_nearly_equal(actual: f64, expected: f64, field: &str) -> anyhow::Result<()> {
    ensure_finite(actual, field)?;
    ensure_finite(expected, field)?;
    let tolerance = 1.0e-12 * actual.abs().max(expected.abs()).max(1.0);
    if (actual - expected).abs() <= tolerance {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "{field} is inconsistent: actual={actual} expected={expected} tolerance={tolerance}"
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use flate2::read::GzDecoder;
    use solver_core::FactorizationState;

    use super::*;
    use crate::{
        compiled_graph::{CompiledReleaseProcess, CompiledReleaseTechnosphereEdge},
        snapshot_index::{SnapshotImpactMapEntry, SnapshotProcessMapEntry},
    };

    fn reviewed_impact(method_index: usize, impact_index: i32) -> SnapshotImpactMapEntry {
        let (method_id, version, _) = RELEASE_METHOD_IDENTITIES[method_index];
        SnapshotImpactMapEntry {
            impact_id: Uuid::parse_str(method_id).unwrap(),
            impact_index,
            impact_version: Some(version.to_owned()),
            impact_key: format!("method:{method_index}"),
            impact_name: format!("Method {method_index}"),
            unit: "kg".to_owned(),
        }
    }

    #[allow(clippy::too_many_lines)]
    fn fixture_writer() -> CalculationBundleWriter {
        let method_indices = (0..RELEASE_METHOD_IDENTITIES.len()).collect::<Vec<_>>();
        fixture_writer_with_method_indices(&method_indices)
    }

    #[allow(clippy::too_many_lines)]
    fn fixture_writer_with_method_indices(method_indices: &[usize]) -> CalculationBundleWriter {
        fixture_writer_with_pivot(method_indices, CompiledExchangeDirection::Output, 1.0)
    }

    #[allow(clippy::too_many_lines)]
    fn fixture_writer_with_pivot(
        method_indices: &[usize],
        reference_direction: CompiledExchangeDirection,
        raw_reference_amount: f64,
    ) -> CalculationBundleWriter {
        let process_id = Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap();
        let flow_id = Uuid::parse_str("22222222-2222-4222-8222-222222222222").unwrap();
        let elementary_id = Uuid::parse_str("33333333-3333-4333-8333-333333333333").unwrap();
        let snapshot_id = Uuid::parse_str("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa").unwrap();
        let config: SnapshotBuildConfig = serde_json::from_value(json!({
            "process_states": "100",
            "selection_mode": "filtered_library",
            "request_roots": [],
            "process_limit": 0,
            "provider_rule": "split_by_process_volume",
            "provider_candidate_eligibility_mode": "reference_output_only",
            "reference_normalization_mode": "strict",
            "allocation_fraction_mode": "strict",
            "allocation_semantics_version": "tidas-quantitative-reference-v2",
            "biosphere_sign_mode": "gross",
            "self_loop_cutoff": 0.999_999,
            "singular_eps": 1e-12,
            "has_lcia": true,
            "method_id": null,
            "method_version": null
        }))
        .unwrap();
        let method_count = method_indices.len();
        let coverage: SnapshotCoverageReport = serde_json::from_value(json!({
            "schema_version": "snapshot_coverage.v2",
            "matching": {
                "input_edges_total": 0,
                "matched_unique_provider": 0,
                "matched_multi_provider": 0,
                "unmatched_no_provider": 0,
                "unique_provider_match_pct": 100.0,
                "any_provider_match_pct": 100.0
            },
            "reference": {
                "process_total": 1,
                "normalized_process_count": 1,
                "missing_reference_count": 0,
                "invalid_reference_count": 0
            },
            "allocation": {
                "exchange_total": 1,
                "allocation_fraction_present_pct": 100.0,
                "allocation_fraction_missing_count": 0,
                "allocation_fraction_invalid_count": 0
            },
            "singular_risk": {
                "risk_level": "low",
                "prefilter_diag_abs_ge_cutoff": 0,
                "postfilter_a_diag_abs_ge_cutoff": 0,
                "m_zero_diagonal_count": 0,
                "m_min_abs_diagonal": 1.0
            },
            "matrix_scale": {
                "process_count": 1,
                "flow_count": 1,
                "impact_count": method_count,
                "a_nnz": 0,
                "b_nnz": 1,
                "c_nnz": method_count,
                "m_nnz_estimated": 1,
                "m_sparsity_estimated": 1.0
            }
        }))
        .unwrap();
        let impact_map = method_indices
            .iter()
            .enumerate()
            .map(|(impact_index, method_index)| {
                reviewed_impact(*method_index, i32::try_from(impact_index).unwrap())
            })
            .collect();
        let index = SnapshotIndexDocument {
            version: 1,
            snapshot_id,
            process_count: 1,
            impact_count: i32::try_from(method_count).unwrap(),
            process_map: vec![SnapshotProcessMapEntry {
                process_id,
                process_index: 0,
                process_version: "01.00.000".to_owned(),
                process_name: None,
                location: None,
            }],
            impact_map,
            calculation_evidence: None,
        };
        let source_document = json!({
            "processDataSet": {
                "processInformation": {
                    "dataSetInformation": { "common:UUID": process_id.to_string() }
                }
            }
        });
        let source_document_sha256 = sha256_bytes(
            canonical_value_json_bytes(&source_document)
                .unwrap()
                .as_slice(),
        );
        let evidence = CompiledReleaseEvidence {
            processes: vec![CompiledReleaseProcess {
                process_idx: 0,
                process_id,
                process_version: "01.00.000".to_owned(),
                quantitative_reference_exchange_internal_id: "0".to_owned(),
                quantitative_reference_flow_id: flow_id,
                quantitative_reference_flow_version: "01.00.000".to_owned(),
                reference_unit: "kg".to_owned(),
                normalized_mean_amount: raw_reference_amount / raw_reference_amount.abs(),
                reference_direction: Some(reference_direction),
                raw_reference_amount: Some(raw_reference_amount),
                signed_raw_reference_coefficient: Some(
                    match reference_direction {
                        CompiledExchangeDirection::Input => -1.0,
                        CompiledExchangeDirection::Output => 1.0,
                    } * raw_reference_amount,
                ),
                normalized_reference_coefficient: Some(
                    (match reference_direction {
                        CompiledExchangeDirection::Input => -1.0,
                        CompiledExchangeDirection::Output => 1.0,
                    } * raw_reference_amount)
                        .signum(),
                ),
            }],
            inventory_exchanges: vec![CompiledReleaseInventoryExchange {
                process_idx: 0,
                exchange_internal_id: Some("1".to_owned()),
                flow_id: elementary_id,
                flow_version: "01.00.000".to_owned(),
                direction: CompiledExchangeDirection::Output,
                unit: "kg".to_owned(),
                location: Some("GLO".to_owned()),
                normalized_mean_amount: 0.25,
                allocation_target_internal_id: "0".to_owned(),
                allocation_fraction: 1.0,
                signed_normalized_coefficient: Some(0.25),
            }],
            technosphere_edges: vec![CompiledReleaseTechnosphereEdge {
                dependent_process_idx: 0,
                residual_exchange_internal_id: "input-1".to_owned(),
                balancing_process_idx: 0,
                balancing_reference_exchange_internal_id: "0".to_owned(),
                residual_coefficient: -2.0,
                reference_coefficient: 1.0,
                routing_weight: 0.25,
                activity_requirement: 0.5,
                flow_id,
                flow_version: "01.00.000".to_owned(),
                location: Some("GLO".to_owned()),
            }],
            biosphere_edges: vec![CompiledReleaseInventoryExchange {
                process_idx: 0,
                exchange_internal_id: Some("1".to_owned()),
                flow_id: elementary_id,
                flow_version: "01.00.000".to_owned(),
                direction: CompiledExchangeDirection::Output,
                unit: "kg".to_owned(),
                location: Some("GLO".to_owned()),
                normalized_mean_amount: 0.25,
                allocation_target_internal_id: "0".to_owned(),
                allocation_fraction: 1.0,
                signed_normalized_coefficient: Some(0.25),
            }],
            source_datasets: vec![CompiledReleaseSourceDataset {
                dataset_type: CompiledReleaseSourceDatasetType::Process,
                role: CompiledReleaseSourceDatasetRole::UnitProcess,
                dataset_id: process_id,
                dataset_version: "01.00.000".to_owned(),
                document_sha256: source_document_sha256,
                document: source_document,
            }],
            source_reference_provenance: None,
        };
        CalculationBundleWriter::new(
            Uuid::parse_str("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb").unwrap(),
            snapshot_id,
            "1".repeat(64),
            1,
            config,
            coverage,
            &index,
            &evidence,
        )
        .unwrap()
    }

    #[test]
    fn portal_localized_text_accepts_legacy_plain_and_untagged_values() {
        assert_eq!(
            portal_localized_text(&json!("Global warming")).unwrap(),
            vec![PortalLocalizedText {
                language: "und".to_owned(),
                value: "Global warming".to_owned(),
            }]
        );
        assert_eq!(
            portal_localized_text(&json!({ "#text": "one kilogram" })).unwrap(),
            vec![PortalLocalizedText {
                language: "und".to_owned(),
                value: "one kilogram".to_owned(),
            }]
        );
    }

    #[test]
    fn writes_directional_lci_lcia_and_stable_manifest() {
        let mut writer = fixture_writer();
        writer
            .write_result_chunk(
                0,
                &[SolveResult {
                    x: Some(vec![2.0]),
                    g: None,
                    h: Some((0..25).map(f64::from).collect()),
                    factorization_state: FactorizationState::Ready,
                }],
            )
            .unwrap();
        let built = writer.finish().unwrap();
        assert_eq!(built.manifest.schema_version, CALCULATION_BUNDLE_FORMAT);
        assert_eq!(built.manifest.artifacts.len(), 8);
        assert_eq!(built.bundle_content_hash.len(), 64);
        assert_eq!(built.downloads.len(), 5);
        assert_eq!(
            built
                .downloads
                .iter()
                .find(|artifact| artifact.metadata.role == "lcia_results_xlsx")
                .unwrap()
                .metadata
                .record_count,
            25
        );
        assert_eq!(
            built
                .downloads
                .iter()
                .find(|artifact| artifact.metadata.role == "lci_inventory_parquet")
                .unwrap()
                .metadata
                .record_count,
            1
        );
        let audit = built
            .downloads
            .iter()
            .find(|artifact| artifact.metadata.role == "calculation_evidence_bundle")
            .unwrap();
        let mut archive = zip::ZipArchive::new(File::open(&audit.local_path).unwrap()).unwrap();
        assert!(archive.by_name("calculation-bundle.json").is_ok());
        for artifact in &built.manifest.artifacts {
            let mut entry = archive.by_name(&artifact.path).unwrap();
            assert_eq!(entry.size(), artifact.byte_size);
            let mut bytes = Vec::new();
            entry.read_to_end(&mut bytes).unwrap();
            assert_eq!(sha256_bytes(&bytes), artifact.sha256);
        }

        let lci = built
            .artifacts
            .iter()
            .find(|artifact| artifact.metadata.kind == "lci")
            .unwrap();
        let mut decoder = GzDecoder::new(File::open(&lci.local_path).unwrap());
        let mut body = String::new();
        decoder.read_to_string(&mut body).unwrap();
        assert!(body.contains("\"meanAmount\":0.5"));
        assert!(body.contains("\"direction\":\"Output\""));

        let technosphere = built
            .artifacts
            .iter()
            .find(|artifact| artifact.metadata.kind == "technosphere_edges")
            .unwrap();
        let mut decoder = GzDecoder::new(File::open(&technosphere.local_path).unwrap());
        let mut body = String::new();
        decoder.read_to_string(&mut body).unwrap();
        assert!(body.contains("\"residualExchangeInternalId\":\"input-1\""));
        assert!(body.contains("\"balancingReferenceExchangeInternalId\":\"0\""));
        assert!(body.contains("\"routingWeight\":0.25"));
        assert!(body.contains("\"activityRequirement\":0.5"));

        let inventory = built
            .artifacts
            .iter()
            .find(|artifact| artifact.metadata.kind == "inventory_axis")
            .unwrap();
        let mut decoder = GzDecoder::new(File::open(&inventory.local_path).unwrap());
        let mut body = String::new();
        decoder.read_to_string(&mut body).unwrap();
        assert!(body.contains("\"allocationTargetInternalId\":\"0\""));
        assert!(body.contains("\"allocationFraction\":1.0"));

        let source_closure = built
            .artifacts
            .iter()
            .find(|artifact| artifact.metadata.kind == "source_closure")
            .unwrap();
        let mut decoder = GzDecoder::new(File::open(&source_closure.local_path).unwrap());
        let mut body = String::new();
        decoder.read_to_string(&mut body).unwrap();
        assert!(body.contains("\"schemaVersion\":\"tiangong.source-closure.dataset.v1\""));
        assert!(body.contains("\"datasetType\":\"process\""));
        assert!(body.contains("\"role\":\"unit_process\""));
        assert!(body.contains(
            "\"path\":\"processes/11111111-1111-4111-8111-111111111111_01.00.000.json\""
        ));
    }

    #[test]
    fn accepts_non_empty_reviewed_method_subsets_in_certified_axis_order() {
        let impacts = validate_impacts(&[
            reviewed_impact(7, 1),
            reviewed_impact(3, 0),
            reviewed_impact(20, 2),
        ])
        .unwrap();

        assert_eq!(impacts.len(), 3);
        assert_eq!(impacts[0].id, reviewed_impact(3, 0).impact_id);
        assert_eq!(impacts[1].id, reviewed_impact(7, 1).impact_id);
        assert_eq!(impacts[2].id, reviewed_impact(20, 2).impact_id);

        let single = validate_impacts(&[reviewed_impact(0, 0)]).unwrap();
        assert_eq!(single.len(), 1);
    }

    #[test]
    fn preserves_input_quantitative_reference_pivot_evidence() {
        let method_indices = (0..RELEASE_METHOD_IDENTITIES.len()).collect::<Vec<_>>();
        let writer =
            fixture_writer_with_pivot(&method_indices, CompiledExchangeDirection::Input, 2.5);
        let process_axis = writer
            .artifacts
            .iter()
            .find(|artifact| artifact.metadata.kind == "process_axis")
            .unwrap();
        assert_eq!(
            process_axis.metadata.schema_version,
            "tiangong.calculation-bundle.process-axis.v2"
        );
        let mut decoder = GzDecoder::new(File::open(&process_axis.local_path).unwrap());
        let mut body = String::new();
        decoder.read_to_string(&mut body).unwrap();
        assert!(body.contains("\"meanAmount\":1.0"));
        assert!(body.contains("\"rawDirection\":\"Input\""));
        assert!(body.contains("\"rawMeanAmount\":2.5"));
        assert!(body.contains("\"signedRawCoefficient\":-2.5"));
        assert!(body.contains("\"normalizationScale\":0.4"));
        assert!(body.contains("\"normalizedCoefficient\":-1.0"));
    }

    #[test]
    fn writes_calculation_bundle_for_single_certified_method() {
        let mut writer = fixture_writer_with_method_indices(&[3]);
        writer
            .write_result_chunk(
                0,
                &[SolveResult {
                    x: Some(vec![1.0]),
                    g: None,
                    h: Some(vec![42.0]),
                    factorization_state: FactorizationState::Ready,
                }],
            )
            .unwrap();

        let built = writer.finish().unwrap();
        assert_eq!(built.manifest.snapshot.impact_count, 1);
        let lcia = built
            .artifacts
            .iter()
            .find(|artifact| artifact.metadata.kind == "lcia")
            .unwrap();
        let mut decoder = GzDecoder::new(File::open(&lcia.local_path).unwrap());
        let mut body = String::new();
        decoder.read_to_string(&mut body).unwrap();
        assert!(body.contains("\"meanAmount\":42.0"));
        assert!(body.contains(RELEASE_METHOD_IDENTITIES[3].0));
    }

    #[test]
    fn rejects_invalid_certified_method_axes() {
        assert!(validate_impacts(&[]).is_err());

        let duplicate = [reviewed_impact(0, 0), reviewed_impact(0, 1)];
        assert!(validate_impacts(&duplicate).is_err());

        let gap = [reviewed_impact(0, 0), reviewed_impact(1, 2)];
        assert!(validate_impacts(&gap).is_err());

        let mut unknown = reviewed_impact(0, 0);
        unknown.impact_id = Uuid::parse_str("ffffffff-ffff-4fff-8fff-ffffffffffff").unwrap();
        assert!(validate_impacts(&[unknown]).is_err());

        let mut missing_version = reviewed_impact(0, 0);
        missing_version.impact_version = None;
        assert!(validate_impacts(&[missing_version]).is_err());
    }

    #[test]
    fn rejects_missing_or_duplicate_result_chunks() {
        let writer = fixture_writer();
        assert!(writer.finish().is_err());

        let mut writer = fixture_writer();
        let result = SolveResult {
            x: Some(vec![1.0]),
            g: None,
            h: Some(vec![0.0; 25]),
            factorization_state: FactorizationState::Ready,
        };
        writer
            .write_result_chunk(0, std::slice::from_ref(&result))
            .unwrap();
        assert!(writer.write_result_chunk(0, &[result]).is_err());
    }

    #[test]
    fn identical_inputs_produce_identical_bundle_and_gzip_hashes() {
        let solve = || SolveResult {
            x: Some(vec![1.0]),
            g: None,
            h: Some(vec![0.0; 25]),
            factorization_state: FactorizationState::Ready,
        };
        let build = || {
            let mut writer = fixture_writer();
            writer.write_result_chunk(0, &[solve()]).unwrap();
            writer.finish().unwrap()
        };
        let first = build();
        let second = build();
        assert_eq!(first.bundle_content_hash, second.bundle_content_hash);
        assert_eq!(first.manifest_sha256, second.manifest_sha256);
        assert_eq!(
            first
                .manifest
                .artifacts
                .iter()
                .map(|artifact| (&artifact.path, &artifact.sha256))
                .collect::<Vec<_>>(),
            second
                .manifest
                .artifacts
                .iter()
                .map(|artifact| (&artifact.path, &artifact.sha256))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn compressed_artifacts_use_gzip_storage_content_type() {
        let mut writer = fixture_writer();
        writer
            .write_result_chunk(
                0,
                &[SolveResult {
                    x: Some(vec![1.0]),
                    g: None,
                    h: Some(vec![0.0; 25]),
                    factorization_state: FactorizationState::Ready,
                }],
            )
            .unwrap();
        let built = writer.finish().unwrap();
        let lci = built
            .manifest
            .artifacts
            .iter()
            .find(|artifact| artifact.kind == "lci")
            .unwrap();
        assert_eq!(lci.media_type, "application/x-ndjson");
        assert_eq!(lci.compression, "gzip");
        assert_eq!(
            calculation_bundle_storage_content_type(lci).unwrap(),
            CALCULATION_BUNDLE_GZIP_CONTENT_TYPE
        );

        let coverage = built
            .manifest
            .artifacts
            .iter()
            .find(|artifact| artifact.kind == "coverage")
            .unwrap();
        assert_eq!(
            calculation_bundle_storage_content_type(coverage).unwrap(),
            "application/json"
        );
    }
}
