//! Exact-version, non-fail-fast source-closure preflight for data-product builds.

use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet, BinaryHeap, VecDeque},
    fs::{self, File},
    future::Future,
    io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU8, AtomicU64, Ordering as AtomicOrdering},
    },
    time::{Duration, Instant},
};

use anyhow::Context as _;
use serde::ser::{Error as SerdeError, SerializeSeq};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use uuid::Uuid;
use zip::{CompressionMethod, ZipWriter, write::SimpleFileOptions};

use crate::{
    calculation_evidence::RELEASE_METHOD_IDENTITIES,
    db::{
        AppState, ScopeClosureSnapshotBuilderArgs, ScopeClosureSnapshotBuilderMode,
        ScopeClosureSnapshotFacts, fetch_scope_closure_snapshot_facts,
        run_scope_closure_snapshot_builder, scope_closure_evidence_hash,
    },
    file_cache::{advise_sequential_access, release_file_cache},
    graph_types::RequestRootProcess,
    pgbouncer_sqlx::{self as sqlx, PgPool, Postgres, QueryBuilder, Row},
    readiness::{MatrixReadinessReport, ReadinessStatus},
    resource::{CancellationToken, ResourceCounters, ResourceMeasurement},
    snapshot_artifacts::ScopeClosureSnapshotBinding,
    storage::ObjectTransferOptions,
    tidas_cli,
    worker_jobs::{WorkerJobProgress, lease_heartbeat_period},
};

#[cfg(test)]
use crate::resource::directory_bytes;

pub const SCOPE_CLOSURE_JOB_KIND: &str = "lcia.scope_closure_check";
pub const SCOPE_CLOSURE_REQUEST_SCHEMA_VERSION: &str = "lcia.scope_closure_check.request.v1";
pub const SCOPE_CLOSURE_RESULT_SCHEMA_VERSION: &str = "lcia.scope_closure_check.result.v1";
pub const SCOPE_CLOSURE_SCANNER_VERSION: &str = "scope-closure-scanner.v1";
pub const TIDAS_BATCH_PROTOCOL: &str = tidas_cli::TIDAS_BATCH_PROTOCOL;
pub const TIDAS_BATCH_PROFILE: &str = tidas_cli::TIDAS_BATCH_PROFILE;
pub const REFERENCE_EDGE_SCHEMA_VERSION: &str = "tidas.reference-edge.v1";
pub const REFERENCE_ISSUE_SCHEMA_VERSION: &str = "tidas.reference-extraction-issue.v1";
const FETCH_BATCH_SIZE: usize = 96;
const VALIDATION_CACHE_LOOKUP_BATCH_SIZE: usize = 256;
const VALIDATION_EXECUTION_BATCH_SIZE: usize = 64;
const VALIDATION_CACHE_RECORD_BATCH_BYTES: usize = 8 * 1024 * 1024;
const VALIDATION_SORT_RUN_BYTES: usize = 32 * 1024 * 1024;
const VALIDATION_ISSUE_SPOOL_MAX_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const VALIDATION_ISSUE_SPOOL_MAX_EVENTS: u64 = 5_000_000;
const SCOPE_CLOSURE_TEMP_FREE_SPACE_RESERVE_BYTES: u64 = 512 * 1024 * 1024;
const DEFAULT_SCOPE_CLOSURE_MEMORY_BUDGET_MIB: u64 = 2048;
const ISSUE_INLINE_ISSUE_SAMPLE_LIMIT: usize = 5_000;
const ISSUE_INLINE_OCCURRENCE_SAMPLE_LIMIT: usize = 100;
const ISSUE_INLINE_AFFECTED_ROOT_SAMPLE_LIMIT: usize = 100;
const ISSUE_PARTITION_MAX_RECORDS: u64 = 25_000;
const ISSUE_PARTITION_MAX_UNCOMPRESSED_BYTES: u64 = 32 * 1024 * 1024;
const ARTIFACT_REGISTRATION_BATCH_SIZE: usize = 500;
const ROOT_IMPACT_INDEX_MAGIC: &[u8] = b"TGLCA-RI-V1\0";
const FROZEN_REFERENCE_GRAPH_MAGIC: &[u8] = b"TGLCA-FG-V1\0";
const XLSX_ISSUE_SAMPLE_LIMIT: usize = 5_000;
const XLSX_OCCURRENCE_SAMPLE_LIMIT: usize = 10_000;
const XLSX_AFFECTED_ROOT_SAMPLE_LIMIT: usize = 10_000;
const XLSX_MAX_WORKSHEET_ROWS: usize = 1_048_576;
const XLSX_MAX_WORKSHEET_UNCOMPRESSED_BYTES: u64 = 64 * 1024 * 1024;
const XLSX_MAX_TOTAL_UNCOMPRESSED_BYTES: u64 = 128 * 1024 * 1024;
const XLSX_MAX_ARCHIVE_BYTES: u64 = 64 * 1024 * 1024;
const SCOPE_CLOSURE_ARTIFACT_RETENTION_SECONDS: i64 = 7 * 24 * 60 * 60;
const SCOPE_CLOSURE_ARTIFACT_MAX_UPLOAD_BYTES: u64 = 8 * 1024 * 1024 * 1024;
const SCOPE_CLOSURE_ARTIFACT_STAGING_SECONDS: i32 = 3_600;

fn scope_closure_memory_budget_bytes() -> u64 {
    std::env::var("SCOPE_CLOSURE_MEMORY_BUDGET_MIB")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_SCOPE_CLOSURE_MEMORY_BUDGET_MIB)
        .saturating_mul(1024 * 1024)
}

#[cfg(target_os = "linux")]
fn scope_closure_resident_bytes() -> Option<u64> {
    fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|line| {
            let kib = line.strip_prefix("VmRSS:")?.trim();
            let kib = kib.strip_suffix("kB")?.trim().parse::<u64>().ok()?;
            kib.checked_mul(1024)
        })
}

#[cfg(not(target_os = "linux"))]
const fn scope_closure_resident_bytes() -> Option<u64> {
    None
}

fn enforce_scope_closure_memory_budget(phase: &str) -> anyhow::Result<()> {
    let Some(resident_bytes) = scope_closure_resident_bytes() else {
        return Ok(());
    };
    let budget_bytes = scope_closure_memory_budget_bytes();
    if resident_bytes > budget_bytes {
        return Err(anyhow::anyhow!(
            "scope_closure_memory_budget_exceeded: phase={phase}, resident_bytes={resident_bytes}, budget_bytes={budget_bytes}"
        ));
    }
    Ok(())
}

fn record_scope_closure_resources(phase: &str, temp_bytes: Option<u64>, rows: Option<u64>) {
    let measurement = ResourceMeasurement::capture(
        phase,
        ResourceCounters {
            temp_bytes,
            rows,
            ..ResourceCounters::default()
        },
    );
    tracing::info!(
        phase,
        measurement = %serde_json::to_string(&measurement).unwrap_or_default(),
        "scope closure resource measurement"
    );
}

#[derive(Debug, Default)]
struct ScopeClosureArtifactProgress {
    stage: AtomicU8,
    records: AtomicU64,
    bytes: AtomicU64,
    partitions: AtomicU64,
}

impl ScopeClosureArtifactProgress {
    fn update(&self, stage: u8, records: u64, bytes: u64, partitions: u64) {
        self.records.store(records, AtomicOrdering::Release);
        self.bytes.store(bytes, AtomicOrdering::Release);
        self.partitions.store(partitions, AtomicOrdering::Release);
        self.stage.store(stage, AtomicOrdering::Release);
    }

    fn snapshot(&self) -> Value {
        let stage = match self.stage.load(AtomicOrdering::Acquire) {
            1 => "merge_input",
            2 => "coalesce",
            3 => "partition_write",
            4 => "compact_evidence",
            5 => "report_finalize",
            _ => "starting",
        };
        json!({
            "stage": stage,
            "records": self.records.load(AtomicOrdering::Acquire),
            "bytes": self.bytes.load(AtomicOrdering::Acquire),
            "partitions": self.partitions.load(AtomicOrdering::Acquire),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DatasetCategory {
    #[serde(alias = "contact")]
    Contacts,
    #[serde(alias = "flowproperty")]
    Flowproperties,
    #[serde(alias = "flow")]
    Flows,
    #[serde(alias = "lciamethod")]
    Lciamethods,
    #[serde(alias = "lifecyclemodel")]
    Lifecyclemodels,
    #[serde(alias = "process")]
    Processes,
    #[serde(alias = "source")]
    Sources,
    #[serde(alias = "unitgroup")]
    Unitgroups,
}

impl DatasetCategory {
    #[must_use]
    pub const fn table_name(&self) -> &'static str {
        match self {
            Self::Contacts => "contacts",
            Self::Flowproperties => "flowproperties",
            Self::Flows => "flows",
            Self::Lciamethods => "lciamethods",
            Self::Lifecyclemodels => "lifecyclemodels",
            Self::Processes => "processes",
            Self::Sources => "sources",
            Self::Unitgroups => "unitgroups",
        }
    }

    fn from_reference_type(raw: &str) -> Option<Self> {
        match normalize_reference_type(raw).as_str() {
            "contact" | "contact data set" => Some(Self::Contacts),
            "flow" | "flow data set" => Some(Self::Flows),
            "flow property" | "flow property data set" => Some(Self::Flowproperties),
            "lcia method" | "lcia method data set" => Some(Self::Lciamethods),
            "life cycle model"
            | "life cycle model data set"
            | "lifecycle model"
            | "lifecycle model data set" => Some(Self::Lifecyclemodels),
            "process" | "process data set" => Some(Self::Processes),
            "source" | "source data set" => Some(Self::Sources),
            "unit group" | "unit group data set" => Some(Self::Unitgroups),
            _ => None,
        }
    }

    fn from_uri(raw: &str) -> Option<Self> {
        raw.split(['/', '\\'])
            .find_map(|part| match part.to_ascii_lowercase().as_str() {
                "contacts" | "contact" => Some(Self::Contacts),
                "flows" | "flow" => Some(Self::Flows),
                "flowproperties" | "flowproperty" | "flow-properties" => Some(Self::Flowproperties),
                "lciamethods" | "lciamethod" | "lcia-methods" => Some(Self::Lciamethods),
                "lifecyclemodels" | "lifecyclemodel" | "life-cycle-models" => {
                    Some(Self::Lifecyclemodels)
                }
                "processes" | "process" => Some(Self::Processes),
                "sources" | "source" => Some(Self::Sources),
                "unitgroups" | "unitgroup" | "unit-groups" => Some(Self::Unitgroups),
                _ => None,
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExactDatasetIdentity {
    pub category: DatasetCategory,
    pub id: Uuid,
    pub version: String,
}

impl ExactDatasetIdentity {
    #[must_use]
    pub fn document_key(&self) -> String {
        format!(
            "{}:{}:{}",
            self.category.table_name(),
            self.id,
            self.version
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestedIdentity {
    pub id: Uuid,
    pub version: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScopeLinkPolicy {
    pub link_semantics_version: String,
    pub flow_identity_policy: String,
    pub allocation_semantics_version: String,
    pub technosphere_boundary_policy: String,
    pub provider_universe_policy: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestedScopeManifest {
    pub schema_version: String,
    pub coverage_mode: String,
    pub eligibility_predicate_version: String,
    #[serde(default)]
    pub processes: Vec<RequestedIdentity>,
    #[serde(default)]
    pub lcia_methods: Vec<RequestedIdentity>,
    pub version_resolution_policy: String,
    pub legacy_omitted_version_policy: String,
    pub certificate_freshness_policy: String,
    pub link_policy: ScopeLinkPolicy,
    #[serde(default)]
    pub process_manifest_hash: Option<String>,
}

impl RequestedScopeManifest {
    fn roots(&self) -> Vec<ExactDatasetIdentity> {
        let processes = self.processes.iter().map(|item| ExactDatasetIdentity {
            category: DatasetCategory::Processes,
            id: item.id,
            version: item.version.clone(),
        });
        let methods = self.lcia_methods.iter().map(|item| ExactDatasetIdentity {
            category: DatasetCategory::Lciamethods,
            id: item.id,
            version: item.version.clone(),
        });
        let mut roots = processes.chain(methods).collect::<Vec<_>>();
        roots.sort();
        roots.dedup();
        roots
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ScopeClosureWorkerInput {
    pub closure_check_id: Uuid,
    pub scan_execution_id: Uuid,
    pub numerical_snapshot_id: Uuid,
    pub requested_scope: RequestedScopeManifest,
    pub requested_scope_hash: String,
    pub policy_fingerprint: String,
    pub data_snapshot_token: String,
    pub data_snapshot_manifest: Value,
    pub data_snapshot_manifest_hash: String,
    pub publication_epoch: i64,
    pub expected_validator_scanner_fingerprint: String,
    pub request_fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DataSnapshotManifest {
    pub schema_version: String,
    pub requested_scope: RequestedScopeManifest,
    pub current_public_release: CurrentPublicRelease,
    #[serde(default)]
    pub datasets: Vec<SnapshotDatasetEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CurrentPublicRelease {
    pub publication_id: Uuid,
    pub release_run_id: Uuid,
    pub release_version: String,
    pub published_at: String,
    pub release_manifest_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotDatasetEntry {
    pub dataset_type: DatasetCategory,
    pub dataset_id: Uuid,
    pub dataset_version: String,
    pub role: String,
    #[serde(default)]
    pub source_process_id: Option<Uuid>,
    #[serde(default)]
    pub source_process_version: Option<String>,
    pub version_significant_hash: String,
    pub semantic_hash: String,
    pub canonical_content_hash: String,
}

impl SnapshotDatasetEntry {
    fn identity(&self) -> ExactDatasetIdentity {
        ExactDatasetIdentity {
            category: self.dataset_type,
            id: self.dataset_id,
            version: self.dataset_version.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClosureDocument {
    pub identity: ExactDatasetIdentity,
    pub payload: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ClosureDocumentRecord {
    identity: ExactDatasetIdentity,
    canonical_content_hash: String,
    offset: u64,
    byte_size: u64,
}

#[derive(Debug)]
struct ClosureDocumentSpool {
    _temp: TempDir,
    path: PathBuf,
    records: Vec<ClosureDocumentRecord>,
    byte_size: u64,
}

impl ClosureDocumentSpool {
    #[cfg(test)]
    fn empty() -> anyhow::Result<Self> {
        ClosureDocumentSpoolWriter::new()?.finish()
    }

    fn len(&self) -> usize {
        self.records.len()
    }

    fn records(&self) -> &[ClosureDocumentRecord] {
        &self.records
    }

    fn load_batch(
        path: &Path,
        records: &[ClosureDocumentRecord],
    ) -> anyhow::Result<Vec<ClosureDocument>> {
        let mut file = File::open(path)?;
        let mut documents = Vec::with_capacity(records.len());
        for record in records {
            file.seek(SeekFrom::Start(record.offset))?;
            let mut bytes = vec![0_u8; usize::try_from(record.byte_size)?];
            file.read_exact(&mut bytes)?;
            let document = serde_json::from_slice::<ClosureDocument>(&bytes)?;
            if document.identity != record.identity {
                return Err(anyhow::anyhow!(
                    "scope closure document spool identity mismatch"
                ));
            }
            documents.push(document);
        }
        Ok(documents)
    }

    fn write_json_array(&self, writer: &mut impl Write) -> anyhow::Result<()> {
        let mut file = File::open(&self.path)?;
        writer.write_all(b"[")?;
        for (index, record) in self.records.iter().enumerate() {
            if index > 0 {
                writer.write_all(b",")?;
            }
            file.seek(SeekFrom::Start(record.offset))?;
            let mut remaining = record.byte_size;
            let mut buffer = vec![0_u8; 64 * 1024].into_boxed_slice();
            while remaining > 0 {
                let chunk = usize::try_from(remaining.min(buffer.len() as u64))?;
                file.read_exact(&mut buffer[..chunk])?;
                writer.write_all(&buffer[..chunk])?;
                remaining -= u64::try_from(chunk)?;
            }
        }
        writer.write_all(b"]")?;
        Ok(())
    }
}

impl Serialize for ClosureDocumentSpool {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut sequence = serializer.serialize_seq(Some(self.records.len()))?;
        for record in &self.records {
            let documents = Self::load_batch(&self.path, std::slice::from_ref(record))
                .map_err(S::Error::custom)?;
            sequence.serialize_element(
                documents
                    .first()
                    .ok_or_else(|| S::Error::custom("document spool record was unreadable"))?,
            )?;
        }
        sequence.end()
    }
}

struct ClosureDocumentSpoolWriter {
    temp: TempDir,
    path: PathBuf,
    writer: BufWriter<File>,
    records: BTreeMap<ExactDatasetIdentity, ClosureDocumentRecord>,
    byte_size: u64,
}

impl ClosureDocumentSpoolWriter {
    fn new() -> anyhow::Result<Self> {
        let temp = TempDir::new()?;
        let path = temp.path().join("closure-documents.jsonl");
        let writer = BufWriter::new(File::create(&path)?);
        Ok(Self {
            temp,
            path,
            writer,
            records: BTreeMap::new(),
            byte_size: 0,
        })
    }

    fn len(&self) -> usize {
        self.records.len()
    }

    fn append(&mut self, document: &ClosureDocument) -> anyhow::Result<()> {
        let bytes = canonical_json_bytes(document)?;
        let byte_size = u64::try_from(bytes.len())?;
        let record = ClosureDocumentRecord {
            identity: document.identity.clone(),
            canonical_content_hash: canonical_json_sha256(&document.payload)?,
            offset: self.byte_size,
            byte_size,
        };
        if self.records.contains_key(&record.identity) {
            return Err(anyhow::anyhow!(
                "scope closure document spool received duplicate identity {}",
                record.identity.document_key()
            ));
        }
        self.writer.write_all(&bytes)?;
        self.byte_size = self
            .byte_size
            .checked_add(byte_size)
            .ok_or_else(|| anyhow::anyhow!("scope closure document spool byte count overflow"))?;
        self.records.insert(record.identity.clone(), record);
        Ok(())
    }

    fn finish(mut self) -> anyhow::Result<ClosureDocumentSpool> {
        self.writer.flush()?;
        drop(self.writer);
        Ok(ClosureDocumentSpool {
            _temp: self.temp,
            path: self.path,
            records: self.records.into_values().collect(),
            byte_size: self.byte_size,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReferenceEdge {
    pub schema_version: String,
    pub document_key: String,
    pub source_category: String,
    pub target_category: String,
    pub target_uuid: String,
    pub requested_version_state: String,
    pub requested_version: Option<String>,
    pub requested_version_raw: Value,
    pub reference_role: String,
    pub json_path: String,
    pub raw_type: Value,
    pub uri: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedReference {
    pub source: ExactDatasetIdentity,
    pub target: ExactDatasetIdentity,
    pub json_path: String,
    pub reference_role: String,
    pub requested_version_state: String,
}

#[derive(Debug, Default)]
struct CompactReferenceGraph {
    identities: Vec<ExactDatasetIdentity>,
    identity_ids: BTreeMap<ExactDatasetIdentity, u32>,
    reverse: Vec<Vec<u32>>,
}

impl CompactReferenceGraph {
    fn from_references(
        references: &[ResolvedReference],
        roots: &[ExactDatasetIdentity],
    ) -> anyhow::Result<Self> {
        let identities = references
            .iter()
            .flat_map(|reference| [&reference.source, &reference.target])
            .chain(roots)
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let identity_ids = identities
            .iter()
            .enumerate()
            .map(|(index, identity)| Ok((identity.clone(), u32::try_from(index)?)))
            .collect::<anyhow::Result<BTreeMap<_, _>>>()?;
        let mut reverse = vec![Vec::new(); identities.len()];
        for reference in references {
            let source = *identity_ids
                .get(&reference.source)
                .ok_or_else(|| anyhow::anyhow!("reference graph omitted source identity"))?;
            let target = *identity_ids
                .get(&reference.target)
                .ok_or_else(|| anyhow::anyhow!("reference graph omitted target identity"))?;
            reverse[usize::try_from(target)?].push(source);
        }
        for predecessors in &mut reverse {
            predecessors.sort_unstable();
            predecessors.dedup();
        }
        Ok(Self {
            identities,
            identity_ids,
            reverse,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReferenceExtractionIssue {
    pub schema_version: String,
    pub issue_code: String,
    pub severity: String,
    pub document_key: String,
    pub source_category: String,
    pub json_path: String,
    pub reference_role: String,
    pub message: String,
    pub details: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReferenceExtractionResult {
    pub schema_version: String,
    pub document_key: String,
    pub source_category: String,
    pub edges: Vec<ReferenceEdge>,
    pub issues: Vec<ReferenceExtractionIssue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClosureIssue {
    pub issue_key: String,
    pub severity: String,
    pub blocking: bool,
    pub issue_code: String,
    pub source: Option<ExactDatasetIdentity>,
    pub json_path: Option<String>,
    pub reference_role: Option<String>,
    pub requested_target_type: Option<String>,
    pub requested_target_id: Option<Uuid>,
    pub requested_target_version: Option<String>,
    pub message: String,
    pub suggested_action: Option<String>,
    pub occurrence_count: u32,
    #[serde(default)]
    pub occurrences: Vec<ClosureIssueOccurrence>,
    #[serde(default)]
    pub affected_root_count: u32,
    pub affected_roots: Vec<ExactDatasetIdentity>,
    pub affected_root_witness_paths: Vec<Vec<ExactDatasetIdentity>>,
    pub witness_path: Vec<ExactDatasetIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClosureIssueOccurrence {
    pub occurrence_key: String,
    pub source: Option<ExactDatasetIdentity>,
    pub json_path: Option<String>,
    pub reference_role: Option<String>,
    #[serde(default)]
    pub details: Value,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ScopeClosureScan {
    pub schema_version: String,
    pub complete: bool,
    pub roots: Vec<ExactDatasetIdentity>,
    documents: ClosureDocumentSpool,
    edges: JsonlValueSpool,
    resolved_references: JsonlValueSpool,
    pub omitted_version_resolutions: Vec<Value>,
    pub issues: Vec<ClosureIssue>,
    pub frontier: Vec<ExactDatasetIdentity>,
    pub provider_universe: Vec<ExactDatasetIdentity>,
    #[serde(skip)]
    reference_graph: CompactReferenceGraph,
    #[serde(skip)]
    tidas_issue_event_count: u64,
    #[serde(skip)]
    issue_relations: Option<IssueRelationSpools>,
}

impl ScopeClosureScan {
    #[must_use]
    pub fn blocker_codes(&self) -> Vec<String> {
        if let Some(relations) = &self.issue_relations {
            return relations.stats.blocker_codes.iter().cloned().collect();
        }
        self.issues
            .iter()
            .filter(|issue| issue.blocking)
            .map(|issue| issue.issue_code.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    fn issue_count(&self) -> u64 {
        self.issue_relations.as_ref().map_or_else(
            || u64::try_from(self.issues.len()).unwrap_or(u64::MAX),
            |relations| relations.stats.issue_count,
        )
    }

    fn blocker_count(&self) -> u64 {
        self.issue_relations.as_ref().map_or_else(
            || {
                u64::try_from(self.issues.iter().filter(|issue| issue.blocking).count())
                    .unwrap_or(u64::MAX)
            },
            |relations| relations.stats.blocker_count,
        )
    }
}

#[derive(Debug, Default)]
struct IssueRelationStats {
    issue_count: u64,
    blocker_count: u64,
    occurrence_count: u64,
    affected_root_count: u64,
    blocker_codes: BTreeSet<String>,
}

#[derive(Debug)]
struct IssueRelationSpools {
    issues: JsonlValueSpool,
    issue_partition_entries: Vec<IssuePartitionManifestEntry>,
    issue_partition_artifacts: Vec<PreparedArtifact>,
    issue_relation_sha256: String,
    root_impact_index: CompactEvidenceFile,
    stats: IssueRelationStats,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RootImpactMode {
    None,
    AllRoots,
    IncludedOrdinals,
    ExcludedOrdinals,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RootImpactReference {
    mode: RootImpactMode,
    #[serde(skip_serializing_if = "Option::is_none")]
    impact_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_node_ordinal: Option<u32>,
    evidence_schema_version: String,
}

#[derive(Debug, Clone)]
struct CompactEvidenceFile {
    temp: Arc<TempDir>,
    path: PathBuf,
    relative_path: String,
    media_type: String,
    record_count: u64,
    uncompressed_byte_size: u64,
    uncompressed_sha256: String,
    compressed_byte_size: u64,
    compressed_sha256: String,
}

impl CompactEvidenceFile {
    fn manifest_entry(&self, relation: &str) -> CompleteMachineEvidenceEntry {
        CompleteMachineEvidenceEntry {
            relation: relation.to_owned(),
            path: self.relative_path.clone(),
            media_type: self.media_type.clone(),
            record_count: self.record_count,
            uncompressed_byte_size: self.uncompressed_byte_size,
            uncompressed_sha256: self.uncompressed_sha256.clone(),
            compressed_byte_size: self.compressed_byte_size,
            compressed_sha256: self.compressed_sha256.clone(),
        }
    }

    fn prepared_artifact(&self) -> anyhow::Result<PreparedArtifact> {
        Ok(PreparedArtifact {
            descriptor: ArtifactManifestEntry {
                artifact_type: "closure_complete_machine_result".to_owned(),
                artifact_role: ScopeClosureArtifactRole::CompleteMachineResult,
                file_name: self.relative_path.clone(),
                content_type: self.media_type.clone(),
                byte_size: usize::try_from(self.compressed_byte_size)?,
                checksum_sha256: self.compressed_sha256.clone(),
            },
            path: self.path.clone(),
            _temp: Arc::clone(&self.temp),
        })
    }
}

struct CompactEvidenceWriter {
    temp: Arc<TempDir>,
    path: PathBuf,
    relative_path: String,
    media_type: String,
    encoder: zstd::stream::write::Encoder<'static, BufWriter<File>>,
    uncompressed_digest: Sha256,
    uncompressed_byte_size: u64,
    record_count: u64,
}

impl CompactEvidenceWriter {
    fn new(relative_path: &str, media_type: &str) -> anyhow::Result<Self> {
        let temp = Arc::new(TempDir::new()?);
        let path = temp.path().join(relative_path);
        let parent = path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("compact evidence path omitted parent"))?;
        fs::create_dir_all(parent)?;
        let file = File::create(&path)?;
        advise_sequential_access(&file);
        let encoder = zstd::stream::write::Encoder::new(BufWriter::new(file), 6)?;
        Ok(Self {
            temp,
            path,
            relative_path: relative_path.to_owned(),
            media_type: media_type.to_owned(),
            encoder,
            uncompressed_digest: Sha256::new(),
            uncompressed_byte_size: 0,
            record_count: 0,
        })
    }

    fn write_all(&mut self, bytes: &[u8]) -> anyhow::Result<()> {
        self.encoder.write_all(bytes)?;
        self.uncompressed_digest.update(bytes);
        self.uncompressed_byte_size = self
            .uncompressed_byte_size
            .checked_add(u64::try_from(bytes.len())?)
            .ok_or_else(|| anyhow::anyhow!("compact evidence byte count overflow"))?;
        Ok(())
    }

    fn record_completed(&mut self) -> anyhow::Result<()> {
        self.record_count = self
            .record_count
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("compact evidence record count overflow"))?;
        Ok(())
    }

    fn finish(self) -> anyhow::Result<CompactEvidenceFile> {
        let Self {
            temp,
            path,
            relative_path,
            media_type,
            encoder,
            uncompressed_digest,
            uncompressed_byte_size,
            record_count,
        } = self;
        let mut output = encoder.finish()?;
        output.flush()?;
        release_file_cache(output.get_ref());
        drop(output);
        let (compressed_byte_size, compressed_sha256) = file_size_and_sha256(&path)?;
        Ok(CompactEvidenceFile {
            temp,
            path,
            relative_path,
            media_type,
            record_count,
            uncompressed_byte_size,
            uncompressed_sha256: hex::encode(uncompressed_digest.finalize()),
            compressed_byte_size,
            compressed_sha256,
        })
    }
}

fn write_u16(writer: &mut CompactEvidenceWriter, value: u16) -> anyhow::Result<()> {
    writer.write_all(&value.to_le_bytes())
}

fn write_u32(writer: &mut CompactEvidenceWriter, value: u32) -> anyhow::Result<()> {
    writer.write_all(&value.to_le_bytes())
}

fn write_u64(writer: &mut CompactEvidenceWriter, value: u64) -> anyhow::Result<()> {
    writer.write_all(&value.to_le_bytes())
}

fn write_compact_string(writer: &mut CompactEvidenceWriter, value: &str) -> anyhow::Result<()> {
    write_u32(writer, u32::try_from(value.len())?)?;
    writer.write_all(value.as_bytes())
}

fn write_delta_varint(writer: &mut CompactEvidenceWriter, ordinals: &[u32]) -> anyhow::Result<()> {
    let mut previous = 0_u32;
    for (index, &ordinal) in ordinals.iter().enumerate() {
        let mut delta = if index == 0 {
            ordinal
        } else {
            ordinal
                .checked_sub(previous)
                .ok_or_else(|| anyhow::anyhow!("root impact ordinals are not sorted"))?
        };
        loop {
            let mut byte = u8::try_from(delta & 0x7f)?;
            delta >>= 7;
            if delta != 0 {
                byte |= 0x80;
            }
            writer.write_all(&[byte])?;
            if delta == 0 {
                break;
            }
        }
        previous = ordinal;
    }
    Ok(())
}

struct RootImpactIndexWriter {
    evidence: CompactEvidenceWriter,
    last_key: Option<String>,
}

impl RootImpactIndexWriter {
    fn new(root_count: usize) -> anyhow::Result<Self> {
        let mut evidence = CompactEvidenceWriter::new(
            "evidence/root-impact-index-v1.bin.zst",
            "application/vnd.tiangong.scope-closure-root-impact-index+zstd",
        )?;
        evidence.write_all(ROOT_IMPACT_INDEX_MAGIC)?;
        write_u16(&mut evidence, 1)?;
        write_u32(&mut evidence, u32::try_from(root_count)?)?;
        Ok(Self {
            evidence,
            last_key: None,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn append(
        &mut self,
        impact_key: &str,
        kind: u8,
        source_node_ordinal: Option<u32>,
        mode: RootImpactMode,
        affected_root_count: u32,
        encoded_ordinals: &[u32],
    ) -> anyhow::Result<()> {
        if self
            .last_key
            .as_deref()
            .is_some_and(|previous| previous >= impact_key)
        {
            return Err(anyhow::anyhow!(
                "root impact keys are not strictly ordered: {impact_key}"
            ));
        }
        self.last_key = Some(impact_key.to_owned());
        write_u16(&mut self.evidence, u16::try_from(impact_key.len())?)?;
        self.evidence.write_all(impact_key.as_bytes())?;
        self.evidence.write_all(&[kind])?;
        write_u32(&mut self.evidence, source_node_ordinal.unwrap_or(u32::MAX))?;
        self.evidence.write_all(&[match mode {
            RootImpactMode::None => 0,
            RootImpactMode::AllRoots => 1,
            RootImpactMode::IncludedOrdinals => 2,
            RootImpactMode::ExcludedOrdinals => 3,
        }])?;
        write_u32(&mut self.evidence, affected_root_count)?;
        write_u32(&mut self.evidence, u32::try_from(encoded_ordinals.len())?)?;
        write_delta_varint(&mut self.evidence, encoded_ordinals)?;
        self.evidence.record_completed()
    }

    fn finish(self) -> anyhow::Result<CompactEvidenceFile> {
        self.evidence.finish()
    }
}

#[derive(Debug)]
struct StableRootOrdinals {
    roots: Vec<ExactDatasetIdentity>,
    ordinal_by_identity: BTreeMap<ExactDatasetIdentity, u32>,
    graph_node_ordinals: Vec<u32>,
}

impl StableRootOrdinals {
    fn new(roots: &[ExactDatasetIdentity], graph: &CompactReferenceGraph) -> anyhow::Result<Self> {
        let roots = roots
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let ordinal_by_identity = roots
            .iter()
            .enumerate()
            .map(|(index, root)| Ok((root.clone(), u32::try_from(index)?)))
            .collect::<anyhow::Result<BTreeMap<_, _>>>()?;
        let graph_node_ordinals = roots
            .iter()
            .map(|root| {
                graph
                    .identity_ids
                    .get(root)
                    .copied()
                    .ok_or_else(|| anyhow::anyhow!("reference graph omitted requested root"))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok(Self {
            roots,
            ordinal_by_identity,
            graph_node_ordinals,
        })
    }
}

fn compact_root_impact_encoding(
    included_ordinals: &[u32],
    root_count: usize,
) -> anyhow::Result<(RootImpactMode, Vec<u32>)> {
    if included_ordinals.is_empty() {
        return Ok((RootImpactMode::None, Vec::new()));
    }
    if included_ordinals.len() == root_count {
        return Ok((RootImpactMode::AllRoots, Vec::new()));
    }
    if included_ordinals.len() <= root_count.saturating_sub(included_ordinals.len()) {
        return Ok((RootImpactMode::IncludedOrdinals, included_ordinals.to_vec()));
    }
    let included = included_ordinals.iter().copied().collect::<BTreeSet<_>>();
    let excluded = (0..u32::try_from(root_count)?)
        .filter(|ordinal| !included.contains(ordinal))
        .collect::<Vec<_>>();
    Ok((RootImpactMode::ExcludedOrdinals, excluded))
}

fn build_frozen_reference_graph_file(
    graph: &CompactReferenceGraph,
    roots: &StableRootOrdinals,
    cancellation: &CancellationToken,
) -> anyhow::Result<CompactEvidenceFile> {
    let mut evidence = CompactEvidenceWriter::new(
        "evidence/frozen-reference-graph-v1.bin.zst",
        "application/vnd.tiangong.scope-closure-frozen-reference-graph+zstd",
    )?;
    evidence.write_all(FROZEN_REFERENCE_GRAPH_MAGIC)?;
    write_u16(&mut evidence, 1)?;
    write_u32(&mut evidence, u32::try_from(graph.identities.len())?)?;
    write_u32(&mut evidence, u32::try_from(roots.roots.len())?)?;
    let edge_count = graph
        .reverse
        .iter()
        .map(Vec::len)
        .try_fold(0_u64, |total, count| {
            total
                .checked_add(u64::try_from(count)?)
                .ok_or_else(|| anyhow::anyhow!("reference graph edge count overflow"))
        })?;
    write_u64(&mut evidence, edge_count)?;
    for (index, identity) in graph.identities.iter().enumerate() {
        if index.is_multiple_of(4_096) {
            cancellation.check("scope_closure_frozen_graph_nodes")?;
        }
        write_compact_string(&mut evidence, identity.category.table_name())?;
        evidence.write_all(identity.id.as_bytes())?;
        write_compact_string(&mut evidence, &identity.version)?;
    }
    for &node_ordinal in &roots.graph_node_ordinals {
        write_u32(&mut evidence, node_ordinal)?;
    }
    for (index, predecessors) in graph.reverse.iter().enumerate() {
        if index.is_multiple_of(4_096) {
            cancellation.check("scope_closure_frozen_graph_edges")?;
        }
        write_u32(&mut evidence, u32::try_from(predecessors.len())?)?;
        write_delta_varint(&mut evidence, predecessors)?;
    }
    evidence.record_count = u64::try_from(graph.identities.len())?;
    evidence.finish()
}

fn compress_tidas_issue_stream(
    events: &JsonlValueSpool,
    cancellation: &CancellationToken,
) -> anyhow::Result<CompactEvidenceFile> {
    let mut evidence =
        CompactEvidenceWriter::new("tidas/issues.ndjson.zst", "application/x-ndjson+zstd")?;
    let file = File::open(&events.path)?;
    advise_sequential_access(&file);
    let mut reader = BufReader::with_capacity(64 * 1024, file);
    let mut buffer = vec![0_u8; 64 * 1024].into_boxed_slice();
    loop {
        cancellation.check("scope_closure_tidas_artifact_write")?;
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        evidence.write_all(&buffer[..read])?;
    }
    evidence.record_count = events.event_count;
    let evidence = evidence.finish()?;
    if evidence.uncompressed_byte_size != events.byte_size
        || evidence.uncompressed_sha256 != events.sha256
    {
        return Err(anyhow::anyhow!(
            "compressed TIDAS issue stream changed logical bytes"
        ));
    }
    Ok(evidence)
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
struct DecodedRootImpactRecord {
    impact_key: String,
    kind: u8,
    source_node_ordinal: Option<u32>,
    mode: RootImpactMode,
    affected_root_count: u32,
    encoded_ordinals: Vec<u32>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
struct DecodedRootImpactIndex {
    root_count: u32,
    records: Vec<DecodedRootImpactRecord>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
struct DecodedFrozenReferenceGraph {
    identities: Vec<ExactDatasetIdentity>,
    root_node_ordinals: Vec<u32>,
    reverse: Vec<Vec<u32>>,
}

#[allow(dead_code)]
fn read_exact_array<const N: usize>(
    reader: &mut std::io::Cursor<&[u8]>,
) -> anyhow::Result<[u8; N]> {
    let mut bytes = [0_u8; N];
    reader.read_exact(&mut bytes)?;
    Ok(bytes)
}

#[allow(dead_code)]
fn read_u8(reader: &mut std::io::Cursor<&[u8]>) -> anyhow::Result<u8> {
    Ok(read_exact_array::<1>(reader)?[0])
}

#[allow(dead_code)]
fn read_u16(reader: &mut std::io::Cursor<&[u8]>) -> anyhow::Result<u16> {
    Ok(u16::from_le_bytes(read_exact_array(reader)?))
}

#[allow(dead_code)]
fn read_u32(reader: &mut std::io::Cursor<&[u8]>) -> anyhow::Result<u32> {
    Ok(u32::from_le_bytes(read_exact_array(reader)?))
}

#[allow(dead_code)]
fn read_u64(reader: &mut std::io::Cursor<&[u8]>) -> anyhow::Result<u64> {
    Ok(u64::from_le_bytes(read_exact_array(reader)?))
}

#[allow(dead_code)]
fn read_compact_string(reader: &mut std::io::Cursor<&[u8]>) -> anyhow::Result<String> {
    let length = usize::try_from(read_u32(reader)?)?;
    let mut bytes = vec![0_u8; length];
    reader.read_exact(&mut bytes)?;
    Ok(String::from_utf8(bytes)?)
}

#[allow(dead_code)]
fn read_delta_varints(
    reader: &mut std::io::Cursor<&[u8]>,
    count: usize,
) -> anyhow::Result<Vec<u32>> {
    let mut ordinals = Vec::with_capacity(count);
    let mut previous = 0_u32;
    for index in 0..count {
        let mut value = 0_u32;
        let mut shift = 0_u32;
        loop {
            if shift >= 35 {
                return Err(anyhow::anyhow!("compact ordinal varint overflow"));
            }
            let byte = read_u8(reader)?;
            value = value
                .checked_add(u32::from(byte & 0x7f) << shift)
                .ok_or_else(|| anyhow::anyhow!("compact ordinal varint overflow"))?;
            if byte & 0x80 == 0 {
                break;
            }
            shift += 7;
        }
        let ordinal = if index == 0 {
            value
        } else {
            previous
                .checked_add(value)
                .ok_or_else(|| anyhow::anyhow!("compact ordinal delta overflow"))?
        };
        if index > 0 && ordinal <= previous {
            return Err(anyhow::anyhow!(
                "compact ordinals are not strictly ascending"
            ));
        }
        ordinals.push(ordinal);
        previous = ordinal;
    }
    Ok(ordinals)
}

#[allow(dead_code)]
fn decode_root_impact_index(bytes: &[u8]) -> anyhow::Result<DecodedRootImpactIndex> {
    let mut reader = std::io::Cursor::new(bytes);
    let mut magic = vec![0_u8; ROOT_IMPACT_INDEX_MAGIC.len()];
    reader.read_exact(&mut magic)?;
    if magic != ROOT_IMPACT_INDEX_MAGIC || read_u16(&mut reader)? != 1 {
        return Err(anyhow::anyhow!("root impact index header mismatch"));
    }
    let root_count = read_u32(&mut reader)?;
    let mut records = Vec::new();
    let mut previous_key = None::<String>;
    while usize::try_from(reader.position())? < bytes.len() {
        let key_length = usize::from(read_u16(&mut reader)?);
        let mut key_bytes = vec![0_u8; key_length];
        reader.read_exact(&mut key_bytes)?;
        let impact_key = String::from_utf8(key_bytes)?;
        if previous_key
            .as_deref()
            .is_some_and(|previous| previous >= impact_key.as_str())
        {
            return Err(anyhow::anyhow!(
                "root impact index keys are not strictly ordered"
            ));
        }
        previous_key = Some(impact_key.clone());
        let kind = read_u8(&mut reader)?;
        if !matches!(kind, 1 | 2) {
            return Err(anyhow::anyhow!("root impact index kind is invalid"));
        }
        let source_node = read_u32(&mut reader)?;
        let source_node_ordinal = (source_node != u32::MAX).then_some(source_node);
        let mode = match read_u8(&mut reader)? {
            0 => RootImpactMode::None,
            1 => RootImpactMode::AllRoots,
            2 => RootImpactMode::IncludedOrdinals,
            3 => RootImpactMode::ExcludedOrdinals,
            _ => return Err(anyhow::anyhow!("root impact index mode is invalid")),
        };
        let affected_root_count = read_u32(&mut reader)?;
        let encoded_count = usize::try_from(read_u32(&mut reader)?)?;
        let encoded_ordinals = read_delta_varints(&mut reader, encoded_count)?;
        if encoded_ordinals
            .last()
            .is_some_and(|ordinal| *ordinal >= root_count)
        {
            return Err(anyhow::anyhow!("root impact index ordinal is out of range"));
        }
        let decoded_count = match mode {
            RootImpactMode::None => {
                if !encoded_ordinals.is_empty() {
                    return Err(anyhow::anyhow!("none root impact has an ordinal payload"));
                }
                0
            }
            RootImpactMode::AllRoots => {
                if !encoded_ordinals.is_empty() {
                    return Err(anyhow::anyhow!("all-roots impact has an ordinal payload"));
                }
                root_count
            }
            RootImpactMode::IncludedOrdinals => u32::try_from(encoded_ordinals.len())?,
            RootImpactMode::ExcludedOrdinals => root_count
                .checked_sub(u32::try_from(encoded_ordinals.len())?)
                .ok_or_else(|| anyhow::anyhow!("excluded root impact exceeds root count"))?,
        };
        if decoded_count != affected_root_count {
            return Err(anyhow::anyhow!("root impact encoded cardinality mismatch"));
        }
        records.push(DecodedRootImpactRecord {
            impact_key,
            kind,
            source_node_ordinal,
            mode,
            affected_root_count,
            encoded_ordinals,
        });
    }
    Ok(DecodedRootImpactIndex {
        root_count,
        records,
    })
}

#[allow(dead_code)]
fn decode_frozen_reference_graph(bytes: &[u8]) -> anyhow::Result<DecodedFrozenReferenceGraph> {
    let mut reader = std::io::Cursor::new(bytes);
    let mut magic = vec![0_u8; FROZEN_REFERENCE_GRAPH_MAGIC.len()];
    reader.read_exact(&mut magic)?;
    if magic != FROZEN_REFERENCE_GRAPH_MAGIC || read_u16(&mut reader)? != 1 {
        return Err(anyhow::anyhow!("frozen reference graph header mismatch"));
    }
    let node_count = usize::try_from(read_u32(&mut reader)?)?;
    let root_count = usize::try_from(read_u32(&mut reader)?)?;
    let expected_edge_count = read_u64(&mut reader)?;
    let mut identities = Vec::with_capacity(node_count);
    for _ in 0..node_count {
        let category = parse_category(&read_compact_string(&mut reader)?)?;
        let id = Uuid::from_bytes(read_exact_array(&mut reader)?);
        let version = read_compact_string(&mut reader)?;
        let identity = ExactDatasetIdentity {
            category,
            id,
            version,
        };
        if identities
            .last()
            .is_some_and(|previous| previous >= &identity)
        {
            return Err(anyhow::anyhow!(
                "frozen reference graph identities are not strictly ordered"
            ));
        }
        identities.push(identity);
    }
    let mut root_node_ordinals = Vec::with_capacity(root_count);
    for _ in 0..root_count {
        let ordinal = read_u32(&mut reader)?;
        if usize::try_from(ordinal)? >= node_count {
            return Err(anyhow::anyhow!(
                "frozen reference graph root ordinal is out of range"
            ));
        }
        root_node_ordinals.push(ordinal);
    }
    let mut reverse = Vec::with_capacity(node_count);
    let mut observed_edge_count = 0_u64;
    for _ in 0..node_count {
        let count = usize::try_from(read_u32(&mut reader)?)?;
        let predecessors = read_delta_varints(&mut reader, count)?;
        if predecessors
            .last()
            .is_some_and(|ordinal| usize::try_from(*ordinal).unwrap_or(usize::MAX) >= node_count)
        {
            return Err(anyhow::anyhow!(
                "frozen reference graph predecessor is out of range"
            ));
        }
        observed_edge_count = observed_edge_count
            .checked_add(u64::try_from(predecessors.len())?)
            .ok_or_else(|| anyhow::anyhow!("frozen reference graph edge count overflow"))?;
        reverse.push(predecessors);
    }
    if usize::try_from(reader.position())? != bytes.len()
        || observed_edge_count != expected_edge_count
    {
        return Err(anyhow::anyhow!(
            "frozen reference graph byte or edge count mismatch"
        ));
    }
    Ok(DecodedFrozenReferenceGraph {
        identities,
        root_node_ordinals,
        reverse,
    })
}

#[allow(dead_code)]
fn decoded_impact_contains_root(
    record: &DecodedRootImpactRecord,
    root_ordinal: u32,
    root_count: u32,
) -> bool {
    match record.mode {
        RootImpactMode::None => false,
        RootImpactMode::AllRoots => root_ordinal < root_count,
        RootImpactMode::IncludedOrdinals => {
            record.encoded_ordinals.binary_search(&root_ordinal).is_ok()
        }
        RootImpactMode::ExcludedOrdinals => {
            root_ordinal < root_count
                && record
                    .encoded_ordinals
                    .binary_search(&root_ordinal)
                    .is_err()
        }
    }
}

#[allow(dead_code)]
fn reconstruct_frozen_graph_witness(
    graph: &DecodedFrozenReferenceGraph,
    source_node_ordinal: u32,
    root_ordinal: u32,
) -> anyhow::Result<Vec<ExactDatasetIdentity>> {
    let root_node_ordinal = *graph
        .root_node_ordinals
        .get(usize::try_from(root_ordinal)?)
        .ok_or_else(|| anyhow::anyhow!("requested root ordinal is out of range"))?;
    let source_index = usize::try_from(source_node_ordinal)?;
    if source_index >= graph.identities.len() {
        return Err(anyhow::anyhow!("source node ordinal is out of range"));
    }
    let mut visited = vec![false; graph.identities.len()];
    let mut parent = vec![None; graph.identities.len()];
    visited[source_index] = true;
    let mut queue = VecDeque::from([source_node_ordinal]);
    while let Some(node) = queue.pop_front() {
        if node == root_node_ordinal {
            return Ok(reconstruct_witness_path(
                root_node_ordinal,
                &parent,
                &graph.identities,
            ));
        }
        for &predecessor in graph
            .reverse
            .get(usize::try_from(node)?)
            .ok_or_else(|| anyhow::anyhow!("frozen graph node is out of range"))?
        {
            let predecessor_index = usize::try_from(predecessor)?;
            if !visited[predecessor_index] {
                visited[predecessor_index] = true;
                parent[predecessor_index] = Some(node);
                queue.push_back(predecessor);
            }
        }
    }
    Err(anyhow::anyhow!(
        "frozen reference graph cannot reconstruct requested witness"
    ))
}

trait ScopeClosureProvider {
    async fn checkpoint(&self, _scanned: usize, _scheduled: usize) -> anyhow::Result<()> {
        Ok(())
    }

    async fn fetch_exact(
        &self,
        identities: &[ExactDatasetIdentity],
    ) -> anyhow::Result<ProviderFetchResult>;

    async fn resolve_omitted_version(
        &self,
        category: DatasetCategory,
        id: Uuid,
        policy: &str,
    ) -> anyhow::Result<OmittedVersionResolution>;
}

#[derive(Debug, Clone, Default)]
struct ProviderFetchResult {
    documents: Vec<ClosureDocument>,
    issues: Vec<ClosureIssue>,
    incomplete_identities: BTreeSet<ExactDatasetIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OmittedVersionResolution {
    selected: Option<ExactDatasetIdentity>,
    candidates: Vec<ExactDatasetIdentity>,
    policy: String,
}

pub struct PgScopeClosureProvider<'a> {
    pool: &'a PgPool,
    lease: Option<(Uuid, Uuid, i32)>,
    snapshot_universe: BTreeMap<ExactDatasetIdentity, SnapshotDatasetEntry>,
}

impl<'a> PgScopeClosureProvider<'a> {
    #[must_use]
    pub fn new(pool: &'a PgPool, snapshot: &DataSnapshotManifest) -> Self {
        Self {
            pool,
            lease: None,
            snapshot_universe: snapshot_dataset_universe(snapshot),
        }
    }

    #[must_use]
    pub fn new_leased(
        pool: &'a PgPool,
        snapshot: &DataSnapshotManifest,
        worker_job_id: Uuid,
        lease_token: Uuid,
        lease_seconds: i32,
    ) -> Self {
        Self {
            pool,
            lease: Some((worker_job_id, lease_token, lease_seconds)),
            snapshot_universe: snapshot_dataset_universe(snapshot),
        }
    }
}

fn snapshot_dataset_universe(
    snapshot: &DataSnapshotManifest,
) -> BTreeMap<ExactDatasetIdentity, SnapshotDatasetEntry> {
    snapshot
        .datasets
        .iter()
        .cloned()
        .map(|entry| (entry.identity(), entry))
        .collect()
}

impl ScopeClosureProvider for PgScopeClosureProvider<'_> {
    async fn checkpoint(&self, scanned: usize, scheduled: usize) -> anyhow::Result<()> {
        if let Some((worker_job_id, lease_token, lease_seconds)) = self.lease {
            crate::worker_jobs::heartbeat_worker_job(
                self.pool,
                worker_job_id,
                lease_token,
                "discover_reference_graph",
                0.18 + 0.22 * bounded_progress_ratio(scanned, scheduled),
                Some(json!({
                    "progressCounters": {
                        "scanned": scanned,
                        "total": scheduled,
                        "unit": "documents"
                    }
                })),
                lease_seconds,
            )
            .await?;
        }
        Ok(())
    }

    async fn fetch_exact(
        &self,
        identities: &[ExactDatasetIdentity],
    ) -> anyhow::Result<ProviderFetchResult> {
        // Read the exact requested identities, including identities absent from
        // the release allowlist.  We never accept a live-only row, but observing
        // it is required to distinguish an ineligible substitution (incomplete)
        // from a complete negative exact-version finding.
        let live_documents = fetch_exact_documents(self.pool, identities).await?;
        enforce_snapshot_boundary(identities, &self.snapshot_universe, live_documents)
    }

    async fn resolve_omitted_version(
        &self,
        category: DatasetCategory,
        id: Uuid,
        policy: &str,
    ) -> anyhow::Result<OmittedVersionResolution> {
        resolve_snapshot_omitted_version(&self.snapshot_universe, category, id, policy)
    }
}

fn bounded_progress_ratio(completed: usize, total: usize) -> f64 {
    let completed = u32::try_from(completed).unwrap_or(u32::MAX);
    let total = u32::try_from(total.max(1)).unwrap_or(u32::MAX);
    (f64::from(completed) / f64::from(total)).min(1.0)
}

fn enforce_snapshot_boundary(
    identities: &[ExactDatasetIdentity],
    snapshot_universe: &BTreeMap<ExactDatasetIdentity, SnapshotDatasetEntry>,
    live_documents: Vec<ClosureDocument>,
) -> anyhow::Result<ProviderFetchResult> {
    let live_by_identity = live_documents
        .into_iter()
        .map(|document| (document.identity.clone(), document))
        .collect::<BTreeMap<_, _>>();
    let mut result = ProviderFetchResult::default();
    for identity in identities {
        let Some(snapshot_entry) = snapshot_universe.get(identity) else {
            if live_by_identity.contains_key(identity) {
                result.issues.push(provider_boundary_issue(
                    "snapshot_dataset_not_allowed",
                    identity,
                    "A live exact dataset exists but is absent from the frozen public-release manifest.",
                    &json!({"snapshotAllowed": false, "liveOnly": true}),
                ));
                result.incomplete_identities.insert(identity.clone());
            }
            continue;
        };
        let Some(document) = live_by_identity.get(identity).cloned() else {
            result.issues.push(provider_boundary_issue(
                "snapshot_dataset_unavailable",
                identity,
                "The frozen public-release dataset is no longer readable from the source table.",
                &json!({"expectedCanonicalContentHash": snapshot_entry.canonical_content_hash}),
            ));
            result.incomplete_identities.insert(identity.clone());
            continue;
        };
        let actual_hash = canonical_json_sha256(&document.payload)?;
        if actual_hash != snapshot_entry.canonical_content_hash {
            result.issues.push(provider_boundary_issue(
                    "snapshot_source_drift",
                    identity,
                    "Live content no longer matches the canonical hash frozen in the public-release manifest.",
                    &json!({
                        "expectedCanonicalContentHash": snapshot_entry.canonical_content_hash,
                        "actualCanonicalContentHash": actual_hash,
                    }),
                ));
            result.incomplete_identities.insert(identity.clone());
            continue;
        }
        result.documents.push(document);
    }
    Ok(result)
}

fn resolve_snapshot_omitted_version(
    snapshot_universe: &BTreeMap<ExactDatasetIdentity, SnapshotDatasetEntry>,
    category: DatasetCategory,
    id: Uuid,
    policy: &str,
) -> anyhow::Result<OmittedVersionResolution> {
    if policy == "reject" {
        return Ok(OmittedVersionResolution {
            selected: None,
            candidates: Vec::new(),
            policy: policy.to_owned(),
        });
    }
    if policy != "latest_eligible" {
        return Err(anyhow::anyhow!(
            "unsupported legacy omitted-version policy: {policy}"
        ));
    }
    let mut candidates = snapshot_universe
        .keys()
        .filter(|identity| identity.category == category && identity.id == id)
        .cloned()
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| left.version.cmp(&right.version));
    Ok(OmittedVersionResolution {
        selected: candidates.last().cloned(),
        candidates,
        policy: policy.to_owned(),
    })
}

pub async fn load_scope_closure_worker_input(
    pool: &PgPool,
    closure_check_id: Uuid,
) -> anyhow::Result<ScopeClosureWorkerInput> {
    let row = sqlx::query(
        r"
        WITH _service_role AS (
            SELECT set_config('request.jwt.claim.role', 'service_role', true)
        )
        SELECT public.svc_lcia_scope_closure_check_get_worker_input($1) AS result
        FROM _service_role
        ",
    )
    .bind(closure_check_id)
    .fetch_one(pool)
    .await?;
    let result = row.try_get::<Value, _>("result")?;
    ensure_rpc_ok(&result, "svc_lcia_scope_closure_check_get_worker_input")?;
    let data = result
        .get("data")
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("closure worker input RPC omitted data"))?;
    let input = serde_json::from_value::<ScopeClosureWorkerInput>(data)?;
    validate_worker_input(&input)?;
    validate_worker_input_hashes(pool, &input).await?;
    Ok(input)
}

#[allow(clippy::too_many_lines)]
pub fn validate_worker_input(input: &ScopeClosureWorkerInput) -> anyhow::Result<()> {
    let snapshot = parse_data_snapshot_manifest(&input.data_snapshot_manifest)?;
    if input.requested_scope.roots().is_empty() {
        return Err(anyhow::anyhow!(
            "requested closure scope has no exact roots"
        ));
    }
    if input.requested_scope.version_resolution_policy != "reference-version-resolution-v1" {
        return Err(anyhow::anyhow!(
            "scope closure requires versionResolutionPolicy=reference-version-resolution-v1"
        ));
    }
    if !matches!(
        input
            .requested_scope
            .link_policy
            .provider_universe_policy
            .as_str(),
        "scope_only" | "eligible_transitive_expansion-v1"
    ) {
        return Err(anyhow::anyhow!(
            "unsupported provider universe policy: {}",
            input.requested_scope.link_policy.provider_universe_policy
        ));
    }
    if !matches!(
        input.requested_scope.legacy_omitted_version_policy.as_str(),
        "reject" | "latest_eligible"
    ) {
        return Err(anyhow::anyhow!(
            "unsupported legacy omitted-version policy: {}",
            input.requested_scope.legacy_omitted_version_policy
        ));
    }
    for root in input.requested_scope.roots() {
        validate_version(root.version.as_str())?;
    }
    for (name, value) in [
        ("requestedScopeHash", input.requested_scope_hash.as_str()),
        ("policyFingerprint", input.policy_fingerprint.as_str()),
        ("dataSnapshotToken", input.data_snapshot_token.as_str()),
        (
            "expectedValidatorScannerFingerprint",
            input.expected_validator_scanner_fingerprint.as_str(),
        ),
        ("requestFingerprint", input.request_fingerprint.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(anyhow::anyhow!("closure worker input requires {name}"));
        }
    }
    if snapshot.schema_version != "lcia.scope-closure-data-snapshot.v2" {
        return Err(anyhow::anyhow!(
            "unsupported immutable data snapshot schema: {}",
            snapshot.schema_version
        ));
    }
    if snapshot.requested_scope != input.requested_scope {
        return Err(anyhow::anyhow!(
            "immutable data snapshot manifest differs from requested scope"
        ));
    }
    if input.data_snapshot_manifest_hash.trim().is_empty() || input.publication_epoch < 0 {
        return Err(anyhow::anyhow!("invalid immutable data snapshot metadata"));
    }
    if input.data_snapshot_manifest.get("requestedScope").is_none() {
        return Err(anyhow::anyhow!(
            "immutable data snapshot omits requestedScope"
        ));
    }
    if snapshot
        .current_public_release
        .release_manifest_hash
        .trim()
        .is_empty()
    {
        return Err(anyhow::anyhow!(
            "immutable data snapshot omits the public release manifest hash"
        ));
    }
    let mut identities = BTreeSet::new();
    for entry in &snapshot.datasets {
        validate_version(entry.dataset_version.as_str())?;
        if !identities.insert(entry.identity()) {
            return Err(anyhow::anyhow!(
                "immutable data snapshot contains a duplicate exact dataset identity"
            ));
        }
        for (name, hash) in [
            (
                "versionSignificantHash",
                entry.version_significant_hash.as_str(),
            ),
            ("semanticHash", entry.semantic_hash.as_str()),
            (
                "canonicalContentHash",
                entry.canonical_content_hash.as_str(),
            ),
        ] {
            if hash.len() != 64 || !hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                return Err(anyhow::anyhow!(
                    "immutable data snapshot dataset has invalid {name}"
                ));
            }
        }
    }
    let universe = snapshot_dataset_universe(&snapshot);
    for root in input.requested_scope.roots() {
        if let Some(entry) = universe.get(&root)
            && root.category == DatasetCategory::Processes
            && entry.role != "unit_process"
        {
            return Err(anyhow::anyhow!(
                "requested process root is not a unit_process in the frozen public release: {}",
                root.document_key()
            ));
        }
    }
    Ok(())
}

fn parse_data_snapshot_manifest(value: &Value) -> anyhow::Result<DataSnapshotManifest> {
    serde_json::from_value(value.clone())
        .map_err(|error| anyhow::anyhow!("invalid immutable data snapshot manifest: {error}"))
}

async fn validate_worker_input_hashes(
    pool: &PgPool,
    input: &ScopeClosureWorkerInput,
) -> anyhow::Result<()> {
    // These bindings originate from PostgreSQL `jsonb::text`, whose spacing is
    // deliberately part of the database hash contract.  Recompute with the
    // authoritative SQL helper instead of assuming Rust's byte encoding.
    let requested_scope = input
        .data_snapshot_manifest
        .get("requestedScope")
        .ok_or_else(|| anyhow::anyhow!("immutable data snapshot omits requestedScope"))?;
    let row = sqlx::query(
        r"
        WITH _service_role AS (
            SELECT set_config('request.jwt.claim.role', 'service_role', true)
        )
        SELECT public.lcia_scope_closure_sha256($1::jsonb) AS requested_scope_hash,
               public.lcia_scope_closure_sha256($2::jsonb) AS snapshot_manifest_hash
        FROM _service_role
        ",
    )
    .bind(requested_scope)
    .bind(&input.data_snapshot_manifest)
    .fetch_one(pool)
    .await?;
    let requested_scope_hash = row.try_get::<String, _>("requested_scope_hash")?;
    let snapshot_manifest_hash = row.try_get::<String, _>("snapshot_manifest_hash")?;
    if requested_scope_hash != input.requested_scope_hash {
        return Err(anyhow::anyhow!("requested scope hash mismatch"));
    }
    if snapshot_manifest_hash != input.data_snapshot_manifest_hash
        || snapshot_manifest_hash != input.data_snapshot_token
    {
        return Err(anyhow::anyhow!(
            "immutable data snapshot token/hash mismatch"
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn collect_scope_closure<P: ScopeClosureProvider>(
    provider: &P,
    manifest: &RequestedScopeManifest,
) -> anyhow::Result<ScopeClosureScan> {
    let roots = manifest.roots();
    let root_set = roots.iter().cloned().collect::<BTreeSet<_>>();
    let mut queue = roots.iter().cloned().collect::<VecDeque<_>>();
    let mut scheduled = root_set.clone();
    let mut documents = ClosureDocumentSpoolWriter::new()?;
    let mut edges = JsonlValueSpoolWriter::new("reference-edges-unsorted.jsonl")?;
    let mut resolved_references = Vec::<ResolvedReference>::new();
    let mut omitted_version_resolutions = Vec::new();
    let mut raw_issues = Vec::<ClosureIssue>::new();
    let mut complete = true;

    while !queue.is_empty() {
        enforce_scope_closure_memory_budget("discover_reference_graph")?;
        provider
            .checkpoint(documents.len(), scheduled.len())
            .await?;
        let batch = (0..FETCH_BATCH_SIZE)
            .filter_map(|_| queue.pop_front())
            .collect::<Vec<_>>();
        let fetched = provider.fetch_exact(&batch).await?;
        if !fetched.incomplete_identities.is_empty() {
            complete = false;
        }
        raw_issues.extend(fetched.issues);
        let incomplete_identities = fetched.incomplete_identities;
        let mut fetched_map = fetched
            .documents
            .into_iter()
            .map(|document| (document.identity.clone(), document))
            .collect::<BTreeMap<_, _>>();

        for requested in batch {
            let Some(document) = fetched_map.remove(&requested) else {
                if !incomplete_identities.contains(&requested) {
                    let explicitly_requested = resolved_references.iter().any(|reference| {
                        reference.target == requested
                            && reference.requested_version_state == "explicit"
                    });
                    raw_issues.push(missing_dataset_issue(&requested, explicitly_requested));
                }
                continue;
            };
            let extraction = extract_references(
                document.identity.document_key().as_str(),
                document.identity.category,
                &document.payload,
            );
            raw_issues.extend(
                extraction
                    .issues
                    .iter()
                    .map(|issue| extraction_issue(&document.identity, issue)),
            );
            for edge in extraction.edges {
                let target_category = parse_category(edge.target_category.as_str())?;
                let target_id = Uuid::parse_str(edge.target_uuid.as_str()).ok();
                let target = match (
                    target_id,
                    edge.requested_version_state.as_str(),
                    edge.requested_version.as_deref(),
                ) {
                    (Some(id), "explicit", Some(version)) => Some(ExactDatasetIdentity {
                        category: target_category,
                        id,
                        version: normalize_exact_version(version)?,
                    }),
                    (Some(id), "omitted", _) => {
                        let resolution = provider
                            .resolve_omitted_version(
                                target_category,
                                id,
                                manifest.legacy_omitted_version_policy.as_str(),
                            )
                            .await?;
                        omitted_version_resolutions.push(json!({
                            "source": document.identity,
                            "jsonPath": edge.json_path,
                            "referenceRole": edge.reference_role,
                            "targetCategory": target_category,
                            "targetId": id,
                            "policy": resolution.policy,
                            "candidateUniverse": "frozen-public-release-manifest",
                            "candidates": resolution.candidates,
                            "selected": resolution.selected,
                        }));
                        resolution.selected
                    }
                    _ => None,
                };
                if edge.requested_version_state == "omitted" && target.is_none() {
                    raw_issues.push(omitted_version_issue(&document.identity, &edge, target_id));
                }
                if let Some(target) = target {
                    if target.category == DatasetCategory::Processes
                        && !root_set.contains(&target)
                        && manifest.link_policy.provider_universe_policy == "scope_only"
                    {
                        raw_issues.push(provider_outside_universe_issue(
                            &document.identity,
                            &target,
                            &edge,
                        ));
                        edges.append(&serde_json::to_value(edge)?)?;
                        continue;
                    }
                    resolved_references.push(ResolvedReference {
                        source: document.identity.clone(),
                        target: target.clone(),
                        json_path: edge.json_path.clone(),
                        reference_role: edge.reference_role.clone(),
                        requested_version_state: edge.requested_version_state.clone(),
                    });
                    if scheduled.insert(target.clone()) {
                        queue.push_back(target);
                    }
                }
                edges.append(&serde_json::to_value(edge)?)?;
            }
            documents.append(&document)?;
        }
    }

    let edge_count = edges.event_count;
    let raw_issue_count = raw_issues.len();
    let root_count = roots.len();
    let document_count = documents.len();
    let documents = documents.finish()?;
    let document_spool_bytes = documents.byte_size;
    let edges = edges.finish()?;
    let (scan, metrics) = tokio::task::spawn_blocking(move || {
        finalize_scope_closure_scan(
            &edges,
            resolved_references,
            omitted_version_resolutions,
            raw_issues,
            &roots,
            complete,
            documents,
            scheduled,
        )
    })
    .await??;
    enforce_scope_closure_memory_budget("scope_closure_finalize")?;
    tracing::info!(
        phase = "scope_closure_finalize",
        edge_count,
        raw_issue_count,
        issue_count = scan.issues.len(),
        root_count,
        document_count,
        document_spool_bytes,
        resident_bytes = scope_closure_resident_bytes().unwrap_or(0),
        sort_inputs_ms = duration_millis(metrics.sort_inputs),
        attach_occurrences_ms = duration_millis(metrics.attach_occurrences),
        coalesce_issues_ms = duration_millis(metrics.coalesce_issues),
        affected_roots_ms = duration_millis(metrics.affected_roots),
        sort_issues_ms = duration_millis(metrics.sort_issues),
        total_ms = duration_millis(metrics.total),
        "scope closure graph finalization completed"
    );
    Ok(scan)
}

#[derive(Debug, Clone, Copy)]
struct ScopeClosureFinalizeMetrics {
    sort_inputs: Duration,
    attach_occurrences: Duration,
    coalesce_issues: Duration,
    affected_roots: Duration,
    sort_issues: Duration,
    total: Duration,
}

const fn duration_millis(duration: Duration) -> u128 {
    duration.as_millis()
}

#[allow(clippy::too_many_arguments)]
fn finalize_scope_closure_scan(
    edges: &JsonlValueSpool,
    mut resolved_references: Vec<ResolvedReference>,
    mut omitted_version_resolutions: Vec<Value>,
    raw_issues: Vec<ClosureIssue>,
    roots: &[ExactDatasetIdentity],
    complete: bool,
    documents: ClosureDocumentSpool,
    scheduled: BTreeSet<ExactDatasetIdentity>,
) -> anyhow::Result<(ScopeClosureScan, ScopeClosureFinalizeMetrics)> {
    let total_started = Instant::now();

    let phase_started = Instant::now();
    let edges = sort_jsonl_spool(edges)?;
    resolved_references.sort();
    sort_by_canonical_value(&mut omitted_version_resolutions);
    let sort_inputs = phase_started.elapsed();

    let mut raw_issues = raw_issues;
    let phase_started = Instant::now();
    attach_reference_occurrences(&mut raw_issues, &resolved_references);
    let attach_occurrences = phase_started.elapsed();

    let phase_started = Instant::now();
    let mut issues = coalesce_issues(raw_issues);
    let coalesce_issues = phase_started.elapsed();

    let phase_started = Instant::now();
    let reference_graph = CompactReferenceGraph::from_references(&resolved_references, roots)?;
    compute_affected_roots_batch(&mut issues, roots, &reference_graph);
    let affected_roots = phase_started.elapsed();

    let mut resolved_reference_writer = JsonlValueSpoolWriter::new("resolved-references.jsonl")?;
    for reference in &resolved_references {
        resolved_reference_writer.append(&serde_json::to_value(reference)?)?;
    }
    let resolved_references = resolved_reference_writer.finish()?;

    let phase_started = Instant::now();
    issues.sort_by(|left, right| left.issue_key.cmp(&right.issue_key));
    let sort_issues = phase_started.elapsed();

    let scan = ScopeClosureScan {
        schema_version: "lcia.scope-closure-scan.v1".to_owned(),
        complete,
        roots: roots.to_vec(),
        documents,
        edges,
        resolved_references,
        omitted_version_resolutions,
        issues,
        frontier: Vec::new(),
        provider_universe: scheduled.into_iter().collect(),
        reference_graph,
        tidas_issue_event_count: 0,
        issue_relations: None,
    };
    let metrics = ScopeClosureFinalizeMetrics {
        sort_inputs,
        attach_occurrences,
        coalesce_issues,
        affected_roots,
        sort_issues,
        total: total_started.elapsed(),
    };
    Ok((scan, metrics))
}

fn attach_reference_occurrences(issues: &mut [ClosureIssue], references: &[ResolvedReference]) {
    let mut by_target: BTreeMap<&ExactDatasetIdentity, Vec<&ResolvedReference>> = BTreeMap::new();
    for reference in references {
        by_target
            .entry(&reference.target)
            .or_default()
            .push(reference);
    }
    for issue in issues {
        let Some(target) = issue.source.as_ref().filter(|source| {
            issue.requested_target_id == Some(source.id)
                && issue.requested_target_version.as_deref() == Some(source.version.as_str())
        }) else {
            continue;
        };
        let matches = by_target.get(target).map_or(&[] as &[_], |v| v.as_slice());
        let mut occurrences = matches
            .iter()
            .map(|reference| ClosureIssueOccurrence {
                occurrence_key: canonical_json_sha256(&json!({
                    "issueKey": issue.issue_key,
                    "source": reference.source,
                    "jsonPath": reference.json_path,
                    "referenceRole": reference.reference_role,
                }))
                .unwrap_or_else(|_| Uuid::new_v4().simple().to_string()),
                source: Some(reference.source.clone()),
                json_path: Some(reference.json_path.clone()),
                reference_role: Some(reference.reference_role.clone()),
                details: json!({
                    "requestedVersionState": reference.requested_version_state,
                    "target": reference.target,
                }),
            })
            .collect::<Vec<_>>();
        occurrences.sort_by(|left, right| left.occurrence_key.cmp(&right.occurrence_key));
        occurrences.dedup_by(|left, right| left.occurrence_key == right.occurrence_key);
        if !occurrences.is_empty() {
            issue.occurrence_count = u32::try_from(occurrences.len()).unwrap_or(u32::MAX);
            issue.reference_role = occurrences
                .first()
                .and_then(|occurrence| occurrence.reference_role.clone());
            issue.occurrences = occurrences;
        }
    }
}

fn compute_affected_roots_batch(
    issues: &mut [ClosureIssue],
    roots: &[ExactDatasetIdentity],
    graph: &CompactReferenceGraph,
) {
    let root_ids = roots
        .iter()
        .filter_map(|root| graph.identity_ids.get(root).copied())
        .collect::<Vec<_>>();
    let mut cache = BTreeMap::<
        u32,
        (
            u32,
            Vec<ExactDatasetIdentity>,
            Vec<Vec<ExactDatasetIdentity>>,
        ),
    >::new();

    for issue in issues {
        let Some(source) = issue.source.as_ref() else {
            issue.affected_root_count =
                u32::try_from(issue.affected_roots.len()).unwrap_or(u32::MAX);
            issue
                .affected_roots
                .truncate(ISSUE_INLINE_AFFECTED_ROOT_SAMPLE_LIMIT);
            issue
                .affected_root_witness_paths
                .truncate(ISSUE_INLINE_AFFECTED_ROOT_SAMPLE_LIMIT);
            continue;
        };
        let Some(source_id) = graph.identity_ids.get(source).copied() else {
            continue;
        };
        let (affected_root_count, affected, witnesses) = cache
            .entry(source_id)
            .or_insert_with(|| compute_single_source_affected_roots(source_id, &root_ids, graph))
            .clone();
        issue.affected_root_count = affected_root_count;
        issue.affected_roots = affected;
        issue.witness_path = witnesses.first().cloned().unwrap_or_default();
        issue.affected_root_witness_paths = witnesses;
    }
}

fn compute_single_source_affected_roots(
    source: u32,
    root_ids: &[u32],
    graph: &CompactReferenceGraph,
) -> (
    u32,
    Vec<ExactDatasetIdentity>,
    Vec<Vec<ExactDatasetIdentity>>,
) {
    let mut affected_root_count = 0_u32;
    let mut affected = Vec::new();
    let mut witnesses = Vec::new();
    visit_single_source_affected_roots(source, root_ids, graph, |root, witness| {
        affected_root_count = affected_root_count.saturating_add(1);
        if affected.len() < ISSUE_INLINE_AFFECTED_ROOT_SAMPLE_LIMIT {
            affected.push(root.clone());
            witnesses.push(witness.to_vec());
        }
        Ok(())
    })
    .expect("in-memory affected-root sampling cannot fail");
    (affected_root_count, affected, witnesses)
}

fn visit_single_source_affected_roots(
    source: u32,
    root_ids: &[u32],
    graph: &CompactReferenceGraph,
    mut visit: impl FnMut(&ExactDatasetIdentity, &[ExactDatasetIdentity]) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let mut parent = vec![None::<u32>; graph.identities.len()];
    let mut visited = vec![false; graph.identities.len()];
    let source_index = usize::try_from(source).expect("u32 identity index fits usize");
    visited[source_index] = true;
    let mut queue = VecDeque::from([source]);

    while let Some(node) = queue.pop_front() {
        if let Some(predecessors) = graph
            .reverse
            .get(usize::try_from(node).expect("u32 identity index fits usize"))
        {
            for &predecessor in predecessors {
                let predecessor_index =
                    usize::try_from(predecessor).expect("u32 identity index fits usize");
                if !visited[predecessor_index] {
                    visited[predecessor_index] = true;
                    parent[predecessor_index] = Some(node);
                    queue.push_back(predecessor);
                }
            }
        }
    }

    for &root in root_ids {
        let root_index = usize::try_from(root).expect("u32 identity index fits usize");
        if visited[root_index] {
            let path = reconstruct_witness_path(root, &parent, &graph.identities);
            visit(&graph.identities[root_index], &path)?;
        }
    }
    Ok(())
}

fn reconstruct_witness_path(
    root: u32,
    parent: &[Option<u32>],
    identities: &[ExactDatasetIdentity],
) -> Vec<ExactDatasetIdentity> {
    let mut path = Vec::new();
    let mut current = Some(root);
    while let Some(node) = current {
        let index = usize::try_from(node).expect("u32 identity index fits usize");
        path.push(identities[index].clone());
        current = parent[index];
    }
    path.reverse();
    path
}

#[cfg(test)]
fn populate_affected_roots(scan: &mut ScopeClosureScan) {
    compute_affected_roots_batch(&mut scan.issues, &scan.roots, &scan.reference_graph);
    scan.issues
        .sort_by(|left, right| left.issue_key.cmp(&right.issue_key));
}

#[must_use]
pub fn extract_references(
    document_key: &str,
    category: DatasetCategory,
    payload: &Value,
) -> ReferenceExtractionResult {
    let mut result = ReferenceExtractionResult {
        schema_version: "tidas.reference-extraction-result.v1".to_owned(),
        document_key: document_key.to_owned(),
        source_category: category.table_name().to_owned(),
        edges: Vec::new(),
        issues: Vec::new(),
    };
    walk_references(payload, "$", None, category, &mut result);
    result
}

fn walk_references(
    node: &Value,
    path: &str,
    parent_key: Option<&str>,
    source_category: DatasetCategory,
    result: &mut ReferenceExtractionResult,
) {
    match node {
        Value::Object(object) => {
            if looks_like_reference(object, parent_key) {
                extract_reference(object, path, parent_key, source_category, result);
            }
            for (key, value) in object {
                walk_references(
                    value,
                    format!("{path}.{key}").as_str(),
                    Some(key),
                    source_category,
                    result,
                );
            }
        }
        Value::Array(items) => {
            for (index, item) in items.iter().enumerate() {
                walk_references(
                    item,
                    format!("{path}[{index}]").as_str(),
                    parent_key,
                    source_category,
                    result,
                );
            }
        }
        _ => {}
    }
}

fn looks_like_reference(object: &Map<String, Value>, parent_key: Option<&str>) -> bool {
    object.contains_key("@refObjectId")
        || object.contains_key("@uri")
        || parent_key.is_some_and(|key| key.to_ascii_lowercase().contains("referenceto"))
}

fn extract_reference(
    object: &Map<String, Value>,
    path: &str,
    parent_key: Option<&str>,
    source_category: DatasetCategory,
    result: &mut ReferenceExtractionResult,
) {
    let raw_type = object.get("@type").cloned().unwrap_or(Value::Null);
    let uri = object.get("@uri").cloned().unwrap_or(Value::Null);
    let target_category = raw_type
        .as_str()
        .and_then(DatasetCategory::from_reference_type)
        .or_else(|| uri.as_str().and_then(DatasetCategory::from_uri));
    let role = reference_role(source_category, path, parent_key, target_category.as_ref());

    if target_category.is_none() {
        result.issues.push(reference_issue(
            result,
            "reference_type_unresolved",
            path,
            role,
            "Reference target type cannot be resolved from @type or @uri.",
            json!({"raw_type": raw_type, "uri": uri}),
        ));
    }

    let Some(raw_id) = object
        .get("@refObjectId")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        result.issues.push(reference_issue(
            result,
            "reference_object_id_missing",
            path,
            role,
            "Recognized reference is missing a non-empty @refObjectId.",
            json!({
                "raw_ref_object_id": object.get("@refObjectId").cloned().unwrap_or(Value::Null),
                "raw_type": raw_type,
                "uri": uri,
            }),
        ));
        return;
    };

    if Uuid::parse_str(raw_id).is_err() || raw_id.to_ascii_lowercase() != raw_id {
        result.issues.push(reference_issue(
            result,
            "reference_uuid_invalid",
            path,
            role,
            "Reference @refObjectId is not a canonical lowercase UUID.",
            json!({"raw_ref_object_id": raw_id}),
        ));
    }

    let raw_version = object.get("@version").cloned().unwrap_or(Value::Null);
    let (version_state, requested_version) = match &raw_version {
        Value::Null => ("omitted", None),
        Value::String(version) if validate_version(version).is_ok() => {
            ("explicit", Some(version.clone()))
        }
        value => {
            result.issues.push(reference_issue(
                result,
                "reference_version_invalid",
                path,
                role,
                "Reference @version must match NN.NN or NN.NN.NNN.",
                json!({"requested_version_raw": value}),
            ));
            ("invalid", value.as_str().map(str::to_owned))
        }
    };

    if let Some(target_category) = target_category {
        result.edges.push(ReferenceEdge {
            schema_version: REFERENCE_EDGE_SCHEMA_VERSION.to_owned(),
            document_key: result.document_key.clone(),
            source_category: source_category.table_name().to_owned(),
            target_category: target_category.table_name().to_owned(),
            target_uuid: raw_id.to_owned(),
            requested_version_state: version_state.to_owned(),
            requested_version,
            requested_version_raw: raw_version,
            reference_role: role.to_owned(),
            json_path: path.to_owned(),
            raw_type,
            uri,
        });
    }
}

fn reference_issue(
    result: &ReferenceExtractionResult,
    issue_code: &str,
    json_path: &str,
    reference_role: &str,
    message: &str,
    details: Value,
) -> ReferenceExtractionIssue {
    ReferenceExtractionIssue {
        schema_version: REFERENCE_ISSUE_SCHEMA_VERSION.to_owned(),
        issue_code: issue_code.to_owned(),
        severity: "error".to_owned(),
        document_key: result.document_key.clone(),
        source_category: result.source_category.clone(),
        json_path: json_path.to_owned(),
        reference_role: reference_role.to_owned(),
        message: message.to_owned(),
        details,
    }
}

fn reference_role<'a>(
    source_category: DatasetCategory,
    path: &str,
    parent_key: Option<&str>,
    target_category: Option<&DatasetCategory>,
) -> &'a str {
    let normalized_path = path.to_ascii_lowercase();
    let normalized_key = parent_key.unwrap_or_default().to_ascii_lowercase();
    if source_category == DatasetCategory::Processes
        && target_category == Some(&DatasetCategory::Flows)
        && normalized_path.contains("exchange")
        && normalized_key == "referencetoflowdataset"
    {
        "process_exchange_flow"
    } else if source_category == DatasetCategory::Lciamethods
        && target_category == Some(&DatasetCategory::Flows)
        && (normalized_path.contains("characterisation")
            || normalized_path.contains("characterization"))
    {
        "lcia_factor_flow"
    } else if source_category == DatasetCategory::Lifecyclemodels
        && target_category == Some(&DatasetCategory::Processes)
    {
        "lifecycle_model_process"
    } else {
        "support_document"
    }
}

fn normalize_reference_type(raw: &str) -> String {
    raw.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}

fn validate_version(version: &str) -> anyhow::Result<()> {
    let components = version.split('.').collect::<Vec<_>>();
    let valid = matches!(components.as_slice(), [a, b] if a.len() == 2 && b.len() == 2 && a.chars().all(|c| c.is_ascii_digit()) && b.chars().all(|c| c.is_ascii_digit()))
        || matches!(components.as_slice(), [a, b, c] if a.len() == 2 && b.len() == 2 && c.len() == 3 && a.chars().all(|v| v.is_ascii_digit()) && b.chars().all(|v| v.is_ascii_digit()) && c.chars().all(|v| v.is_ascii_digit()));
    if valid {
        Ok(())
    } else {
        Err(anyhow::anyhow!("invalid exact TIDAS version: {version}"))
    }
}

fn normalize_exact_version(version: &str) -> anyhow::Result<String> {
    validate_version(version)?;
    if version.matches('.').count() == 1 {
        Ok(format!("{version}.000"))
    } else {
        Ok(version.to_owned())
    }
}

fn parse_category(value: &str) -> anyhow::Result<DatasetCategory> {
    match value {
        "contacts" => Ok(DatasetCategory::Contacts),
        "flowproperties" => Ok(DatasetCategory::Flowproperties),
        "flows" => Ok(DatasetCategory::Flows),
        "lciamethods" => Ok(DatasetCategory::Lciamethods),
        "lifecyclemodels" => Ok(DatasetCategory::Lifecyclemodels),
        "processes" => Ok(DatasetCategory::Processes),
        "sources" => Ok(DatasetCategory::Sources),
        "unitgroups" => Ok(DatasetCategory::Unitgroups),
        _ => Err(anyhow::anyhow!(
            "unsupported closure dataset category: {value}"
        )),
    }
}

fn extraction_issue(
    source: &ExactDatasetIdentity,
    issue: &ReferenceExtractionIssue,
) -> ClosureIssue {
    let issue_key = canonical_json_sha256(&json!({
        "code": issue.issue_code,
        "source": source,
        "path": issue.json_path,
        "role": issue.reference_role,
    }))
    .unwrap_or_else(|_| Uuid::new_v4().simple().to_string());
    ClosureIssue {
        issue_key: issue_key.clone(),
        severity: "blocker".to_owned(),
        blocking: true,
        issue_code: issue.issue_code.clone(),
        source: Some(source.clone()),
        json_path: Some(issue.json_path.clone()),
        reference_role: Some(issue.reference_role.clone()),
        requested_target_type: None,
        requested_target_id: None,
        requested_target_version: None,
        message: issue.message.clone(),
        suggested_action: Some(
            "Repair the source reference and rerun closure preflight.".to_owned(),
        ),
        occurrence_count: 1,
        occurrences: vec![ClosureIssueOccurrence {
            occurrence_key: format!("{issue_key}:0"),
            source: Some(source.clone()),
            json_path: Some(issue.json_path.clone()),
            reference_role: Some(issue.reference_role.clone()),
            details: issue.details.clone(),
        }],
        affected_root_count: 0,
        affected_roots: Vec::new(),
        affected_root_witness_paths: Vec::new(),
        witness_path: Vec::new(),
    }
}

fn missing_dataset_issue(
    target: &ExactDatasetIdentity,
    explicitly_requested: bool,
) -> ClosureIssue {
    let issue_code = if explicitly_requested {
        "reference_exact_version_missing"
    } else {
        "reference_target_missing"
    };
    let issue_key = canonical_json_sha256(&json!({"code": issue_code, "target": target}))
        .unwrap_or_else(|_| Uuid::new_v4().simple().to_string());
    ClosureIssue {
        issue_key: issue_key.clone(),
        severity: "blocker".to_owned(),
        blocking: true,
        issue_code: issue_code.to_owned(),
        source: Some(target.clone()),
        json_path: None,
        reference_role: None,
        requested_target_type: Some(target.category.table_name().to_owned()),
        requested_target_id: Some(target.id),
        requested_target_version: Some(target.version.clone()),
        message: format!(
            "Exact referenced dataset {} was not found.",
            target.document_key()
        ),
        suggested_action: Some("Publish or repair the exact referenced revision.".to_owned()),
        occurrence_count: 1,
        occurrences: vec![ClosureIssueOccurrence {
            occurrence_key: format!("{issue_key}:0"),
            source: Some(target.clone()),
            json_path: None,
            reference_role: None,
            details: json!({}),
        }],
        affected_root_count: 0,
        affected_roots: Vec::new(),
        affected_root_witness_paths: Vec::new(),
        witness_path: Vec::new(),
    }
}

fn provider_boundary_issue(
    code: &str,
    identity: &ExactDatasetIdentity,
    message: &str,
    details: &Value,
) -> ClosureIssue {
    let issue_key = canonical_json_sha256(&json!({
        "code": code,
        "identity": identity,
    }))
    .unwrap_or_else(|_| Uuid::new_v4().simple().to_string());
    let evidence = canonical_value(&details);
    ClosureIssue {
        issue_key: issue_key.clone(),
        severity: "blocker".to_owned(),
        blocking: true,
        issue_code: code.to_owned(),
        source: Some(identity.clone()),
        json_path: None,
        reference_role: None,
        requested_target_type: Some(identity.category.table_name().to_owned()),
        requested_target_id: Some(identity.id),
        requested_target_version: Some(identity.version.clone()),
        message: format!("{message} Evidence: {evidence}"),
        suggested_action: Some(
            "Recreate the closure request from a consistent published release snapshot.".to_owned(),
        ),
        occurrence_count: 1,
        occurrences: vec![ClosureIssueOccurrence {
            occurrence_key: format!("{issue_key}:0"),
            source: Some(identity.clone()),
            json_path: None,
            reference_role: None,
            details: details.clone(),
        }],
        affected_root_count: 0,
        affected_roots: Vec::new(),
        affected_root_witness_paths: Vec::new(),
        witness_path: Vec::new(),
    }
}

fn provider_outside_universe_issue(
    source: &ExactDatasetIdentity,
    target: &ExactDatasetIdentity,
    edge: &ReferenceEdge,
) -> ClosureIssue {
    let issue_key = canonical_json_sha256(&json!({
        "code": "provider_outside_scope_universe",
        "source": source,
        "path": edge.json_path,
        "target": target,
    }))
    .unwrap_or_else(|_| Uuid::new_v4().simple().to_string());
    ClosureIssue {
        issue_key: issue_key.clone(),
        severity: "blocker".to_owned(),
        blocking: true,
        issue_code: "provider_outside_scope_universe".to_owned(),
        source: Some(source.clone()),
        json_path: Some(edge.json_path.clone()),
        reference_role: Some(edge.reference_role.clone()),
        requested_target_type: Some(target.category.table_name().to_owned()),
        requested_target_id: Some(target.id),
        requested_target_version: Some(target.version.clone()),
        message: "Referenced process is outside the frozen scope-only provider universe."
            .to_owned(),
        suggested_action: Some(
            "Include the provider as a root or use the tracked transitive-expansion policy."
                .to_owned(),
        ),
        occurrence_count: 1,
        occurrences: vec![ClosureIssueOccurrence {
            occurrence_key: format!("{issue_key}:0"),
            source: Some(source.clone()),
            json_path: Some(edge.json_path.clone()),
            reference_role: Some(edge.reference_role.clone()),
            details: json!({"target": target}),
        }],
        affected_root_count: 0,
        affected_roots: Vec::new(),
        affected_root_witness_paths: Vec::new(),
        witness_path: Vec::new(),
    }
}

fn omitted_version_issue(
    source: &ExactDatasetIdentity,
    edge: &ReferenceEdge,
    target_id: Option<Uuid>,
) -> ClosureIssue {
    let issue_key = canonical_json_sha256(&json!({
        "code": "reference_version_omitted",
        "source": source,
        "path": edge.json_path,
        "target": edge.target_uuid,
    }))
    .unwrap_or_else(|_| Uuid::new_v4().simple().to_string());
    ClosureIssue {
        issue_key: issue_key.clone(),
        severity: "blocker".to_owned(),
        blocking: true,
        issue_code: "reference_version_omitted".to_owned(),
        source: Some(source.clone()),
        json_path: Some(edge.json_path.clone()),
        reference_role: Some(edge.reference_role.clone()),
        requested_target_type: Some(edge.target_category.clone()),
        requested_target_id: target_id,
        requested_target_version: None,
        message: "Reference omits @version and the selected policy did not resolve it.".to_owned(),
        suggested_action: Some("Bind the reference to an exact published version.".to_owned()),
        occurrence_count: 1,
        occurrences: vec![ClosureIssueOccurrence {
            occurrence_key: format!("{issue_key}:0"),
            source: Some(source.clone()),
            json_path: Some(edge.json_path.clone()),
            reference_role: Some(edge.reference_role.clone()),
            details: json!({"targetId": target_id}),
        }],
        affected_root_count: 0,
        affected_roots: Vec::new(),
        affected_root_witness_paths: Vec::new(),
        witness_path: Vec::new(),
    }
}

fn coalesce_issues(issues: Vec<ClosureIssue>) -> Vec<ClosureIssue> {
    let mut output = BTreeMap::<String, ClosureIssue>::new();
    for issue in issues {
        coalesce_issue_into(&mut output, issue);
    }
    finish_coalesced_issues(output)
}

fn coalesce_issue_into(output: &mut BTreeMap<String, ClosureIssue>, mut issue: ClosureIssue) {
    issue
        .occurrences
        .sort_by(|left, right| left.occurrence_key.cmp(&right.occurrence_key));
    issue
        .occurrences
        .dedup_by(|left, right| left.occurrence_key == right.occurrence_key);
    match output.entry(issue.issue_key.clone()) {
        std::collections::btree_map::Entry::Occupied(mut existing) => {
            let existing = existing.get_mut();
            for occurrence in issue.occurrences {
                match existing.occurrences.binary_search_by(|candidate| {
                    candidate.occurrence_key.cmp(&occurrence.occurrence_key)
                }) {
                    Ok(_) => {}
                    Err(index) => existing.occurrences.insert(index, occurrence),
                }
            }
            existing.occurrence_count =
                u32::try_from(existing.occurrences.len()).unwrap_or(u32::MAX);
        }
        std::collections::btree_map::Entry::Vacant(entry) => {
            issue.occurrence_count = u32::try_from(issue.occurrences.len()).unwrap_or(u32::MAX);
            entry.insert(issue);
        }
    }
}

fn finish_coalesced_issues(mut output: BTreeMap<String, ClosureIssue>) -> Vec<ClosureIssue> {
    for issue in output.values_mut() {
        issue
            .occurrences
            .sort_by(|left, right| left.occurrence_key.cmp(&right.occurrence_key));
        issue
            .occurrences
            .dedup_by(|left, right| left.occurrence_key == right.occurrence_key);
        issue.occurrence_count = u32::try_from(issue.occurrences.len()).unwrap_or(u32::MAX);
    }
    output.into_values().collect()
}

fn normalize_database_issue_severities(issues: &mut [ClosureIssue]) -> anyhow::Result<()> {
    for issue in issues {
        issue.severity = match (issue.blocking, issue.severity.as_str()) {
            (true, "blocker" | "error" | "fatal") => "blocker".to_owned(),
            (false, "warning") => "warning".to_owned(),
            (false, "info") => "info".to_owned(),
            (true, severity @ ("warning" | "info")) => {
                return Err(anyhow::anyhow!(
                    "blocking closure issue {} cannot use non-blocking severity {severity}",
                    issue.issue_code
                ));
            }
            (false, severity @ ("blocker" | "error" | "fatal")) => {
                return Err(anyhow::anyhow!(
                    "non-blocking closure issue {} cannot use blocking severity {severity}",
                    issue.issue_code
                ));
            }
            (_, severity) => {
                return Err(anyhow::anyhow!(
                    "closure issue {} has unsupported severity {severity}",
                    issue.issue_code
                ));
            }
        };
    }
    Ok(())
}

async fn fetch_exact_documents(
    pool: &PgPool,
    identities: &[ExactDatasetIdentity],
) -> anyhow::Result<Vec<ClosureDocument>> {
    let mut grouped = BTreeMap::<DatasetCategory, Vec<&ExactDatasetIdentity>>::new();
    for identity in identities {
        grouped.entry(identity.category).or_default().push(identity);
    }
    let mut documents = Vec::new();
    for (category, group) in grouped {
        let read_keys = group
            .iter()
            .map(|identity| {
                let locator_id = if category == DatasetCategory::Lciamethods {
                    lcia_method_artifact_locator_id(identity)
                } else {
                    identity.id
                };
                ((*identity).clone(), locator_id)
            })
            .collect::<Vec<_>>();
        let mut builder = exact_documents_query_builder(category, &read_keys);
        let rows = builder.build().fetch_all(pool).await?;
        for row in rows {
            let locator_id = row.try_get::<Uuid, _>("id")?;
            let version = row.try_get::<String, _>("version")?;
            let requested = read_keys
                .iter()
                .find(|(identity, expected_locator)| {
                    *expected_locator == locator_id && identity.version == version
                })
                .map(|(identity, _)| identity.clone())
                .ok_or_else(|| anyhow::anyhow!("LCIA/source fetch returned an unexpected row"))?;
            documents.push(ClosureDocument {
                identity: requested,
                payload: row.try_get("document")?,
            });
        }
    }
    documents.sort_by(|left, right| left.identity.cmp(&right.identity));
    Ok(documents)
}

fn exact_documents_query_builder(
    category: DatasetCategory,
    read_keys: &[(ExactDatasetIdentity, Uuid)],
) -> QueryBuilder<'static, Postgres> {
    let table = category.table_name();
    let document_expression = if category == DatasetCategory::Lciamethods {
        "COALESCE(json, json_ordered::jsonb)"
    } else {
        "json_ordered::jsonb"
    };
    let mut builder = QueryBuilder::<Postgres>::new(format!(
        "SELECT id, btrim(version::text) AS version, {document_expression} AS document FROM public.{table} WHERE (id, btrim(version::text)) IN ("
    ));
    for (index, (identity, locator_id)) in read_keys.iter().enumerate() {
        if index > 0 {
            builder.push(", ");
        }
        builder
            .push("(")
            .push_bind(*locator_id)
            .push(", ")
            .push_bind(identity.version.clone())
            .push(")");
    }
    builder.push(") ORDER BY id, btrim(version::text)");
    builder
}

fn lcia_method_artifact_locator_id(identity: &ExactDatasetIdentity) -> Uuid {
    RELEASE_METHOD_IDENTITIES
        .iter()
        .find(|(method_id, version, _)| {
            Uuid::parse_str(method_id) == Ok(identity.id) && *version == identity.version.as_str()
        })
        .and_then(|(_, _, locator_id)| Uuid::parse_str(locator_id).ok())
        .unwrap_or(identity.id)
}

pub fn canonical_json_sha256<T: Serialize>(value: &T) -> anyhow::Result<String> {
    let value = serde_json::to_value(value)?;
    let mut encoded = Vec::new();
    write_canonical_json(&value, &mut encoded)?;
    Ok(hex::encode(Sha256::digest(encoded)))
}

fn canonical_value<T: Serialize>(value: &T) -> String {
    canonical_json_bytes(value)
        .map(|bytes| String::from_utf8_lossy(bytes.as_slice()).into_owned())
        .unwrap_or_default()
}

fn sort_by_canonical_value<T: Serialize>(values: &mut [T]) {
    values.sort_by_cached_key(canonical_value);
}

fn canonical_json_bytes<T: Serialize>(value: &T) -> anyhow::Result<Vec<u8>> {
    let value = serde_json::to_value(value)?;
    let mut output = Vec::new();
    write_canonical_json(&value, &mut output)?;
    Ok(output)
}

fn write_canonical_json(value: &Value, output: &mut Vec<u8>) -> anyhow::Result<()> {
    match value {
        Value::Object(object) => {
            output.push(b'{');
            let mut entries = object.iter().collect::<Vec<_>>();
            entries.sort_by_key(|(key, _)| *key);
            for (index, (key, item)) in entries.into_iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                serde_json::to_writer(&mut *output, key)?;
                output.push(b':');
                write_canonical_json(item, output)?;
            }
            output.push(b'}');
        }
        Value::Array(items) => {
            output.push(b'[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                write_canonical_json(item, output)?;
            }
            output.push(b']');
        }
        _ => serde_json::to_writer(output, value)?,
    }
    Ok(())
}

fn ensure_rpc_ok(result: &Value, name: &str) -> anyhow::Result<()> {
    if result.get("ok").and_then(Value::as_bool) == Some(true) {
        Ok(())
    } else {
        Err(anyhow::anyhow!("{name} returned non-ok result: {result}"))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScopeClosureEvidence {
    pub schema_version: String,
    pub source_fingerprint: String,
    pub resolution_map_hash: String,
    pub closure_bundle_hash: String,
    pub closure_bundle_artifact_id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_artifact_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_index_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_build_contract_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_format: Option<String>,
    pub report_artifact_manifest_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence_hash: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScopeClosureExecutionResult {
    pub closure_check_id: Uuid,
    pub worker_job_id: Uuid,
    pub status: String,
    pub scan_completeness: String,
    pub certificate_hash: Option<String>,
    pub evidence: ScopeClosureEvidence,
    pub report_artifact_id: Uuid,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ScanExecutionClaim {
    Acquired,
    Busy,
    Completed { completed_check_id: Uuid },
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct ArtifactManifestEntry {
    artifact_type: String,
    artifact_role: ScopeClosureArtifactRole,
    file_name: String,
    content_type: String,
    byte_size: usize,
    checksum_sha256: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ScopeClosureArtifactRole {
    ClosureReport,
    CompleteMachineResult,
    ClosureBundle,
}

#[derive(Debug, Clone)]
struct PreparedArtifact {
    descriptor: ArtifactManifestEntry,
    path: PathBuf,
    _temp: Arc<TempDir>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ScopeClosureArtifactWriteSetHeader {
    write_set_id: Uuid,
    closure_check_id: Uuid,
    worker_job_id: Uuid,
    request_id: Uuid,
    publication_mode: String,
    reused_from_check_id: Option<Uuid>,
    status: String,
    write_token: Uuid,
    contract_version: String,
    expected_descriptor_count: u64,
    registered_descriptor_count: u64,
    registered_batch_count: u64,
    descriptor_set_sha256: String,
    required_primary_roles: Value,
    upload_eligible: bool,
    #[serde(default)]
    artifact_map: BTreeMap<String, Uuid>,
    #[serde(default)]
    batches: Vec<ScopeClosureArtifactWriteSetBatch>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ScopeClosureArtifactWriteSetBatch {
    batch_id: Uuid,
    item_count: u64,
    first_ordinal: u64,
    last_ordinal: u64,
}

struct ScopeClosureArtifactWriteSetExpectation<'a> {
    closure_check_id: Uuid,
    worker_job_id: Uuid,
    request_id: Uuid,
    publication_mode: &'static str,
    reused_from_check_id: Option<Uuid>,
    expected_descriptor_count: u64,
    descriptor_set_sha256: &'a str,
    required_primary_roles: &'a Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct IssuePartitionManifestEntry {
    relation: String,
    path: String,
    media_type: String,
    record_count: u64,
    uncompressed_byte_size: u64,
    uncompressed_sha256: String,
    compressed_byte_size: u64,
    compressed_sha256: String,
    first_issue_key: String,
    last_issue_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(not(test), allow(dead_code))]
struct IssueRelationStreamHashesV2 {
    issues: String,
    occurrences: String,
    affected_roots: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(not(test), allow(dead_code))]
struct IssuePartitionManifestV2 {
    schema_version: String,
    closure_check_id: Uuid,
    logical_issue_stream_sha256: String,
    logical_issue_event_count: u64,
    partition_max_records: u64,
    partition_max_uncompressed_bytes: u64,
    issue_count: u64,
    occurrence_count: u64,
    affected_root_count: u64,
    relation_stream_sha256: IssueRelationStreamHashesV2,
    rpc_issue_sample_limit: usize,
    rpc_occurrence_sample_limit_per_issue: usize,
    rpc_affected_root_sample_limit_per_issue: usize,
    xlsx_issue_sample_limit: usize,
    xlsx_occurrence_sample_limit: usize,
    xlsx_affected_root_sample_limit: usize,
    partitions: Vec<IssuePartitionManifestEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CompleteMachineEvidenceEntry {
    relation: String,
    path: String,
    media_type: String,
    record_count: u64,
    uncompressed_byte_size: u64,
    uncompressed_sha256: String,
    compressed_byte_size: u64,
    compressed_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct IssueRelationStreamHashesV3 {
    issues: String,
    tidas_issue_stream: String,
    root_impact_index: String,
    frozen_reference_graph: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct IssueManifestOrdering {
    issue_key: String,
    root_ordinal: String,
    graph_node_ordinal: String,
    root_impact_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct IssueManifestCompatibility {
    readable_schema_versions: Vec<String>,
    v2_affected_root_projection: String,
    public_transport: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct IssuePartitionManifestV3 {
    schema_version: String,
    closure_check_id: Uuid,
    logical_issue_stream_sha256: String,
    logical_issue_event_count: u64,
    logical_issue_stream_byte_size: u64,
    partition_max_records: u64,
    partition_max_uncompressed_bytes: u64,
    issue_count: u64,
    occurrence_count: u64,
    affected_root_count: u64,
    expanded_affected_root_record_count: u64,
    root_impact_record_count: u64,
    root_count: u64,
    graph_node_count: u64,
    graph_edge_count: u64,
    relation_stream_sha256: IssueRelationStreamHashesV3,
    ordering: IssueManifestOrdering,
    rpc_issue_sample_limit: usize,
    rpc_occurrence_sample_limit_per_issue: usize,
    rpc_affected_root_sample_limit_per_issue: usize,
    xlsx_issue_sample_limit: usize,
    xlsx_occurrence_sample_limit: usize,
    xlsx_affected_root_sample_limit: usize,
    compatibility: IssueManifestCompatibility,
    evidence: Vec<CompleteMachineEvidenceEntry>,
    partitions: Vec<IssuePartitionManifestEntry>,
}

struct IssuePartitionAccumulator {
    temp: Arc<TempDir>,
    relation: &'static str,
    max_records: u64,
    max_uncompressed_bytes: u64,
    active: Option<ActiveIssuePartition>,
    entries: Vec<IssuePartitionManifestEntry>,
    artifacts: Vec<PreparedArtifact>,
    relation_uncompressed_digest: Sha256,
}

struct ActiveIssuePartition {
    relative_path: String,
    path: PathBuf,
    encoder: zstd::stream::write::Encoder<'static, BufWriter<File>>,
    uncompressed_digest: Sha256,
    record_count: u64,
    uncompressed_bytes: u64,
    first_issue_key: String,
    last_issue_key: String,
}

#[derive(Debug)]
struct TidasBatchValidation {
    describe: Value,
    final_event: Value,
    issue_events: JsonlValueSpool,
}

#[derive(Debug)]
struct ClosureBundleFile {
    temp: Arc<TempDir>,
    path: PathBuf,
    byte_size: u64,
    sha256: String,
}

#[derive(Debug)]
struct JsonlValueSpool {
    _temp: TempDir,
    path: PathBuf,
    event_count: u64,
    byte_size: u64,
    sha256: String,
}

impl JsonlValueSpool {
    #[cfg(test)]
    fn empty(file_name: &str) -> anyhow::Result<Self> {
        JsonlValueSpoolWriter::new(file_name)?.finish()
    }

    fn len(&self) -> usize {
        usize::try_from(self.event_count).unwrap_or(usize::MAX)
    }

    fn visit(&self, visit: impl FnMut(Value) -> anyhow::Result<()>) -> anyhow::Result<()> {
        tidas_cli::visit_jsonl(&self.path, visit)
    }
}

impl Serialize for JsonlValueSpool {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let file = File::open(&self.path).map_err(S::Error::custom)?;
        let mut sequence = serializer.serialize_seq(usize::try_from(self.event_count).ok())?;
        for line in BufReader::new(file).lines() {
            let line = line.map_err(S::Error::custom)?;
            if line.trim().is_empty() {
                continue;
            }
            let value = serde_json::from_str::<Value>(&line).map_err(S::Error::custom)?;
            sequence.serialize_element(&value)?;
        }
        sequence.end()
    }
}

struct JsonlValueSpoolWriter {
    temp: TempDir,
    path: PathBuf,
    writer: BufWriter<File>,
    digest: Sha256,
    event_count: u64,
    byte_size: u64,
    enforce_validation_limits: bool,
}

#[derive(Debug)]
#[cfg_attr(not(test), allow(dead_code))]
struct SortedJsonlRuns {
    _temp: TempDir,
    run_paths: Vec<PathBuf>,
    event_count: u64,
    byte_size: u64,
}

struct SortedJsonlRunWriter {
    role: &'static str,
    run_bytes: usize,
    merge_fan_in: usize,
    temp: TempDir,
    run_paths: Vec<PathBuf>,
    buffered: Vec<Vec<u8>>,
    buffered_bytes: usize,
    event_count: u64,
    byte_size: u64,
}

const SORT_MERGE_FAN_IN: usize = 64;
const RELATION_TEMP_ADMISSION_SAFETY_PERCENT: u64 = 125;
const RELATION_TEMP_ACTIVE_WINDOW_COUNT: u64 = 4;

impl SortedJsonlRunWriter {
    fn new(role: &'static str) -> anyhow::Result<Self> {
        Ok(Self {
            role,
            run_bytes: VALIDATION_SORT_RUN_BYTES,
            merge_fan_in: SORT_MERGE_FAN_IN,
            temp: TempDir::new()?,
            run_paths: Vec::new(),
            buffered: Vec::new(),
            buffered_bytes: 0,
            event_count: 0,
            byte_size: 0,
        })
    }

    #[cfg(test)]
    fn with_limits(
        role: &'static str,
        run_bytes: usize,
        merge_fan_in: usize,
    ) -> anyhow::Result<Self> {
        if run_bytes == 0 || merge_fan_in < 2 {
            return Err(anyhow::anyhow!(
                "derived relation sort limits must use positive runs and fan-in >= 2"
            ));
        }
        let mut writer = Self::new(role)?;
        writer.run_bytes = run_bytes;
        writer.merge_fan_in = merge_fan_in;
        Ok(writer)
    }

    fn append(&mut self, value: &Value) -> anyhow::Result<()> {
        let bytes = canonical_json_bytes(value)?;
        let record_bytes = bytes
            .len()
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("derived relation record byte count overflow"))?;
        if u64::try_from(record_bytes)? > ISSUE_PARTITION_MAX_UNCOMPRESSED_BYTES {
            return Err(anyhow::anyhow!(
                "derived_relation_record_too_large: role={}, bytes={}, max={}",
                self.role,
                record_bytes,
                ISSUE_PARTITION_MAX_UNCOMPRESSED_BYTES
            ));
        }
        self.byte_size = self
            .byte_size
            .checked_add(u64::try_from(record_bytes)?)
            .ok_or_else(|| anyhow::anyhow!("derived relation byte count overflow"))?;
        self.event_count = self
            .event_count
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("derived relation event count overflow"))?;
        self.buffered_bytes = self
            .buffered_bytes
            .checked_add(record_bytes)
            .ok_or_else(|| anyhow::anyhow!("derived relation run byte count overflow"))?;
        self.buffered.push(bytes);
        if self.buffered_bytes >= self.run_bytes {
            self.flush_run()?;
        }
        Ok(())
    }

    fn flush_run(&mut self) -> anyhow::Result<()> {
        if self.buffered.is_empty() {
            return Ok(());
        }
        ensure_relation_temp_free_space(
            self.temp.path(),
            u64::try_from(self.buffered_bytes)?,
            self.role,
        )?;
        let path = self.temp.path().join(format!(
            "{}-run-{:06}.jsonl",
            self.role,
            self.run_paths.len()
        ));
        write_sorted_jsonl_run_to_path(&path, &mut self.buffered)?;
        self.run_paths.push(path);
        self.buffered_bytes = 0;
        Ok(())
    }

    fn finish(mut self) -> anyhow::Result<SortedJsonlRuns> {
        self.flush_run()?;
        let mut pass = 0_usize;
        while self.run_paths.len() > self.merge_fan_in {
            let previous = std::mem::take(&mut self.run_paths);
            let mut compacted = Vec::new();
            for (group_index, group) in previous.chunks(self.merge_fan_in).enumerate() {
                let planned_bytes = group.iter().try_fold(0_u64, |total, path| {
                    Ok::<_, anyhow::Error>(total.saturating_add(fs::metadata(path)?.len()))
                })?;
                ensure_relation_temp_free_space(self.temp.path(), planned_bytes, self.role)?;
                let output = self.temp.path().join(format!(
                    "{}-merge-{pass:03}-{group_index:06}.jsonl",
                    self.role
                ));
                merge_sorted_jsonl_runs(group, Some(&output), |_| Ok(()))?;
                for path in group {
                    fs::remove_file(path)?;
                }
                compacted.push(output);
            }
            self.run_paths = compacted;
            pass = pass.saturating_add(1);
        }
        Ok(SortedJsonlRuns {
            _temp: self.temp,
            run_paths: self.run_paths,
            event_count: self.event_count,
            byte_size: self.byte_size,
        })
    }
}

impl SortedJsonlRuns {
    fn visit(&self, mut visit: impl FnMut(Value) -> anyhow::Result<()>) -> anyhow::Result<()> {
        merge_sorted_jsonl_runs(&self.run_paths, None, |line| {
            visit(serde_json::from_slice(line)?)
        })?;
        Ok(())
    }
}

fn relation_temp_admission_bytes(raw_event_count: u64, raw_byte_size: u64) -> u64 {
    let average_raw_event_bytes = raw_byte_size
        .checked_add(raw_event_count.saturating_sub(1))
        .and_then(|bytes| bytes.checked_div(raw_event_count.max(1)))
        .unwrap_or(raw_byte_size);

    // The initial admission covers only stages whose size can be derived from the observed raw
    // stream. Affected-root fan-out is topology-dependent and is therefore admitted incrementally
    // from actual bytes at each bounded sort-run and merge boundary. In particular, the number of
    // requested roots is not a per-event fan-out estimate.
    let merge_runs = average_raw_event_bytes
        .saturating_mul(raw_event_count)
        .saturating_mul(5);
    let issue_runs = merge_runs;
    let occurrence_runs = average_raw_event_bytes
        .saturating_mul(raw_event_count)
        .saturating_mul(2);
    let active_outputs =
        ISSUE_PARTITION_MAX_UNCOMPRESSED_BYTES.saturating_mul(RELATION_TEMP_ACTIVE_WINDOW_COUNT);
    raw_byte_size
        .saturating_add(merge_runs.saturating_mul(2))
        .saturating_add(issue_runs)
        .saturating_add(occurrence_runs)
        .saturating_add(active_outputs)
        .saturating_mul(RELATION_TEMP_ADMISSION_SAFETY_PERCENT)
        .saturating_add(99)
        / 100
}

fn admit_relation_temp_space(events: &JsonlValueSpool) -> anyhow::Result<u64> {
    let planned = relation_temp_admission_bytes(events.event_count, events.byte_size);
    let available = fs2::available_space(events.path.as_path())?;
    ensure_relation_temp_capacity(
        available,
        planned,
        "initial_observed_raw",
        Some(events.event_count),
        Some(events.byte_size),
    )?;
    Ok(planned)
}

fn ensure_relation_temp_free_space(
    path: &Path,
    planned_bytes: u64,
    stage: &str,
) -> anyhow::Result<()> {
    ensure_relation_temp_capacity(
        fs2::available_space(path)?,
        planned_bytes,
        stage,
        None,
        None,
    )
}

fn ensure_relation_temp_capacity(
    available: u64,
    planned: u64,
    stage: &str,
    raw_events: Option<u64>,
    raw_bytes: Option<u64>,
) -> anyhow::Result<()> {
    let required = planned
        .checked_add(SCOPE_CLOSURE_TEMP_FREE_SPACE_RESERVE_BYTES)
        .ok_or_else(|| anyhow::anyhow!("scope closure relation temp-space requirement overflow"))?;
    if available >= required {
        return Ok(());
    }
    Err(anyhow::anyhow!(
        "scope_closure_relation_temp_space_low: stage={stage}, available={available}, required={required}, planned={planned}, reserve={SCOPE_CLOSURE_TEMP_FREE_SPACE_RESERVE_BYTES}, raw_events={}, raw_bytes={}, safety_percent={RELATION_TEMP_ADMISSION_SAFETY_PERCENT}",
        raw_events.map_or_else(|| "measured".to_owned(), |value| value.to_string()),
        raw_bytes.map_or_else(|| "measured".to_owned(), |value| value.to_string())
    ))
}

impl JsonlValueSpoolWriter {
    fn new(file_name: &str) -> anyhow::Result<Self> {
        let temp = TempDir::new()?;
        let path = temp.path().join(file_name);
        let file = File::create(&path)?;
        advise_sequential_access(&file);
        let writer = BufWriter::new(file);
        Ok(Self {
            temp,
            path,
            writer,
            digest: Sha256::new(),
            event_count: 0,
            byte_size: 0,
            enforce_validation_limits: true,
        })
    }

    fn new_derived(file_name: &str) -> anyhow::Result<Self> {
        let mut writer = Self::new(file_name)?;
        writer.enforce_validation_limits = false;
        Ok(writer)
    }

    fn append(&mut self, event: &Value) -> anyhow::Result<()> {
        self.append_canonical_bytes(canonical_json_bytes(event)?)
    }

    fn append_raw_jsonl_line(&mut self, bytes: &[u8]) -> anyhow::Result<()> {
        if bytes.last() != Some(&b'\n') {
            return Err(anyhow::anyhow!(
                "validation issue stream line must end with a newline"
            ));
        }
        self.writer.write_all(bytes)?;
        self.digest.update(bytes);
        self.record_append(bytes.len())
    }

    fn append_canonical_bytes(&mut self, mut bytes: Vec<u8>) -> anyhow::Result<()> {
        bytes.push(b'\n');
        self.writer.write_all(&bytes)?;
        self.digest.update(&bytes);
        self.record_append(bytes.len())
    }

    fn record_append(&mut self, byte_count: usize) -> anyhow::Result<()> {
        self.byte_size = self
            .byte_size
            .checked_add(u64::try_from(byte_count)?)
            .ok_or_else(|| anyhow::anyhow!("validation issue spool byte count overflow"))?;
        self.event_count = self
            .event_count
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("validation issue spool event count overflow"))?;
        if self.enforce_validation_limits
            && (self.byte_size > VALIDATION_ISSUE_SPOOL_MAX_BYTES
                || self.event_count > VALIDATION_ISSUE_SPOOL_MAX_EVENTS)
        {
            return Err(anyhow::anyhow!(
                "validation issue spool exceeded bounded capacity: bytes={}/{}, events={}/{}",
                self.byte_size,
                VALIDATION_ISSUE_SPOOL_MAX_BYTES,
                self.event_count,
                VALIDATION_ISSUE_SPOOL_MAX_EVENTS
            ));
        }
        Ok(())
    }

    fn finish(mut self) -> anyhow::Result<JsonlValueSpool> {
        self.writer.flush()?;
        release_file_cache(self.writer.get_ref());
        drop(self.writer);
        Ok(JsonlValueSpool {
            _temp: self.temp,
            path: self.path,
            event_count: self.event_count,
            byte_size: self.byte_size,
            sha256: hex::encode(self.digest.finalize()),
        })
    }
}

fn sort_jsonl_spool(spool: &JsonlValueSpool) -> anyhow::Result<JsonlValueSpool> {
    sort_jsonl_spool_with_run_bytes(spool, VALIDATION_SORT_RUN_BYTES)
}

fn sort_jsonl_spool_with_run_bytes(
    spool: &JsonlValueSpool,
    run_bytes: usize,
) -> anyhow::Result<JsonlValueSpool> {
    if run_bytes == 0 {
        return Err(anyhow::anyhow!(
            "validation issue sort run budget must be positive"
        ));
    }
    let runs = TempDir::new()?;
    ensure_temp_free_space(runs.path(), spool.byte_size.saturating_mul(2))?;
    let mut run_paths = Vec::new();
    let mut buffered = Vec::<Vec<u8>>::new();
    let mut buffered_bytes = 0_usize;
    spool.visit(|event| {
        let bytes = canonical_json_bytes(&event)?;
        buffered_bytes = buffered_bytes.saturating_add(bytes.len());
        buffered.push(bytes);
        if buffered_bytes >= run_bytes {
            run_paths.push(write_sorted_jsonl_run(
                runs.path(),
                run_paths.len(),
                &mut buffered,
            )?);
            buffered_bytes = 0;
        }
        Ok(())
    })?;
    if !buffered.is_empty() {
        run_paths.push(write_sorted_jsonl_run(
            runs.path(),
            run_paths.len(),
            &mut buffered,
        )?);
    }

    let mut output = JsonlValueSpoolWriter::new("validation-issues-sorted.jsonl")?;
    let mut readers = run_paths
        .iter()
        .map(|path| File::open(path).map(BufReader::new))
        .collect::<std::io::Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::<Reverse<(Vec<u8>, usize)>>::new();
    for (index, reader) in readers.iter_mut().enumerate() {
        if let Some(line) = read_canonical_jsonl_line(reader)? {
            heap.push(Reverse((line, index)));
        }
    }
    while let Some(Reverse((line, index))) = heap.pop() {
        output.append_canonical_bytes(line)?;
        if let Some(next) = read_canonical_jsonl_line(&mut readers[index])? {
            heap.push(Reverse((next, index)));
        }
    }
    let output = output.finish()?;
    if output.event_count != spool.event_count {
        return Err(anyhow::anyhow!(
            "validation issue external sort count mismatch: expected {}, got {}",
            spool.event_count,
            output.event_count
        ));
    }
    Ok(output)
}

fn ensure_temp_free_space(path: &Path, planned_bytes: u64) -> anyhow::Result<()> {
    let available = fs2::available_space(path)?;
    let required = planned_bytes
        .checked_add(SCOPE_CLOSURE_TEMP_FREE_SPACE_RESERVE_BYTES)
        .ok_or_else(|| anyhow::anyhow!("scope closure temporary-space requirement overflow"))?;
    if available < required {
        return Err(anyhow::anyhow!(
            "scope_closure_temp_space_low: available={available}, required={required}, planned={planned_bytes}, reserve={SCOPE_CLOSURE_TEMP_FREE_SPACE_RESERVE_BYTES}"
        ));
    }
    Ok(())
}

fn write_sorted_jsonl_run(
    directory: &Path,
    index: usize,
    buffered: &mut Vec<Vec<u8>>,
) -> anyhow::Result<PathBuf> {
    let path = directory.join(format!("run-{index:06}.jsonl"));
    write_sorted_jsonl_run_to_path(&path, buffered)?;
    Ok(path)
}

fn write_sorted_jsonl_run_to_path(path: &Path, buffered: &mut Vec<Vec<u8>>) -> anyhow::Result<()> {
    buffered.sort();
    let file = File::create(path)?;
    advise_sequential_access(&file);
    let mut writer = BufWriter::new(file);
    for line in buffered.drain(..) {
        writer.write_all(&line)?;
        writer.write_all(b"\n")?;
    }
    writer.flush()?;
    release_file_cache(writer.get_ref());
    Ok(())
}

fn merge_sorted_jsonl_runs(
    run_paths: &[PathBuf],
    output_path: Option<&Path>,
    mut visit: impl FnMut(&[u8]) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let mut readers = run_paths
        .iter()
        .map(|path| {
            let file = File::open(path)?;
            advise_sequential_access(&file);
            Ok::<_, std::io::Error>(BufReader::new(file))
        })
        .collect::<std::io::Result<Vec<_>>>()?;
    let mut output = output_path
        .map(|path| {
            let file = File::create(path)?;
            advise_sequential_access(&file);
            Ok::<_, std::io::Error>(BufWriter::new(file))
        })
        .transpose()?;
    let mut heap = BinaryHeap::<Reverse<(Vec<u8>, usize)>>::new();
    for (index, reader) in readers.iter_mut().enumerate() {
        if let Some(line) = read_canonical_jsonl_line(reader)? {
            heap.push(Reverse((line, index)));
        }
    }
    while let Some(Reverse((line, index))) = heap.pop() {
        if let Some(output) = output.as_mut() {
            output.write_all(&line)?;
            output.write_all(b"\n")?;
        }
        visit(&line)?;
        if let Some(next) = read_canonical_jsonl_line(&mut readers[index])? {
            heap.push(Reverse((next, index)));
        }
    }
    for reader in &readers {
        release_file_cache(reader.get_ref());
    }
    if let Some(mut output) = output {
        output.flush()?;
        release_file_cache(output.get_ref());
    }
    Ok(())
}

fn read_canonical_jsonl_line(reader: &mut BufReader<File>) -> anyhow::Result<Option<Vec<u8>>> {
    let mut line = Vec::new();
    if reader.read_until(b'\n', &mut line)? == 0 {
        return Ok(None);
    }
    if line.last() == Some(&b'\n') {
        line.pop();
    }
    if line.last() == Some(&b'\r') {
        line.pop();
    }
    Ok(Some(line))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ScopeClosureDiscoveredProcess {
    id: Uuid,
    version: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ScopeClosureSnapshotDiscovery {
    schema_version: String,
    process_axis: Vec<ScopeClosureDiscoveredProcess>,
    readiness: MatrixReadinessReport,
}

async fn scan_and_validate_scope<P: ScopeClosureProvider>(
    provider: &P,
    pool: &PgPool,
    worker_job_id: Uuid,
    requested_scope: &RequestedScopeManifest,
) -> anyhow::Result<(ScopeClosureScan, TidasBatchValidation)> {
    let mut scan = collect_scope_closure(provider, requested_scope).await?;
    let validation =
        run_tidas_batch_validation_cached(pool, worker_job_id, &scan.documents).await?;
    let (scan, validation) = tokio::task::spawn_blocking(move || {
        merge_tidas_validation_issues(&mut scan, &validation.issue_events)?;
        scan.issues
            .sort_by(|left, right| left.issue_key.cmp(&right.issue_key));
        Ok::<_, anyhow::Error>((scan, validation))
    })
    .await??;
    Ok((scan, validation))
}

async fn scan_and_validate_scope_with_heartbeat<P: ScopeClosureProvider>(
    provider: &P,
    pool: &PgPool,
    worker_job_id: Uuid,
    requested_scope: &RequestedScopeManifest,
    progress: &WorkerJobProgress<'_>,
    closure_check_id: Uuid,
    lease_seconds: i32,
) -> anyhow::Result<(ScopeClosureScan, TidasBatchValidation)> {
    let operation = scan_and_validate_scope(provider, pool, worker_job_id, requested_scope);
    tokio::pin!(operation);
    let mut heartbeat = tokio::time::interval(lease_heartbeat_period(lease_seconds));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    heartbeat.tick().await;
    loop {
        tokio::select! {
            result = &mut operation => return result,
            _ = heartbeat.tick() => {
                progress
                    .heartbeat(
                        "discover_and_validate_scope",
                        0.4,
                        Some(json!({
                            "closureCheckId": closure_check_id,
                            "longRunningOperation": true,
                            "progressCounters": {
                                "scanned": 0,
                                "total": requested_scope.roots().len(),
                                "unit": "scopeRoots"
                            },
                        })),
                    )
                    .await?;
            }
        }
    }
}

fn build_closure_bundle(
    input: &ScopeClosureWorkerInput,
    validation: &TidasBatchValidation,
    scan: &ScopeClosureScan,
    resolution_map: &JsonlValueSpool,
) -> anyhow::Result<ClosureBundleFile> {
    let temp = Arc::new(TempDir::new()?);
    let projected_bundle_bytes = scan
        .documents
        .byte_size
        .saturating_add(scan.edges.byte_size)
        .saturating_add(scan.resolved_references.byte_size)
        .saturating_add(resolution_map.byte_size)
        .saturating_add(4 * 1024 * 1024);
    ensure_temp_free_space(temp.path(), projected_bundle_bytes)?;
    let path = temp.path().join("closure-bundle-v3.json");
    let mut writer = BufWriter::new(File::create(&path)?);
    writer.write_all(b"{")?;
    write_canonical_field(
        &mut writer,
        "dataSnapshotToken",
        &input.data_snapshot_token,
        false,
    )?;
    write_canonical_field(
        &mut writer,
        "policyFingerprint",
        &input.policy_fingerprint,
        true,
    )?;
    write_canonical_field(
        &mut writer,
        "requestedScopeHash",
        &input.requested_scope_hash,
        true,
    )?;
    writer.write_all(b",\"resolutionMap\":")?;
    write_spooled_json_array(&mut writer, resolution_map)?;
    writer.write_all(b",\"scan\":")?;
    write_scope_closure_scan_v3(&mut writer, scan)?;
    write_canonical_field(
        &mut writer,
        "schemaVersion",
        &"lcia.scope-closure-bundle.v3",
        true,
    )?;
    writer.write_all(b",\"tidasValidation\":")?;
    write_tidas_validation_v3(&mut writer, validation)?;
    write_canonical_field(
        &mut writer,
        "validatorScannerFingerprint",
        &input.expected_validator_scanner_fingerprint,
        true,
    )?;
    writer.write_all(b"}")?;
    writer.flush()?;
    drop(writer);
    let (byte_size, sha256) = file_size_and_sha256(&path)?;
    Ok(ClosureBundleFile {
        temp,
        path,
        byte_size,
        sha256,
    })
}

fn write_canonical_field<W: Write, T: Serialize>(
    writer: &mut W,
    key: &str,
    value: &T,
    comma: bool,
) -> anyhow::Result<()> {
    if comma {
        writer.write_all(b",")?;
    }
    serde_json::to_writer(&mut *writer, key)?;
    writer.write_all(b":")?;
    writer.write_all(&canonical_json_bytes(value)?)?;
    Ok(())
}

fn write_canonical_array<'a, W, T>(
    writer: &mut W,
    values: impl IntoIterator<Item = &'a T>,
) -> anyhow::Result<()>
where
    W: Write,
    T: Serialize + 'a,
{
    writer.write_all(b"[")?;
    let mut comma = false;
    for value in values {
        if comma {
            writer.write_all(b",")?;
        }
        writer.write_all(&canonical_json_bytes(value)?)?;
        comma = true;
    }
    writer.write_all(b"]")?;
    Ok(())
}

fn write_scope_closure_scan_v3<W: Write>(
    writer: &mut W,
    scan: &ScopeClosureScan,
) -> anyhow::Result<()> {
    writer.write_all(b"{")?;
    write_canonical_field(writer, "complete", &scan.complete, false)?;
    writer.write_all(b",\"documents\":")?;
    scan.documents.write_json_array(writer)?;
    writer.write_all(b",\"edges\":")?;
    write_spooled_json_array(writer, &scan.edges)?;
    writer.write_all(b",\"frontier\":")?;
    write_canonical_array(writer, &scan.frontier)?;
    writer.write_all(b",\"issueSummary\":")?;
    if let Some(relations) = scan.issue_relations.as_ref() {
        writer.write_all(&canonical_json_bytes(&json!({
            "affectedRootCount": relations.stats.affected_root_count,
            "blockerCodes": relations.stats.blocker_codes,
            "blockerCount": relations.stats.blocker_count,
            "canonical": true,
            "completeMachineResultClientKey": "manifest.json",
            "expandedAffectedRootRecordCount": 0,
            "issueCount": relations.stats.issue_count,
            "issueSchemaVersion": "lcia.scope-closure-issue.v3",
            "occurrenceCount": relations.stats.occurrence_count,
            "rawTidasIssueEventCount": scan.tidas_issue_event_count,
        }))?)?;
    } else {
        writer.write_all(&canonical_json_bytes(&json!({
            "canonical": false,
            "completeMachineResultClientKey": "manifest.json",
            "issueCountBeforeTidasCoalescing": scan.issues.len(),
            "issueSchemaVersion": "lcia.scope-closure-issue.v3",
            "rawTidasIssueEventCount": scan.tidas_issue_event_count,
        }))?)?;
    }
    writer.write_all(b",\"omittedVersionResolutions\":")?;
    write_canonical_array(writer, &scan.omitted_version_resolutions)?;
    writer.write_all(b",\"providerUniverse\":")?;
    write_canonical_array(writer, &scan.provider_universe)?;
    writer.write_all(b",\"resolvedReferences\":")?;
    write_spooled_json_array(writer, &scan.resolved_references)?;
    writer.write_all(b",\"roots\":")?;
    write_canonical_array(writer, &scan.roots)?;
    write_canonical_field(writer, "schemaVersion", &scan.schema_version, true)?;
    writer.write_all(b"}")?;
    Ok(())
}

fn write_tidas_validation_v3<W: Write>(
    writer: &mut W,
    validation: &TidasBatchValidation,
) -> anyhow::Result<()> {
    writer.write_all(b"{")?;
    write_canonical_field(writer, "describe", &validation.describe, false)?;
    write_canonical_field(writer, "finalEvent", &validation.final_event, true)?;
    write_canonical_field(
        writer,
        "issueStream",
        &json!({
            "compression": "zstd",
            "eventCount": validation.issue_events.event_count,
            "logicalByteSize": validation.issue_events.byte_size,
            "logicalSha256": validation.issue_events.sha256,
            "path": "tidas/issues.ndjson.zst",
            "schemaVersion": "lcia.scope-closure-tidas-issue-stream.v1",
        }),
        true,
    )?;
    writer.write_all(b"}")?;
    Ok(())
}

fn write_spooled_json_array<W: Write>(
    writer: &mut W,
    spool: &JsonlValueSpool,
) -> anyhow::Result<()> {
    writer.write_all(b"[")?;
    let mut comma = false;
    spool.visit(|event| {
        if comma {
            writer.write_all(b",")?;
        }
        writer.write_all(&canonical_json_bytes(&event)?)?;
        comma = true;
        Ok(())
    })?;
    writer.write_all(b"]")?;
    Ok(())
}

fn spooled_json_array_sha256(spool: &JsonlValueSpool) -> anyhow::Result<String> {
    let mut digest = Sha256::new();
    digest.update(b"[");
    let mut comma = false;
    spool.visit(|event| {
        if comma {
            digest.update(b",");
        }
        digest.update(canonical_json_bytes(&event)?);
        comma = true;
        Ok(())
    })?;
    digest.update(b"]");
    Ok(hex::encode(digest.finalize()))
}

fn closure_scan_allows_numerical_snapshot(scan: &ScopeClosureScan) -> bool {
    scan.complete && scan.tidas_issue_event_count == 0 && scan.blocker_codes().is_empty()
}

fn scope_process_axis(scope: &RequestedScopeManifest) -> Vec<RequestRootProcess> {
    scope
        .processes
        .iter()
        .map(|process| RequestRootProcess::new(process.id, process.version.clone()))
        .collect()
}

async fn scope_closure_snapshot_binding(
    pool: &PgPool,
    effective_scope: &RequestedScopeManifest,
    data_snapshot_token: &str,
    closure_bundle_hash: &str,
) -> anyhow::Result<ScopeClosureSnapshotBinding> {
    let effective_scope_json = serde_json::to_value(effective_scope)?;
    let row = sqlx::query(
        r"
        WITH _service_role AS (
            SELECT set_config('request.jwt.claim.role', 'service_role', true)
        )
        SELECT public.lcia_scope_closure_sha256($1::jsonb) AS effective_scope_hash
        FROM _service_role
        ",
    )
    .bind(&effective_scope_json)
    .fetch_one(pool)
    .await?;
    Ok(ScopeClosureSnapshotBinding {
        schema_version: "lcia.scope-closure-snapshot-binding.v1".to_owned(),
        effective_scope_hash: row.try_get("effective_scope_hash")?,
        data_snapshot_token: data_snapshot_token.to_owned(),
        closure_bundle_hash: closure_bundle_hash.to_owned(),
    })
}

fn parse_scope_closure_snapshot_discovery(
    discovery: Option<&Value>,
) -> anyhow::Result<ScopeClosureSnapshotDiscovery> {
    let mut discovery: ScopeClosureSnapshotDiscovery = serde_json::from_value(
        discovery
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("scope closure discovery omitted its result"))?,
    )?;
    if discovery.schema_version != "lcia.scope-closure-snapshot-discovery.v1" {
        return Err(anyhow::anyhow!(
            "unsupported scope closure discovery schema: {}",
            discovery.schema_version
        ));
    }
    discovery.process_axis.sort_by(|left, right| {
        (left.id, left.version.as_str()).cmp(&(right.id, right.version.as_str()))
    });
    if discovery.process_axis.is_empty()
        || discovery
            .process_axis
            .iter()
            .any(|process| process.version.trim().is_empty())
    {
        return Err(anyhow::anyhow!(
            "scope closure discovery returned an empty or invalid process axis"
        ));
    }
    let original_len = discovery.process_axis.len();
    discovery
        .process_axis
        .dedup_by(|left, right| left.id == right.id && left.version == right.version);
    if discovery.process_axis.len() != original_len {
        return Err(anyhow::anyhow!(
            "scope closure discovery returned duplicate process identities"
        ));
    }
    Ok(discovery)
}

fn freeze_discovered_process_axis(
    requested_scope: &RequestedScopeManifest,
    process_axis: &[ScopeClosureDiscoveredProcess],
) -> anyhow::Result<RequestedScopeManifest> {
    let discovered = process_axis
        .iter()
        .map(|process| (process.id, process.version.as_str()))
        .collect::<BTreeSet<_>>();
    let missing_roots = requested_scope
        .processes
        .iter()
        .filter(|process| !discovered.contains(&(process.id, process.version.as_str())))
        .map(|process| format!("{}@{}", process.id, process.version))
        .collect::<Vec<_>>();
    if !missing_roots.is_empty() {
        return Err(anyhow::anyhow!(
            "scope closure discovery omitted administrative process roots: {}",
            missing_roots.join(",")
        ));
    }
    let mut frozen = requested_scope.clone();
    frozen.processes = process_axis
        .iter()
        .map(|process| RequestedIdentity {
            id: process.id,
            version: process.version.clone(),
        })
        .collect();
    frozen.process_manifest_hash = Some(canonical_json_sha256(&json!({
        "processes": frozen.processes,
    }))?);
    Ok(frozen)
}

fn bounded_all_root_evidence(
    roots: &[ExactDatasetIdentity],
) -> (Vec<ExactDatasetIdentity>, Vec<Vec<ExactDatasetIdentity>>) {
    let affected_roots = roots
        .iter()
        .take(ISSUE_INLINE_AFFECTED_ROOT_SAMPLE_LIMIT)
        .cloned()
        .collect::<Vec<_>>();
    let witness_paths = affected_roots
        .iter()
        .map(|root| vec![root.clone()])
        .collect();
    (affected_roots, witness_paths)
}

fn add_process_axis_drift_issue(
    scan: &mut ScopeClosureScan,
    frozen_axis: &[RequestRootProcess],
    effective_scope: &RequestedScopeManifest,
) -> anyhow::Result<()> {
    let frozen = frozen_axis
        .iter()
        .map(|process| (process.process_id, process.process_version.as_str()))
        .collect::<BTreeSet<_>>();
    let observed = effective_scope
        .processes
        .iter()
        .map(|process| (process.id, process.version.as_str()))
        .collect::<BTreeSet<_>>();
    if frozen == observed {
        return Ok(());
    }
    let details = json!({
        "missing": frozen
            .difference(&observed)
            .map(|(id, version)| format!("{id}@{version}"))
            .collect::<Vec<_>>(),
        "unexpected": observed
            .difference(&frozen)
            .map(|(id, version)| format!("{id}@{version}"))
            .collect::<Vec<_>>(),
    });
    let (affected_roots, affected_root_witness_paths) = bounded_all_root_evidence(&scan.roots);
    scan.issues.push(ClosureIssue {
        issue_key: format!(
            "scope_closure_process_axis_drift:{}",
            canonical_json_sha256(&details)?
        ),
        severity: "error".to_owned(),
        blocking: true,
        issue_code: "scope_closure_process_axis_drift".to_owned(),
        source: None,
        json_path: None,
        reference_role: Some("signed_flow_process_axis".to_owned()),
        requested_target_type: Some("processes".to_owned()),
        requested_target_id: None,
        requested_target_version: None,
        message: "The administrative rescan did not preserve the frozen signed-flow process axis."
            .to_owned(),
        suggested_action: Some(
            "Repair the frozen release references or provider closure before retrying.".to_owned(),
        ),
        occurrence_count: 1,
        occurrences: vec![ClosureIssueOccurrence {
            occurrence_key: "scope_closure_process_axis_drift".to_owned(),
            source: None,
            json_path: None,
            reference_role: Some("signed_flow_process_axis".to_owned()),
            details,
        }],
        affected_root_count: u32::try_from(scan.roots.len()).unwrap_or(u32::MAX),
        affected_roots,
        affected_root_witness_paths,
        witness_path: Vec::new(),
    });
    Ok(())
}

fn merge_matrix_readiness_blockers(
    scan: &mut ScopeClosureScan,
    readiness: &MatrixReadinessReport,
) -> anyhow::Result<()> {
    if readiness.status == ReadinessStatus::Passed && readiness.blockers.is_empty() {
        return Ok(());
    }
    for blocker in &readiness.blockers {
        let details = json!({
            "readinessSchemaVersion": readiness.schema_version,
            "nextAction": readiness.next_action,
            "finding": blocker,
        });
        let (affected_roots, affected_root_witness_paths) = bounded_all_root_evidence(&scan.roots);
        scan.issues.push(ClosureIssue {
            issue_key: format!(
                "matrix_readiness:{}:{}",
                blocker.code,
                canonical_json_sha256(&details)?
            ),
            severity: "error".to_owned(),
            blocking: true,
            issue_code: format!("matrix_readiness_{}", blocker.code),
            source: None,
            json_path: None,
            reference_role: Some("numerical_snapshot_readiness".to_owned()),
            requested_target_type: None,
            requested_target_id: None,
            requested_target_version: None,
            message: blocker.message.clone(),
            suggested_action: Some(readiness.next_action.clone()),
            occurrence_count: 1,
            occurrences: vec![ClosureIssueOccurrence {
                occurrence_key: format!("matrix_readiness_{}", blocker.code),
                source: None,
                json_path: None,
                reference_role: Some("numerical_snapshot_readiness".to_owned()),
                details,
            }],
            affected_root_count: u32::try_from(scan.roots.len()).unwrap_or(u32::MAX),
            affected_roots,
            affected_root_witness_paths,
            witness_path: Vec::new(),
        });
    }
    if readiness.status == ReadinessStatus::Failed && readiness.blockers.is_empty() {
        let (affected_roots, affected_root_witness_paths) = bounded_all_root_evidence(&scan.roots);
        scan.issues.push(ClosureIssue {
            issue_key: "matrix_readiness_failed_without_blockers".to_owned(),
            severity: "error".to_owned(),
            blocking: true,
            issue_code: "matrix_readiness_failed_without_blockers".to_owned(),
            source: None,
            json_path: None,
            reference_role: Some("numerical_snapshot_readiness".to_owned()),
            requested_target_type: None,
            requested_target_id: None,
            requested_target_version: None,
            message: "Matrix readiness failed without a machine-readable blocker.".to_owned(),
            suggested_action: Some(readiness.next_action.clone()),
            occurrence_count: 1,
            occurrences: Vec::new(),
            affected_root_count: u32::try_from(scan.roots.len()).unwrap_or(u32::MAX),
            affected_roots,
            affected_root_witness_paths,
            witness_path: Vec::new(),
        });
    }
    Ok(())
}

fn administrative_only_evidence(
    source_fingerprint: String,
    resolution_map_hash: String,
    closure_bundle_hash: String,
    closure_bundle_artifact_id: Uuid,
    report_artifact_manifest_hash: String,
) -> ScopeClosureEvidence {
    ScopeClosureEvidence {
        schema_version: "lcia.scope-closure-evidence.v2".to_owned(),
        source_fingerprint,
        resolution_map_hash,
        closure_bundle_hash,
        closure_bundle_artifact_id,
        snapshot_id: None,
        snapshot_hash: None,
        snapshot_artifact_id: None,
        snapshot_index_sha256: None,
        snapshot_build_contract_hash: None,
        artifact_format: None,
        report_artifact_manifest_hash,
        evidence_hash: None,
    }
}

fn evidence_from_snapshot_facts(
    source_fingerprint: String,
    resolution_map_hash: String,
    closure_bundle_hash: String,
    closure_bundle_artifact_id: Uuid,
    report_artifact_manifest_hash: String,
    facts: &ScopeClosureSnapshotFacts,
) -> ScopeClosureEvidence {
    let evidence_hash = scope_closure_evidence_hash(
        source_fingerprint.as_str(),
        resolution_map_hash.as_str(),
        closure_bundle_hash.as_str(),
        closure_bundle_artifact_id,
        facts,
    );
    ScopeClosureEvidence {
        schema_version: "lcia.scope-closure-evidence.v2".to_owned(),
        source_fingerprint,
        resolution_map_hash,
        closure_bundle_hash,
        closure_bundle_artifact_id,
        snapshot_id: Some(facts.snapshot_id),
        snapshot_hash: Some(facts.snapshot_hash.clone()),
        snapshot_artifact_id: Some(facts.snapshot_artifact_id),
        snapshot_index_sha256: Some(facts.snapshot_index_sha256.clone()),
        snapshot_build_contract_hash: Some(facts.snapshot_build_contract_hash.clone()),
        artifact_format: Some(facts.artifact_format.clone()),
        report_artifact_manifest_hash,
        evidence_hash: Some(evidence_hash),
    }
}

/// Executes a leased closure job and atomically projects its terminal result.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub async fn execute_scope_closure_job(
    state: &AppState,
    worker_job_id: Uuid,
    lease_token: Uuid,
    lease_seconds: i32,
    closure_check_id: Uuid,
    scan_execution_id: Uuid,
    data_snapshot_token: &str,
    request_fingerprint: &str,
) -> anyhow::Result<ScopeClosureExecutionResult> {
    let progress = WorkerJobProgress::new(&state.pool, worker_job_id, lease_token, lease_seconds);
    progress
        .heartbeat(
            "load_scope",
            0.08,
            Some(json!({
                "closureCheckId": closure_check_id,
                "progressCounters": {"scanned": 0, "total": 1, "unit": "scope"},
            })),
        )
        .await?;
    let input = load_scope_closure_worker_input(&state.pool, closure_check_id).await?;
    let data_snapshot_manifest = parse_data_snapshot_manifest(&input.data_snapshot_manifest)?;
    if input.closure_check_id != closure_check_id {
        return Err(anyhow::anyhow!("closure worker input identity mismatch"));
    }
    if input.request_fingerprint != request_fingerprint {
        return Err(anyhow::anyhow!("closure request fingerprint mismatch"));
    }
    if input.scan_execution_id != scan_execution_id
        || input.data_snapshot_token != data_snapshot_token
    {
        return Err(anyhow::anyhow!("closure scan/snapshot envelope mismatch"));
    }
    if input.expected_validator_scanner_fingerprint != "scope-closure-validator-scanner.v1" {
        return Err(anyhow::anyhow!(
            "unsupported validator/scanner fingerprint: {}",
            input.expected_validator_scanner_fingerprint
        ));
    }
    let wait_started = std::time::Instant::now();
    let mut wait_backoff = std::time::Duration::from_secs(1);
    loop {
        match claim_scan_execution(&state.pool, scan_execution_id, worker_job_id, lease_token)
            .await?
        {
            ScanExecutionClaim::Acquired => break,
            ScanExecutionClaim::Completed { completed_check_id } => {
                return reuse_completed_scan_execution(
                    state,
                    closure_check_id,
                    worker_job_id,
                    lease_token,
                    completed_check_id,
                    &progress,
                )
                .await;
            }
            ScanExecutionClaim::Busy => {
                if wait_started.elapsed() >= std::time::Duration::from_secs(3_600) {
                    return Err(anyhow::anyhow!("shared_scan_wait_timeout"));
                }
                progress
                    .heartbeat(
                        "waiting_for_shared_scan",
                        0.12,
                        Some(json!({
                            "closureCheckId": closure_check_id,
                            "scanExecutionId": scan_execution_id,
                            "progressCounters": {"scanned": 0, "total": 1, "unit": "sharedScan"},
                        })),
                    )
                    .await?;
                tokio::time::sleep(wait_backoff).await;
                wait_backoff = (wait_backoff * 2).min(std::time::Duration::from_secs(10));
            }
        }
    }

    progress
        .heartbeat(
            "discover_reference_graph",
            0.18,
            Some(json!({
                "closureCheckId": closure_check_id,
                "progressCounters": {
                    "scanned": 0,
                    "total": input.requested_scope.roots().len(),
                    "unit": "documents"
                },
            })),
        )
        .await?;
    let provider = PgScopeClosureProvider::new_leased(
        &state.pool,
        &data_snapshot_manifest,
        worker_job_id,
        lease_token,
        lease_seconds,
    );
    let (mut scan, mut validation) = scan_and_validate_scope_with_heartbeat(
        &provider,
        &state.pool,
        worker_job_id,
        &input.requested_scope,
        &progress,
        closure_check_id,
        lease_seconds,
    )
    .await?;

    progress
        .heartbeat(
            "validate_documents",
            0.46,
            Some(json!({
                "closureCheckId": closure_check_id,
                "progressCounters": {
                    "scanned": scan.documents.len(),
                    "total": scan.provider_universe.len(),
                    "unit": "documents"
                },
            })),
        )
        .await?;

    progress
        .heartbeat(
            "validate_reference_graph",
            0.62,
            Some(json!({
                "closureCheckId": closure_check_id,
                "progressCounters": {
                    "scanned": scan.edges.len(),
                    "total": scan.edges.len(),
                    "unit": "references"
                },
            })),
        )
        .await?;
    scan.issues
        .sort_by(|left, right| left.issue_key.cmp(&right.issue_key));

    let mut effective_scope =
        build_effective_scope_manifest(&input.requested_scope, &scan.documents);
    let mut frozen_process_axis = scope_process_axis(&effective_scope);

    if closure_scan_allows_numerical_snapshot(&scan) {
        let input_for_administrative_bundle = input.clone();
        let prepare_administrative_bundle = tokio::task::spawn_blocking(move || {
            let resolution_map =
                build_resolution_map_spool(&scan.edges, &scan.omitted_version_resolutions)?;
            let bundle = build_closure_bundle(
                &input_for_administrative_bundle,
                &validation,
                &scan,
                &resolution_map,
            )?;
            Ok::<_, anyhow::Error>(bundle.sha256)
        });
        tokio::pin!(prepare_administrative_bundle);
        let mut administrative_bundle_heartbeat =
            tokio::time::interval(lease_heartbeat_period(lease_seconds));
        administrative_bundle_heartbeat
            .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        administrative_bundle_heartbeat.tick().await;
        let administrative_bundle_hash = loop {
            tokio::select! {
                result = &mut prepare_administrative_bundle => {
                    break result??;
                },
                _ = administrative_bundle_heartbeat.tick() => {
                    progress
                        .heartbeat(
                            "prepare_administrative_closure_bundle",
                            0.68,
                            Some(json!({
                                "closureCheckId": closure_check_id,
                                "longRunningOperation": true,
                            })),
                        )
                        .await?;
                }
            }
        };
        let discovery_binding = scope_closure_snapshot_binding(
            &state.pool,
            &effective_scope,
            input.data_snapshot_token.as_str(),
            administrative_bundle_hash.as_str(),
        )
        .await?;
        progress
            .heartbeat(
                "discover_signed_flow_providers",
                0.72,
                Some(json!({
                    "closureCheckId": closure_check_id,
                    "progressCounters": {
                        "scanned": frozen_process_axis.len(),
                        "total": frozen_process_axis.len(),
                        "unit": "administrativeProcessRoots"
                    },
                })),
            )
            .await?;
        let discovery_execution = run_scope_closure_snapshot_builder(
            state,
            deterministic_uuid_from_hash(administrative_bundle_hash.as_str())?,
            frozen_process_axis.as_slice(),
            &ScopeClosureSnapshotBuilderArgs {
                mode: ScopeClosureSnapshotBuilderMode::Discovery,
                binding: serde_json::to_value(discovery_binding)?,
                data_snapshot: input.data_snapshot_manifest.clone(),
            },
        )
        .await?;
        let discovery = parse_scope_closure_snapshot_discovery(
            discovery_execution.scope_closure_discovery.as_ref(),
        )?;
        let final_requested_scope =
            freeze_discovered_process_axis(&input.requested_scope, &discovery.process_axis)?;
        frozen_process_axis = scope_process_axis(&final_requested_scope);

        progress
            .heartbeat(
                "scan_discovered_provider_processes",
                0.79,
                Some(json!({
                    "closureCheckId": closure_check_id,
                    "progressCounters": {
                        "scanned": 0,
                        "total": frozen_process_axis.len(),
                        "unit": "frozenProcessAxis"
                    },
                })),
            )
            .await?;
        (scan, validation) = scan_and_validate_scope_with_heartbeat(
            &provider,
            &state.pool,
            worker_job_id,
            &final_requested_scope,
            &progress,
            closure_check_id,
            lease_seconds,
        )
        .await?;
        effective_scope = build_effective_scope_manifest(&final_requested_scope, &scan.documents);
        add_process_axis_drift_issue(&mut scan, frozen_process_axis.as_slice(), &effective_scope)?;
        merge_matrix_readiness_blockers(&mut scan, &discovery.readiness)?;
        scan.issues
            .sort_by(|left, right| left.issue_key.cmp(&right.issue_key));
    }

    let input_for_artifacts = input.clone();
    let artifact_build_cancellation = CancellationToken::default();
    let blocking_cancellation = artifact_build_cancellation.clone();
    let artifact_build_progress = Arc::new(ScopeClosureArtifactProgress::default());
    let blocking_progress = Arc::clone(&artifact_build_progress);
    let prepare_artifacts = tokio::task::spawn_blocking(move || {
        build_issue_relation_spools_with_cancellation_and_progress(
            &mut scan,
            &validation.issue_events,
            &blocking_cancellation,
            Some(&blocking_progress),
        )?;
        blocking_cancellation.check("scope_closure_resolution_map")?;
        let resolution_map =
            build_resolution_map_spool(&scan.edges, &scan.omitted_version_resolutions)?;
        let resolution_map_hash = spooled_json_array_sha256(&resolution_map)?;
        blocking_cancellation.check("scope_closure_bundle")?;
        let closure_bundle =
            build_closure_bundle(&input_for_artifacts, &validation, &scan, &resolution_map)?;
        let closure_bundle_hash = closure_bundle.sha256.clone();
        let source_fingerprint = source_fingerprint(&scan.documents)?;
        blocking_cancellation.check("scope_closure_artifact_finalize")?;
        let mut artifacts = prepare_closure_content_artifacts_with_cancellation(
            closure_bundle,
            closure_check_id,
            &scan,
            &validation,
            &blocking_cancellation,
            Some(&blocking_progress),
        )?;
        let artifact_bytes = artifacts.iter().try_fold(0_u64, |total, artifact| {
            total
                .checked_add(u64::try_from(artifact.descriptor.byte_size)?)
                .ok_or_else(|| anyhow::anyhow!("closure artifact byte count overflow"))
        })?;
        blocking_progress.update(
            5,
            scan.issue_relations
                .as_ref()
                .map_or(0, |relations| relations.stats.issue_count),
            artifact_bytes,
            u64::try_from(artifacts.len())?,
        );
        enforce_scope_closure_memory_budget("closure_artifacts_built")?;
        artifacts.sort_by(|left, right| {
            left.descriptor
                .artifact_type
                .cmp(&right.descriptor.artifact_type)
        });
        let artifact_manifest = artifacts
            .iter()
            .map(|artifact| artifact.descriptor.clone())
            .collect::<Vec<_>>();
        let content_artifact_manifest_hash = canonical_json_sha256(&artifact_manifest)?;
        Ok::<_, anyhow::Error>((
            scan,
            artifacts,
            closure_bundle_hash,
            source_fingerprint,
            resolution_map_hash,
            content_artifact_manifest_hash,
        ))
    });
    tokio::pin!(prepare_artifacts);
    let mut artifact_heartbeat = tokio::time::interval(lease_heartbeat_period(lease_seconds));
    artifact_heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    artifact_heartbeat.tick().await;
    let (
        scan,
        artifacts,
        closure_bundle_hash,
        source_fingerprint,
        resolution_map_hash,
        content_artifact_manifest_hash,
    ) = loop {
        tokio::select! {
            result = &mut prepare_artifacts => break result??,
            _ = artifact_heartbeat.tick() => {
                if let Err(error) = progress
                    .heartbeat(
                        "prepare_closure_artifacts",
                        0.82,
                        Some(json!({
                            "closureCheckId": closure_check_id,
                            "longRunningOperation": true,
                            "progressCounters": artifact_build_progress.snapshot(),
                        })),
                    )
                    .await
                {
                    artifact_build_cancellation.cancel();
                    let _ = (&mut prepare_artifacts).await;
                    return Err(error.context(
                        "closure artifact preparation cancelled after lease heartbeat failure",
                    ));
                }
            }
        }
    };

    progress
        .heartbeat(
            "validate_lcia_readiness",
            0.84,
            Some(json!({
                "closureCheckId": closure_check_id,
                "progressCounters": {
                    "scanned": input.requested_scope.lcia_methods.len(),
                    "total": input.requested_scope.lcia_methods.len(),
                    "unit": "lciaMethods"
                },
            })),
        )
        .await?;
    let persisted = persist_closure_artifacts(
        state,
        worker_job_id,
        closure_check_id,
        &artifacts,
        content_artifact_manifest_hash.as_str(),
        None,
        Some(&progress),
    )
    .await?;
    let report_artifact_id = persisted
        .get("closure_report_xlsx")
        .copied()
        .ok_or_else(|| anyhow::anyhow!("closure XLSX report artifact was not persisted"))?;
    let closure_bundle_artifact_id = persisted
        .get("closure_bundle")
        .copied()
        .ok_or_else(|| anyhow::anyhow!("closure bundle artifact was not persisted"))?;
    let report_artifact_manifest_hash =
        report_artifact_manifest_hash(&state.pool, report_artifact_id).await?;
    let mut blocker_codes = scan.blocker_codes();
    if !scan.complete {
        blocker_codes.push("scope_closure_scan_incomplete".to_owned());
        blocker_codes.sort();
        blocker_codes.dedup();
    }
    let status = if scan.complete && blocker_codes.is_empty() {
        "passed"
    } else {
        "blocked"
    };
    let scan_completeness = if scan.complete {
        "complete"
    } else {
        "incomplete"
    };
    let (evidence, snapshot_artifact_id) = if status == "passed" {
        progress
            .heartbeat(
                "build_bound_numerical_snapshot",
                0.9,
                Some(json!({
                    "closureCheckId": closure_check_id,
                    "progressCounters": {
                        "scanned": 0,
                        "total": frozen_process_axis.len(),
                        "unit": "frozenProcessAxis"
                    },
                })),
            )
            .await?;
        let binding = scope_closure_snapshot_binding(
            &state.pool,
            &effective_scope,
            input.data_snapshot_token.as_str(),
            closure_bundle_hash.as_str(),
        )
        .await?;
        let built = run_scope_closure_snapshot_builder(
            state,
            input.numerical_snapshot_id,
            frozen_process_axis.as_slice(),
            &ScopeClosureSnapshotBuilderArgs {
                mode: ScopeClosureSnapshotBuilderMode::Build,
                binding: serde_json::to_value(&binding)?,
                data_snapshot: input.data_snapshot_manifest.clone(),
            },
        )
        .await?;
        ensure_preallocated_snapshot_identity(
            input.numerical_snapshot_id,
            built.resolved_snapshot_id,
        )?;
        let facts = fetch_scope_closure_snapshot_facts(
            state,
            built.resolved_snapshot_id,
            &binding,
            frozen_process_axis.as_slice(),
        )
        .await?;
        let evidence = evidence_from_snapshot_facts(
            source_fingerprint,
            resolution_map_hash,
            closure_bundle_hash,
            closure_bundle_artifact_id,
            report_artifact_manifest_hash,
            &facts,
        );
        (evidence, Some(facts.snapshot_artifact_id))
    } else {
        (
            administrative_only_evidence(
                source_fingerprint,
                resolution_map_hash,
                closure_bundle_hash,
                closure_bundle_artifact_id,
                report_artifact_manifest_hash,
            ),
            None,
        )
    };

    progress
        .heartbeat(
            "finalize_evidence",
            0.95,
            Some(json!({
                "closureCheckId": closure_check_id,
                "progressCounters": {
                    "scanned": artifacts.len(),
                    "total": artifacts.len(),
                    "unit": "artifacts"
                },
            })),
        )
        .await?;
    let result_summary = json!({
        "schemaVersion": "lcia.scope-closure-summary.v1",
        "documentCount": scan.documents.len(),
        "referenceCount": scan.edges.len(),
        "issueCount": scan.issue_count(),
        "issueSampleLimit": ISSUE_INLINE_ISSUE_SAMPLE_LIMIT,
        "issueDetailsTruncated": scan.issue_count() > u64::try_from(ISSUE_INLINE_ISSUE_SAMPLE_LIMIT).unwrap_or(u64::MAX),
        "blockerCount": scan.blocker_count(),
        "evidenceHash": evidence.evidence_hash,
        "snapshotId": evidence.snapshot_id,
        "snapshotHash": evidence.snapshot_hash,
        "snapshotArtifactId": evidence.snapshot_artifact_id,
        "snapshotIndexSha256": evidence.snapshot_index_sha256,
        "snapshotBuildContractHash": evidence.snapshot_build_contract_hash,
        "artifacts": persisted,
    });
    let rpc_result = record_scope_closure_result_v3(
        &state.pool,
        closure_check_id,
        worker_job_id,
        lease_token,
        status,
        scan_completeness,
        &effective_scope,
        &evidence,
        &result_summary,
        &scan.issues,
        &blocker_codes,
        report_artifact_id,
        closure_bundle_artifact_id,
        snapshot_artifact_id,
    )
    .await?;
    let certificate_hash = rpc_result
        .get("data")
        .and_then(|data| data.get("certificateHash"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    Ok(ScopeClosureExecutionResult {
        closure_check_id,
        worker_job_id,
        status: status.to_owned(),
        scan_completeness: scan_completeness.to_owned(),
        certificate_hash,
        evidence,
        report_artifact_id,
        blocker_codes,
    })
}

async fn claim_scan_execution(
    pool: &PgPool,
    scan_execution_id: Uuid,
    worker_job_id: Uuid,
    lease_token: Uuid,
) -> anyhow::Result<ScanExecutionClaim> {
    let row = sqlx::query(
        r"
        WITH _service_role AS (
            SELECT set_config('request.jwt.claim.role', 'service_role', true)
        )
        SELECT public.svc_lcia_scope_closure_claim_scan_execution($1, $2, $3) AS result
        FROM _service_role
        ",
    )
    .bind(scan_execution_id)
    .bind(worker_job_id)
    .bind(lease_token)
    .fetch_one(pool)
    .await?;
    let result = row.try_get::<Value, _>("result")?;
    ensure_rpc_ok(&result, "svc_lcia_scope_closure_claim_scan_execution")?;
    parse_scan_execution_claim(result.get("data").unwrap_or(&Value::Null))
}

fn parse_scan_execution_claim(data: &Value) -> anyhow::Result<ScanExecutionClaim> {
    if data.get("acquired").and_then(Value::as_bool) == Some(true) {
        return Ok(ScanExecutionClaim::Acquired);
    }
    if data.get("completed").and_then(Value::as_bool) == Some(true) {
        let completed_check_id = data
            .get("completedCheckId")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("completed scan claim omitted completedCheckId"))?
            .parse()?;
        return Ok(ScanExecutionClaim::Completed { completed_check_id });
    }
    Ok(ScanExecutionClaim::Busy)
}

#[allow(clippy::too_many_lines)]
async fn reuse_completed_scan_execution(
    state: &AppState,
    closure_check_id: Uuid,
    worker_job_id: Uuid,
    lease_token: Uuid,
    completed_check_id: Uuid,
    progress: &WorkerJobProgress<'_>,
) -> anyhow::Result<ScopeClosureExecutionResult> {
    let row = sqlx::query(
        r"
        WITH _service_role AS (
            SELECT set_config('request.jwt.claim.role', 'service_role', true)
        )
        SELECT public.svc_lcia_scope_closure_reuse_completed_scan($1, $2, $3, $4) AS result
        FROM _service_role
        ",
    )
    .bind(closure_check_id)
    .bind(worker_job_id)
    .bind(lease_token)
    .bind(completed_check_id)
    .fetch_one(&state.pool)
    .await?;
    let result = row.try_get::<Value, _>("result")?;
    ensure_rpc_ok(&result, "svc_lcia_scope_closure_reuse_completed_scan")?;
    let data = result
        .get("data")
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("reuse completed scan RPC omitted data"))?;
    if data.get("reuseAvailable").and_then(Value::as_bool) != Some(true) {
        return Err(anyhow::anyhow!("completed scan is not reusable"));
    }
    let issues = load_reused_issues(&state.pool, completed_check_id).await?;
    let temp = Arc::new(TempDir::new()?);
    let xlsx_path = temp.path().join("closure-report-v1.xlsx");
    build_xlsx_report_file(&xlsx_path, closure_check_id, &issues)?;
    let artifact = prepare_file_artifact(
        temp,
        "closure_report_xlsx",
        ScopeClosureArtifactRole::ClosureReport,
        "closure-report-v1.xlsx",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        xlsx_path,
    )?;
    let content_manifest_hash = canonical_json_sha256(&vec![artifact.descriptor.clone()])?;
    let persisted = persist_closure_artifacts(
        state,
        worker_job_id,
        closure_check_id,
        std::slice::from_ref(&artifact),
        content_manifest_hash.as_str(),
        Some(completed_check_id),
        Some(progress),
    )
    .await?;
    let report_artifact_id = persisted
        .get("closure_report_xlsx")
        .copied()
        .ok_or_else(|| anyhow::anyhow!("reused scan report artifact was not persisted"))?;
    let report_hash = report_artifact_manifest_hash(&state.pool, report_artifact_id).await?;
    let result_summary = json!({
        "schemaVersion": "lcia.scope-closure-summary.v1",
        "issueCount": issues.len(),
        "blockerCount": issues.iter().filter(|issue| issue.blocking).count(),
        "evidenceHash": required_json_text(
            data.get("evidence")
                .ok_or_else(|| anyhow::anyhow!("reusable scan omitted evidence"))?,
            "evidenceHash",
        )?,
        "artifacts": persisted,
        "reusedFromCheckId": completed_check_id,
        "reportArtifactId": report_artifact_id,
        "reportArtifactManifestHash": report_hash,
    });
    let finalize = finalize_reused_scan_execution(
        &state.pool,
        closure_check_id,
        worker_job_id,
        lease_token,
        completed_check_id,
        report_artifact_id,
        &result_summary,
    )
    .await?;
    let evidence_json = data
        .get("evidence")
        .ok_or_else(|| anyhow::anyhow!("reusable scan omitted evidence"))?;
    let evidence = ScopeClosureEvidence {
        schema_version: "lcia.scope-closure-evidence.v2".to_owned(),
        source_fingerprint: required_json_text(evidence_json, "sourceFingerprint")?,
        resolution_map_hash: required_json_text(evidence_json, "resolutionMapHash")?,
        closure_bundle_hash: required_json_text(evidence_json, "closureBundleHash")?,
        closure_bundle_artifact_id: required_json_text(evidence_json, "closureBundleArtifactId")?
            .parse()?,
        snapshot_id: Some(required_json_text(evidence_json, "snapshotId")?.parse()?),
        snapshot_hash: Some(required_json_text(evidence_json, "snapshotHash")?),
        snapshot_artifact_id: Some(
            required_json_text(evidence_json, "snapshotArtifactId")?.parse()?,
        ),
        snapshot_index_sha256: Some(required_json_text(evidence_json, "snapshotIndexSha256")?),
        snapshot_build_contract_hash: Some(required_json_text(
            evidence_json,
            "snapshotBuildContractHash",
        )?),
        artifact_format: Some(required_json_text(evidence_json, "artifactFormat")?),
        report_artifact_manifest_hash: report_hash,
        evidence_hash: Some(required_json_text(evidence_json, "evidenceHash")?),
    };
    let status = required_json_text(&data, "status")?;
    let scan_completeness = required_json_text(&data, "scanCompleteness")?;
    let blocker_codes = data
        .get("blockerCodes")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect();
    Ok(ScopeClosureExecutionResult {
        closure_check_id,
        worker_job_id,
        status,
        scan_completeness,
        certificate_hash: finalize
            .get("data")
            .and_then(|item| item.get("certificateHash"))
            .and_then(Value::as_str)
            .map(str::to_owned),
        evidence,
        report_artifact_id,
        blocker_codes,
    })
}

async fn finalize_reused_scan_execution(
    pool: &PgPool,
    closure_check_id: Uuid,
    worker_job_id: Uuid,
    lease_token: Uuid,
    completed_check_id: Uuid,
    report_artifact_id: Uuid,
    result_summary: &Value,
) -> anyhow::Result<Value> {
    let row = sqlx::query(
        r"
        WITH _service_role AS (
            SELECT set_config('request.jwt.claim.role', 'service_role', true)
        )
        SELECT public.svc_lcia_scope_closure_finalize_reused_scan(
            $1, $2, $3, $4, $5, $6::jsonb
        ) AS result
        FROM _service_role
        ",
    )
    .bind(closure_check_id)
    .bind(worker_job_id)
    .bind(lease_token)
    .bind(completed_check_id)
    .bind(report_artifact_id)
    .bind(result_summary)
    .fetch_one(pool)
    .await?;
    let result = row.try_get::<Value, _>("result")?;
    ensure_rpc_ok(&result, "svc_lcia_scope_closure_finalize_reused_scan")?;
    Ok(result)
}

fn required_json_text(value: &Value, key: &str) -> anyhow::Result<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())
        .map(str::to_owned)
        .ok_or_else(|| anyhow::anyhow!("reusable scan omitted {key}"))
}

#[allow(clippy::too_many_lines)]
async fn load_reused_issues(
    pool: &PgPool,
    completed_check_id: Uuid,
) -> anyhow::Result<Vec<ClosureIssue>> {
    let rows = sqlx::query(
        r"
        SELECT issue_key, severity, blocking, issue_code,
               source_dataset_type, source_dataset_id, source_dataset_version,
               json_path, reference_role, requested_target_type,
               requested_target_id, requested_target_version, message,
               suggested_action, occurrence_count, affected_root_count,
               COALESCE((
                 SELECT jsonb_agg(jsonb_build_object(
                   'occurrenceKey', o.occurrence_key,
                   'sourceDatasetType', o.source_dataset_type,
                   'sourceDatasetId', o.source_dataset_id,
                   'sourceDatasetVersion', o.source_dataset_version,
                   'jsonPath', o.json_path,
                   'referenceRole', o.reference_role,
                   'details', o.details
                 ) ORDER BY o.occurrence_key)
                 FROM public.lcia_scope_closure_issue_occurrences o
                 WHERE o.closure_issue_id = i.id
               ), '[]'::jsonb) AS occurrences,
               COALESCE((
                 SELECT jsonb_agg(jsonb_build_object(
                   'datasetType', r.root_dataset_type,
                   'id', r.root_dataset_id,
                   'version', r.root_dataset_version,
                   'witnessPath', r.witness_path
                 ) ORDER BY r.root_dataset_type, r.root_dataset_id, r.root_dataset_version)
                 FROM public.lcia_scope_closure_issue_roots r
                 WHERE r.closure_issue_id = i.id
               ), '[]'::jsonb) AS affected_roots
        FROM public.lcia_scope_closure_issues i
        WHERE closure_check_id = $1
        ORDER BY issue_key
        ",
    )
    .bind(completed_check_id)
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(|row| {
            let source_category = row
                .try_get::<Option<String>, _>("source_dataset_type")?
                .map(|category| parse_category(category.as_str()))
                .transpose()?;
            let source_id = row.try_get::<Option<Uuid>, _>("source_dataset_id")?;
            let source_version = row.try_get::<Option<String>, _>("source_dataset_version")?;
            let source = match (source_category, source_id, source_version) {
                (Some(category), Some(id), Some(version)) => Some(ExactDatasetIdentity {
                    category,
                    id,
                    version,
                }),
                _ => None,
            };
            let affected_roots_json = row.try_get::<Value, _>("affected_roots")?;
            let affected_roots = affected_roots_json
                .as_array()
                .into_iter()
                .flatten()
                .map(|root| {
                    Ok(ExactDatasetIdentity {
                        category: parse_category(
                            required_json_text(root, "datasetType")?.as_str(),
                        )?,
                        id: required_json_text(root, "id")?.parse()?,
                        version: required_json_text(root, "version")?,
                    })
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            let affected_root_witness_paths: Vec<Vec<ExactDatasetIdentity>> = affected_roots_json
                .as_array()
                .into_iter()
                .flatten()
                .map(|root| {
                    root.get("witnessPath")
                        .and_then(Value::as_array)
                        .into_iter()
                        .flatten()
                        .filter_map(|node| serde_json::from_value(node.clone()).ok())
                        .collect::<Vec<_>>()
                })
                .collect();
            let witness_path = affected_root_witness_paths
                .first()
                .cloned()
                .unwrap_or_default();
            let occurrences = row
                .try_get::<Value, _>("occurrences")?
                .as_array()
                .into_iter()
                .flatten()
                .map(|occurrence| {
                    let source_category = occurrence
                        .get("sourceDatasetType")
                        .and_then(Value::as_str)
                        .map(parse_category)
                        .transpose()?;
                    let source_id = occurrence
                        .get("sourceDatasetId")
                        .and_then(Value::as_str)
                        .map(Uuid::parse_str)
                        .transpose()?;
                    let source_version = occurrence
                        .get("sourceDatasetVersion")
                        .and_then(Value::as_str)
                        .map(str::to_owned);
                    let source = match (source_category, source_id, source_version) {
                        (Some(category), Some(id), Some(version)) => Some(ExactDatasetIdentity {
                            category,
                            id,
                            version,
                        }),
                        _ => None,
                    };
                    Ok(ClosureIssueOccurrence {
                        occurrence_key: required_json_text(occurrence, "occurrenceKey")?,
                        source,
                        json_path: occurrence
                            .get("jsonPath")
                            .and_then(Value::as_str)
                            .map(str::to_owned),
                        reference_role: occurrence
                            .get("referenceRole")
                            .and_then(Value::as_str)
                            .map(str::to_owned),
                        details: occurrence
                            .get("details")
                            .cloned()
                            .unwrap_or_else(|| json!({})),
                    })
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            Ok(ClosureIssue {
                issue_key: row.try_get("issue_key")?,
                severity: row.try_get("severity")?,
                blocking: row.try_get("blocking")?,
                issue_code: row.try_get("issue_code")?,
                source,
                json_path: row.try_get("json_path")?,
                reference_role: row.try_get("reference_role")?,
                requested_target_type: row.try_get("requested_target_type")?,
                requested_target_id: row.try_get("requested_target_id")?,
                requested_target_version: row.try_get("requested_target_version")?,
                message: row.try_get("message")?,
                suggested_action: row.try_get("suggested_action")?,
                occurrence_count: u32::try_from(row.try_get::<i32, _>("occurrence_count")?.max(1))?,
                occurrences,
                affected_root_count: u32::try_from(
                    row.try_get::<i32, _>("affected_root_count")?.max(0),
                )?,
                affected_roots,
                affected_root_witness_paths,
                witness_path,
            })
        })
        .collect()
}

pub async fn record_scope_closure_failure(
    pool: &PgPool,
    closure_check_id: Uuid,
    worker_job_id: Uuid,
    lease_token: Uuid,
    _error: &str,
) -> anyhow::Result<Value> {
    let row = sqlx::query(
        r"
        WITH _service_role AS (
            SELECT set_config('request.jwt.claim.role', 'service_role', true)
        )
        SELECT public.svc_lcia_scope_closure_fail_before_scan(
            $1, $2, $3, 'scope_closure_execution_failed'
        ) AS result
        FROM _service_role
        ",
    )
    .bind(closure_check_id)
    .bind(worker_job_id)
    .bind(lease_token)
    .fetch_one(pool)
    .await?;
    let result = row.try_get::<Value, _>("result")?;
    ensure_rpc_ok(&result, "svc_lcia_scope_closure_fail_before_scan")?;
    Ok(result)
}

#[allow(clippy::too_many_arguments)]
async fn record_scope_closure_result_v3(
    pool: &PgPool,
    closure_check_id: Uuid,
    worker_job_id: Uuid,
    lease_token: Uuid,
    status: &str,
    scan_completeness: &str,
    effective_scope: &RequestedScopeManifest,
    evidence: &ScopeClosureEvidence,
    result_summary: &Value,
    issues: &[ClosureIssue],
    blocker_codes: &[String],
    report_artifact_id: Uuid,
    closure_bundle_artifact_id: Uuid,
    snapshot_artifact_id: Option<Uuid>,
) -> anyhow::Result<Value> {
    ensure_closure_bundle_artifact_projection(evidence, closure_bundle_artifact_id)?;
    enforce_scope_closure_memory_budget("result_rpc_projection_start")?;
    let issues = issues
        .iter()
        .take(ISSUE_INLINE_ISSUE_SAMPLE_LIMIT)
        .map(issue_rpc_projection)
        .collect::<Vec<_>>();
    enforce_scope_closure_memory_budget("result_rpc_projection_complete")?;
    record_scope_closure_result_v3_raw(
        pool,
        closure_check_id,
        worker_job_id,
        lease_token,
        status,
        scan_completeness,
        &serde_json::to_value(effective_scope)?,
        &serde_json::to_value(evidence)?,
        result_summary,
        &serde_json::to_value(issues)?,
        blocker_codes,
        Some(report_artifact_id),
        Some(closure_bundle_artifact_id),
        snapshot_artifact_id,
    )
    .await
}

fn ensure_closure_bundle_artifact_projection(
    evidence: &ScopeClosureEvidence,
    rpc_argument: Uuid,
) -> anyhow::Result<()> {
    if evidence.closure_bundle_artifact_id != rpc_argument {
        return Err(anyhow::anyhow!(
            "scope closure evidence bundle artifact differs from the record_result_v3 argument: evidence={} argument={rpc_argument}",
            evidence.closure_bundle_artifact_id
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn record_scope_closure_result_v3_raw(
    pool: &PgPool,
    closure_check_id: Uuid,
    worker_job_id: Uuid,
    lease_token: Uuid,
    status: &str,
    scan_completeness: &str,
    effective_scope: &Value,
    evidence: &Value,
    result_summary: &Value,
    issues: &Value,
    blocker_codes: &[String],
    report_artifact_id: Option<Uuid>,
    closure_bundle_artifact_id: Option<Uuid>,
    snapshot_artifact_id: Option<Uuid>,
) -> anyhow::Result<Value> {
    let row = sqlx::query(
        r"
        WITH _service_role AS (
            SELECT set_config('request.jwt.claim.role', 'service_role', true)
        )
        SELECT public.svc_lcia_scope_closure_check_record_result_v3(
            $1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8::jsonb,
            $9::jsonb, $10::text[], $11, $12, $13
        ) AS result
        FROM _service_role
        ",
    )
    .bind(closure_check_id)
    .bind(worker_job_id)
    .bind(lease_token)
    .bind(status)
    .bind(scan_completeness)
    .bind(effective_scope)
    .bind(evidence)
    .bind(result_summary)
    .bind(issues)
    .bind(blocker_codes)
    .bind(report_artifact_id)
    .bind(closure_bundle_artifact_id)
    .bind(snapshot_artifact_id)
    .fetch_one(pool)
    .await?;
    let result = row.try_get::<Value, _>("result")?;
    ensure_rpc_ok(&result, "svc_lcia_scope_closure_check_record_result_v3")?;
    Ok(result)
}

fn issue_rpc_projection(issue: &ClosureIssue) -> Value {
    let occurrences = issue
        .occurrences
        .iter()
        .take(ISSUE_INLINE_OCCURRENCE_SAMPLE_LIMIT)
        .map(|occurrence| {
            json!({
                "occurrenceKey": occurrence.occurrence_key,
                "sourceDatasetType": occurrence.source.as_ref().map(|item| item.category.table_name()),
                "sourceDatasetId": occurrence.source.as_ref().map(|item| item.id),
                "sourceDatasetVersion": occurrence.source.as_ref().map(|item| item.version.as_str()),
                "jsonPath": occurrence.json_path,
                "referenceRole": occurrence.reference_role,
                "details": occurrence.details,
            })
        })
        .collect::<Vec<_>>();
    let affected_roots = issue
        .affected_roots
        .iter()
        .enumerate()
        .map(|(index, root)| {
            json!({
                "datasetType": root.category.table_name(),
                "id": root.id,
                "version": root.version,
                "impactRole": "root",
                "witnessPath": issue.affected_root_witness_paths
                    .get(index)
                    .unwrap_or(&issue.witness_path),
            })
        })
        .collect::<Vec<_>>();
    json!({
        "issueKey": issue.issue_key,
        "severity": issue.severity,
        "blocking": issue.blocking,
        "issueCode": issue.issue_code,
        "sourceDatasetType": issue.source.as_ref().map(|item| item.category.table_name()),
        "sourceDatasetId": issue.source.as_ref().map(|item| item.id),
        "sourceDatasetVersion": issue.source.as_ref().map(|item| item.version.as_str()),
        "jsonPath": issue.json_path,
        "referenceRole": issue.reference_role,
        "requestedTargetType": issue.requested_target_type,
        "requestedTargetId": issue.requested_target_id,
        "requestedTargetVersion": issue.requested_target_version,
        "message": issue.message,
        "suggestedAction": issue.suggested_action,
        "occurrenceCount": issue.occurrence_count,
        "affectedRootCount": issue.affected_root_count,
        "details": {
            "witnessPath": issue.witness_path,
            "occurrenceSampleLimit": ISSUE_INLINE_OCCURRENCE_SAMPLE_LIMIT,
            "occurrencesTruncated": usize::try_from(issue.occurrence_count).unwrap_or(usize::MAX) > issue.occurrences.len().min(ISSUE_INLINE_OCCURRENCE_SAMPLE_LIMIT),
            "affectedRootSampleLimit": ISSUE_INLINE_AFFECTED_ROOT_SAMPLE_LIMIT,
            "affectedRootsTruncated": usize::try_from(issue.affected_root_count).unwrap_or(usize::MAX) > issue.affected_roots.len(),
        },
        "occurrences": occurrences,
        "affectedRoots": affected_roots,
    })
}

fn build_effective_scope_manifest(
    requested: &RequestedScopeManifest,
    documents: &ClosureDocumentSpool,
) -> RequestedScopeManifest {
    let mut effective = requested.clone();
    effective.processes = documents
        .records()
        .iter()
        .filter(|record| record.identity.category == DatasetCategory::Processes)
        .map(|record| RequestedIdentity {
            id: record.identity.id,
            version: record.identity.version.clone(),
        })
        .collect();
    effective.lcia_methods = documents
        .records()
        .iter()
        .filter(|record| record.identity.category == DatasetCategory::Lciamethods)
        .map(|record| RequestedIdentity {
            id: record.identity.id,
            version: record.identity.version.clone(),
        })
        .collect();
    effective.processes.sort_by(|left, right| {
        (left.id, left.version.as_str()).cmp(&(right.id, right.version.as_str()))
    });
    effective.lcia_methods.sort_by(|left, right| {
        (left.id, left.version.as_str()).cmp(&(right.id, right.version.as_str()))
    });
    effective.process_manifest_hash = canonical_json_sha256(&json!({
        "processes": effective.processes,
    }))
    .ok();
    effective
}

fn source_fingerprint(documents: &ClosureDocumentSpool) -> anyhow::Result<String> {
    let source = documents
        .records()
        .iter()
        .map(|record| {
            json!({
                "identity": record.identity,
                "contentSha256": record.canonical_content_hash,
            })
        })
        .collect::<Vec<_>>();
    canonical_json_sha256(&source)
}

fn build_resolution_map_spool(
    edges: &JsonlValueSpool,
    omitted_resolutions: &[Value],
) -> anyhow::Result<JsonlValueSpool> {
    let mut writer = JsonlValueSpoolWriter::new("resolution-map-unsorted.jsonl")?;
    edges.visit(|edge| {
        writer.append(&json!({
            "kind": "reference-request",
            "source": edge.get("document_key"),
            "jsonPath": edge.get("json_path"),
            "role": edge.get("reference_role"),
            "targetCategory": edge.get("target_category"),
            "targetId": edge.get("target_uuid"),
            "requestedVersionState": edge.get("requested_version_state"),
            "requestedVersion": edge.get("requested_version"),
        }))?;
        Ok(())
    })?;
    for resolution in omitted_resolutions {
        writer.append(&json!({
            "kind": "omitted-version-resolution",
            "provenance": resolution,
        }))?;
    }
    let unsorted = writer.finish()?;
    sort_jsonl_spool(&unsorted)
}

impl IssuePartitionAccumulator {
    fn new(temp: Arc<TempDir>, relation: &'static str) -> Self {
        Self {
            temp,
            relation,
            max_records: ISSUE_PARTITION_MAX_RECORDS,
            max_uncompressed_bytes: ISSUE_PARTITION_MAX_UNCOMPRESSED_BYTES,
            active: None,
            entries: Vec::new(),
            artifacts: Vec::new(),
            relation_uncompressed_digest: Sha256::new(),
        }
    }

    #[cfg(test)]
    fn with_limits(
        temp: Arc<TempDir>,
        relation: &'static str,
        max_records: u64,
        max_uncompressed_bytes: u64,
    ) -> Self {
        assert!(max_records > 0);
        assert!(max_uncompressed_bytes > 0);
        Self {
            temp,
            relation,
            max_records,
            max_uncompressed_bytes,
            active: None,
            entries: Vec::new(),
            artifacts: Vec::new(),
            relation_uncompressed_digest: Sha256::new(),
        }
    }

    fn push(&mut self, issue_key: &str, value: &Value) -> anyhow::Result<()> {
        let mut bytes = canonical_json_bytes(value)?;
        bytes.push(b'\n');
        let record_bytes = u64::try_from(bytes.len())?;
        if record_bytes > self.max_uncompressed_bytes {
            return Err(anyhow::anyhow!(
                "artifact_limit_exceeded: relation={}, issue_key={}, record_bytes={}, max_partition_bytes={}",
                self.relation,
                issue_key,
                record_bytes,
                self.max_uncompressed_bytes
            ));
        }
        let record_limit_reached = self
            .active
            .as_ref()
            .is_some_and(|active| active.record_count >= self.max_records);
        let byte_limit_reached = self.active.as_ref().is_some_and(|active| {
            active.uncompressed_bytes.saturating_add(record_bytes) > self.max_uncompressed_bytes
        });
        if record_limit_reached || byte_limit_reached {
            self.flush()?;
        }
        if self.active.is_none() {
            self.active = Some(self.open_partition(issue_key)?);
        }
        let active = self.active.as_mut().expect("active partition opened");
        active.encoder.write_all(&bytes)?;
        active.uncompressed_digest.update(&bytes);
        self.relation_uncompressed_digest.update(&bytes);
        active.record_count = active
            .record_count
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("partition record count overflow"))?;
        active.uncompressed_bytes = active
            .uncompressed_bytes
            .checked_add(record_bytes)
            .ok_or_else(|| anyhow::anyhow!("partition uncompressed byte size overflow"))?;
        issue_key.clone_into(&mut active.last_issue_key);
        Ok(())
    }

    fn open_partition(&self, issue_key: &str) -> anyhow::Result<ActiveIssuePartition> {
        let index = self.entries.len();
        let relative_path = format!("{}/part-{index:06}.ndjson.zst", self.relation);
        let path = self.temp.path().join(&relative_path);
        let parent = path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("partition path omitted parent"))?;
        fs::create_dir_all(parent)?;
        let file = File::create(&path)?;
        advise_sequential_access(&file);
        let output = BufWriter::new(file);
        let encoder = zstd::stream::write::Encoder::new(output, 6)?;
        Ok(ActiveIssuePartition {
            relative_path,
            path,
            encoder,
            uncompressed_digest: Sha256::new(),
            record_count: 0,
            uncompressed_bytes: 0,
            first_issue_key: issue_key.to_owned(),
            last_issue_key: issue_key.to_owned(),
        })
    }

    fn flush(&mut self) -> anyhow::Result<()> {
        let Some(active) = self.active.take() else {
            return Ok(());
        };
        let mut output = active.encoder.finish()?;
        output.flush()?;
        release_file_cache(output.get_ref());
        drop(output);

        let (compressed_byte_size, compressed_sha256) = file_size_and_sha256(&active.path)?;
        let entry = IssuePartitionManifestEntry {
            relation: self.relation.to_owned(),
            path: active.relative_path.clone(),
            media_type: "application/x-ndjson+zstd".to_owned(),
            record_count: active.record_count,
            uncompressed_byte_size: active.uncompressed_bytes,
            uncompressed_sha256: hex::encode(active.uncompressed_digest.finalize()),
            compressed_byte_size,
            compressed_sha256: compressed_sha256.clone(),
            first_issue_key: active.first_issue_key,
            last_issue_key: active.last_issue_key,
        };
        self.artifacts.push(PreparedArtifact {
            descriptor: ArtifactManifestEntry {
                artifact_type: "closure_complete_machine_result".to_owned(),
                artifact_role: ScopeClosureArtifactRole::CompleteMachineResult,
                file_name: active.relative_path,
                content_type: "application/x-ndjson+zstd".to_owned(),
                byte_size: usize::try_from(compressed_byte_size)?,
                checksum_sha256: compressed_sha256,
            },
            path: active.path,
            _temp: Arc::clone(&self.temp),
        });
        self.entries.push(entry);
        Ok(())
    }

    fn finish(
        mut self,
    ) -> anyhow::Result<(
        Vec<IssuePartitionManifestEntry>,
        Vec<PreparedArtifact>,
        String,
    )> {
        self.flush()?;
        Ok((
            self.entries,
            self.artifacts,
            hex::encode(self.relation_uncompressed_digest.finalize()),
        ))
    }
}

fn issue_partition_record_v3(
    issue: &ClosureIssue,
    root_impact: &RootImpactReference,
    roots: &StableRootOrdinals,
) -> anyhow::Result<Value> {
    let occurrence_samples = issue
        .occurrences
        .iter()
        .take(ISSUE_INLINE_OCCURRENCE_SAMPLE_LIMIT)
        .collect::<Vec<_>>();
    let affected_root_samples = issue
        .affected_roots
        .iter()
        .enumerate()
        .take(ISSUE_INLINE_AFFECTED_ROOT_SAMPLE_LIMIT)
        .map(|(index, root)| {
            let root_ordinal = roots
                .ordinal_by_identity
                .get(root)
                .copied()
                .ok_or_else(|| anyhow::anyhow!("issue sample references an unknown root"))?;
            Ok(json!({
                "rootOrdinal": root_ordinal,
                "root": root,
                "witnessPath": issue
                    .affected_root_witness_paths
                    .get(index)
                    .unwrap_or(&issue.witness_path),
            }))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(json!({
        "schemaVersion": "lcia.scope-closure-issue.v3",
        "issueKey": issue.issue_key,
        "severity": issue.severity,
        "blocker": issue.blocking,
        "code": issue.issue_code,
        "source": issue.source,
        "path": issue.json_path,
        "referenceRole": issue.reference_role,
        "requestedTargetType": issue.requested_target_type,
        "requestedTargetId": issue.requested_target_id,
        "requestedTargetVersion": issue.requested_target_version,
        "message": issue.message,
        "suggestedAction": issue.suggested_action,
        "occurrenceCount": issue.occurrence_count,
        "affectedRootCount": issue.affected_root_count,
        "rootImpact": root_impact,
        "occurrenceSamples": occurrence_samples,
        "occurrenceSamplesTruncated": usize::try_from(issue.occurrence_count)
            .unwrap_or(usize::MAX) > occurrence_samples.len(),
        "affectedRootSamples": affected_root_samples,
        "affectedRootSamplesTruncated": usize::try_from(issue.affected_root_count)
            .unwrap_or(usize::MAX) > affected_root_samples.len(),
    }))
}

#[cfg(test)]
fn affected_root_partition_record(
    issue_key: &str,
    root: &ExactDatasetIdentity,
    witness_path: &[ExactDatasetIdentity],
) -> Value {
    json!({
        "schemaVersion": "lcia.scope-closure-affected-root.v1",
        "issueKey": issue_key,
        "root": root,
        "impactRole": "root",
        "witnessPath": witness_path,
    })
}

#[allow(clippy::too_many_lines)]
#[cfg(test)]
fn prepare_issue_partition_artifacts(
    closure_check_id: Uuid,
    scan: &ScopeClosureScan,
    validation: &TidasBatchValidation,
    temp: Arc<TempDir>,
) -> anyhow::Result<Vec<PreparedArtifact>> {
    prepare_issue_partition_artifacts_with_cancellation(
        closure_check_id,
        scan,
        validation,
        temp,
        &CancellationToken::default(),
        None,
    )
}

#[allow(clippy::too_many_lines)]
fn prepare_issue_partition_artifacts_with_cancellation(
    closure_check_id: Uuid,
    scan: &ScopeClosureScan,
    validation: &TidasBatchValidation,
    temp: Arc<TempDir>,
    cancellation: &CancellationToken,
    progress: Option<&ScopeClosureArtifactProgress>,
) -> anyhow::Result<Vec<PreparedArtifact>> {
    cancellation.check("scope_closure_issue_artifact_start")?;
    let relations = scan
        .issue_relations
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("issue relation spools were not prepared"))?;
    let stable_roots = StableRootOrdinals::new(&scan.roots, &scan.reference_graph)?;
    let frozen_reference_graph =
        build_frozen_reference_graph_file(&scan.reference_graph, &stable_roots, cancellation)?;
    let tidas_issue_stream = compress_tidas_issue_stream(&validation.issue_events, cancellation)?;
    let mut entries = relations.issue_partition_entries.clone();
    let mut artifacts = relations.issue_partition_artifacts.clone();
    let mut evidence = vec![
        relations
            .root_impact_index
            .manifest_entry("root-impact-index"),
        frozen_reference_graph.manifest_entry("frozen-reference-graph"),
        tidas_issue_stream.manifest_entry("tidas-issue-stream"),
    ];
    artifacts.push(relations.root_impact_index.prepared_artifact()?);
    artifacts.push(frozen_reference_graph.prepared_artifact()?);
    artifacts.push(tidas_issue_stream.prepared_artifact()?);
    let partition_bytes = artifacts.iter().fold(0_u64, |total, artifact| {
        total.saturating_add(u64::try_from(artifact.descriptor.byte_size).unwrap_or(u64::MAX))
    });
    if let Some(progress) = progress {
        progress.update(
            4,
            relations.stats.issue_count,
            partition_bytes,
            u64::try_from(artifacts.len())?,
        );
    }
    record_scope_closure_resources(
        "write_issue_partitions_complete",
        Some(partition_bytes),
        Some(relations.stats.affected_root_count),
    );
    entries.sort_by(|left, right| left.path.cmp(&right.path));
    evidence.sort_by(|left, right| left.path.cmp(&right.path));
    artifacts.sort_by(|left, right| left.descriptor.file_name.cmp(&right.descriptor.file_name));
    let graph_edge_count =
        scan.reference_graph
            .reverse
            .iter()
            .map(Vec::len)
            .try_fold(0_u64, |total, count| {
                total
                    .checked_add(u64::try_from(count)?)
                    .ok_or_else(|| anyhow::anyhow!("reference graph edge count overflow"))
            })?;
    let manifest = IssuePartitionManifestV3 {
        schema_version: "lcia.scope-closure-issue-manifest.v3".to_owned(),
        closure_check_id,
        logical_issue_stream_sha256: validation.issue_events.sha256.clone(),
        logical_issue_event_count: validation.issue_events.event_count,
        logical_issue_stream_byte_size: validation.issue_events.byte_size,
        partition_max_records: ISSUE_PARTITION_MAX_RECORDS,
        partition_max_uncompressed_bytes: ISSUE_PARTITION_MAX_UNCOMPRESSED_BYTES,
        issue_count: relations.stats.issue_count,
        occurrence_count: relations.stats.occurrence_count,
        affected_root_count: relations.stats.affected_root_count,
        expanded_affected_root_record_count: 0,
        root_impact_record_count: relations.root_impact_index.record_count,
        root_count: u64::try_from(stable_roots.roots.len())?,
        graph_node_count: u64::try_from(scan.reference_graph.identities.len())?,
        graph_edge_count,
        relation_stream_sha256: IssueRelationStreamHashesV3 {
            issues: relations.issue_relation_sha256.clone(),
            tidas_issue_stream: tidas_issue_stream.uncompressed_sha256,
            root_impact_index: relations.root_impact_index.uncompressed_sha256.clone(),
            frozen_reference_graph: frozen_reference_graph.uncompressed_sha256,
        },
        ordering: IssueManifestOrdering {
            issue_key: "UTF-8 ascending".to_owned(),
            root_ordinal: "exact dataset identity ascending".to_owned(),
            graph_node_ordinal: "exact dataset identity ascending".to_owned(),
            root_impact_key: "UTF-8 ascending".to_owned(),
        },
        rpc_issue_sample_limit: ISSUE_INLINE_ISSUE_SAMPLE_LIMIT,
        rpc_occurrence_sample_limit_per_issue: ISSUE_INLINE_OCCURRENCE_SAMPLE_LIMIT,
        rpc_affected_root_sample_limit_per_issue: ISSUE_INLINE_AFFECTED_ROOT_SAMPLE_LIMIT,
        xlsx_issue_sample_limit: XLSX_ISSUE_SAMPLE_LIMIT,
        xlsx_occurrence_sample_limit: XLSX_OCCURRENCE_SAMPLE_LIMIT,
        xlsx_affected_root_sample_limit: XLSX_AFFECTED_ROOT_SAMPLE_LIMIT,
        compatibility: IssueManifestCompatibility {
            readable_schema_versions: vec![
                "lcia.scope-closure-issue-manifest.v2".to_owned(),
                "lcia.scope-closure-issue-manifest.v3".to_owned(),
            ],
            v2_affected_root_projection:
                "derive issue×root rows and witnesses on demand from rootImpact and frozen-reference-graph"
                    .to_owned(),
            public_transport: vec!["closure-report-v1.xlsx".to_owned(), "manifest.json".to_owned()],
        },
        evidence,
        partitions: entries,
    };
    let manifest_path = temp.path().join("manifest.json");
    fs::write(&manifest_path, canonical_json_bytes(&manifest)?)?;
    artifacts.push(prepare_file_artifact(
        temp,
        "closure_complete_machine_result",
        ScopeClosureArtifactRole::CompleteMachineResult,
        "manifest.json",
        "application/vnd.tiangong.scope-closure-manifest+json",
        manifest_path,
    )?);
    Ok(artifacts)
}

#[cfg(test)]
fn prepare_closure_content_artifacts(
    closure_bundle: ClosureBundleFile,
    closure_check_id: Uuid,
    scan: &ScopeClosureScan,
    validation: &TidasBatchValidation,
) -> anyhow::Result<Vec<PreparedArtifact>> {
    prepare_closure_content_artifacts_with_cancellation(
        closure_bundle,
        closure_check_id,
        scan,
        validation,
        &CancellationToken::default(),
        None,
    )
}

fn prepare_closure_content_artifacts_with_cancellation(
    closure_bundle: ClosureBundleFile,
    closure_check_id: Uuid,
    scan: &ScopeClosureScan,
    validation: &TidasBatchValidation,
    cancellation: &CancellationToken,
    progress: Option<&ScopeClosureArtifactProgress>,
) -> anyhow::Result<Vec<PreparedArtifact>> {
    let ClosureBundleFile {
        temp,
        path: bundle_path,
        byte_size: bundle_byte_size,
        sha256: bundle_sha256,
    } = closure_bundle;
    cancellation.check("scope_closure_xlsx_report")?;
    let xlsx_path = temp.path().join("closure-report-v1.xlsx");
    build_scan_xlsx_report_file(&xlsx_path, closure_check_id, scan)?;

    let mut artifacts = vec![
        PreparedArtifact {
            descriptor: ArtifactManifestEntry {
                artifact_type: "closure_bundle".to_owned(),
                artifact_role: ScopeClosureArtifactRole::ClosureBundle,
                file_name: "closure-bundle-v3.json".to_owned(),
                content_type: "application/json".to_owned(),
                byte_size: usize::try_from(bundle_byte_size)?,
                checksum_sha256: bundle_sha256,
            },
            path: bundle_path,
            _temp: Arc::clone(&temp),
        },
        prepare_file_artifact(
            Arc::clone(&temp),
            "closure_report_xlsx",
            ScopeClosureArtifactRole::ClosureReport,
            "closure-report-v1.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            xlsx_path,
        )?,
    ];
    artifacts.extend(prepare_issue_partition_artifacts_with_cancellation(
        closure_check_id,
        scan,
        validation,
        Arc::clone(&temp),
        cancellation,
        progress,
    )?);
    Ok(artifacts)
}

fn prepare_file_artifact(
    temp: Arc<TempDir>,
    artifact_type: &str,
    artifact_role: ScopeClosureArtifactRole,
    file_name: &str,
    content_type: &str,
    path: PathBuf,
) -> anyhow::Result<PreparedArtifact> {
    let (byte_size, checksum_sha256) = file_size_and_sha256(&path)?;
    Ok(PreparedArtifact {
        descriptor: ArtifactManifestEntry {
            artifact_type: artifact_type.to_owned(),
            artifact_role,
            file_name: file_name.to_owned(),
            content_type: content_type.to_owned(),
            byte_size: usize::try_from(byte_size)?,
            checksum_sha256,
        },
        path,
        _temp: temp,
    })
}

fn file_size_and_sha256(path: &Path) -> anyhow::Result<(u64, String)> {
    let mut file = File::open(path)?;
    let mut digest = Sha256::new();
    let mut byte_size = 0_u64;
    let mut buffer = vec![0_u8; 64 * 1024].into_boxed_slice();
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
        byte_size = byte_size
            .checked_add(u64::try_from(read)?)
            .ok_or_else(|| anyhow::anyhow!("artifact byte size overflow"))?;
    }
    Ok((byte_size, hex::encode(digest.finalize())))
}

fn closure_artifact_metadata(
    artifact: &PreparedArtifact,
    closure_check_id: Uuid,
    content_artifact_manifest_hash: &str,
) -> Value {
    let mut metadata = json!({
        "schemaVersion": "lcia.scope-closure-artifact.v2",
        "closureCheckId": closure_check_id,
        "fileName": artifact.descriptor.file_name,
        "artifactRole": artifact.descriptor.artifact_role,
        "retentionSeconds": SCOPE_CLOSURE_ARTIFACT_RETENTION_SECONDS,
        "contentArtifactManifestHash": content_artifact_manifest_hash,
    });
    if artifact.descriptor.artifact_role == ScopeClosureArtifactRole::ClosureBundle {
        metadata["completeMachineResultClientKey"] = json!("manifest.json");
    }
    metadata
}

fn is_semantic_closure_artifact(artifact: &PreparedArtifact) -> bool {
    artifact.descriptor.artifact_role != ScopeClosureArtifactRole::CompleteMachineResult
        || artifact.descriptor.file_name == "manifest.json"
}

#[allow(clippy::too_many_lines)]
async fn persist_closure_artifacts(
    state: &AppState,
    worker_job_id: Uuid,
    closure_check_id: Uuid,
    artifacts: &[PreparedArtifact],
    content_artifact_manifest_hash: &str,
    reused_from_check_id: Option<Uuid>,
    progress: Option<&WorkerJobProgress<'_>>,
) -> anyhow::Result<BTreeMap<String, Uuid>> {
    let progress = progress.ok_or_else(|| {
        anyhow::anyhow!("v2 artifact registration requires an active Worker lease")
    })?;
    let request_id = deterministic_contract_uuid(&format!(
        "scope-closure-write-set-v2:{closure_check_id}:{content_artifact_manifest_hash}"
    ));
    let mut uploaded = Vec::<String>::new();
    let mut staged = Vec::<(&PreparedArtifact, String)>::new();
    for artifact in artifacts {
        let relative_key = format!(
            "scope-closure/{closure_check_id}/{request_id}/{}",
            artifact.descriptor.file_name
        );
        let object_key = state.object_store.prefixed_object_key(&relative_key)?;
        staged.push((artifact, object_key));
    }
    staged.sort_by(|left, right| {
        left.0
            .descriptor
            .file_name
            .cmp(&right.0.descriptor.file_name)
    });
    if staged
        .windows(2)
        .any(|pair| pair[0].0.descriptor.file_name == pair[1].0.descriptor.file_name)
    {
        return Err(anyhow::anyhow!(
            "closure artifact descriptors contain duplicate client keys"
        ));
    }
    let request_items = staged
        .iter()
        .enumerate()
        .map(|(ordinal, (artifact, object_key))| {
            Ok(json!({
                "ordinal": ordinal + 1,
                "clientKey": artifact.descriptor.file_name,
                "artifactType": artifact.descriptor.artifact_type,
                "artifactRole": artifact.descriptor.artifact_role,
                "bucket": state.object_store.bucket_name(),
                "objectPath": object_key,
                "mediaType": artifact.descriptor.content_type,
                "size": artifact.descriptor.byte_size,
                "checksumSha256": artifact.descriptor.checksum_sha256,
                "metadata": closure_artifact_metadata(
                    artifact,
                    closure_check_id,
                    content_artifact_manifest_hash,
                ),
            }))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let descriptor_set_sha256 = canonical_descriptor_set_sha256(&request_items)?;
    let required_primary_roles = closure_artifact_required_primary_roles(reused_from_check_id);
    let expected_descriptor_count = u64::try_from(request_items.len())?;
    let expectation = ScopeClosureArtifactWriteSetExpectation {
        closure_check_id,
        worker_job_id,
        request_id,
        publication_mode: if reused_from_check_id.is_some() {
            "reused"
        } else {
            "fresh"
        },
        reused_from_check_id,
        expected_descriptor_count,
        descriptor_set_sha256: &descriptor_set_sha256,
        required_primary_roles: &required_primary_roles,
    };
    let mut header = create_closure_artifact_write_set_v2(
        &state.pool,
        closure_check_id,
        worker_job_id,
        progress.lease_token(),
        request_id,
        u64::try_from(request_items.len())?,
        &descriptor_set_sha256,
        &required_primary_roles,
        reused_from_check_id,
    )
    .await?;
    match header.status.as_str() {
        "ready" => {
            validate_closure_artifact_write_set_header(&header, &expectation, "ready", false)?;
            validate_closure_artifact_map(&header, artifacts)?;
            return semantic_closure_artifact_ids(&header, artifacts);
        }
        "staging" => {
            validate_closure_artifact_write_set_header(&header, &expectation, "staging", true)?;
            validate_closure_artifact_map(&header, artifacts)?;
        }
        "registration_open" => {
            validate_closure_artifact_write_set_header(
                &header,
                &expectation,
                "registration_open",
                false,
            )?;
            for (batch_index, batch) in request_items
                .chunks(ARTIFACT_REGISTRATION_BATCH_SIZE)
                .enumerate()
            {
                let batch_digest = canonical_json_sha256(&Value::Array(batch.to_vec()))?;
                let batch_id = deterministic_contract_uuid(&format!(
                    "scope-closure-write-set-v2:{request_id}:batch-{batch_index:06}:{batch_digest}"
                ));
                let registration = register_closure_artifact_write_set_batch_v2(
                    &state.pool,
                    &header,
                    worker_job_id,
                    progress.lease_token(),
                    batch_id,
                    batch,
                )
                .await;
                let status = read_closure_artifact_write_set_status_v2(
                    &state.pool,
                    closure_check_id,
                    worker_job_id,
                    progress.lease_token(),
                    request_id,
                )
                .await;
                header = resolve_artifact_registration_readback(registration, status)?;
                validate_closure_artifact_write_set_header(
                    &header,
                    &expectation,
                    "registration_open",
                    false,
                )?;
                let expected_registered = u64::try_from(
                    request_items
                        .len()
                        .min((batch_index + 1) * ARTIFACT_REGISTRATION_BATCH_SIZE),
                )?;
                if header.registered_descriptor_count < expected_registered {
                    let error = anyhow::anyhow!(
                        "artifact_write_set_v2_batch_replay_mismatch: expected_at_least={expected_registered}, actual={}",
                        header.registered_descriptor_count
                    );
                    fail_closure_artifact_write_set(
                        &state.pool,
                        &header,
                        worker_job_id,
                        progress.lease_token(),
                        &error,
                    )
                    .await;
                    return Err(error);
                }
                heartbeat_closure_artifact_registration(
                    progress,
                    closure_check_id,
                    header.registered_descriptor_count,
                    expected_descriptor_count,
                    request_items
                        .iter()
                        .take(usize::try_from(header.registered_descriptor_count)?)
                        .filter_map(|item| item.get("size").and_then(Value::as_u64))
                        .sum(),
                )
                .await?;
            }
            header = read_closure_artifact_write_set_status_v2(
                &state.pool,
                closure_check_id,
                worker_job_id,
                progress.lease_token(),
                request_id,
            )
            .await?;
            validate_closure_artifact_write_set_header(
                &header,
                &expectation,
                "registration_open",
                false,
            )?;
            if header.registered_descriptor_count != header.expected_descriptor_count {
                let error = anyhow::anyhow!("artifact_write_set_v2_incomplete");
                fail_closure_artifact_write_set(
                    &state.pool,
                    &header,
                    worker_job_id,
                    progress.lease_token(),
                    &error,
                )
                .await;
                return Err(error
                    .context("artifact write set remained incomplete after bounded registration"));
            }
            let seal = seal_closure_artifact_write_set_v2(
                &state.pool,
                &header,
                worker_job_id,
                progress.lease_token(),
            )
            .await;
            let status = read_closure_artifact_write_set_status_v2(
                &state.pool,
                closure_check_id,
                worker_job_id,
                progress.lease_token(),
                request_id,
            )
            .await;
            let (sealed_header, seal_error) = resolve_artifact_seal_readback(seal, status)?;
            header = sealed_header;
            if let Err(error) =
                validate_closure_artifact_write_set_header(&header, &expectation, "staging", true)
                    .and_then(|()| validate_closure_artifact_map(&header, artifacts))
            {
                fail_closure_artifact_write_set(
                    &state.pool,
                    &header,
                    worker_job_id,
                    progress.lease_token(),
                    &error,
                )
                .await;
                return Err(match seal_error {
                    None => error,
                    Some(seal_error) => seal_error.context(format!(
                        "atomic seal failed and readback did not prove staging: {error:#}"
                    )),
                });
            }
        }
        status => {
            return Err(anyhow::anyhow!(
                "artifact_write_set_v2_invalid_state: {status}"
            ));
        }
    }

    for (index, (artifact, object_key)) in staged.iter().enumerate() {
        let cancellation = CancellationToken::default();
        let heartbeat_period = lease_heartbeat_period(progress.lease_seconds());
        let upload = Box::pin(
            state.object_store.upload_object_key_file_bounded(
                object_key,
                artifact.descriptor.content_type.as_str(),
                &artifact.path,
                ObjectTransferOptions::new(SCOPE_CLOSURE_ARTIFACT_MAX_UPLOAD_BYTES)
                    .with_expected_sha256(artifact.descriptor.checksum_sha256.clone())
                    .with_cancellation(cancellation.clone()),
            ),
        );
        if let Err(error) =
            supervise_cancellable_operation(upload, cancellation, heartbeat_period, || {
                heartbeat_closure_artifact_upload(
                    Some(progress),
                    closure_check_id,
                    index,
                    staged.len(),
                )
            })
            .await
        {
            fail_closure_artifact_write_set(
                &state.pool,
                &header,
                worker_job_id,
                progress.lease_token(),
                &error,
            )
            .await;
            let mut cleanup_keys = uploaded.clone();
            cleanup_keys.push(object_key.clone());
            cleanup_uploaded_artifacts(state, &cleanup_keys).await;
            return Err(error.context("failed to upload closure artifact write set"));
        }
        uploaded.push(object_key.clone());
    }

    let finalize_attempt = fence_closure_artifact_finalize(
        || {
            heartbeat_closure_artifact_upload(
                Some(progress),
                closure_check_id,
                staged.len(),
                staged.len(),
            )
        },
        || {
            finalize_closure_artifact_write_set(
                &state.pool,
                &header,
                closure_check_id,
                worker_job_id,
                progress.lease_token(),
                request_id,
            )
        },
    )
    .await;
    let finalized = match finalize_attempt {
        Ok(finalized) => finalized,
        Err(error) => {
            let readback = read_closure_artifact_write_set_status_v2(
                &state.pool,
                closure_check_id,
                worker_job_id,
                progress.lease_token(),
                request_id,
            )
            .await;
            match readback {
                Ok(candidate)
                    if validate_closure_artifact_write_set_header(
                        &candidate,
                        &expectation,
                        "ready",
                        false,
                    )
                    .and_then(|()| validate_closure_artifact_map(&candidate, artifacts))
                    .is_ok() =>
                {
                    candidate
                }
                Ok(candidate) => {
                    if candidate.status != "ready" {
                        fail_closure_artifact_write_set(
                            &state.pool,
                            &candidate,
                            worker_job_id,
                            progress.lease_token(),
                            &error,
                        )
                        .await;
                        cleanup_uploaded_artifacts(state, &uploaded).await;
                    }
                    return Err(error.context(format!(
                        "finalize failed and status readback did not prove the exact ready set: status={}",
                        candidate.status
                    )));
                }
                Err(readback_error) => {
                    return Err(error.context(format!(
                        "finalize outcome is unknown; deterministic staged objects were retained for retry/reconciliation because status readback failed: {readback_error:#}"
                    )));
                }
            }
        }
    };
    if let Err(error) =
        validate_closure_artifact_write_set_header(&finalized, &expectation, "ready", false)
            .and_then(|()| validate_closure_artifact_map(&finalized, artifacts))
    {
        if finalized.status != "ready" {
            fail_closure_artifact_write_set(
                &state.pool,
                &finalized,
                worker_job_id,
                progress.lease_token(),
                &error,
            )
            .await;
            cleanup_uploaded_artifacts(state, &uploaded).await;
        }
        return Err(
            error.context("closure artifact finalize returned an inconsistent write-set readback")
        );
    }
    semantic_closure_artifact_ids(&finalized, artifacts)
}

fn semantic_closure_artifact_ids(
    header: &ScopeClosureArtifactWriteSetHeader,
    artifacts: &[PreparedArtifact],
) -> anyhow::Result<BTreeMap<String, Uuid>> {
    let mut persisted = BTreeMap::new();
    for artifact in artifacts {
        if is_semantic_closure_artifact(artifact) {
            let artifact_id = header
                .artifact_map
                .get(&artifact.descriptor.file_name)
                .copied()
                .ok_or_else(|| anyhow::anyhow!("finalized artifact map omitted semantic key"))?;
            persisted.insert(artifact.descriptor.artifact_type.clone(), artifact_id);
        }
    }
    Ok(persisted)
}

fn resolve_artifact_registration_readback(
    registration: anyhow::Result<()>,
    status: anyhow::Result<ScopeClosureArtifactWriteSetHeader>,
) -> anyhow::Result<ScopeClosureArtifactWriteSetHeader> {
    match (registration, status) {
        (_, Ok(status)) => Ok(status),
        (Err(registration_error), Err(status_error)) => Err(registration_error.context(format!(
            "artifact registration status readback also failed: {status_error:#}"
        ))),
        (Ok(()), Err(status_error)) => {
            Err(status_error.context("artifact registration status readback failed"))
        }
    }
}

fn resolve_artifact_seal_readback(
    seal: anyhow::Result<()>,
    status: anyhow::Result<ScopeClosureArtifactWriteSetHeader>,
) -> anyhow::Result<(ScopeClosureArtifactWriteSetHeader, Option<anyhow::Error>)> {
    match status {
        Ok(status) => Ok((status, seal.err())),
        Err(status_error) => match seal {
            Ok(()) => Err(status_error.context(
                "sealed artifact write set could not be read back; upload was not started",
            )),
            Err(seal_error) => Err(seal_error.context(format!(
                "seal status is unknown and readback failed; upload was not started: {status_error:#}"
            ))),
        },
    }
}

fn canonical_descriptor_set_sha256(descriptors: &[Value]) -> anyhow::Result<String> {
    canonical_json_sha256(&json!({
        "contractVersion": "lcia.scope-closure-artifact-write-set.v2",
        "descriptors": descriptors,
    }))
}

fn deterministic_contract_uuid(identity: &str) -> Uuid {
    let digest = Sha256::digest(identity.as_bytes());
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

fn closure_artifact_required_primary_roles(reused_from_check_id: Option<Uuid>) -> Value {
    if reused_from_check_id.is_some() {
        return json!([{
            "artifactRole": "closure_report",
            "artifactType": "closure_report_xlsx",
            "mediaType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "exactCount": 1,
        }]);
    }
    json!([
        {
            "artifactRole": "closure_report",
            "artifactType": "closure_report_xlsx",
            "mediaType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "exactCount": 1,
        },
        {
            "artifactRole": "complete_machine_result",
            "artifactType": "closure_complete_machine_result",
            "mediaType": "application/vnd.tiangong.scope-closure-manifest+json",
            "exactCount": 1,
        },
        {
            "artifactRole": "closure_bundle",
            "artifactType": "closure_bundle",
            "mediaType": "application/json",
            "exactCount": 1,
        },
    ])
}

#[allow(clippy::too_many_arguments)]
async fn create_closure_artifact_write_set_v2(
    pool: &PgPool,
    closure_check_id: Uuid,
    worker_job_id: Uuid,
    worker_lease_token: Uuid,
    request_id: Uuid,
    expected_descriptor_count: u64,
    descriptor_set_sha256: &str,
    required_primary_roles: &Value,
    reused_from_check_id: Option<Uuid>,
) -> anyhow::Result<ScopeClosureArtifactWriteSetHeader> {
    let row = sqlx::query(
        r"
        WITH _service_role AS (
          SELECT set_config('request.jwt.claim.role', 'service_role', true)
        )
        SELECT public.svc_lcia_scope_closure_artifact_write_set_create_v2(
          $1, $2, $3, $4, 'lcia.scope-closure-artifact-write-set.v2',
          $5, $6, $7::jsonb, $8, $9
        ) AS result
        FROM _service_role
        ",
    )
    .bind(closure_check_id)
    .bind(worker_job_id)
    .bind(worker_lease_token)
    .bind(request_id)
    .bind(i32::try_from(expected_descriptor_count)?)
    .bind(descriptor_set_sha256)
    .bind(required_primary_roles)
    .bind(SCOPE_CLOSURE_ARTIFACT_STAGING_SECONDS)
    .bind(reused_from_check_id)
    .fetch_one(pool)
    .await?;
    let result = row.try_get::<Value, _>("result")?;
    parse_closure_artifact_write_set_header(
        &result,
        "svc_lcia_scope_closure_artifact_write_set_create_v2",
    )
}

async fn register_closure_artifact_write_set_batch_v2(
    pool: &PgPool,
    header: &ScopeClosureArtifactWriteSetHeader,
    worker_job_id: Uuid,
    worker_lease_token: Uuid,
    batch_id: Uuid,
    items: &[Value],
) -> anyhow::Result<()> {
    if items.is_empty() || items.len() > ARTIFACT_REGISTRATION_BATCH_SIZE {
        return Err(anyhow::anyhow!("artifact_write_set_v2_invalid"));
    }
    let row = sqlx::query(
        r"
        WITH _service_role AS (
          SELECT set_config('request.jwt.claim.role', 'service_role', true)
        )
        SELECT public.svc_lcia_scope_closure_artifact_write_set_register_batch_v2(
          $1, $2, $3, $4, $5, $6::jsonb
        ) AS result
        FROM _service_role
        ",
    )
    .bind(header.write_set_id)
    .bind(header.write_token)
    .bind(worker_job_id)
    .bind(worker_lease_token)
    .bind(batch_id)
    .bind(Value::Array(items.to_vec()))
    .fetch_one(pool)
    .await?;
    let result = row.try_get::<Value, _>("result")?;
    ensure_rpc_ok(
        &result,
        "svc_lcia_scope_closure_artifact_write_set_register_batch_v2",
    )
}

async fn read_closure_artifact_write_set_status_v2(
    pool: &PgPool,
    closure_check_id: Uuid,
    worker_job_id: Uuid,
    worker_lease_token: Uuid,
    request_id: Uuid,
) -> anyhow::Result<ScopeClosureArtifactWriteSetHeader> {
    let row = sqlx::query(
        r"
        WITH _service_role AS (
          SELECT set_config('request.jwt.claim.role', 'service_role', true)
        )
        SELECT public.svc_lcia_scope_closure_artifact_write_set_status_v2(
          $1, $2, $3, $4
        ) AS result
        FROM _service_role
        ",
    )
    .bind(closure_check_id)
    .bind(worker_job_id)
    .bind(worker_lease_token)
    .bind(request_id)
    .fetch_one(pool)
    .await?;
    let result = row.try_get::<Value, _>("result")?;
    parse_closure_artifact_write_set_header(
        &result,
        "svc_lcia_scope_closure_artifact_write_set_status_v2",
    )
}

async fn seal_closure_artifact_write_set_v2(
    pool: &PgPool,
    header: &ScopeClosureArtifactWriteSetHeader,
    worker_job_id: Uuid,
    worker_lease_token: Uuid,
) -> anyhow::Result<()> {
    let row = sqlx::query(
        r"
        WITH _service_role AS (
          SELECT set_config('request.jwt.claim.role', 'service_role', true)
        )
        SELECT public.svc_lcia_scope_closure_artifact_write_set_seal_v2(
          $1, $2, $3, $4
        ) AS result
        FROM _service_role
        ",
    )
    .bind(header.write_set_id)
    .bind(header.write_token)
    .bind(worker_job_id)
    .bind(worker_lease_token)
    .fetch_one(pool)
    .await?;
    let result = row.try_get::<Value, _>("result")?;
    ensure_rpc_ok(&result, "svc_lcia_scope_closure_artifact_write_set_seal_v2")
}

async fn finalize_closure_artifact_write_set(
    pool: &PgPool,
    header: &ScopeClosureArtifactWriteSetHeader,
    closure_check_id: Uuid,
    worker_job_id: Uuid,
    worker_lease_token: Uuid,
    request_id: Uuid,
) -> anyhow::Result<ScopeClosureArtifactWriteSetHeader> {
    let row = sqlx::query(
        r"
        WITH _service_role AS (
          SELECT set_config('request.jwt.claim.role', 'service_role', true)
        )
        SELECT public.svc_lcia_scope_closure_artifact_write_set_finalize_v2(
          $1, $2, $3, $4
        ) AS result
        FROM _service_role
        ",
    )
    .bind(header.write_set_id)
    .bind(header.write_token)
    .bind(worker_job_id)
    .bind(worker_lease_token)
    .fetch_one(pool)
    .await?;
    let result = row.try_get::<Value, _>("result")?;
    ensure_rpc_ok(
        &result,
        "svc_lcia_scope_closure_artifact_write_set_finalize_v2",
    )?;
    read_closure_artifact_write_set_status_v2(
        pool,
        closure_check_id,
        worker_job_id,
        worker_lease_token,
        request_id,
    )
    .await
}

async fn fail_closure_artifact_write_set(
    pool: &PgPool,
    header: &ScopeClosureArtifactWriteSetHeader,
    worker_job_id: Uuid,
    worker_lease_token: Uuid,
    error: &anyhow::Error,
) {
    let message = format!("{error:#}").chars().take(1_000).collect::<String>();
    let result = sqlx::query(
        r"
        WITH _service_role AS (
          SELECT set_config('request.jwt.claim.role', 'service_role', true)
        )
        SELECT public.svc_lcia_scope_closure_artifact_write_set_fail_v2(
          $1, $2, $3, $4, $5
        ) AS result
        FROM _service_role
        ",
    )
    .bind(header.write_set_id)
    .bind(header.write_token)
    .bind(worker_job_id)
    .bind(worker_lease_token)
    .bind(message)
    .fetch_one(pool)
    .await;
    match result {
        Ok(row) => match row.try_get::<Value, _>("result").and_then(|value| {
            ensure_rpc_ok(&value, "svc_lcia_scope_closure_artifact_write_set_fail_v2")
                .map_err(|error| sqlx::Error::Protocol(error.to_string()))
        }) {
            Ok(()) => {}
            Err(failure_error) => {
                tracing::warn!(
                    write_set_id = %header.write_set_id,
                    error = %failure_error,
                    "Database rejected closure artifact write-set reconciliation mark"
                );
            }
        },
        Err(failure_error) => {
            tracing::warn!(
                write_set_id = %header.write_set_id,
                error = %failure_error,
                "failed to mark closure artifact write set for reconciliation"
            );
        }
    }
}

fn parse_closure_artifact_write_set_header(
    result: &Value,
    rpc_name: &str,
) -> anyhow::Result<ScopeClosureArtifactWriteSetHeader> {
    ensure_rpc_ok(result, rpc_name)?;
    let data = result
        .get("data")
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("{rpc_name} omitted data"))?;
    Ok(serde_json::from_value(data)?)
}

fn validate_closure_artifact_write_set_header(
    header: &ScopeClosureArtifactWriteSetHeader,
    expected: &ScopeClosureArtifactWriteSetExpectation<'_>,
    expected_status: &str,
    expected_upload_eligible: bool,
) -> anyhow::Result<()> {
    if header.status != expected_status
        || header.write_token.is_nil()
        || header.closure_check_id != expected.closure_check_id
        || header.worker_job_id != expected.worker_job_id
        || header.request_id != expected.request_id
        || header.publication_mode != expected.publication_mode
        || header.reused_from_check_id != expected.reused_from_check_id
        || header.contract_version != "lcia.scope-closure-artifact-write-set.v2"
        || header.expected_descriptor_count != expected.expected_descriptor_count
        || header.registered_descriptor_count > expected.expected_descriptor_count
        || header.descriptor_set_sha256 != expected.descriptor_set_sha256
        || header.required_primary_roles != *expected.required_primary_roles
        || header.upload_eligible != expected_upload_eligible
        || header.registered_batch_count != u64::try_from(header.batches.len())?
    {
        return Err(anyhow::anyhow!(
            "Database returned an inconsistent closure artifact write-set header"
        ));
    }
    let mut next_ordinal = 1_u64;
    let mut batch_ids = BTreeSet::new();
    for batch in &header.batches {
        let expected_last = batch
            .first_ordinal
            .checked_add(batch.item_count.saturating_sub(1))
            .ok_or_else(|| anyhow::anyhow!("artifact batch ordinal overflow"))?;
        if batch.batch_id.is_nil()
            || !batch_ids.insert(batch.batch_id)
            || batch.item_count == 0
            || batch.first_ordinal != next_ordinal
            || batch.last_ordinal != expected_last
        {
            return Err(anyhow::anyhow!(
                "Database returned an inconsistent artifact batch receipt"
            ));
        }
        next_ordinal = batch
            .last_ordinal
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("artifact batch ordinal overflow"))?;
    }
    if next_ordinal.saturating_sub(1) != header.registered_descriptor_count
        || (header.status == "registration_open" && !header.artifact_map.is_empty())
        || (matches!(header.status.as_str(), "staging" | "ready")
            && header.registered_descriptor_count != header.expected_descriptor_count)
    {
        return Err(anyhow::anyhow!(
            "Database returned inconsistent staged artifact registration evidence"
        ));
    }
    Ok(())
}

fn validate_closure_artifact_map(
    header: &ScopeClosureArtifactWriteSetHeader,
    artifacts: &[PreparedArtifact],
) -> anyhow::Result<()> {
    let expected_keys = artifacts
        .iter()
        .map(|artifact| artifact.descriptor.file_name.as_str())
        .collect::<BTreeSet<_>>();
    if header.artifact_map.len() != artifacts.len()
        || header
            .artifact_map
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>()
            != expected_keys
        || header.artifact_map.values().any(Uuid::is_nil)
        || header
            .artifact_map
            .values()
            .copied()
            .collect::<BTreeSet<_>>()
            .len()
            != artifacts.len()
    {
        return Err(anyhow::anyhow!(
            "Database returned an inconsistent closure artifact map"
        ));
    }
    Ok(())
}

async fn supervise_cancellable_operation<T, Operation, Heartbeat, HeartbeatFuture>(
    operation: Operation,
    cancellation: CancellationToken,
    heartbeat_period: Duration,
    mut heartbeat: Heartbeat,
) -> anyhow::Result<T>
where
    Operation: Future<Output = anyhow::Result<T>>,
    Heartbeat: FnMut() -> HeartbeatFuture,
    HeartbeatFuture: Future<Output = anyhow::Result<()>>,
{
    heartbeat().await?;
    tokio::pin!(operation);
    let mut interval = tokio::time::interval(heartbeat_period.max(Duration::from_millis(1)));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    interval.tick().await;
    loop {
        tokio::select! {
            result = &mut operation => return result,
            _ = interval.tick() => {
                if let Err(error) = heartbeat().await {
                    cancellation.cancel();
                    let _ = (&mut operation).await;
                    return Err(error.context(
                        "operation cancelled after lease heartbeat failure",
                    ));
                }
            }
        }
    }
}

async fn heartbeat_closure_artifact_upload(
    progress: Option<&WorkerJobProgress<'_>>,
    closure_check_id: Uuid,
    index: usize,
    total: usize,
) -> anyhow::Result<()> {
    let Some(progress) = progress else {
        return Ok(());
    };
    progress
        .heartbeat(
            "upload_closure_artifacts",
            0.82 + 0.02 * bounded_progress_ratio(index, total),
            Some(json!({
                "closureCheckId": closure_check_id,
                "progressCounters": {
                    "scanned": index,
                    "total": total,
                    "unit": "artifacts"
                },
            })),
        )
        .await
        .map_err(|error| error.context("closure artifact upload lease heartbeat failed"))
}

async fn heartbeat_closure_artifact_registration(
    progress: &WorkerJobProgress<'_>,
    closure_check_id: Uuid,
    registered_records: u64,
    total_records: u64,
    registered_bytes: u64,
) -> anyhow::Result<()> {
    progress
        .heartbeat(
            "register_closure_artifacts",
            0.80 + 0.02
                * bounded_progress_ratio(
                    usize::try_from(registered_records).unwrap_or(usize::MAX),
                    usize::try_from(total_records).unwrap_or(usize::MAX),
                ),
            Some(json!({
                "closureCheckId": closure_check_id,
                "progressCounters": {
                    "records": registered_records,
                    "bytes": registered_bytes,
                    "partitions": registered_records,
                    "totalRecords": total_records,
                    "unit": "artifactDescriptors"
                },
            })),
        )
        .await
        .map_err(|error| error.context("closure artifact registration lease heartbeat failed"))
}

async fn fence_closure_artifact_finalize<
    Heartbeat,
    HeartbeatFuture,
    Finalize,
    FinalizeFuture,
    Finalized,
>(
    heartbeat: Heartbeat,
    finalize: Finalize,
) -> anyhow::Result<Finalized>
where
    Heartbeat: FnOnce() -> HeartbeatFuture,
    HeartbeatFuture: Future<Output = anyhow::Result<()>>,
    Finalize: FnOnce() -> FinalizeFuture,
    FinalizeFuture: Future<Output = anyhow::Result<Finalized>>,
{
    heartbeat()
        .await
        .context("closure artifact pre-commit lease fence failed")?;
    finalize()
        .await
        .context("failed to atomically finalize closure artifact write set")
}

async fn cleanup_uploaded_artifacts(state: &AppState, object_keys: &[String]) {
    for object_key in object_keys {
        if let Err(error) = state.object_store.delete_object_key(object_key).await {
            tracing::warn!(
                object_key,
                error = %error,
                "failed to compensate closure artifact upload"
            );
        }
    }
}

async fn report_artifact_manifest_hash(pool: &PgPool, artifact_id: Uuid) -> anyhow::Result<String> {
    let row = sqlx::query(
        r"
        SELECT public.lcia_scope_closure_sha256(jsonb_build_object(
            'artifactId', id,
            'bucket', storage_bucket,
            'objectPath', storage_path,
            'mediaType', content_type,
            'byteSize', byte_size,
            'checksumSha256', checksum_sha256
        )) AS manifest_hash
        FROM public.worker_job_artifacts
        WHERE id = $1
        ",
    )
    .bind(artifact_id)
    .fetch_one(pool)
    .await?;
    Ok(row.try_get("manifest_hash")?)
}

#[allow(clippy::too_many_lines)]
async fn run_tidas_batch_validation_cached(
    pool: &PgPool,
    worker_job_id: Uuid,
    documents: &ClosureDocumentSpool,
) -> anyhow::Result<TidasBatchValidation> {
    let handshake = tokio::task::spawn_blocking(tidas_cli::handshake).await??;
    let describe = handshake.validation_describe;
    let mut aggregate = JsonlValueSpoolWriter::new("validation-issues-unsorted.jsonl")?;
    let mut cache_hit_count = 0_usize;
    let mut validated_count = 0_usize;

    for document_chunk in documents
        .records()
        .chunks(VALIDATION_CACHE_LOOKUP_BATCH_SIZE)
    {
        enforce_scope_closure_memory_budget("validation_cache_lookup")?;
        let records_for_keys = document_chunk.to_vec();
        let describe_for_keys = describe.clone();
        let cache_keys = tokio::task::spawn_blocking(move || {
            records_for_keys
                .iter()
                .map(|record| document_validation_cache_key(record, &describe_for_keys))
                .collect::<anyhow::Result<Vec<_>>>()
        })
        .await??;
        let cached = lookup_document_validation_evidence(pool, &cache_keys).await?;
        let cached_by_key = cached
            .into_iter()
            .map(|item| (document_evidence_key(&item), item))
            .collect::<BTreeMap<_, _>>();
        let mut missing = Vec::new();

        for (record, key) in document_chunk.iter().zip(&cache_keys) {
            if let Some(hit) = cached_by_key.get(&document_evidence_key(key)) {
                let issue_artifact_ref = hit.get("issueArtifactRef").ok_or_else(|| {
                    anyhow::anyhow!("cached TIDAS evidence omitted issueArtifactRef")
                })?;
                let expected_artifact_hash = hit
                    .get("issueArtifactHash")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                    anyhow::anyhow!("cached TIDAS evidence omitted issueArtifactHash")
                })?;
                if canonical_json_sha256(issue_artifact_ref)? != expected_artifact_hash {
                    return Err(anyhow::anyhow!(
                        "cached TIDAS evidence issue artifact hash mismatch"
                    ));
                }
                for event in issue_artifact_ref
                    .get("issues")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    aggregate.append(event)?;
                }
                cache_hit_count += 1;
            } else {
                missing.push(record.clone());
            }
        }

        for validation_batch in missing.chunks(VALIDATION_EXECUTION_BATCH_SIZE) {
            enforce_scope_closure_memory_budget("tidas_validation_batch")?;
            let owned_records = validation_batch.to_vec();
            let document_spool_path = documents.path.clone();
            let describe_for_validation = describe.clone();
            let uncached = tokio::task::spawn_blocking(move || {
                let owned_batch =
                    ClosureDocumentSpool::load_batch(&document_spool_path, &owned_records)?;
                run_tidas_batch_validation(&owned_batch, describe_for_validation)
            })
            .await??;
            validated_count += validation_batch.len();

            let mut issues_by_document = BTreeMap::<String, Vec<Value>>::new();
            uncached.issue_events.visit(|event| {
                let document_key = event
                    .get("document_key")
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow::anyhow!("TIDAS issue event omitted document_key"))?
                    .to_owned();
                aggregate.append(&event)?;
                issues_by_document
                    .entry(document_key)
                    .or_default()
                    .push(event);
                Ok(())
            })?;

            let mut records = Vec::new();
            let mut record_bytes = 0_usize;
            for record in validation_batch {
                let issues = issues_by_document
                    .remove(&record.identity.document_key())
                    .unwrap_or_default();
                let record =
                    build_document_validation_evidence(record, &describe, issues.as_slice())?;
                let encoded_bytes = canonical_json_bytes(&record)?.len();
                if !records.is_empty()
                    && record_bytes.saturating_add(encoded_bytes)
                        > VALIDATION_CACHE_RECORD_BATCH_BYTES
                {
                    record_document_validation_evidence(pool, worker_job_id, records.as_slice())
                        .await?;
                    records.clear();
                    record_bytes = 0;
                }
                if encoded_bytes > VALIDATION_CACHE_RECORD_BATCH_BYTES {
                    return Err(anyhow::anyhow!(
                        "validation cache record exceeds the {VALIDATION_CACHE_RECORD_BATCH_BYTES} byte memory budget"
                    ));
                }
                record_bytes = record_bytes.saturating_add(encoded_bytes);
                records.push(record);
            }
            if !records.is_empty() {
                record_document_validation_evidence(pool, worker_job_id, &records).await?;
            }
            enforce_scope_closure_memory_budget("validation_cache_record")?;
        }
    }

    let unsorted_issue_events = aggregate.finish()?;
    let issue_events = sort_jsonl_spool(&unsorted_issue_events)?;
    tracing::info!(
        phase = "scope_closure_tidas_validation",
        document_count = documents.len(),
        cache_hit_count,
        validated_count,
        issue_event_count = issue_events.event_count,
        issue_spool_bytes = issue_events.byte_size,
        issue_spool_sha256 = issue_events.sha256,
        resident_bytes = scope_closure_resident_bytes().unwrap_or(0),
        "scope closure TIDAS validation completed"
    );
    let final_event = json!({
        "type": "final",
        "schema_version": "tidas.validation-final-event.v1",
        "protocol": TIDAS_BATCH_PROTOCOL,
        "profile": TIDAS_BATCH_PROFILE,
        "completed": true,
        "logical_issue_stream_sha256": issue_events.sha256.as_str(),
        "summary": {
            "document_count": documents.len(),
            "issue_count": issue_events.event_count,
            "cache_hit_count": cache_hit_count,
            "validated_count": validated_count,
        },
        "fingerprints": describe,
    });
    Ok(TidasBatchValidation {
        describe,
        final_event,
        issue_events,
    })
}

fn run_tidas_batch_validation(
    documents: &[ClosureDocument],
    describe: Value,
) -> anyhow::Result<TidasBatchValidation> {
    if documents.is_empty() {
        let issue_events = JsonlValueSpoolWriter::new("validation-issues.jsonl")?.finish()?;
        return Ok(TidasBatchValidation {
            describe,
            final_event: json!({
                "type": "final",
                "completed": true,
                "summary": {"document_count": 0, "issue_count": 0},
            }),
            issue_events,
        });
    }
    let (temp, input_dir, manifest_path) = spool_tidas_batch_documents(documents)?;
    let input_dir_arg = input_dir.to_string_lossy().into_owned();
    let manifest_arg = manifest_path.to_string_lossy().into_owned();
    let events_path = temp.path().join("events.jsonl");
    let events_arg = events_path.to_string_lossy().into_owned();
    let output = tidas_cli::run_json(&[
        "validate",
        input_dir_arg.as_str(),
        "--protocol",
        TIDAS_BATCH_PROTOCOL,
        "--input-manifest",
        manifest_arg.as_str(),
        "--events",
        events_arg.as_str(),
        "--format",
        "json",
        "--progress",
        "never",
    ])?;
    if output.report.get("command").and_then(Value::as_str) != Some("validate")
        || output.report.get("status").and_then(Value::as_str) != Some("succeeded")
        || output.report.get("completeness").and_then(Value::as_str) != Some("complete")
    {
        return Err(anyhow::anyhow!(
            "tidas_report_invalid: document batch did not return a complete successful validate report"
        ));
    }
    let artifact = output
        .report
        .get("artifacts")
        .and_then(Value::as_array)
        .and_then(|artifacts| {
            artifacts.iter().find(|artifact| {
                artifact.get("media_type").and_then(Value::as_str) == Some("application/x-ndjson")
            })
        })
        .ok_or_else(|| {
            anyhow::anyhow!("tidas_report_invalid: document batch omitted event spool artifact")
        })?;
    tidas_cli::verify_artifact(&events_path, artifact)?;
    let mut issue_events = JsonlValueSpoolWriter::new("validation-issues.jsonl")?;
    let mut final_event = None;
    let mut final_count = 0_u64;
    let mut observed_after_final = false;
    tidas_cli::visit_jsonl_raw(&events_path, |event, raw_line| {
        if final_event.is_some() {
            observed_after_final = true;
        }
        if event.get("type").and_then(Value::as_str) == Some("final") {
            final_count += 1;
            final_event = Some(event);
        } else if event.get("type").and_then(Value::as_str) == Some("issue") {
            issue_events.append_raw_jsonl_line(raw_line)?;
        }
        Ok(())
    })?;
    if final_count != 1 || observed_after_final {
        return Err(anyhow::anyhow!(
            "TIDAS batch validator must emit exactly one terminal final event"
        ));
    }
    let final_event = final_event
        .ok_or_else(|| anyhow::anyhow!("TIDAS batch validator omitted terminal final event"))?;
    if output.report.pointer("/summary/validation_batch_final") != Some(&final_event) {
        return Err(anyhow::anyhow!(
            "tidas_spool_final_mismatch: operation report final event differs from event spool"
        ));
    }
    if final_event.get("completed").and_then(Value::as_bool) != Some(true)
        || final_event.get("protocol").and_then(Value::as_str) != Some(TIDAS_BATCH_PROTOCOL)
        || final_event.get("profile").and_then(Value::as_str) != Some(TIDAS_BATCH_PROFILE)
    {
        return Err(anyhow::anyhow!(
            "TIDAS batch validator final event does not match the requested protocol/profile"
        ));
    }
    let issue_events = issue_events.finish()?;
    validate_tidas_final_event(
        &final_event,
        issue_events.event_count,
        issue_events.sha256.as_str(),
        documents.len(),
    )?;
    Ok(TidasBatchValidation {
        describe,
        final_event,
        issue_events,
    })
}

fn spool_tidas_batch_documents(
    documents: &[ClosureDocument],
) -> anyhow::Result<(TempDir, PathBuf, PathBuf)> {
    let temp = TempDir::new()?;
    let input_dir = temp.path().join("documents");
    fs::create_dir(&input_dir)?;
    let manifest_path = temp.path().join("manifest.jsonl");
    let mut manifest = Vec::new();
    for (index, document) in documents.iter().enumerate() {
        let file_name = format!("{index:08}.json");
        let document_bytes = canonical_json_bytes(&document.payload)?;
        fs::write(input_dir.join(&file_name), &document_bytes)?;
        manifest.extend(canonical_json_bytes(&json!({
            "document_key": document.identity.document_key(),
            "category": document.identity.category.table_name(),
            "relative_path": file_name,
            "content_sha256": sha256_hex(&document_bytes),
            "identity": {
                "dataset_type": document.identity.category.table_name(),
                "dataset_id": document.identity.id,
                "dataset_version": document.identity.version,
            },
        }))?);
        manifest.push(b'\n');
    }
    fs::write(&manifest_path, manifest)?;
    Ok((temp, input_dir, manifest_path))
}

fn validate_tidas_final_event(
    final_event: &Value,
    issue_event_count: u64,
    logical_issue_stream_sha256: &str,
    document_count: usize,
) -> anyhow::Result<()> {
    let reported_documents = final_event
        .pointer("/summary/document_count")
        .and_then(Value::as_u64)
        .and_then(|count| usize::try_from(count).ok());
    let reported_issues = final_event
        .pointer("/summary/issue_count")
        .and_then(Value::as_u64);
    if reported_documents != Some(document_count) || reported_issues != Some(issue_event_count) {
        return Err(anyhow::anyhow!(
            "TIDAS batch validator final summary does not match the observed stream"
        ));
    }
    if final_event
        .get("logical_issue_stream_sha256")
        .and_then(Value::as_str)
        != Some(logical_issue_stream_sha256)
    {
        return Err(anyhow::anyhow!(
            "TIDAS batch validator issue stream byte hash mismatch"
        ));
    }
    Ok(())
}

fn document_validation_cache_key(
    document: &ClosureDocumentRecord,
    describe: &Value,
) -> anyhow::Result<Value> {
    let package_version = describe
        .pointer("/package/version")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("TIDAS validator describe omitted package version"))?;
    let engines = describe
        .get("engines")
        .ok_or_else(|| anyhow::anyhow!("tidas_handshake_invalid: describe omitted engines"))?;
    let ruleset_catalog = describe.get("ruleset_catalog").ok_or_else(|| {
        anyhow::anyhow!("tidas_handshake_invalid: describe omitted ruleset catalog")
    })?;
    let asset_fingerprint = describe
        .get("asset_fingerprint")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            anyhow::anyhow!("tidas_handshake_invalid: describe omitted asset fingerprint")
        })?;
    if !describe
        .get("report_schema_versions")
        .and_then(Value::as_array)
        .is_some_and(|schemas| {
            schemas
                .iter()
                .any(|schema| schema.as_str() == Some("tidas.validation-report.v1"))
        })
    {
        return Err(anyhow::anyhow!(
            "tidas_protocol_mismatch: describe omitted tidas.validation-report.v1"
        ));
    }
    Ok(json!({
        "datasetType": document.identity.category.table_name(),
        "datasetId": document.identity.id,
        "datasetVersion": document.identity.version,
        "canonicalContentHash": document.canonical_content_hash,
        "documentValidatorVersion": package_version,
        "documentValidationProfile": TIDAS_BATCH_PROFILE,
        "validationReportSchemaVersion": "tidas.validation-report.v1",
        "validatorEngineFingerprint": canonical_json_sha256(&json!({
            "engines": engines,
            "rulesetCatalog": ruleset_catalog,
        }))?,
        // The v0.1 Rust CLI publishes one fingerprint over all bundled schemas,
        // indexes, methodologies, rulesets, XSD, and XSLT assets.
        "tidasSchemaLockSha256": asset_fingerprint,
    }))
}

fn build_document_validation_evidence(
    document: &ClosureDocumentRecord,
    describe: &Value,
    issues: &[Value],
) -> anyhow::Result<Value> {
    let mut record = document_validation_cache_key(document, describe)?;
    let Value::Object(record) = &mut record else {
        unreachable!("cache key is an object")
    };
    record.insert(
        "status".to_owned(),
        Value::String(
            if issues.is_empty() {
                "passed"
            } else {
                "failed"
            }
            .to_owned(),
        ),
    );
    record.insert(
        "summary".to_owned(),
        json!({"issueCount": issues.len(), "completed": true}),
    );
    record.insert("issueArtifactRef".to_owned(), json!({"issues": issues}));
    record.insert(
        "issueArtifactHash".to_owned(),
        Value::String(canonical_json_sha256(
            record
                .get("issueArtifactRef")
                .expect("issueArtifactRef was inserted"),
        )?),
    );
    Ok(Value::Object(record.clone()))
}

fn document_evidence_key(value: &Value) -> String {
    [
        "datasetType",
        "datasetId",
        "datasetVersion",
        "canonicalContentHash",
        "documentValidatorVersion",
        "documentValidationProfile",
        "validationReportSchemaVersion",
        "validatorEngineFingerprint",
        "tidasSchemaLockSha256",
    ]
    .iter()
    .map(|key| value.get(key).map(Value::to_string).unwrap_or_default())
    .collect::<Vec<_>>()
    .join("|")
}

async fn lookup_document_validation_evidence(
    pool: &PgPool,
    keys: &[Value],
) -> anyhow::Result<Vec<Value>> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let row = sqlx::query(
        r"
        WITH _service_role AS (
            SELECT set_config('request.jwt.claim.role', 'service_role', true)
        )
        SELECT public.svc_lcia_document_validation_evidence_lookup($1::jsonb) AS result
        FROM _service_role
        ",
    )
    .bind(serde_json::to_value(keys)?)
    .fetch_one(pool)
    .await?;
    let result = row.try_get::<Value, _>("result")?;
    ensure_rpc_ok(&result, "svc_lcia_document_validation_evidence_lookup")?;
    Ok(result
        .get("data")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default())
}

async fn record_document_validation_evidence(
    pool: &PgPool,
    worker_job_id: Uuid,
    records: &[Value],
) -> anyhow::Result<()> {
    let row = sqlx::query(
        r"
        WITH _service_role AS (
            SELECT set_config('request.jwt.claim.role', 'service_role', true)
        )
        SELECT public.svc_lcia_document_validation_evidence_record($1::jsonb, $2) AS result
        FROM _service_role
        ",
    )
    .bind(serde_json::to_value(records)?)
    .bind(worker_job_id)
    .fetch_one(pool)
    .await?;
    let result = row.try_get::<Value, _>("result")?;
    ensure_rpc_ok(&result, "svc_lcia_document_validation_evidence_record")
}

fn merge_tidas_validation_issues(
    scan: &mut ScopeClosureScan,
    events: &JsonlValueSpool,
) -> anyhow::Result<()> {
    scan.tidas_issue_event_count = events.event_count;
    enforce_scope_closure_memory_budget("stage_tidas_validation_issue_spool")?;
    Ok(())
}

fn tidas_event_issue(
    documents: &ClosureDocumentSpool,
    event: &Value,
) -> anyhow::Result<ClosureIssue> {
    let document_key = event
        .get("document_key")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let source = find_document_identity(documents, document_key);
    let details = event.get("issue").cloned().unwrap_or_else(|| json!({}));
    let issue_code = details
        .get("issue_code")
        .or_else(|| details.get("code"))
        .and_then(Value::as_str)
        .unwrap_or("tidas_document_invalid");
    let location = details
        .get("location")
        .or_else(|| details.get("path"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    let issue_key = canonical_json_sha256(&json!({
        "code": issue_code,
        "source": source,
        "path": location,
        "message": details.get("message"),
    }))?;
    Ok(ClosureIssue {
        issue_key: issue_key.clone(),
        severity: "blocker".to_owned(),
        blocking: true,
        issue_code: format!("tidas_{issue_code}"),
        source: source.clone(),
        json_path: location.clone(),
        reference_role: None,
        requested_target_type: None,
        requested_target_id: None,
        requested_target_version: None,
        message: details
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("TIDAS document validation failed")
            .to_owned(),
        suggested_action: Some(
            "Repair the schema-invalid document and rerun closure preflight.".to_owned(),
        ),
        occurrence_count: 1,
        occurrences: vec![ClosureIssueOccurrence {
            occurrence_key: format!("{issue_key}:0"),
            source,
            json_path: location,
            reference_role: None,
            details,
        }],
        affected_root_count: 0,
        affected_roots: Vec::new(),
        affected_root_witness_paths: Vec::new(),
        witness_path: Vec::new(),
    })
}

fn issue_source_sort_key(source: Option<&ExactDatasetIdentity>) -> anyhow::Result<String> {
    source.map_or_else(|| Ok(String::new()), canonical_json_sha256)
}

fn append_issue_merge_records(
    writer: &mut SortedJsonlRunWriter,
    mut issue: ClosureIssue,
) -> anyhow::Result<()> {
    let source_key = issue_source_sort_key(issue.source.as_ref())?;
    let occurrences = std::mem::take(&mut issue.occurrences);
    issue.occurrence_count = 0;
    if occurrences.is_empty() {
        writer.append(&json!([
            source_key,
            issue.issue_key,
            "",
            issue,
            Value::Null
        ]))?;
        return Ok(());
    }
    for occurrence in occurrences {
        writer.append(&json!([
            source_key,
            issue.issue_key,
            occurrence.occurrence_key,
            issue,
            occurrence
        ]))?;
    }
    Ok(())
}

fn issue_merge_record(
    value: &Value,
) -> anyhow::Result<(String, ClosureIssue, Option<ClosureIssueOccurrence>)> {
    let mut fields = value
        .as_array()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("issue merge record must be an array"))?;
    if fields.len() != 5 {
        return Err(anyhow::anyhow!(
            "issue merge record field count mismatch: {}",
            fields.len()
        ));
    }
    let occurrence = if fields[4].is_null() {
        None
    } else {
        Some(serde_json::from_value(
            fields.pop().expect("field count checked"),
        )?)
    };
    if occurrence.is_none() {
        fields.pop();
    }
    let issue = serde_json::from_value(fields.pop().expect("field count checked"))?;
    let source_key = fields
        .first()
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("issue merge record source key must be text"))?
        .to_owned();
    Ok((source_key, issue, occurrence))
}

struct CoalescedIssueState {
    source_key: String,
    issue: ClosureIssue,
    last_occurrence_key: Option<String>,
}

impl CoalescedIssueState {
    fn new(source_key: String, mut issue: ClosureIssue) -> Self {
        issue.occurrence_count = 0;
        issue.occurrences.clear();
        Self {
            source_key,
            issue,
            last_occurrence_key: None,
        }
    }

    fn push_occurrence(
        &mut self,
        occurrence: Option<ClosureIssueOccurrence>,
        stats: &mut IssueRelationStats,
    ) {
        let Some(occurrence) = occurrence else {
            return;
        };
        if self.last_occurrence_key.as_deref() == Some(occurrence.occurrence_key.as_str()) {
            return;
        }
        self.last_occurrence_key = Some(occurrence.occurrence_key.clone());
        self.issue.occurrence_count = self.issue.occurrence_count.saturating_add(1);
        stats.occurrence_count = stats.occurrence_count.saturating_add(1);
        if self.issue.occurrences.len() < ISSUE_INLINE_OCCURRENCE_SAMPLE_LIMIT {
            self.issue.occurrences.push(occurrence);
        }
    }
}

#[derive(Default)]
struct SourceReachability {
    source_key: Option<String>,
    source_node_ordinal: Option<u32>,
    visited: Vec<bool>,
    parent: Vec<Option<u32>>,
    affected_root_ordinals: Vec<u32>,
    impact_mode: Option<RootImpactMode>,
    encoded_ordinals: Vec<u32>,
}

fn load_source_reachability(
    cache: &mut SourceReachability,
    source_key: &str,
    source: Option<&ExactDatasetIdentity>,
    graph: &CompactReferenceGraph,
    roots: &StableRootOrdinals,
    cancellation: &CancellationToken,
) -> anyhow::Result<bool> {
    if cache.source_key.as_deref() == Some(source_key) {
        return Ok(false);
    }
    cache.source_key = Some(source_key.to_owned());
    cache.source_node_ordinal = None;
    cache.visited.clear();
    cache.parent.clear();
    cache.affected_root_ordinals.clear();
    cache.impact_mode = None;
    cache.encoded_ordinals.clear();
    let Some(source_id) = source.and_then(|source| graph.identity_ids.get(source).copied()) else {
        return Ok(true);
    };
    cache.source_node_ordinal = Some(source_id);
    cache.visited.resize(graph.identities.len(), false);
    cache.parent.resize(graph.identities.len(), None);
    let source_index = usize::try_from(source_id).expect("u32 identity index fits usize");
    cache.visited[source_index] = true;
    let mut queue = VecDeque::from([source_id]);
    let mut visited_nodes = 0_u64;
    while let Some(node) = queue.pop_front() {
        visited_nodes = visited_nodes.saturating_add(1);
        if visited_nodes.is_multiple_of(4_096) {
            cancellation.check("scope_closure_root_impact")?;
        }
        if let Some(predecessors) = graph
            .reverse
            .get(usize::try_from(node).expect("u32 identity index fits usize"))
        {
            for &predecessor in predecessors {
                let index = usize::try_from(predecessor).expect("u32 identity index fits usize");
                if !cache.visited[index] {
                    cache.visited[index] = true;
                    cache.parent[index] = Some(node);
                    queue.push_back(predecessor);
                }
            }
        }
    }
    for (root_ordinal, &root_node_ordinal) in roots.graph_node_ordinals.iter().enumerate() {
        let root_index = usize::try_from(root_node_ordinal).expect("u32 identity index fits usize");
        if cache.visited[root_index] {
            cache
                .affected_root_ordinals
                .push(u32::try_from(root_ordinal)?);
        }
    }
    let (mode, encoded_ordinals) =
        compact_root_impact_encoding(&cache.affected_root_ordinals, roots.roots.len())?;
    cache.impact_mode = Some(mode);
    cache.encoded_ordinals = encoded_ordinals;
    Ok(true)
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn finalize_coalesced_issue(
    mut pending: CoalescedIssueState,
    roots: &StableRootOrdinals,
    graph: &CompactReferenceGraph,
    reachability: &mut SourceReachability,
    canonical_issue_writer: &mut SortedJsonlRunWriter,
    root_impact_writer: &mut RootImpactIndexWriter,
    cancellation: &CancellationToken,
    relation_stats: &mut IssueRelationStats,
) -> anyhow::Result<()> {
    let changed_source = load_source_reachability(
        reachability,
        &pending.source_key,
        pending.issue.source.as_ref(),
        graph,
        roots,
        cancellation,
    )?;
    let root_impact = if pending.issue.source.is_some() {
        let impact_key = format!("source:{}", pending.source_key);
        let mode = reachability
            .impact_mode
            .ok_or_else(|| anyhow::anyhow!("source impact omitted compact encoding"))?;
        if changed_source {
            root_impact_writer.append(
                &impact_key,
                1,
                reachability.source_node_ordinal,
                mode,
                u32::try_from(reachability.affected_root_ordinals.len())?,
                &reachability.encoded_ordinals,
            )?;
        }
        RootImpactReference {
            mode,
            impact_key: Some(impact_key),
            source_node_ordinal: reachability.source_node_ordinal,
            evidence_schema_version: "lcia.scope-closure-root-impact-index.v1".to_owned(),
        }
    } else {
        let expected_count = usize::try_from(pending.issue.affected_root_count)?;
        let included_ordinals = if expected_count == roots.roots.len() {
            (0..u32::try_from(roots.roots.len())?).collect::<Vec<_>>()
        } else {
            if expected_count != pending.issue.affected_roots.len() {
                return Err(anyhow::anyhow!(
                    "source-less issue {} has incomplete affected-root evidence: count={}, samples={}",
                    pending.issue.issue_key,
                    expected_count,
                    pending.issue.affected_roots.len()
                ));
            }
            let mut ordinals = pending
                .issue
                .affected_roots
                .iter()
                .map(|root| {
                    roots.ordinal_by_identity.get(root).copied().ok_or_else(|| {
                        anyhow::anyhow!(
                            "source-less issue {} references an unknown root",
                            pending.issue.issue_key
                        )
                    })
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            ordinals.sort_unstable();
            ordinals.dedup();
            if ordinals.len() != expected_count {
                return Err(anyhow::anyhow!(
                    "source-less issue {} affected-root identities are not unique",
                    pending.issue.issue_key
                ));
            }
            ordinals
        };
        let (mode, encoded_ordinals) =
            compact_root_impact_encoding(&included_ordinals, roots.roots.len())?;
        let impact_key = if matches!(
            mode,
            RootImpactMode::IncludedOrdinals | RootImpactMode::ExcludedOrdinals
        ) {
            let impact_key = format!("issue:{}", pending.issue.issue_key);
            root_impact_writer.append(
                &impact_key,
                2,
                None,
                mode,
                u32::try_from(included_ordinals.len())?,
                &encoded_ordinals,
            )?;
            Some(impact_key)
        } else {
            None
        };
        RootImpactReference {
            mode,
            impact_key,
            source_node_ordinal: None,
            evidence_schema_version: "lcia.scope-closure-root-impact-index.v1".to_owned(),
        }
    };

    if !reachability.visited.is_empty() {
        pending.issue.affected_root_count =
            u32::try_from(reachability.affected_root_ordinals.len())?;
        pending.issue.affected_roots.clear();
        pending.issue.affected_root_witness_paths.clear();
        pending.issue.witness_path.clear();
        for &root_ordinal in reachability
            .affected_root_ordinals
            .iter()
            .take(ISSUE_INLINE_AFFECTED_ROOT_SAMPLE_LIMIT)
        {
            let root_index = usize::try_from(root_ordinal)?;
            let root = roots
                .roots
                .get(root_index)
                .ok_or_else(|| anyhow::anyhow!("root ordinal is out of bounds"))?;
            let root_node_ordinal = roots.graph_node_ordinals[root_index];
            let witness = reconstruct_witness_path(
                root_node_ordinal,
                &reachability.parent,
                &graph.identities,
            );
            pending.issue.affected_roots.push(root.clone());
            pending
                .issue
                .affected_root_witness_paths
                .push(witness.clone());
            if pending.issue.witness_path.is_empty() {
                pending.issue.witness_path = witness;
            }
        }
    } else if pending.issue.source.is_none()
        && usize::try_from(pending.issue.affected_root_count).ok() == Some(roots.roots.len())
    {
        pending.issue.affected_roots.clear();
        pending.issue.affected_root_witness_paths.clear();
        for root in roots
            .roots
            .iter()
            .take(ISSUE_INLINE_AFFECTED_ROOT_SAMPLE_LIMIT)
        {
            pending.issue.affected_roots.push(root.clone());
            pending
                .issue
                .affected_root_witness_paths
                .push(vec![root.clone()]);
        }
    }
    relation_stats.affected_root_count = relation_stats
        .affected_root_count
        .checked_add(u64::from(pending.issue.affected_root_count))
        .ok_or_else(|| anyhow::anyhow!("logical affected-root count overflow"))?;

    relation_stats.issue_count = relation_stats.issue_count.saturating_add(1);
    if pending.issue.blocking {
        relation_stats.blocker_count = relation_stats.blocker_count.saturating_add(1);
        relation_stats
            .blocker_codes
            .insert(pending.issue.issue_code.clone());
    }
    let partition_record = issue_partition_record_v3(&pending.issue, &root_impact, roots)?;
    canonical_issue_writer.append(&json!([
        pending.issue.issue_key,
        pending.issue,
        partition_record,
    ]))?;
    Ok(())
}

#[allow(clippy::too_many_lines)]
#[cfg(test)]
fn build_issue_relation_spools(
    scan: &mut ScopeClosureScan,
    events: &JsonlValueSpool,
) -> anyhow::Result<()> {
    build_issue_relation_spools_with_cancellation(scan, events, &CancellationToken::default())
}

#[allow(clippy::too_many_lines)]
#[cfg(test)]
fn build_issue_relation_spools_with_cancellation(
    scan: &mut ScopeClosureScan,
    events: &JsonlValueSpool,
    cancellation: &CancellationToken,
) -> anyhow::Result<()> {
    build_issue_relation_spools_with_cancellation_and_progress(scan, events, cancellation, None)
}

#[allow(clippy::too_many_lines)]
fn build_issue_relation_spools_with_cancellation_and_progress(
    scan: &mut ScopeClosureScan,
    events: &JsonlValueSpool,
    cancellation: &CancellationToken,
    progress: Option<&ScopeClosureArtifactProgress>,
) -> anyhow::Result<()> {
    cancellation.check("scope_closure_coalesce_start")?;
    if let Some(progress) = progress {
        progress.update(1, 0, events.byte_size, 0);
    }
    let initial_admission_bytes = admit_relation_temp_space(events)?;
    tracing::info!(
        initial_admission_bytes,
        raw_events = events.event_count,
        raw_bytes = events.byte_size,
        root_count = scan.roots.len(),
        admission_strategy = "observed_raw_then_measured_topology_watermarks",
        "scope closure relation temporary space admitted"
    );
    normalize_database_issue_severities(&mut scan.issues)?;
    let mut merge_input = SortedJsonlRunWriter::new("issue-merge-input")?;
    for issue in std::mem::take(&mut scan.issues) {
        append_issue_merge_records(&mut merge_input, issue)?;
    }
    let mut event_count = 0_u64;
    events.visit(|event| {
        event_count = event_count.saturating_add(1);
        if event_count.is_multiple_of(4_096) {
            cancellation.check("scope_closure_merge_input")?;
            enforce_scope_closure_memory_budget("prepare_issue_merge_runs")?;
            if let Some(progress) = progress {
                progress.update(1, event_count, events.byte_size, 0);
            }
        }
        append_issue_merge_records(
            &mut merge_input,
            tidas_event_issue(&scan.documents, &event)?,
        )
    })?;
    let sorted_input = merge_input.finish()?;
    let stable_roots = StableRootOrdinals::new(&scan.roots, &scan.reference_graph)?;
    let mut canonical_issue_writer = SortedJsonlRunWriter::new("canonical-issues-v3")?;
    let mut root_impact_writer = RootImpactIndexWriter::new(stable_roots.roots.len())?;
    let mut stats = IssueRelationStats::default();
    let mut reachability = SourceReachability::default();
    let mut current = None::<CoalescedIssueState>;
    let mut observed = 0_u64;
    if let Some(progress) = progress {
        progress.update(2, 0, sorted_input.byte_size, 0);
    }
    sorted_input.visit(|record| {
        observed = observed.saturating_add(1);
        if observed.is_multiple_of(4_096) {
            cancellation.check("scope_closure_coalesce")?;
            enforce_scope_closure_memory_budget("coalesce_sorted_issue_runs")?;
            if let Some(progress) = progress {
                progress.update(2, observed, sorted_input.byte_size, 0);
            }
        }
        let (source_key, issue, occurrence) = issue_merge_record(&record)?;
        let starts_new_issue = current
            .as_ref()
            .is_some_and(|state| state.issue.issue_key != issue.issue_key);
        if starts_new_issue {
            finalize_coalesced_issue(
                current.take().expect("current issue exists"),
                &stable_roots,
                &scan.reference_graph,
                &mut reachability,
                &mut canonical_issue_writer,
                &mut root_impact_writer,
                cancellation,
                &mut stats,
            )?;
        }
        let pending = current.get_or_insert_with(|| CoalescedIssueState::new(source_key, issue));
        pending.push_occurrence(occurrence, &mut stats);
        Ok(())
    })?;
    if let Some(state) = current {
        finalize_coalesced_issue(
            state,
            &stable_roots,
            &scan.reference_graph,
            &mut reachability,
            &mut canonical_issue_writer,
            &mut root_impact_writer,
            cancellation,
            &mut stats,
        )?;
    }
    let canonical_issues = canonical_issue_writer.finish()?;
    let partition_temp = Arc::new(TempDir::new()?);
    let mut issue_partition_writer =
        IssuePartitionAccumulator::new(Arc::clone(&partition_temp), "issues");
    let mut issue_spool_writer = JsonlValueSpoolWriter::new_derived("coalesced-issues-v3.jsonl")?;
    let mut partitioned = 0_u64;
    if let Some(progress) = progress {
        progress.update(3, 0, 0, 0);
    }
    canonical_issues.visit(|record| {
        partitioned = partitioned.saturating_add(1);
        if partitioned.is_multiple_of(1_024) {
            cancellation.check("scope_closure_partition_write")?;
            enforce_scope_closure_memory_budget("write_canonical_issue_partitions")?;
            if let Some(progress) = progress {
                progress.update(
                    3,
                    partitioned,
                    issue_spool_writer.byte_size,
                    u64::try_from(
                        issue_partition_writer.entries.len()
                            + usize::from(issue_partition_writer.active.is_some()),
                    )?,
                );
            }
        }
        let fields = record
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("canonical issue record must be an array"))?;
        let issue_key = fields
            .first()
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("canonical issue record omitted key"))?;
        let issue = fields
            .get(1)
            .ok_or_else(|| anyhow::anyhow!("canonical issue record omitted issue"))?;
        let partition_record = fields
            .get(2)
            .ok_or_else(|| anyhow::anyhow!("canonical issue record omitted partition payload"))?;
        issue_spool_writer.append(issue)?;
        issue_partition_writer.push(issue_key, partition_record)
    })?;
    let issues = issue_spool_writer.finish()?;
    let (issue_partition_entries, issue_partition_artifacts, issue_relation_sha256) =
        issue_partition_writer.finish()?;
    let root_impact_index = root_impact_writer.finish()?;
    if issues.event_count != stats.issue_count
        || issue_partition_entries
            .iter()
            .map(|entry| entry.record_count)
            .sum::<u64>()
            != stats.issue_count
    {
        return Err(anyhow::anyhow!(
            "canonical issue counts diverged: spool={}/{}, partitions={}/{}",
            issues.event_count,
            stats.issue_count,
            issue_partition_entries
                .iter()
                .map(|entry| entry.record_count)
                .sum::<u64>(),
            stats.issue_count,
        ));
    }
    let issue_partition_bytes = issue_partition_artifacts
        .iter()
        .fold(0_u64, |total, artifact| {
            total.saturating_add(u64::try_from(artifact.descriptor.byte_size).unwrap_or(u64::MAX))
        });
    let relation_bytes = issues
        .byte_size
        .saturating_add(issue_partition_bytes)
        .saturating_add(root_impact_index.compressed_byte_size);
    tracing::info!(
        logical_issue_bytes = issues.byte_size,
        canonical_issue_partition_bytes = issue_partition_bytes,
        root_impact_index_bytes = root_impact_index.compressed_byte_size,
        root_impact_record_count = root_impact_index.record_count,
        expanded_affected_root_records = 0,
        "scope closure issue-oriented v3 artifacts completed"
    );
    record_scope_closure_resources(
        "build_issue_relation_runs_complete",
        Some(relation_bytes),
        Some(stats.issue_count),
    );
    if let Some(progress) = progress {
        progress.update(
            3,
            stats.issue_count,
            relation_bytes,
            u64::try_from(issue_partition_entries.len())?.saturating_add(1),
        );
    }
    let relations = IssueRelationSpools {
        issues,
        issue_partition_entries,
        issue_partition_artifacts,
        issue_relation_sha256,
        root_impact_index,
        stats,
    };
    relations.issues.visit(|record| {
        if scan.issues.len() < ISSUE_INLINE_ISSUE_SAMPLE_LIMIT {
            scan.issues.push(serde_json::from_value(record)?);
        }
        Ok(())
    })?;
    scan.issue_relations = Some(relations);
    enforce_scope_closure_memory_budget("merge_tidas_validation_issues_complete")?;
    Ok(())
}

fn find_document_identity(
    documents: &ClosureDocumentSpool,
    document_key: &str,
) -> Option<ExactDatasetIdentity> {
    let mut parts = document_key.splitn(3, ':');
    let category = parse_category(parts.next()?).ok()?;
    let id = Uuid::parse_str(parts.next()?).ok()?;
    let version = parts.next()?;
    let identity = ExactDatasetIdentity {
        category,
        id,
        version: version.to_owned(),
    };
    documents
        .records()
        .binary_search_by(|record| record.identity.cmp(&identity))
        .ok()
        .map(|index| documents.records()[index].identity.clone())
}

#[allow(clippy::too_many_lines)]
#[cfg(test)]
fn build_xlsx_report(closure_check_id: Uuid, issues: &[ClosureIssue]) -> anyhow::Result<Vec<u8>> {
    let cursor = std::io::Cursor::new(Vec::new());
    Ok(write_xlsx_report(cursor, closure_check_id, issues, None)?.into_inner())
}

fn build_xlsx_report_file(
    path: &Path,
    closure_check_id: Uuid,
    issues: &[ClosureIssue],
) -> anyhow::Result<()> {
    write_xlsx_report(File::create(path)?, closure_check_id, issues, None)?;
    Ok(())
}

fn build_scan_xlsx_report_file(
    path: &Path,
    closure_check_id: Uuid,
    scan: &ScopeClosureScan,
) -> anyhow::Result<()> {
    let stats = scan
        .issue_relations
        .as_ref()
        .map(|relations| &relations.stats);
    write_xlsx_report(File::create(path)?, closure_check_id, &scan.issues, stats)?;
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn write_xlsx_report<W: Write + Seek>(
    mut writer: W,
    closure_check_id: Uuid,
    issues: &[ClosureIssue],
    stats: Option<&IssueRelationStats>,
) -> anyhow::Result<W> {
    let issue_count = stats.map_or_else(
        || u64::try_from(issues.len()).unwrap_or(u64::MAX),
        |stats| stats.issue_count,
    );
    let blocker_count = stats.map_or_else(
        || u64::try_from(issues.iter().filter(|issue| issue.blocking).count()).unwrap_or(u64::MAX),
        |stats| stats.blocker_count,
    );
    let warning_count = issue_count.saturating_sub(blocker_count);
    let occurrence_count = stats.map_or_else(
        || {
            issues
                .iter()
                .map(|issue| u64::from(issue.occurrence_count))
                .sum::<u64>()
        },
        |stats| stats.occurrence_count,
    );
    let affected_root_count = stats.map_or_else(
        || {
            issues
                .iter()
                .map(|issue| u64::from(issue.affected_root_count))
                .sum::<u64>()
        },
        |stats| stats.affected_root_count,
    );
    let summary_rows = vec![
        vec!["Metric".to_owned(), "Value".to_owned()],
        vec!["Closure check ID".to_owned(), closure_check_id.to_string()],
        vec!["Issue count".to_owned(), issue_count.to_string()],
        vec!["Blocker count".to_owned(), blocker_count.to_string()],
        vec!["Warning count".to_owned(), warning_count.to_string()],
        vec!["Occurrence count".to_owned(), occurrence_count.to_string()],
        vec![
            "Affected root relation count".to_owned(),
            affected_root_count.to_string(),
        ],
        vec![
            "Complete machine-readable detail".to_owned(),
            "See manifest.json and NDJSON+zstd partitions".to_owned(),
        ],
        vec![
            "Issue sample limit".to_owned(),
            XLSX_ISSUE_SAMPLE_LIMIT.to_string(),
        ],
        vec![
            "Occurrence sample limit".to_owned(),
            XLSX_OCCURRENCE_SAMPLE_LIMIT.to_string(),
        ],
        vec![
            "Affected root sample limit".to_owned(),
            XLSX_AFFECTED_ROOT_SAMPLE_LIMIT.to_string(),
        ],
    ];

    let headers = [
        "Issue key",
        "Issue code",
        "Severity",
        "Message",
        "Source type",
        "Source id",
        "Source version",
        "JSON path",
        "Reference role",
        "Target type",
        "Target id",
        "Target version",
        "Occurrences",
        "Affected roots",
        "Suggested action",
    ];
    let issue_rows = std::iter::once(headers.iter().map(|value| (*value).to_owned()).collect())
        .chain(issues.iter().take(XLSX_ISSUE_SAMPLE_LIMIT).map(|issue| {
            let source = issue.source.as_ref();
            vec![
                issue.issue_key.clone(),
                issue.issue_code.clone(),
                issue.severity.clone(),
                issue.message.clone(),
                source
                    .map(|item| item.category.table_name().to_owned())
                    .unwrap_or_default(),
                source.map(|item| item.id.to_string()).unwrap_or_default(),
                source.map(|item| item.version.clone()).unwrap_or_default(),
                issue.json_path.clone().unwrap_or_default(),
                issue.reference_role.clone().unwrap_or_default(),
                issue.requested_target_type.clone().unwrap_or_default(),
                issue
                    .requested_target_id
                    .map(|id| id.to_string())
                    .unwrap_or_default(),
                issue.requested_target_version.clone().unwrap_or_default(),
                issue.occurrence_count.to_string(),
                issue.affected_root_count.to_string(),
                issue.suggested_action.clone().unwrap_or_default(),
            ]
        }))
        .collect::<Vec<_>>();

    let occurrence_header = vec![
        "Issue key".to_owned(),
        "Occurrence key".to_owned(),
        "Source type".to_owned(),
        "Source id".to_owned(),
        "Source version".to_owned(),
        "JSON path".to_owned(),
        "Reference role".to_owned(),
        "Details".to_owned(),
    ];
    let occurrence_rows = std::iter::once(occurrence_header)
        .chain(
            issues
                .iter()
                .flat_map(|issue| {
                    issue.occurrences.iter().map(move |occurrence| {
                        let source = occurrence.source.as_ref();
                        vec![
                            issue.issue_key.clone(),
                            occurrence.occurrence_key.clone(),
                            source
                                .map(|item| item.category.table_name().to_owned())
                                .unwrap_or_default(),
                            source.map(|item| item.id.to_string()).unwrap_or_default(),
                            source.map(|item| item.version.clone()).unwrap_or_default(),
                            occurrence.json_path.clone().unwrap_or_default(),
                            occurrence.reference_role.clone().unwrap_or_default(),
                            canonical_value(&occurrence.details),
                        ]
                    })
                })
                .take(XLSX_OCCURRENCE_SAMPLE_LIMIT),
        )
        .collect::<Vec<_>>();

    let affected_header = vec![
        "Issue key".to_owned(),
        "Dataset type".to_owned(),
        "Dataset id".to_owned(),
        "Dataset version".to_owned(),
        "Witness path".to_owned(),
    ];
    let affected_rows = std::iter::once(affected_header)
        .chain(
            issues
                .iter()
                .flat_map(|issue| {
                    issue
                        .affected_roots
                        .iter()
                        .enumerate()
                        .map(move |(index, root)| {
                            let witness = issue
                                .affected_root_witness_paths
                                .get(index)
                                .unwrap_or(&issue.witness_path);
                            vec![
                                issue.issue_key.clone(),
                                root.category.table_name().to_owned(),
                                root.id.to_string(),
                                root.version.clone(),
                                canonical_value(witness),
                            ]
                        })
                })
                .take(XLSX_AFFECTED_ROOT_SAMPLE_LIMIT),
        )
        .collect::<Vec<_>>();

    let worksheets = [
        summary_rows.as_slice(),
        issue_rows.as_slice(),
        occurrence_rows.as_slice(),
        affected_rows.as_slice(),
    ];
    preflight_xlsx_worksheets(&worksheets)?;

    let mut zip = ZipWriter::new(writer);
    let options = SimpleFileOptions::default().compression_method(CompressionMethod::Deflated);
    zip.start_file("[Content_Types].xml", options)?;
    zip.write_all(br#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/><Override PartName="/xl/worksheets/sheet2.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/><Override PartName="/xl/worksheets/sheet3.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/><Override PartName="/xl/worksheets/sheet4.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/></Types>"#)?;
    zip.start_file("_rels/.rels", options)?;
    zip.write_all(br#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>"#)?;
    zip.start_file("xl/workbook.xml", options)?;
    zip.write_all(br#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Summary" sheetId="1" r:id="rId1"/><sheet name="Closure Issues" sheetId="2" r:id="rId2"/><sheet name="Occurrences" sheetId="3" r:id="rId3"/><sheet name="Affected Datasets" sheetId="4" r:id="rId4"/></sheets></workbook>"#)?;
    zip.start_file("xl/_rels/workbook.xml.rels", options)?;
    zip.write_all(br#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/><Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet2.xml"/><Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet3.xml"/><Relationship Id="rId4" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet4.xml"/></Relationships>"#)?;
    write_xlsx_worksheet(&mut zip, options, 1, summary_rows)?;
    write_xlsx_worksheet(&mut zip, options, 2, issue_rows)?;
    write_xlsx_worksheet(&mut zip, options, 3, occurrence_rows)?;
    write_xlsx_worksheet(&mut zip, options, 4, affected_rows)?;
    writer = zip.finish()?;
    let archive_bytes = writer.stream_position()?;
    if archive_bytes > XLSX_MAX_ARCHIVE_BYTES {
        return Err(anyhow::anyhow!(
            "artifact_limit_exceeded: xlsx archive bytes {archive_bytes} exceed {XLSX_MAX_ARCHIVE_BYTES}"
        ));
    }
    Ok(writer)
}

fn preflight_xlsx_worksheets(worksheets: &[&[Vec<String>]]) -> anyhow::Result<()> {
    let mut total_bytes = 0_u64;
    for (index, rows) in worksheets.iter().enumerate() {
        if rows.len() > XLSX_MAX_WORKSHEET_ROWS {
            return Err(anyhow::anyhow!(
                "artifact_limit_exceeded: worksheet {} rows {} exceed {}",
                index + 1,
                rows.len(),
                XLSX_MAX_WORKSHEET_ROWS
            ));
        }
        let worksheet_bytes = estimate_xlsx_worksheet_bytes(rows)?;
        if worksheet_bytes > XLSX_MAX_WORKSHEET_UNCOMPRESSED_BYTES {
            return Err(anyhow::anyhow!(
                "artifact_limit_exceeded: worksheet {} bytes {} exceed {}",
                index + 1,
                worksheet_bytes,
                XLSX_MAX_WORKSHEET_UNCOMPRESSED_BYTES
            ));
        }
        total_bytes = total_bytes
            .checked_add(worksheet_bytes)
            .ok_or_else(|| anyhow::anyhow!("xlsx total byte estimate overflow"))?;
    }
    if total_bytes > XLSX_MAX_TOTAL_UNCOMPRESSED_BYTES {
        return Err(anyhow::anyhow!(
            "artifact_limit_exceeded: xlsx total worksheet bytes {total_bytes} exceed {XLSX_MAX_TOTAL_UNCOMPRESSED_BYTES}"
        ));
    }
    Ok(())
}

fn estimate_xlsx_worksheet_bytes(rows: &[Vec<String>]) -> anyhow::Result<u64> {
    let mut bytes = u64::try_from(
        b"<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><worksheet xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\"><sheetData></sheetData></worksheet>".len(),
    )?;
    for (row_index, row) in rows.iter().enumerate() {
        bytes = bytes.saturating_add(u64::try_from(
            format!("<row r=\"{}\"></row>", row_index + 1).len(),
        )?);
        for (column_index, value) in row.iter().enumerate() {
            let reference = format!("{}{}", xlsx_column_name(column_index), row_index + 1);
            bytes = bytes.saturating_add(u64::try_from(
                format!(
                    "<c r=\"{reference}\" t=\"inlineStr\"><is><t>{}</t></is></c>",
                    xml_escape(value)
                )
                .len(),
            )?);
        }
    }
    Ok(bytes)
}

fn write_xlsx_worksheet<W, I>(
    zip: &mut ZipWriter<W>,
    options: SimpleFileOptions,
    sheet_number: usize,
    rows: I,
) -> anyhow::Result<()>
where
    W: Write + Seek,
    I: IntoIterator<Item = Vec<String>>,
{
    zip.start_file(format!("xl/worksheets/sheet{sheet_number}.xml"), options)?;
    zip.write_all(
        b"<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><worksheet xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\"><sheetData>",
    )?;
    for (index, row) in rows.into_iter().enumerate() {
        write_xlsx_row(zip, index + 1, row)?;
    }
    zip.write_all(b"</sheetData></worksheet>")?;
    Ok(())
}

fn write_xlsx_row<W, I>(writer: &mut W, row: usize, values: I) -> anyhow::Result<()>
where
    W: Write,
    I: IntoIterator<Item = String>,
{
    write!(writer, "<row r=\"{row}\">")?;
    for (column, value) in values.into_iter().enumerate() {
        let reference = format!("{}{}", xlsx_column_name(column), row);
        write!(
            writer,
            "<c r=\"{reference}\" t=\"inlineStr\"><is><t>{}</t></is></c>",
            xml_escape(value.as_str())
        )?;
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

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn deterministic_uuid_from_hash(hash: &str) -> anyhow::Result<Uuid> {
    let bytes = hex::decode(hash)?;
    let mut uuid_bytes = [0_u8; 16];
    uuid_bytes.copy_from_slice(
        bytes
            .get(..16)
            .ok_or_else(|| anyhow::anyhow!("closure bundle hash is too short"))?,
    );
    uuid_bytes[6] = (uuid_bytes[6] & 0x0f) | 0x50;
    uuid_bytes[8] = (uuid_bytes[8] & 0x3f) | 0x80;
    Ok(Uuid::from_bytes(uuid_bytes))
}

fn ensure_preallocated_snapshot_identity(expected: Uuid, resolved: Uuid) -> anyhow::Result<()> {
    if expected != resolved {
        return Err(anyhow::anyhow!(
            "scope closure snapshot builder changed the database-preallocated identity: expected={expected} got={resolved}"
        ));
    }
    Ok(())
}

/// Verifies that a package-build payload is bound to reusable frozen evidence.
pub async fn validate_package_closure_binding(
    pool: &PgPool,
    binding: &PackageClosureBinding<'_>,
) -> anyhow::Result<()> {
    let row = sqlx::query(
        r"
        SELECT c.status, c.scan_completeness, c.certificate_status,
               c.certificate_hash, c.effective_scope_hash,
               c.data_snapshot_token, c.source_fingerprint, c.resolution_map_hash,
               c.closure_bundle_hash, c.snapshot_id, c.snapshot_hash,
               c.report_artifact_manifest_hash, c.evidence_hash,
               c.requested_scope_manifest->>'certificateFreshnessPolicy' AS freshness_policy,
               EXISTS (
                 SELECT 1
                 FROM public.lcia_scope_closure_data_snapshots s
                 JOIN public.lca_release_publications p
                   ON p.is_current = true AND p.status = 'current'
                 JOIN public.lca_release_runs r ON r.id = p.release_run_id
                 WHERE s.data_snapshot_token = c.data_snapshot_token
                   AND s.root_manifest->'currentPublicRelease'->>'releaseRunId' = r.id::text
                   AND s.root_manifest->'currentPublicRelease'->>'releaseManifestHash' = r.release_manifest_hash
               ) AS current_release_matches
        FROM public.lcia_scope_closure_checks c
        WHERE c.id = $1
        ",
    )
    .bind(binding.closure_check_id)
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| anyhow::anyhow!("closure_check_not_found"))?;
    let certificate_status = row.try_get::<String, _>("certificate_status")?;
    let status = row.try_get::<String, _>("status")?;
    let scan_completeness = row.try_get::<String, _>("scan_completeness")?;
    let actual_certificate_hash = row.try_get::<Option<String>, _>("certificate_hash")?;
    let actual_scope_hash = row.try_get::<Option<String>, _>("effective_scope_hash")?;
    let actual_snapshot_token = row.try_get::<String, _>("data_snapshot_token")?;
    let freshness_policy = row
        .try_get::<Option<String>, _>("freshness_policy")?
        .unwrap_or_else(|| "frozen-artifact-reusable-v1".to_owned());
    let current_release_matches = row.try_get::<bool, _>("current_release_matches")?;
    let complete_evidence = [
        row.try_get::<Option<String>, _>("source_fingerprint")?,
        row.try_get::<Option<String>, _>("resolution_map_hash")?,
        row.try_get::<Option<String>, _>("closure_bundle_hash")?,
        row.try_get::<Option<Uuid>, _>("snapshot_id")?
            .map(|id| id.to_string()),
        row.try_get::<Option<String>, _>("snapshot_hash")?,
        row.try_get::<Option<String>, _>("report_artifact_manifest_hash")?,
        row.try_get::<Option<String>, _>("evidence_hash")?,
    ]
    .iter()
    .all(Option::is_some);
    if status != "passed"
        || scan_completeness != "complete"
        || certificate_status != "valid"
        || !freshness_policy_accepts_current_release(
            freshness_policy.as_str(),
            current_release_matches,
        )
        || actual_certificate_hash.as_deref() != Some(binding.closure_certificate_hash)
        || actual_scope_hash.as_deref() != Some(binding.effective_scope_hash)
        || actual_snapshot_token != binding.data_snapshot_token
        || row.try_get::<Option<Uuid>, _>("snapshot_id")? != Some(binding.snapshot_id)
        || row
            .try_get::<Option<String>, _>("snapshot_hash")?
            .as_deref()
            != Some(binding.snapshot_hash)
        || row
            .try_get::<Option<String>, _>("closure_bundle_hash")?
            .as_deref()
            != Some(binding.closure_bundle_hash)
        || row
            .try_get::<Option<String>, _>("report_artifact_manifest_hash")?
            .as_deref()
            != Some(binding.report_artifact_manifest_hash)
        || !complete_evidence
    {
        return Err(anyhow::anyhow!("closure_evidence_mismatch"));
    }
    Ok(())
}

fn freshness_policy_accepts_current_release(policy: &str, current_release_matches: bool) -> bool {
    match policy {
        "frozen-artifact-reusable-v1" => true,
        "current-membership-required-v1" => current_release_matches,
        _ => false,
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PackageClosureBinding<'a> {
    pub closure_check_id: Uuid,
    pub closure_certificate_hash: &'a str,
    pub effective_scope_hash: &'a str,
    pub data_snapshot_token: &'a str,
    pub snapshot_id: Uuid,
    pub snapshot_hash: &'a str,
    pub closure_bundle_hash: &'a str,
    pub report_artifact_manifest_hash: &'a str,
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use serde_json::json;
    use zip::ZipArchive;

    use crate::pgbouncer_sqlx::Execute;

    use super::*;

    #[derive(Clone, Default)]
    struct FakeProvider {
        documents: BTreeMap<ExactDatasetIdentity, ClosureDocument>,
        fetches: Arc<Mutex<Vec<Vec<ExactDatasetIdentity>>>>,
        checkpoints: Arc<AtomicUsize>,
        fail_checkpoint: Option<usize>,
        reverse_fetch: bool,
        omitted_resolutions: BTreeMap<(DatasetCategory, Uuid), ExactDatasetIdentity>,
        omitted_calls: Arc<AtomicUsize>,
    }

    impl ScopeClosureProvider for FakeProvider {
        async fn checkpoint(&self, _scanned: usize, _scheduled: usize) -> anyhow::Result<()> {
            let call = self.checkpoints.fetch_add(1, Ordering::SeqCst) + 1;
            if self.fail_checkpoint == Some(call) {
                return Err(anyhow::anyhow!("cancelled"));
            }
            Ok(())
        }

        async fn fetch_exact(
            &self,
            identities: &[ExactDatasetIdentity],
        ) -> anyhow::Result<ProviderFetchResult> {
            assert!(identities.len() <= FETCH_BATCH_SIZE);
            self.fetches.lock().unwrap().push(identities.to_vec());
            let mut output = identities
                .iter()
                .filter_map(|identity| self.documents.get(identity).cloned())
                .collect::<Vec<_>>();
            if self.reverse_fetch {
                output.reverse();
            }
            Ok(ProviderFetchResult {
                documents: output,
                ..ProviderFetchResult::default()
            })
        }

        async fn resolve_omitted_version(
            &self,
            category: DatasetCategory,
            id: Uuid,
            policy: &str,
        ) -> anyhow::Result<OmittedVersionResolution> {
            self.omitted_calls.fetch_add(1, Ordering::SeqCst);
            if policy == "reject" {
                return Ok(OmittedVersionResolution {
                    selected: None,
                    candidates: Vec::new(),
                    policy: policy.to_owned(),
                });
            }
            let selected = self.omitted_resolutions.get(&(category, id)).cloned();
            Ok(OmittedVersionResolution {
                candidates: selected.iter().cloned().collect(),
                selected,
                policy: policy.to_owned(),
            })
        }
    }

    fn id(value: &str) -> Uuid {
        value.parse().unwrap()
    }

    fn test_artifact_write_set_header(status: &str) -> ScopeClosureArtifactWriteSetHeader {
        ScopeClosureArtifactWriteSetHeader {
            write_set_id: id("17717717-0677-4677-8677-177177177177"),
            closure_check_id: id("17717717-0177-4177-8177-177177177177"),
            worker_job_id: id("17717717-0277-4277-8277-177177177177"),
            request_id: id("17717717-0477-4477-8477-177177177177"),
            publication_mode: "fresh".to_owned(),
            reused_from_check_id: None,
            status: status.to_owned(),
            write_token: id("17717717-0777-4777-8777-177177177177"),
            contract_version: "lcia.scope-closure-artifact-write-set.v2".to_owned(),
            expected_descriptor_count: 1,
            registered_descriptor_count: 1,
            registered_batch_count: 1,
            descriptor_set_sha256: "d".repeat(64),
            required_primary_roles: closure_artifact_required_primary_roles(None),
            upload_eligible: status == "staging",
            artifact_map: BTreeMap::new(),
            batches: vec![ScopeClosureArtifactWriteSetBatch {
                batch_id: id("17717717-0877-4877-8877-177177177177"),
                item_count: 1,
                first_ordinal: 1,
                last_ordinal: 1,
            }],
        }
    }

    fn scope_closure_worker_input_json() -> Value {
        json!({
            "closureCheckId": "10101010-1010-4010-8010-101010101010",
            "scanExecutionId": "20202020-2020-4020-8020-202020202020",
            "numericalSnapshotId": "30303030-3030-4030-8030-303030303030",
            "requestedScope": {
                "schemaVersion": "lcia.scope-manifest.v1",
                "coverageMode": "subset",
                "eligibilityPredicateVersion": "published-state-code-100-199:v1",
                "processes": [],
                "lciaMethods": [],
                "versionResolutionPolicy": "reference-version-resolution-v1",
                "legacyOmittedVersionPolicy": "reject",
                "certificateFreshnessPolicy": "frozen-artifact-reusable-v1",
                "linkPolicy": {
                    "linkSemanticsVersion": "signed-flow-balance-v1",
                    "flowIdentityPolicy": "exact-flow-version-reference-unit-v2",
                    "allocationSemanticsVersion": "tidas-reference-allocation-v3",
                    "technosphereBoundaryPolicy": "closed",
                    "providerUniversePolicy": "scope_only"
                }
            },
            "requestedScopeHash": "1".repeat(64),
            "policyFingerprint": "2".repeat(64),
            "dataSnapshotToken": "3".repeat(64),
            "dataSnapshotManifest": {},
            "dataSnapshotManifestHash": "4".repeat(64),
            "publicationEpoch": 1,
            "expectedValidatorScannerFingerprint": "scope-closure-validator-scanner.v1",
            "requestFingerprint": "5".repeat(64)
        })
    }

    fn identity(category: DatasetCategory, value: &str) -> ExactDatasetIdentity {
        ExactDatasetIdentity {
            category,
            id: id(value),
            version: "01.00.000".to_owned(),
        }
    }

    fn manifest(processes: Vec<ExactDatasetIdentity>) -> RequestedScopeManifest {
        RequestedScopeManifest {
            schema_version: "lcia.scope-manifest.v1".to_owned(),
            coverage_mode: "subset".to_owned(),
            eligibility_predicate_version: "published-state-code-100-199:v1".to_owned(),
            processes: processes
                .into_iter()
                .map(|item| RequestedIdentity {
                    id: item.id,
                    version: item.version,
                })
                .collect(),
            lcia_methods: Vec::new(),
            version_resolution_policy: "reference-version-resolution-v1".to_owned(),
            legacy_omitted_version_policy: "reject".to_owned(),
            certificate_freshness_policy: "frozen-artifact-reusable-v1".to_owned(),
            link_policy: ScopeLinkPolicy {
                link_semantics_version: "signed-flow-balance-v1".to_owned(),
                flow_identity_policy: "exact-flow-version-reference-unit-v2".to_owned(),
                allocation_semantics_version: "tidas-reference-allocation-v3".to_owned(),
                technosphere_boundary_policy: "closed".to_owned(),
                provider_universe_policy: "scope_only".to_owned(),
            },
            process_manifest_hash: None,
        }
    }

    fn reference(category: &str, target: Uuid, version: Option<&str>) -> Value {
        let mut value = json!({
            "@type": format!("{category} data set"),
            "@refObjectId": target,
            "@uri": format!("../{category}/{target}.json"),
        });
        if let Some(version) = version {
            value["@version"] = json!(version);
        }
        value
    }

    fn snapshot_entry(identity: &ExactDatasetIdentity, payload: &Value) -> SnapshotDatasetEntry {
        SnapshotDatasetEntry {
            dataset_type: identity.category,
            dataset_id: identity.id,
            dataset_version: identity.version.clone(),
            role: "support".to_owned(),
            source_process_id: None,
            source_process_version: None,
            version_significant_hash: "1".repeat(64),
            semantic_hash: "2".repeat(64),
            canonical_content_hash: canonical_json_sha256(payload).unwrap(),
        }
    }

    #[test]
    fn closure_reads_reviewed_lcia_method_by_artifact_locator() {
        let method = ExactDatasetIdentity {
            category: DatasetCategory::Lciamethods,
            id: id("503699e0-eca9-4089-8bf8-e0f49c93e578"),
            version: "01.01.000".to_owned(),
        };
        assert_eq!(
            lcia_method_artifact_locator_id(&method),
            id("9ec743ea-6b00-400d-a53b-61547a3fc03c")
        );
    }

    #[test]
    fn reference_extraction_matches_tidas_tools_golden_fixture() {
        let fixture: Value = serde_json::from_str(include_str!(
            "../tests/fixtures/reference_extraction_v1/golden.json"
        ))
        .unwrap();
        for case in fixture["cases"].as_array().unwrap() {
            let category = parse_category(case["category"].as_str().unwrap()).unwrap();
            let result = extract_references(
                case["document_key"].as_str().unwrap(),
                category,
                &case["payload"],
            );
            if let Some(expected) = case.get("expected") {
                assert_eq!(serde_json::to_value(result).unwrap(), *expected);
            } else {
                let targets = result
                    .edges
                    .iter()
                    .map(|edge| edge.target_uuid.as_str())
                    .collect::<Vec<_>>();
                let expected = case["expected_edge_targets"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|value| value.as_str().unwrap())
                    .collect::<Vec<_>>();
                assert_eq!(targets, expected);
            }
        }
    }

    #[tokio::test]
    async fn union_traversal_is_cycle_safe_shared_and_non_fail_fast() {
        let root_a = identity(
            DatasetCategory::Processes,
            "11111111-1111-1111-1111-111111111111",
        );
        let root_b = identity(
            DatasetCategory::Processes,
            "22222222-2222-2222-2222-222222222222",
        );
        let shared = identity(
            DatasetCategory::Sources,
            "33333333-3333-3333-3333-333333333333",
        );
        let missing = identity(
            DatasetCategory::Contacts,
            "44444444-4444-4444-4444-444444444444",
        );
        let documents = [
            ClosureDocument {
                identity: root_a.clone(),
                payload: json!({"references": [
                    reference("process", root_b.id, Some("01.00.000")),
                    reference("source", shared.id, Some("01.00.000")),
                ]}),
            },
            ClosureDocument {
                identity: root_b.clone(),
                payload: json!({"references": [
                    reference("process", root_a.id, Some("01.00.000")),
                    reference("source", shared.id, Some("01.00.000")),
                ]}),
            },
            ClosureDocument {
                identity: shared.clone(),
                payload: json!({"referenceToContact": reference(
                    "contact",
                    missing.id,
                    Some("01.00.000")
                )}),
            },
        ]
        .into_iter()
        .map(|document| (document.identity.clone(), document))
        .collect();
        let provider = FakeProvider {
            documents,
            ..FakeProvider::default()
        };

        let scan = collect_scope_closure(&provider, &manifest(vec![root_a, root_b]))
            .await
            .unwrap();

        assert!(scan.complete);
        assert_eq!(scan.documents.len(), 3);
        assert_eq!(scan.edges.len(), 5);
        assert_eq!(scan.issues.len(), 1);
        assert_eq!(scan.issues[0].issue_code, "reference_exact_version_missing");
        assert_eq!(scan.issues[0].affected_roots.len(), 2);
        let fetched = provider.fetches.lock().unwrap();
        assert_eq!(
            fetched
                .iter()
                .flatten()
                .filter(|item| *item == &shared)
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn explicit_version_never_falls_back_and_omitted_version_keeps_provenance() {
        let root = identity(
            DatasetCategory::Processes,
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        );
        let target_id = id("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");
        let provider = FakeProvider {
            documents: [ClosureDocument {
                identity: root.clone(),
                payload: json!({"references": [
                    reference("source", target_id, Some("01.00.000")),
                    reference("source", target_id, None),
                ]}),
            }]
            .into_iter()
            .map(|document| (document.identity.clone(), document))
            .collect(),
            ..FakeProvider::default()
        };

        let scan = collect_scope_closure(&provider, &manifest(vec![root.clone()]))
            .await
            .unwrap();

        assert!(
            scan.issues
                .iter()
                .any(|issue| issue.issue_code == "reference_exact_version_missing")
        );
        let omitted = scan
            .issues
            .iter()
            .find(|issue| issue.issue_code == "reference_version_omitted")
            .unwrap();
        assert_eq!(omitted.source.as_ref(), Some(&root));
        assert_eq!(omitted.json_path.as_deref(), Some("$.references[1]"));
        assert_eq!(provider.omitted_calls.load(Ordering::SeqCst), 1);
        assert_eq!(scan.omitted_version_resolutions.len(), 1);
        assert_eq!(
            scan.omitted_version_resolutions[0]["candidateUniverse"],
            "frozen-public-release-manifest"
        );
    }

    #[test]
    fn frozen_snapshot_rejects_same_identity_with_live_content_drift() {
        let exact = identity(
            DatasetCategory::Processes,
            "abababab-abab-abab-abab-abababababab",
        );
        let frozen = json!({"name": "frozen"});
        let universe = [(exact.clone(), snapshot_entry(&exact, &frozen))]
            .into_iter()
            .collect();
        let result = enforce_snapshot_boundary(
            std::slice::from_ref(&exact),
            &universe,
            vec![ClosureDocument {
                identity: exact.clone(),
                payload: json!({"name": "mutated"}),
            }],
        )
        .unwrap();
        assert!(result.documents.is_empty());
        assert!(result.incomplete_identities.contains(&exact));
        assert_eq!(result.issues[0].issue_code, "snapshot_source_drift");
    }

    #[test]
    fn frozen_snapshot_rejects_live_dataset_absent_from_release_manifest() {
        let exact = identity(
            DatasetCategory::Sources,
            "acacacac-acac-acac-acac-acacacacacac",
        );
        let result = enforce_snapshot_boundary(
            std::slice::from_ref(&exact),
            &BTreeMap::new(),
            vec![ClosureDocument {
                identity: exact.clone(),
                payload: json!({"live": true}),
            }],
        )
        .unwrap();
        assert!(result.documents.is_empty());
        assert!(result.incomplete_identities.contains(&exact));
        assert_eq!(result.issues[0].issue_code, "snapshot_dataset_not_allowed");
    }

    #[test]
    fn omitted_version_winner_and_candidates_come_only_from_frozen_release() {
        let dataset_id = id("adadadad-adad-adad-adad-adadadadadad");
        let identities = ["01.00.000", "03.00.000", "02.00.000"]
            .into_iter()
            .map(|version| ExactDatasetIdentity {
                category: DatasetCategory::Sources,
                id: dataset_id,
                version: version.to_owned(),
            })
            .collect::<Vec<_>>();
        let universe = identities
            .iter()
            .map(|identity| {
                (
                    identity.clone(),
                    snapshot_entry(identity, &json!({"version": identity.version})),
                )
            })
            .collect();
        let resolution = resolve_snapshot_omitted_version(
            &universe,
            DatasetCategory::Sources,
            dataset_id,
            "latest_eligible",
        )
        .unwrap();
        assert_eq!(resolution.candidates.len(), 3);
        assert_eq!(
            resolution
                .selected
                .as_ref()
                .map(|item| item.version.as_str()),
            Some("03.00.000")
        );
        assert_eq!(resolution.policy, "latest_eligible");
    }

    #[test]
    fn tidas_final_event_must_close_the_exact_observed_issue_stream() {
        // This is deliberately not Worker-canonical key order. The published
        // TIDAS field closes the exact issue NDJSON bytes, not a reserialized
        // serde_json::Value.
        let issue_stream =
            br#"{"type":"issue","document_key":"sources:1:01.00.000","issue":{"code":"invalid"}}"#;
        let mut issue_stream = issue_stream.to_vec();
        issue_stream.push(b'\n');
        let mut observed = JsonlValueSpoolWriter::new("raw-issues.jsonl").unwrap();
        observed.append_raw_jsonl_line(&issue_stream).unwrap();
        let observed = observed.finish().unwrap();
        assert_eq!(fs::read(&observed.path).unwrap(), issue_stream);
        let final_event = json!({
            "type": "final",
            "protocol": TIDAS_BATCH_PROTOCOL,
            "profile": TIDAS_BATCH_PROFILE,
            "completed": true,
            "summary": {"document_count": 1, "issue_count": 1},
            "logical_issue_stream_sha256": sha256_hex(&issue_stream),
        });
        validate_tidas_final_event(&final_event, 1, observed.sha256.as_str(), 1).unwrap();

        let mut drifted = final_event;
        drifted["logical_issue_stream_sha256"] = json!("0".repeat(64));
        assert!(validate_tidas_final_event(&drifted, 1, observed.sha256.as_str(), 1).is_err());
    }

    #[test]
    fn external_issue_sort_is_deterministic_across_bounded_runs() {
        let mut writer = JsonlValueSpoolWriter::new("unsorted.jsonl").unwrap();
        let mut expected = Vec::new();
        for index in (0..500_u64).rev() {
            let event = json!({
                "type": "issue",
                "document_key": format!("sources:{index}:01.00.000"),
                "issue": {"code": "invalid", "message": "x".repeat(usize::try_from(index).unwrap() % 17)},
            });
            expected.push(event.clone());
            writer.append(&event).unwrap();
        }
        sort_by_canonical_value(&mut expected);

        let unsorted = writer.finish().unwrap();
        let sorted = sort_jsonl_spool_with_run_bytes(&unsorted, 512).unwrap();
        let mut actual = Vec::new();
        sorted
            .visit(|event| {
                actual.push(event);
                Ok(())
            })
            .unwrap();

        assert_eq!(actual, expected);
        assert_eq!(sorted.event_count, 500);
        let mut logical_stream = Vec::new();
        for event in &actual {
            logical_stream.extend(canonical_json_bytes(event).unwrap());
            logical_stream.push(b'\n');
        }
        assert_eq!(sorted.sha256, sha256_hex(&logical_stream));
    }

    #[test]
    fn derived_relation_runs_are_deterministic_across_bounded_fan_in() {
        let values = (0..1_000_u64)
            .rev()
            .map(|index| {
                json!([
                    format!("{:064x}", index % 97),
                    format!("{:064x}", index),
                    {"ordinal": index, "details": "x".repeat(64)}
                ])
            })
            .collect::<Vec<_>>();
        let collect = || {
            let mut writer = SortedJsonlRunWriter::with_limits("test-relations", 512, 3).unwrap();
            for value in &values {
                writer.append(value).unwrap();
            }
            let runs = writer.finish().unwrap();
            let mut observed = Vec::new();
            runs.visit(|value| {
                observed.push(value);
                Ok(())
            })
            .unwrap();
            (runs, observed)
        };
        let (first_runs, first) = collect();
        let (second_runs, second) = collect();
        let mut expected = values.clone();
        sort_by_canonical_value(&mut expected);
        assert_eq!(first, expected);
        assert_eq!(second, expected);
        assert_eq!(first, second);
        assert_eq!(first_runs.event_count, 1_000);
        assert_eq!(first_runs.byte_size, second_runs.byte_size);
        assert!(first_runs.run_paths.len() <= 3);
        assert!(second_runs.run_paths.len() <= 3);
    }

    #[test]
    fn derived_relation_runs_do_not_inherit_validation_total_caps() {
        let mut writer = SortedJsonlRunWriter::with_limits("affected-roots", 64, 2).unwrap();
        writer.byte_size = VALIDATION_ISSUE_SPOOL_MAX_BYTES;
        writer.event_count = VALIDATION_ISSUE_SPOOL_MAX_EVENTS;
        writer
            .append(&json!(["issue", "root", {"complete": true}]))
            .unwrap();
        let runs = writer.finish().unwrap();
        assert!(runs.byte_size > VALIDATION_ISSUE_SPOOL_MAX_BYTES);
        assert!(runs.event_count > VALIDATION_ISSUE_SPOOL_MAX_EVENTS);
    }

    #[test]
    fn repeated_validation_occurrences_are_deduplicated_while_streaming() {
        let occurrence = ClosureIssueOccurrence {
            occurrence_key: "same-occurrence".to_owned(),
            source: None,
            json_path: Some("$.fixture".to_owned()),
            reference_role: None,
            details: json!({"source": "generated"}),
        };
        let issue = ClosureIssue {
            issue_key: "same-issue".to_owned(),
            severity: "warning".to_owned(),
            blocking: false,
            issue_code: "generated_duplicate".to_owned(),
            source: None,
            json_path: Some("$.fixture".to_owned()),
            reference_role: None,
            requested_target_type: None,
            requested_target_id: None,
            requested_target_version: None,
            message: "generated duplicate".to_owned(),
            suggested_action: None,
            occurrence_count: 1,
            occurrences: vec![occurrence],
            affected_root_count: 0,
            affected_roots: Vec::new(),
            affected_root_witness_paths: Vec::new(),
            witness_path: Vec::new(),
        };
        let mut coalesced = BTreeMap::new();
        for _ in 0..100_000 {
            coalesce_issue_into(&mut coalesced, issue.clone());
        }

        let issue = coalesced.get("same-issue").unwrap();
        assert_eq!(issue.occurrence_count, 1);
        assert_eq!(issue.occurrences.len(), 1);
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize)]
    #[serde(rename_all = "camelCase")]
    struct ReconstructedMachineResult {
        schema_version: String,
        partition_count: usize,
        issue_count: u64,
        occurrence_count: u64,
        affected_root_count: u64,
        expanded_affected_root_record_count: u64,
        root_impact_record_count: u64,
        uncompressed_byte_size: u64,
        issue_stream_sha256: String,
        legacy_relation_stream_sha256: Option<IssueRelationStreamHashesV2>,
    }

    fn reconstruct_complete_machine_result(
        artifacts: &[PreparedArtifact],
        expected_closure_check_id: Uuid,
    ) -> anyhow::Result<ReconstructedMachineResult> {
        let manifest = artifacts
            .iter()
            .filter(|artifact| artifact.descriptor.file_name == "manifest.json")
            .collect::<Vec<_>>();
        if manifest.len() != 1 {
            return Err(anyhow::anyhow!(
                "complete machine result must contain exactly one manifest"
            ));
        }
        let value: Value = serde_json::from_reader(BufReader::new(File::open(&manifest[0].path)?))?;
        match value.get("schemaVersion").and_then(Value::as_str) {
            Some("lcia.scope-closure-issue-manifest.v2") => {
                reconstruct_complete_machine_result_v2(artifacts, expected_closure_check_id)
            }
            Some("lcia.scope-closure-issue-manifest.v3") => {
                reconstruct_complete_machine_result_v3(artifacts, expected_closure_check_id)
            }
            _ => Err(anyhow::anyhow!(
                "complete machine result manifest schema is unsupported"
            )),
        }
    }

    #[allow(clippy::too_many_lines)]
    fn reconstruct_complete_machine_result_v3(
        artifacts: &[PreparedArtifact],
        expected_closure_check_id: Uuid,
    ) -> anyhow::Result<ReconstructedMachineResult> {
        let manifest_artifact = artifacts
            .iter()
            .find(|artifact| artifact.descriptor.file_name == "manifest.json")
            .ok_or_else(|| anyhow::anyhow!("complete machine result omitted manifest"))?;
        if manifest_artifact.descriptor.artifact_role
            != ScopeClosureArtifactRole::CompleteMachineResult
        {
            return Err(anyhow::anyhow!(
                "manifest artifact role is not complete_machine_result"
            ));
        }
        let (manifest_size, manifest_sha256) = file_size_and_sha256(&manifest_artifact.path)?;
        if manifest_size != u64::try_from(manifest_artifact.descriptor.byte_size)?
            || manifest_sha256 != manifest_artifact.descriptor.checksum_sha256
        {
            return Err(anyhow::anyhow!("manifest descriptor identity mismatch"));
        }
        let manifest: IssuePartitionManifestV3 =
            serde_json::from_reader(BufReader::new(File::open(&manifest_artifact.path)?))?;
        if manifest.schema_version != "lcia.scope-closure-issue-manifest.v3"
            || manifest.closure_check_id != expected_closure_check_id
            || manifest.expanded_affected_root_record_count != 0
            || manifest.ordering
                != (IssueManifestOrdering {
                    issue_key: "UTF-8 ascending".to_owned(),
                    root_ordinal: "exact dataset identity ascending".to_owned(),
                    graph_node_ordinal: "exact dataset identity ascending".to_owned(),
                    root_impact_key: "UTF-8 ascending".to_owned(),
                })
        {
            return Err(anyhow::anyhow!(
                "v3 manifest identity, expansion, or ordering contract mismatch"
            ));
        }
        let mut sorted_partitions = manifest
            .partitions
            .iter()
            .map(|entry| entry.path.as_str())
            .collect::<Vec<_>>();
        let original_partitions = sorted_partitions.clone();
        sorted_partitions.sort_unstable();
        if original_partitions != sorted_partitions {
            return Err(anyhow::anyhow!(
                "v3 manifest partitions are not deterministically ordered"
            ));
        }
        let mut sorted_evidence = manifest
            .evidence
            .iter()
            .map(|entry| entry.path.as_str())
            .collect::<Vec<_>>();
        let original_evidence = sorted_evidence.clone();
        sorted_evidence.sort_unstable();
        if original_evidence != sorted_evidence {
            return Err(anyhow::anyhow!(
                "v3 manifest evidence is not deterministically ordered"
            ));
        }
        let valid_relative_path = |path: &str| {
            !Path::new(path).is_absolute()
                && !path.contains('\\')
                && path
                    .split('/')
                    .all(|segment| !segment.is_empty() && segment != "." && segment != "..")
        };
        let mut machine_artifacts = BTreeMap::new();
        for artifact in artifacts.iter().filter(|artifact| {
            artifact.descriptor.artifact_role == ScopeClosureArtifactRole::CompleteMachineResult
                && artifact.descriptor.file_name != "manifest.json"
        }) {
            if !valid_relative_path(&artifact.descriptor.file_name)
                || machine_artifacts
                    .insert(artifact.descriptor.file_name.as_str(), artifact)
                    .is_some()
            {
                return Err(anyhow::anyhow!(
                    "v3 complete machine result contains a duplicate or invalid artifact path"
                ));
            }
        }
        let expected_paths = manifest
            .partitions
            .iter()
            .map(|entry| entry.path.as_str())
            .chain(manifest.evidence.iter().map(|entry| entry.path.as_str()))
            .collect::<BTreeSet<_>>();
        if expected_paths.len() != manifest.partitions.len() + manifest.evidence.len()
            || expected_paths != machine_artifacts.keys().copied().collect::<BTreeSet<_>>()
        {
            return Err(anyhow::anyhow!(
                "v3 manifest membership differs from complete machine artifacts"
            ));
        }

        let mut decoded_root_impact = None::<DecodedRootImpactIndex>;
        let mut decoded_graph = None::<DecodedFrozenReferenceGraph>;
        let mut total_uncompressed_bytes = 0_u64;
        for entry in &manifest.evidence {
            if !valid_relative_path(&entry.path)
                || !matches!(
                    entry.relation.as_str(),
                    "root-impact-index" | "frozen-reference-graph" | "tidas-issue-stream"
                )
            {
                return Err(anyhow::anyhow!(
                    "v3 evidence relation or path is unsupported"
                ));
            }
            let artifact = machine_artifacts
                .get(entry.path.as_str())
                .ok_or_else(|| anyhow::anyhow!("v3 evidence artifact is missing"))?;
            let (compressed_size, compressed_sha256) = file_size_and_sha256(&artifact.path)?;
            if compressed_size != entry.compressed_byte_size
                || compressed_size != u64::try_from(artifact.descriptor.byte_size)?
                || compressed_sha256 != entry.compressed_sha256
                || compressed_sha256 != artifact.descriptor.checksum_sha256
                || artifact.descriptor.content_type != entry.media_type
            {
                return Err(anyhow::anyhow!(
                    "v3 compressed evidence descriptor mismatch: {}",
                    entry.path
                ));
            }
            let compressed_reader = zstd::stream::read::Decoder::new(File::open(&artifact.path)?)?;
            let mut reader = BufReader::with_capacity(64 * 1024, compressed_reader);
            let collect_binary = entry.relation != "tidas-issue-stream";
            let mut logical = collect_binary.then(Vec::new);
            let mut digest = Sha256::new();
            let mut byte_count = 0_u64;
            let mut newline_count = 0_u64;
            let mut buffer = vec![0_u8; 64 * 1024].into_boxed_slice();
            loop {
                let read = reader.read(&mut buffer)?;
                if read == 0 {
                    break;
                }
                let chunk = &buffer[..read];
                digest.update(chunk);
                byte_count = byte_count
                    .checked_add(u64::try_from(read)?)
                    .ok_or_else(|| anyhow::anyhow!("v3 evidence byte count overflow"))?;
                let mut chunk_newline_count = 0_u64;
                for &byte in chunk {
                    chunk_newline_count =
                        chunk_newline_count.saturating_add(u64::from(byte == b'\n'));
                }
                newline_count = newline_count
                    .checked_add(chunk_newline_count)
                    .ok_or_else(|| anyhow::anyhow!("v3 evidence record count overflow"))?;
                if let Some(logical) = &mut logical {
                    logical.extend_from_slice(chunk);
                }
            }
            let logical_sha256 = hex::encode(digest.finalize());
            if byte_count != entry.uncompressed_byte_size
                || logical_sha256 != entry.uncompressed_sha256
            {
                return Err(anyhow::anyhow!(
                    "v3 logical evidence descriptor mismatch: {}",
                    entry.path
                ));
            }
            match entry.relation.as_str() {
                "root-impact-index" => {
                    let root_impact_index = decode_root_impact_index(
                        logical
                            .as_deref()
                            .ok_or_else(|| anyhow::anyhow!("root impact bytes not collected"))?,
                    )?;
                    if u64::try_from(root_impact_index.records.len())? != entry.record_count
                        || entry.record_count != manifest.root_impact_record_count
                        || u64::from(root_impact_index.root_count) != manifest.root_count
                        || logical_sha256 != manifest.relation_stream_sha256.root_impact_index
                    {
                        return Err(anyhow::anyhow!(
                            "v3 root impact index count or hash mismatch"
                        ));
                    }
                    decoded_root_impact = Some(root_impact_index);
                }
                "frozen-reference-graph" => {
                    let frozen_graph = decode_frozen_reference_graph(
                        logical
                            .as_deref()
                            .ok_or_else(|| anyhow::anyhow!("frozen graph bytes not collected"))?,
                    )?;
                    let edge_count = frozen_graph.reverse.iter().map(Vec::len).try_fold(
                        0_u64,
                        |total, count| {
                            total.checked_add(u64::try_from(count)?).ok_or_else(|| {
                                anyhow::anyhow!("decoded reference edge count overflow")
                            })
                        },
                    )?;
                    if u64::try_from(frozen_graph.identities.len())? != manifest.graph_node_count
                        || u64::try_from(frozen_graph.root_node_ordinals.len())?
                            != manifest.root_count
                        || edge_count != manifest.graph_edge_count
                        || entry.record_count != manifest.graph_node_count
                        || logical_sha256 != manifest.relation_stream_sha256.frozen_reference_graph
                    {
                        return Err(anyhow::anyhow!(
                            "v3 frozen reference graph count or hash mismatch"
                        ));
                    }
                    decoded_graph = Some(frozen_graph);
                }
                "tidas-issue-stream" => {
                    if newline_count != entry.record_count
                        || entry.record_count != manifest.logical_issue_event_count
                        || byte_count != manifest.logical_issue_stream_byte_size
                        || logical_sha256 != manifest.logical_issue_stream_sha256
                        || logical_sha256 != manifest.relation_stream_sha256.tidas_issue_stream
                    {
                        return Err(anyhow::anyhow!(
                            "v3 TIDAS logical stream count or hash mismatch"
                        ));
                    }
                }
                _ => unreachable!(),
            }
            total_uncompressed_bytes = total_uncompressed_bytes
                .checked_add(byte_count)
                .ok_or_else(|| anyhow::anyhow!("v3 machine result byte count overflow"))?;
        }
        let root_impact = decoded_root_impact
            .ok_or_else(|| anyhow::anyhow!("v3 root impact evidence is missing"))?;
        let graph =
            decoded_graph.ok_or_else(|| anyhow::anyhow!("v3 frozen graph evidence is missing"))?;
        let impact_by_key = root_impact
            .records
            .iter()
            .map(|record| (record.impact_key.as_str(), record))
            .collect::<BTreeMap<_, _>>();
        let mut referenced_impacts = BTreeSet::<String>::new();
        let mut previous_issue_key = None::<String>;
        let mut issue_count = 0_u64;
        let mut occurrence_count = 0_u64;
        let mut affected_root_count = 0_u64;
        let mut issue_digest = Sha256::new();
        for (partition_index, entry) in manifest.partitions.iter().enumerate() {
            let expected_path = format!("issues/part-{partition_index:06}.ndjson.zst");
            if entry.relation != "issues"
                || entry.path != expected_path
                || entry.media_type != "application/x-ndjson+zstd"
                || !valid_relative_path(&entry.path)
            {
                return Err(anyhow::anyhow!(
                    "v3 issue partition identity is invalid: {}",
                    entry.path
                ));
            }
            let artifact = machine_artifacts
                .get(entry.path.as_str())
                .ok_or_else(|| anyhow::anyhow!("v3 issue partition is missing"))?;
            let (compressed_size, compressed_sha256) = file_size_and_sha256(&artifact.path)?;
            if compressed_size != entry.compressed_byte_size
                || compressed_size != u64::try_from(artifact.descriptor.byte_size)?
                || compressed_sha256 != entry.compressed_sha256
                || compressed_sha256 != artifact.descriptor.checksum_sha256
                || artifact.descriptor.content_type != entry.media_type
            {
                return Err(anyhow::anyhow!(
                    "v3 compressed issue partition descriptor mismatch"
                ));
            }
            let decoder = zstd::stream::read::Decoder::new(File::open(&artifact.path)?)?;
            let mut reader = BufReader::with_capacity(64 * 1024, decoder);
            let mut line = Vec::new();
            let mut partition_digest = Sha256::new();
            let mut partition_count = 0_u64;
            let mut partition_bytes = 0_u64;
            let mut first_issue_key = None::<String>;
            let mut last_issue_key = None::<String>;
            loop {
                line.clear();
                let read = reader.read_until(b'\n', &mut line)?;
                if read == 0 {
                    break;
                }
                if line.last() != Some(&b'\n')
                    || u64::try_from(line.len())? > manifest.partition_max_uncompressed_bytes
                {
                    return Err(anyhow::anyhow!(
                        "v3 issue record is unterminated or exceeds the reader window"
                    ));
                }
                partition_digest.update(&line);
                issue_digest.update(&line);
                partition_bytes = partition_bytes
                    .checked_add(u64::try_from(line.len())?)
                    .ok_or_else(|| anyhow::anyhow!("v3 issue partition byte count overflow"))?;
                line.pop();
                let record: Value = serde_json::from_slice(&line)?;
                if record.get("schemaVersion").and_then(Value::as_str)
                    != Some("lcia.scope-closure-issue.v3")
                {
                    return Err(anyhow::anyhow!("v3 issue record schema mismatch"));
                }
                let issue_key = record
                    .get("issueKey")
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow::anyhow!("v3 issue record omitted issueKey"))?;
                if previous_issue_key
                    .as_deref()
                    .is_some_and(|previous| previous >= issue_key)
                {
                    return Err(anyhow::anyhow!(
                        "v3 issue order is not strictly deterministic"
                    ));
                }
                previous_issue_key = Some(issue_key.to_owned());
                first_issue_key.get_or_insert_with(|| issue_key.to_owned());
                last_issue_key = Some(issue_key.to_owned());
                if record.get("severity").and_then(Value::as_str).is_none()
                    || record.get("blocker").and_then(Value::as_bool).is_none()
                    || record.get("code").and_then(Value::as_str).is_none()
                    || record.get("message").and_then(Value::as_str).is_none()
                    || !record
                        .get("path")
                        .is_some_and(|value| value.is_null() || value.as_str().is_some())
                {
                    return Err(anyhow::anyhow!(
                        "v3 issue record omitted a canonical issue field"
                    ));
                }
                let issue_occurrences = record
                    .get("occurrenceCount")
                    .and_then(Value::as_u64)
                    .ok_or_else(|| anyhow::anyhow!("v3 issue omitted occurrenceCount"))?;
                let issue_roots = record
                    .get("affectedRootCount")
                    .and_then(Value::as_u64)
                    .ok_or_else(|| anyhow::anyhow!("v3 issue omitted affectedRootCount"))?;
                let occurrence_samples = record
                    .get("occurrenceSamples")
                    .and_then(Value::as_array)
                    .ok_or_else(|| anyhow::anyhow!("v3 issue omitted occurrenceSamples"))?;
                let root_samples = record
                    .get("affectedRootSamples")
                    .and_then(Value::as_array)
                    .ok_or_else(|| anyhow::anyhow!("v3 issue omitted affectedRootSamples"))?;
                if occurrence_samples.len() > manifest.rpc_occurrence_sample_limit_per_issue
                    || root_samples.len() > manifest.rpc_affected_root_sample_limit_per_issue
                    || record
                        .get("occurrenceSamplesTruncated")
                        .and_then(Value::as_bool)
                        != Some(issue_occurrences > u64::try_from(occurrence_samples.len())?)
                    || record
                        .get("affectedRootSamplesTruncated")
                        .and_then(Value::as_bool)
                        != Some(issue_roots > u64::try_from(root_samples.len())?)
                {
                    return Err(anyhow::anyhow!(
                        "v3 issue samples are unbounded or truncation flags drifted"
                    ));
                }
                let impact: RootImpactReference = serde_json::from_value(
                    record
                        .get("rootImpact")
                        .cloned()
                        .ok_or_else(|| anyhow::anyhow!("v3 issue omitted rootImpact"))?,
                )?;
                if let Some(impact_key) = impact.impact_key.as_deref() {
                    let indexed = impact_by_key.get(impact_key).ok_or_else(|| {
                        anyhow::anyhow!("v3 issue references an unknown root impact")
                    })?;
                    if indexed.mode != impact.mode
                        || indexed.source_node_ordinal != impact.source_node_ordinal
                        || u64::from(indexed.affected_root_count) != issue_roots
                    {
                        return Err(anyhow::anyhow!("v3 issue root impact projection mismatch"));
                    }
                    referenced_impacts.insert(impact_key.to_owned());
                    for sample in root_samples {
                        let root_ordinal = u32::try_from(
                            sample
                                .get("rootOrdinal")
                                .and_then(Value::as_u64)
                                .ok_or_else(|| anyhow::anyhow!("root sample omitted ordinal"))?,
                        )?;
                        if !decoded_impact_contains_root(
                            indexed,
                            root_ordinal,
                            root_impact.root_count,
                        ) {
                            return Err(anyhow::anyhow!(
                                "v3 root sample is absent from compact impact index"
                            ));
                        }
                        if let Some(source_node_ordinal) = indexed.source_node_ordinal {
                            let witness = reconstruct_frozen_graph_witness(
                                &graph,
                                source_node_ordinal,
                                root_ordinal,
                            )?;
                            if serde_json::to_value(witness)?
                                != sample.get("witnessPath").cloned().unwrap_or(Value::Null)
                            {
                                return Err(anyhow::anyhow!(
                                    "v3 on-demand witness differs from bounded sample"
                                ));
                            }
                        }
                    }
                } else {
                    let expected = match impact.mode {
                        RootImpactMode::None => 0,
                        RootImpactMode::AllRoots => manifest.root_count,
                        RootImpactMode::IncludedOrdinals | RootImpactMode::ExcludedOrdinals => {
                            return Err(anyhow::anyhow!(
                                "v3 compact impact mode omitted its index key"
                            ));
                        }
                    };
                    if issue_roots != expected {
                        return Err(anyhow::anyhow!(
                            "v3 inline all/none root impact count mismatch"
                        ));
                    }
                }
                occurrence_count = occurrence_count
                    .checked_add(issue_occurrences)
                    .ok_or_else(|| anyhow::anyhow!("v3 occurrence count overflow"))?;
                affected_root_count = affected_root_count
                    .checked_add(issue_roots)
                    .ok_or_else(|| anyhow::anyhow!("v3 affected-root count overflow"))?;
                issue_count = issue_count
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("v3 issue count overflow"))?;
                partition_count = partition_count
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("v3 partition record count overflow"))?;
            }
            if partition_count != entry.record_count
                || partition_bytes != entry.uncompressed_byte_size
                || hex::encode(partition_digest.finalize()) != entry.uncompressed_sha256
                || first_issue_key.as_deref() != Some(entry.first_issue_key.as_str())
                || last_issue_key.as_deref() != Some(entry.last_issue_key.as_str())
            {
                return Err(anyhow::anyhow!(
                    "v3 uncompressed issue partition descriptor mismatch"
                ));
            }
            total_uncompressed_bytes = total_uncompressed_bytes
                .checked_add(partition_bytes)
                .ok_or_else(|| anyhow::anyhow!("v3 machine result byte count overflow"))?;
        }
        if referenced_impacts
            != impact_by_key
                .keys()
                .map(|key| (*key).to_owned())
                .collect::<BTreeSet<_>>()
        {
            return Err(anyhow::anyhow!(
                "v3 root impact index contains unreferenced records"
            ));
        }
        let issue_stream_sha256 = hex::encode(issue_digest.finalize());
        if issue_count != manifest.issue_count
            || occurrence_count != manifest.occurrence_count
            || affected_root_count != manifest.affected_root_count
            || issue_stream_sha256 != manifest.relation_stream_sha256.issues
        {
            return Err(anyhow::anyhow!(
                "v3 reconstructed counts or issue hash differ from manifest"
            ));
        }
        Ok(ReconstructedMachineResult {
            schema_version: manifest.schema_version,
            partition_count: manifest.partitions.len(),
            issue_count,
            occurrence_count,
            affected_root_count,
            expanded_affected_root_record_count: 0,
            root_impact_record_count: manifest.root_impact_record_count,
            uncompressed_byte_size: total_uncompressed_bytes,
            issue_stream_sha256,
            legacy_relation_stream_sha256: None,
        })
    }

    #[allow(clippy::too_many_lines)]
    fn reconstruct_complete_machine_result_v2(
        artifacts: &[PreparedArtifact],
        expected_closure_check_id: Uuid,
    ) -> anyhow::Result<ReconstructedMachineResult> {
        let manifest_artifacts = artifacts
            .iter()
            .filter(|artifact| artifact.descriptor.file_name == "manifest.json")
            .collect::<Vec<_>>();
        if manifest_artifacts.len() != 1 {
            return Err(anyhow::anyhow!(
                "complete machine result must contain exactly one manifest"
            ));
        }
        let manifest_artifact = manifest_artifacts[0];
        if manifest_artifact.descriptor.artifact_role
            != ScopeClosureArtifactRole::CompleteMachineResult
        {
            return Err(anyhow::anyhow!(
                "manifest artifact role is not complete_machine_result"
            ));
        }
        let (manifest_size, manifest_sha256) = file_size_and_sha256(&manifest_artifact.path)?;
        if manifest_size != u64::try_from(manifest_artifact.descriptor.byte_size)?
            || manifest_sha256 != manifest_artifact.descriptor.checksum_sha256
        {
            return Err(anyhow::anyhow!("manifest descriptor identity mismatch"));
        }
        let manifest: IssuePartitionManifestV2 =
            serde_json::from_reader(BufReader::new(File::open(&manifest_artifact.path)?))?;
        if manifest.schema_version != "lcia.scope-closure-issue-manifest.v2"
            || manifest.closure_check_id != expected_closure_check_id
        {
            return Err(anyhow::anyhow!(
                "manifest schemaVersion or closureCheckId mismatch"
            ));
        }
        let mut sorted_paths = manifest
            .partitions
            .iter()
            .map(|entry| entry.path.as_str())
            .collect::<Vec<_>>();
        let original_paths = sorted_paths.clone();
        sorted_paths.sort_unstable();
        if original_paths != sorted_paths {
            return Err(anyhow::anyhow!(
                "manifest partition membership is not deterministically ordered"
            ));
        }

        let mut partition_artifacts = BTreeMap::new();
        for artifact in artifacts
            .iter()
            .filter(|artifact| artifact.descriptor.file_name.ends_with(".ndjson.zst"))
        {
            if partition_artifacts
                .insert(artifact.descriptor.file_name.as_str(), artifact)
                .is_some()
            {
                return Err(anyhow::anyhow!(
                    "duplicate partition artifact identity: {}",
                    artifact.descriptor.file_name
                ));
            }
        }
        let manifest_paths = manifest
            .partitions
            .iter()
            .map(|entry| entry.path.as_str())
            .collect::<BTreeSet<_>>();
        if manifest_paths.len() != manifest.partitions.len()
            || manifest_paths != partition_artifacts.keys().copied().collect::<BTreeSet<_>>()
        {
            return Err(anyhow::anyhow!(
                "manifest membership differs from local partition artifacts"
            ));
        }

        let mut relation_partition_indexes = BTreeMap::<String, usize>::new();
        let mut relation_counts = BTreeMap::<String, u64>::new();
        let mut relation_digests = BTreeMap::<String, Sha256>::new();
        let mut previous_sort_keys = BTreeMap::<String, String>::new();
        let mut total_uncompressed_bytes = 0_u64;

        for entry in &manifest.partitions {
            if !matches!(
                entry.relation.as_str(),
                "issues" | "occurrences" | "affected-roots"
            ) {
                return Err(anyhow::anyhow!(
                    "manifest contains unsupported relation: {}",
                    entry.relation
                ));
            }
            if Path::new(&entry.path).is_absolute()
                || entry.path.contains('\\')
                || entry
                    .path
                    .split('/')
                    .any(|segment| segment.is_empty() || segment == "." || segment == "..")
            {
                return Err(anyhow::anyhow!(
                    "manifest contains invalid relative partition identity: {}",
                    entry.path
                ));
            }
            let partition_index = relation_partition_indexes
                .entry(entry.relation.clone())
                .or_default();
            let expected_path = format!("{}/part-{partition_index:06}.ndjson.zst", entry.relation);
            if entry.path != expected_path {
                return Err(anyhow::anyhow!(
                    "partition identity is not contiguous: expected={expected_path} actual={}",
                    entry.path
                ));
            }
            *partition_index += 1;
            if entry.media_type != "application/x-ndjson+zstd" {
                return Err(anyhow::anyhow!(
                    "partition media type mismatch: {}",
                    entry.path
                ));
            }

            let artifact = partition_artifacts
                .get(entry.path.as_str())
                .ok_or_else(|| anyhow::anyhow!("partition artifact is missing"))?;
            if artifact.descriptor.artifact_role != ScopeClosureArtifactRole::CompleteMachineResult
            {
                return Err(anyhow::anyhow!(
                    "partition artifact role is not complete_machine_result: {}",
                    entry.path
                ));
            }
            let (compressed_size, compressed_sha256) = file_size_and_sha256(&artifact.path)?;
            if compressed_size != entry.compressed_byte_size
                || compressed_size != u64::try_from(artifact.descriptor.byte_size)?
                || compressed_sha256 != entry.compressed_sha256
                || compressed_sha256 != artifact.descriptor.checksum_sha256
                || artifact.descriptor.content_type != entry.media_type
            {
                return Err(anyhow::anyhow!(
                    "compressed partition descriptor mismatch: {}",
                    entry.path
                ));
            }

            let decoder = zstd::stream::read::Decoder::new(File::open(&artifact.path)?)?;
            let mut reader = BufReader::with_capacity(64 * 1024, decoder);
            let mut line = Vec::new();
            let mut partition_digest = Sha256::new();
            let relation_digest = relation_digests.entry(entry.relation.clone()).or_default();
            let mut partition_count = 0_u64;
            let mut partition_bytes = 0_u64;
            let mut first_issue_key = None::<String>;
            let mut last_issue_key = None::<String>;
            loop {
                line.clear();
                let read = reader.read_until(b'\n', &mut line)?;
                if read == 0 {
                    break;
                }
                if line.last() != Some(&b'\n')
                    || u64::try_from(line.len())? > manifest.partition_max_uncompressed_bytes
                {
                    return Err(anyhow::anyhow!(
                        "partition record is unterminated or exceeds the bounded reader window: {}",
                        entry.path
                    ));
                }
                partition_digest.update(&line);
                relation_digest.update(&line);
                partition_bytes = partition_bytes
                    .checked_add(u64::try_from(line.len())?)
                    .ok_or_else(|| anyhow::anyhow!("partition uncompressed byte count overflow"))?;
                line.pop();
                let record: Value = serde_json::from_slice(&line)?;
                let issue_key = record
                    .get("issueKey")
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow::anyhow!("partition record omitted issueKey"))?;
                let sort_key = match entry.relation.as_str() {
                    "issues" => issue_key.to_owned(),
                    "occurrences" => format!(
                        "{issue_key}\0{}",
                        record
                            .get("occurrenceKey")
                            .and_then(Value::as_str)
                            .ok_or_else(|| anyhow::anyhow!(
                                "occurrence partition record omitted occurrenceKey"
                            ))?
                    ),
                    "affected-roots" => format!(
                        "{issue_key}\0{}",
                        canonical_json_sha256(record.get("root").ok_or_else(
                            || anyhow::anyhow!("affected-root partition record omitted root")
                        )?)?
                    ),
                    _ => unreachable!(),
                };
                if previous_sort_keys
                    .get(&entry.relation)
                    .is_some_and(|previous| previous >= &sort_key)
                {
                    return Err(anyhow::anyhow!(
                        "partition relation order is not strictly deterministic: {}",
                        entry.path
                    ));
                }
                previous_sort_keys.insert(entry.relation.clone(), sort_key);
                first_issue_key.get_or_insert_with(|| issue_key.to_owned());
                last_issue_key = Some(issue_key.to_owned());
                partition_count = partition_count
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("partition record count overflow"))?;
            }
            if partition_count != entry.record_count
                || partition_bytes != entry.uncompressed_byte_size
                || hex::encode(partition_digest.finalize()) != entry.uncompressed_sha256
                || first_issue_key.as_deref() != Some(entry.first_issue_key.as_str())
                || last_issue_key.as_deref() != Some(entry.last_issue_key.as_str())
            {
                return Err(anyhow::anyhow!(
                    "uncompressed partition descriptor mismatch: {}",
                    entry.path
                ));
            }
            *relation_counts.entry(entry.relation.clone()).or_default() += partition_count;
            total_uncompressed_bytes = total_uncompressed_bytes
                .checked_add(partition_bytes)
                .ok_or_else(|| anyhow::anyhow!("machine result byte count overflow"))?;
        }

        let reconstructed_hashes = IssueRelationStreamHashesV2 {
            issues: hex::encode(
                relation_digests
                    .remove("issues")
                    .unwrap_or_default()
                    .finalize(),
            ),
            occurrences: hex::encode(
                relation_digests
                    .remove("occurrences")
                    .unwrap_or_default()
                    .finalize(),
            ),
            affected_roots: hex::encode(
                relation_digests
                    .remove("affected-roots")
                    .unwrap_or_default()
                    .finalize(),
            ),
        };
        let reconstructed = ReconstructedMachineResult {
            schema_version: manifest.schema_version.clone(),
            partition_count: manifest.partitions.len(),
            issue_count: relation_counts.get("issues").copied().unwrap_or_default(),
            occurrence_count: relation_counts
                .get("occurrences")
                .copied()
                .unwrap_or_default(),
            affected_root_count: relation_counts
                .get("affected-roots")
                .copied()
                .unwrap_or_default(),
            expanded_affected_root_record_count: relation_counts
                .get("affected-roots")
                .copied()
                .unwrap_or_default(),
            root_impact_record_count: 0,
            uncompressed_byte_size: total_uncompressed_bytes,
            issue_stream_sha256: reconstructed_hashes.issues.clone(),
            legacy_relation_stream_sha256: Some(reconstructed_hashes),
        };
        if reconstructed.issue_count != manifest.issue_count
            || reconstructed.occurrence_count != manifest.occurrence_count
            || reconstructed.affected_root_count != manifest.affected_root_count
            || reconstructed.legacy_relation_stream_sha256.as_ref()
                != Some(&manifest.relation_stream_sha256)
        {
            return Err(anyhow::anyhow!(
                "reconstructed complete machine result differs from manifest global counts/hashes"
            ));
        }
        Ok(reconstructed)
    }

    #[test]
    fn generated_partitions_are_deterministic_and_bounded_at_1x_2x_5x_10x() {
        fn build(record_count: usize) -> (Vec<IssuePartitionManifestEntry>, Vec<Vec<u8>>) {
            let temp = Arc::new(TempDir::new().unwrap());
            let mut writer = IssuePartitionAccumulator::new(Arc::clone(&temp), "issues");
            for index in 0..record_count {
                let issue_key = format!("issue-{index:08}");
                writer
                    .push(
                        &issue_key,
                        &json!({
                            "schemaVersion": "lcia.scope-closure-issue.v2",
                            "issueKey": issue_key,
                            "message": "x".repeat(320),
                        }),
                    )
                    .unwrap();
            }
            let (entries, artifacts, _) = writer.finish().unwrap();
            let compressed = artifacts
                .iter()
                .map(|artifact| fs::read(&artifact.path).unwrap())
                .collect::<Vec<_>>();
            for (entry, artifact_bytes) in entries.iter().zip(&compressed) {
                assert!(entry.record_count <= ISSUE_PARTITION_MAX_RECORDS);
                assert!(entry.uncompressed_byte_size <= ISSUE_PARTITION_MAX_UNCOMPRESSED_BYTES);
                assert_eq!(entry.compressed_sha256, sha256_hex(artifact_bytes));
                let decoded = zstd::stream::decode_all(Cursor::new(artifact_bytes)).unwrap();
                assert_eq!(
                    entry.uncompressed_sha256,
                    sha256_hex(&decoded),
                    "uncompressed partition checksum drifted"
                );
                assert_eq!(
                    entry.record_count,
                    u64::try_from(decoded.split(|byte| *byte == b'\n').count() - 1).unwrap()
                );
            }
            assert_eq!(
                entries.iter().map(|entry| entry.record_count).sum::<u64>(),
                u64::try_from(record_count).unwrap()
            );
            (entries, compressed)
        }

        for multiplier in [1, 2, 5, 10] {
            let record_count = 3_500 * multiplier;
            let first = build(record_count);
            let second = build(record_count);
            assert_eq!(first, second);
        }
    }

    #[test]
    #[ignore = "local capacity gate: writes and external-sorts the qualified 1,088,760-event spool"]
    fn qualified_million_event_spool_stays_within_fixed_runs() {
        let event_count = std::env::var("SCOPE_CLOSURE_CAPACITY_EVENTS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(1_088_760);
        let message = "x".repeat(440);
        let started = Instant::now();
        let mut writer = JsonlValueSpoolWriter::new("qualified-unsorted.jsonl").unwrap();
        for index in (0..event_count).rev() {
            writer
                .append(&json!({
                    "type": "issue",
                    "document_key": format!("sources:{index:07}:01.00.000"),
                    "issue": {
                        "code": "qualified_capacity_issue",
                        "location": "$.fixture",
                        "message": message,
                    },
                }))
                .unwrap();
        }
        let unsorted = writer.finish().unwrap();
        assert_eq!(unsorted.event_count, event_count);
        assert!(
            unsorted.byte_size >= 512 * 1024 * 1024,
            "qualified spool was unexpectedly small: {} bytes",
            unsorted.byte_size
        );

        let sorted = sort_jsonl_spool(&unsorted).unwrap();
        let mut observed = 0_u64;
        let mut previous = None::<Vec<u8>>;
        sorted
            .visit(|event| {
                let current = canonical_json_bytes(&event)?;
                if let Some(previous) = previous.as_ref() {
                    assert!(previous <= &current);
                }
                previous = Some(current);
                observed += 1;
                Ok(())
            })
            .unwrap();

        assert_eq!(observed, event_count);
        assert_eq!(sorted.event_count, event_count);
        assert!(
            started.elapsed().as_secs() <= 180,
            "qualified issue spool exceeded 3 minutes: {:?}",
            started.elapsed()
        );
    }

    fn collect_real_package_documents(
        directory: &Path,
        writer: &mut ClosureDocumentSpoolWriter,
    ) -> anyhow::Result<()> {
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                collect_real_package_documents(&path, writer)?;
                continue;
            }
            if path.extension().and_then(std::ffi::OsStr::to_str) != Some("json") {
                continue;
            }
            let category = path
                .parent()
                .and_then(Path::file_name)
                .and_then(std::ffi::OsStr::to_str)
                .ok_or_else(|| anyhow::anyhow!("package document omitted category"))?;
            let Some((id, version)) = path
                .file_stem()
                .and_then(std::ffi::OsStr::to_str)
                .and_then(|name| name.rsplit_once('_'))
            else {
                continue;
            };
            let Ok(id) = Uuid::parse_str(id) else {
                continue;
            };
            writer.append(&ClosureDocument {
                identity: ExactDatasetIdentity {
                    category: parse_category(category)?,
                    id,
                    version: version.to_owned(),
                },
                payload: json!({}),
            })?;
        }
        Ok(())
    }

    fn package_issue_document_key(event: &Value) -> Option<String> {
        let issue = event.get("issue")?;
        let category = issue.get("category")?.as_str()?;
        let file_stem = Path::new(issue.get("file_path")?.as_str()?)
            .file_stem()?
            .to_str()?;
        let (id, version) = file_stem.rsplit_once('_')?;
        Some(format!("{category}:{id}:{version}"))
    }

    fn capacity_scan(
        documents: ClosureDocumentSpool,
        roots: Vec<ExactDatasetIdentity>,
        reference_graph: CompactReferenceGraph,
    ) -> ScopeClosureScan {
        ScopeClosureScan {
            schema_version: "lcia.scope-closure-scan.v1".to_owned(),
            complete: true,
            roots,
            documents,
            edges: JsonlValueSpool::empty("capacity-edges.jsonl").unwrap(),
            resolved_references: JsonlValueSpool::empty("capacity-resolved.jsonl").unwrap(),
            omitted_version_resolutions: Vec::new(),
            issues: Vec::new(),
            frontier: Vec::new(),
            provider_universe: Vec::new(),
            reference_graph,
            tidas_issue_event_count: 0,
            issue_relations: None,
        }
    }

    struct ProductionCapacityGraph {
        sources: [ExactDatasetIdentity; 2],
        roots: Vec<ExactDatasetIdentity>,
        references: Vec<ResolvedReference>,
        documents: Vec<ClosureDocument>,
    }

    fn production_capacity_graph() -> ProductionCapacityGraph {
        let sources = [
            ExactDatasetIdentity {
                category: DatasetCategory::Sources,
                id: Uuid::from_u128(171_001),
                version: "01.00.000".to_owned(),
            },
            ExactDatasetIdentity {
                category: DatasetCategory::Sources,
                id: Uuid::from_u128(171_002),
                version: "01.00.000".to_owned(),
            },
        ];
        let roots = (0..7_u128)
            .map(|index| ExactDatasetIdentity {
                category: DatasetCategory::Processes,
                id: Uuid::from_u128(171_100 + index),
                version: "01.00.000".to_owned(),
            })
            .collect::<Vec<_>>();
        let intermediates = (0..7_u128)
            .map(|index| ExactDatasetIdentity {
                category: DatasetCategory::Processes,
                id: Uuid::from_u128(171_200 + index),
                version: "01.00.000".to_owned(),
            })
            .collect::<Vec<_>>();
        let mut references = Vec::new();
        for (index, (root, intermediate)) in roots.iter().zip(&intermediates).enumerate() {
            references.push(ResolvedReference {
                source: root.clone(),
                target: intermediate.clone(),
                json_path: "$.processDataSet.exchanges.exchange.referenceToFlowDataSet".to_owned(),
                reference_role: "process_exchange_flow".to_owned(),
                requested_version_state: "explicit".to_owned(),
            });
            for source in sources
                .iter()
                .take(if index == roots.len() - 1 { 1 } else { 2 })
            {
                references.push(ResolvedReference {
                    source: intermediate.clone(),
                    target: source.clone(),
                    json_path:
                        "$.processDataSet.modellingAndValidation.dataSources.referenceToDataSource"
                            .to_owned(),
                    reference_role: "support_document".to_owned(),
                    requested_version_state: "explicit".to_owned(),
                });
            }
        }
        let documents = roots
            .iter()
            .chain(&intermediates)
            .chain(&sources)
            .cloned()
            .map(|identity| ClosureDocument {
                identity,
                payload: json!({}),
            })
            .collect();
        ProductionCapacityGraph {
            sources,
            roots,
            references,
            documents,
        }
    }

    #[test]
    fn relation_temp_admission_uses_measured_topology_not_global_root_product() {
        const VOLUME_BYTES: u64 = 24 * 1024 * 1024 * 1024;
        const RAW_EVENTS: u64 = 516_313;
        const RAW_BYTES: u64 = 565_431_699;
        const TOTAL_ROOTS: u64 = 5_608;
        const REACHABLE_ROOTS_PER_EVENT: u64 = 7;
        const AFFECTED_RELATIONS: u64 = 3_200_000;

        let initial_planned = relation_temp_admission_bytes(RAW_EVENTS, RAW_BYTES);
        ensure_relation_temp_capacity(
            VOLUME_BYTES,
            initial_planned,
            "initial_observed_raw",
            Some(RAW_EVENTS),
            Some(RAW_BYTES),
        )
        .unwrap();

        // Model the observed production-shaped persistent run footprint, then prove a maximum
        // fan-in merge window plus reserve still fits. The global root count is intentionally not
        // multiplied by raw events; actual topology yielded only about seven relations per event.
        let observed_run_bytes = RAW_BYTES
            .saturating_add(2_436_178_101)
            .saturating_add(728_570_037)
            .saturating_add(2_160_000_000);
        let available_after_observed_runs = VOLUME_BYTES.saturating_sub(observed_run_bytes);
        let merge_window =
            ISSUE_PARTITION_MAX_UNCOMPRESSED_BYTES.saturating_mul(SORT_MERGE_FAN_IN as u64);
        ensure_relation_temp_capacity(
            available_after_observed_runs,
            merge_window,
            "coalesced-affected-roots",
            None,
            None,
        )
        .unwrap();

        assert_eq!(TOTAL_ROOTS, 5_608);
        assert_eq!(REACHABLE_ROOTS_PER_EVENT, 7);
        assert_eq!(AFFECTED_RELATIONS, 3_200_000);
        assert!(
            initial_planned + SCOPE_CLOSURE_TEMP_FREE_SPACE_RESERVE_BYTES < VOLUME_BYTES,
            "24 GiB-class volume must admit the production-shaped workload"
        );
    }

    #[test]
    fn relation_temp_watermark_rejects_genuinely_insufficient_space() {
        let planned = ISSUE_PARTITION_MAX_UNCOMPRESSED_BYTES;
        let required = planned + SCOPE_CLOSURE_TEMP_FREE_SPACE_RESERVE_BYTES;
        let mut temp_path = None;
        let result = (|| -> anyhow::Result<()> {
            let writer = SortedJsonlRunWriter::new("coalesced-affected-roots")?;
            temp_path = Some(writer.temp.path().to_path_buf());
            ensure_relation_temp_capacity(
                required - 1,
                planned,
                "coalesced-affected-roots",
                None,
                None,
            )?;
            drop(writer);
            Ok(())
        })();
        let error = result.unwrap_err().to_string();
        assert!(error.contains("scope_closure_relation_temp_space_low"));
        assert!(error.contains("stage=coalesced-affected-roots"));
        assert!(error.contains(&format!("required={required}")));
        assert!(error.contains("raw_events=measured"));
        assert!(
            !temp_path.unwrap().exists(),
            "incremental admission failure must release its temporary directory"
        );
    }

    fn pad_capacity_event(event: &mut Value, target_jsonl_bytes: usize) {
        let current = canonical_json_bytes(event).unwrap().len().saturating_add(1);
        if current >= target_jsonl_bytes {
            return;
        }
        event["issue"]["capacity_padding"] =
            Value::String("x".repeat(target_jsonl_bytes.saturating_sub(current)));
        let adjusted = canonical_json_bytes(event).unwrap().len().saturating_add(1);
        if adjusted < target_jsonl_bytes {
            let padding = event["issue"]["capacity_padding"]
                .as_str()
                .unwrap()
                .to_owned();
            event["issue"]["capacity_padding"] = Value::String(format!(
                "{padding}{}",
                "x".repeat(target_jsonl_bytes - adjusted)
            ));
        }
    }

    #[test]
    #[ignore = "local capacity gate: real package or high-unique generated issue merge/report"]
    #[allow(clippy::too_many_lines, clippy::used_underscore_binding)]
    fn qualified_streaming_issue_merge_report_capacity() {
        let output_dir = PathBuf::from(
            std::env::var("SCOPE_CLOSURE_CAPACITY_OUTPUT").unwrap_or_else(|_| {
                TempDir::new()
                    .expect("capacity output temp")
                    .keep()
                    .display()
                    .to_string()
            }),
        );
        fs::create_dir_all(&output_dir).unwrap();
        let mut event_writer = JsonlValueSpoolWriter::new("capacity-issue-events.jsonl").unwrap();
        let (documents, roots, reference_graph) = if let Ok(package_dir) =
            std::env::var("SCOPE_CLOSURE_REAL_PACKAGE_DIR")
        {
            let mut documents = ClosureDocumentSpoolWriter::new().unwrap();
            collect_real_package_documents(Path::new(&package_dir), &mut documents).unwrap();
            let issue_spool = std::env::var("SCOPE_CLOSURE_REAL_ISSUE_SPOOL")
                .expect("real package mode requires SCOPE_CLOSURE_REAL_ISSUE_SPOOL");
            let target_raw_events = std::env::var("SCOPE_CLOSURE_PRODUCTION_RAW_EVENTS")
                .ok()
                .map_or(0, |value| {
                    value.parse::<u64>().expect("production raw event target")
                });
            let target_relations = std::env::var("SCOPE_CLOSURE_PRODUCTION_RELATIONS")
                .ok()
                .map_or(0, |value| {
                    value.parse::<u64>().expect("production relation target")
                });
            let production_graph = (target_raw_events > 0).then(production_capacity_graph);
            if let Some(graph) = &production_graph {
                for document in &graph.documents {
                    documents.append(document).unwrap();
                }
                let six_roots = target_raw_events.saturating_mul(6);
                assert!(
                    target_relations >= six_roots
                        && target_relations <= target_raw_events.saturating_mul(7),
                    "production relation target must be between six and seven roots per event"
                );
            }
            let seventh_root_events =
                target_relations.saturating_sub(target_raw_events.saturating_mul(6));
            let mut observed_events = 0_u64;
            tidas_cli::visit_jsonl(Path::new(&issue_spool), |mut event| {
                if target_raw_events > 0 {
                    if observed_events >= target_raw_events {
                        return Ok(());
                    }
                    let graph = production_graph
                        .as_ref()
                        .expect("production graph requested by target");
                    let source_index = usize::from(observed_events >= seventh_root_events);
                    event["document_key"] = json!(graph.sources[source_index].document_key());
                    let base_location = event
                        .pointer("/issue/location")
                        .or_else(|| event.pointer("/issue/path"))
                        .and_then(Value::as_str)
                        .unwrap_or("$.productionCapacity");
                    event["issue"]["location"] =
                        json!(format!("{base_location}#capacity[{observed_events:06}]"));
                    pad_capacity_event(&mut event, 1_096);
                    observed_events = observed_events.saturating_add(1);
                } else if let Some(document_key) = package_issue_document_key(&event) {
                    event["document_key"] = json!(document_key);
                }
                event_writer.append(&event)
            })
            .unwrap();
            if target_raw_events > 0 {
                assert_eq!(observed_events, target_raw_events);
            }
            let (roots, reference_graph) = production_graph.map_or_else(
                || (Vec::new(), CompactReferenceGraph::default()),
                |graph| {
                    let reference_graph =
                        CompactReferenceGraph::from_references(&graph.references, &graph.roots)
                            .unwrap();
                    (graph.roots, reference_graph)
                },
            );
            (documents.finish().unwrap(), roots, reference_graph)
        } else {
            let multiplier = std::env::var("SCOPE_CLOSURE_SCALE_MULTIPLIER")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(1);
            assert!([1, 2, 5, 10].contains(&multiplier));
            let base_events = std::env::var("SCOPE_CLOSURE_SCALE_BASE_EVENTS")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(50_000);
            let event_count = base_events.checked_mul(multiplier).unwrap();
            let details = "x".repeat(480);
            let graph = production_capacity_graph();
            let reference_graph =
                CompactReferenceGraph::from_references(&graph.references, &graph.roots).unwrap();
            let mut documents = ClosureDocumentSpoolWriter::new().unwrap();
            for document in &graph.documents {
                documents.append(document).unwrap();
            }
            for index in 0..event_count {
                // Match the observed production relation density: one seventh-root source
                // occurrence for every four six-root source occurrences (6.2 roots/event).
                let source = &graph.sources[usize::from(!index.is_multiple_of(5))];
                let mut event = json!({
                    "type": "issue",
                    "document_key": source.document_key(),
                    "issue": {
                        "issue_code": "generated_high_unique",
                        "location": format!("$.generated[{index}]"),
                        "message": format!("generated high-unique issue {index}: {details}"),
                        "context": {
                            "distribution": "high-unique-key",
                            "ordinal": index,
                        },
                    },
                });
                pad_capacity_event(&mut event, 1_096);
                event_writer.append(&event).unwrap();
            }
            (documents.finish().unwrap(), graph.roots, reference_graph)
        };
        let issue_events = event_writer.finish().unwrap();
        let input_event_count = issue_events.event_count;
        let input_spool_bytes = issue_events.byte_size;
        let input_spool_sha256 = issue_events.sha256.clone();
        let validation = TidasBatchValidation {
            describe: json!({"asset_fingerprint": "issue-171-capacity"}),
            final_event: json!({
                "type": "final",
                "completed": true,
                "summary": {"issue_count": input_event_count},
            }),
            issue_events,
        };
        let mut scan = capacity_scan(documents, roots, reference_graph);
        let document_count = scan.documents.len();
        build_issue_relation_spools(&mut scan, &validation.issue_events).unwrap();
        let after_relation_runs = ResourceMeasurement::capture(
            "capacity_after_relation_runs",
            ResourceCounters::default(),
        );
        let closure_check_id = id("17117117-1171-4171-8171-171171171171");
        let input: ScopeClosureWorkerInput =
            serde_json::from_value(scope_closure_worker_input_json()).unwrap();
        let resolution_map =
            build_resolution_map_spool(&scan.edges, &scan.omitted_version_resolutions).unwrap();
        let closure_bundle =
            build_closure_bundle(&input, &validation, &scan, &resolution_map).unwrap();
        let artifacts =
            prepare_closure_content_artifacts(closure_bundle, closure_check_id, &scan, &validation)
                .unwrap();
        let reconstructed =
            reconstruct_complete_machine_result(&artifacts, closure_check_id).unwrap();
        let relations = scan.issue_relations.as_ref().unwrap();
        let mut partition_bytes = 0_u64;
        let mut total_artifact_bytes = 0_u64;
        let mut artifact_manifest = Vec::new();
        let mut closure_bundle_bytes = 0_u64;
        let mut closure_bundle_sha256 = String::new();
        let mut xlsx_bytes = 0_u64;
        let mut xlsx_sha256 = String::new();
        for artifact in &artifacts {
            let destination = output_dir.join(&artifact.descriptor.file_name);
            fs::create_dir_all(destination.parent().unwrap()).unwrap();
            fs::copy(&artifact.path, &destination).unwrap();
            let artifact_bytes = u64::try_from(artifact.descriptor.byte_size).unwrap();
            total_artifact_bytes = total_artifact_bytes.saturating_add(artifact_bytes);
            if artifact.descriptor.file_name == "closure-bundle-v3.json" {
                closure_bundle_bytes = artifact_bytes;
                artifact
                    .descriptor
                    .checksum_sha256
                    .clone_into(&mut closure_bundle_sha256);
            } else if artifact.descriptor.file_name == "closure-report-v1.xlsx" {
                xlsx_bytes = artifact_bytes;
                artifact
                    .descriptor
                    .checksum_sha256
                    .clone_into(&mut xlsx_sha256);
            }
            if artifact.descriptor.artifact_role == ScopeClosureArtifactRole::CompleteMachineResult
            {
                partition_bytes = partition_bytes.saturating_add(artifact_bytes);
            }
            artifact_manifest.push(artifact.descriptor.clone());
        }
        let mut temp_roots = BTreeSet::from([
            scan.documents._temp.path().to_path_buf(),
            scan.edges._temp.path().to_path_buf(),
            resolution_map._temp.path().to_path_buf(),
            validation.issue_events._temp.path().to_path_buf(),
            relations.issues._temp.path().to_path_buf(),
            relations.root_impact_index.temp.path().to_path_buf(),
        ]);
        for artifact in &artifacts {
            temp_roots.insert(artifact._temp.path().to_path_buf());
        }
        let temporary_bytes = temp_roots.iter().fold(0_u64, |total, path| {
            total.saturating_add(directory_bytes(path).unwrap_or(0))
        });
        let cache_reclaim_before = ResourceMeasurement::capture(
            "capacity_before_cache_reclaim",
            ResourceCounters {
                temp_bytes: Some(temporary_bytes),
                ..ResourceCounters::default()
            },
        );
        for artifact in &artifacts {
            let file = File::open(&artifact.path).unwrap();
            release_file_cache(&file);
        }
        let cache_reclaim_after = ResourceMeasurement::capture(
            "capacity_after_cache_reclaim",
            ResourceCounters {
                temp_bytes: Some(temporary_bytes),
                ..ResourceCounters::default()
            },
        );
        assert_eq!(reconstructed.issue_count, relations.stats.issue_count);
        assert_eq!(
            reconstructed.occurrence_count,
            relations.stats.occurrence_count
        );
        assert_eq!(
            reconstructed.affected_root_count,
            relations.stats.affected_root_count
        );
        assert_eq!(reconstructed.expanded_affected_root_record_count, 0);
        assert!(
            relations.root_impact_index.record_count <= relations.stats.issue_count,
            "root impact must be indexed once per source/exception, never once per relation"
        );
        if let Ok(target) = std::env::var("SCOPE_CLOSURE_PRODUCTION_RELATIONS") {
            let target = target.parse::<u64>().unwrap();
            assert_eq!(relations.stats.affected_root_count, target);
        } else if std::env::var_os("SCOPE_CLOSURE_REAL_PACKAGE_DIR").is_none() {
            let expected_relations = input_event_count
                .saturating_mul(6)
                .saturating_add(input_event_count.saturating_add(4) / 5);
            assert_eq!(relations.stats.affected_root_count, expected_relations);
            assert_eq!(
                relations.root_impact_index.record_count, 2,
                "the generated production distribution must stay source-indexed"
            );
        }
        let issue_partition_bytes =
            relations
                .issue_partition_artifacts
                .iter()
                .fold(0_u64, |total, artifact| {
                    total.saturating_add(
                        u64::try_from(artifact.descriptor.byte_size).unwrap_or(u64::MAX),
                    )
                });
        let after_artifacts = ResourceMeasurement::capture(
            "capacity_after_artifacts",
            ResourceCounters {
                temp_bytes: Some(
                    relations
                        .issues
                        .byte_size
                        .saturating_add(issue_partition_bytes)
                        .saturating_add(relations.root_impact_index.compressed_byte_size)
                        .saturating_add(total_artifact_bytes),
                ),
                rows: Some(
                    relations
                        .stats
                        .issue_count
                        .saturating_add(relations.root_impact_index.record_count),
                ),
                ..ResourceCounters::default()
            },
        );
        let summary = json!({
            "schemaVersion": "lcia.scope-closure-capacity-result.v2",
            "inputMode": if std::env::var_os("SCOPE_CLOSURE_REAL_PACKAGE_DIR").is_some() {
                "external-open-data-package"
            } else {
                "production-distribution-generated"
            },
            "scaleMultiplier": std::env::var("SCOPE_CLOSURE_SCALE_MULTIPLIER")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(1),
            "scaleBaseEvents": std::env::var("SCOPE_CLOSURE_SCALE_BASE_EVENTS")
                .ok()
                .and_then(|value| value.parse::<u64>().ok()),
            "documentCount": document_count,
            "inputEventCount": input_event_count,
            "inputSpoolBytes": input_spool_bytes,
            "inputSpoolSha256": input_spool_sha256,
            "issueCount": relations.stats.issue_count,
            "occurrenceCount": relations.stats.occurrence_count,
            "affectedRootCount": relations.stats.affected_root_count,
            "recoveredRelationCounts": {
                "issues": reconstructed.issue_count,
                "occurrences": reconstructed.occurrence_count,
                "affectedRoots": reconstructed.affected_root_count,
            },
            "machineResultReconstruction": &reconstructed,
            "physicalRepresentation": {
                "expandedAffectedRootRecords": reconstructed.expanded_affected_root_record_count,
                "issueSpoolBytes": relations.issues.byte_size,
                "issuePartitionBytes": issue_partition_bytes,
                "issuePartitionCount": relations.issue_partition_entries.len(),
                "rootImpactIndexBytes": relations.root_impact_index.compressed_byte_size,
                "rootImpactRecordCount": relations.root_impact_index.record_count,
            },
            "tempAdmission": {
                "strategy": "observed_raw_then_measured_topology_watermarks",
                "initialPlannedBytes": relation_temp_admission_bytes(
                    input_event_count,
                    input_spool_bytes,
                ),
                "initialRequiredBytes": relation_temp_admission_bytes(
                    input_event_count,
                    input_spool_bytes,
                )
                    .saturating_add(SCOPE_CLOSURE_TEMP_FREE_SPACE_RESERVE_BYTES),
                "reserveBytes": SCOPE_CLOSURE_TEMP_FREE_SPACE_RESERVE_BYTES,
            },
            "totalArtifactBytes": total_artifact_bytes,
            "artifactCount": artifacts.len(),
            "descriptorCount": artifacts.len(),
            "partitionAndManifestBytes": partition_bytes,
            "partitionAndManifestCount": artifacts
                .iter()
                .filter(|artifact| {
                    artifact.descriptor.artifact_role
                        == ScopeClosureArtifactRole::CompleteMachineResult
                })
                .count(),
            "partitionUncompressedBytes": reconstructed.uncompressed_byte_size,
            "temporaryBytes": temporary_bytes,
            "closureBundleBytes": closure_bundle_bytes,
            "closureBundleSha256": closure_bundle_sha256,
            "xlsxBytes": xlsx_bytes,
            "xlsxSha256": xlsx_sha256,
            "resourceMeasurements": {
                "afterRelationRuns": after_relation_runs,
                "afterArtifacts": after_artifacts,
            },
            "cacheReclaim": {
                "requestedFileCount": artifacts.len(),
                "mechanism": if cfg!(target_os = "linux") {
                    "posix_fadvise_dontneed"
                } else if cfg!(target_os = "macos") {
                    "f_nocache_and_sync"
                } else {
                    "best_effort_noop"
                },
                "before": cache_reclaim_before,
                "after": cache_reclaim_after,
            },
            "artifacts": artifact_manifest,
        });
        fs::write(
            output_dir.join("capacity-result.json"),
            canonical_json_bytes(&summary).unwrap(),
        )
        .unwrap();
        println!("{}", canonical_value(&summary));
    }

    #[test]
    #[ignore = "local regression proof: writes the production-shaped sidecar until the legacy 2 GiB cap fails"]
    fn qualified_legacy_affected_root_spool_reproduces_two_gib_cap() {
        let graph = production_capacity_graph();
        let root = &graph.roots[0];
        let intermediate = graph
            .documents
            .iter()
            .map(|document| &document.identity)
            .find(|identity| {
                identity.category == DatasetCategory::Processes && !graph.roots.contains(identity)
            })
            .unwrap();
        let witness = vec![root.clone(), intermediate.clone(), graph.sources[0].clone()];
        let mut writer =
            JsonlValueSpoolWriter::new("legacy-coalesced-affected-roots.jsonl").unwrap();
        let mut failure = None;
        for index in 0..3_200_000_u64 {
            let issue_key = format!("{index:064x}");
            let record = json!([
                issue_key,
                canonical_json_sha256(root).unwrap(),
                affected_root_partition_record(&format!("{index:064x}"), root, &witness)
            ]);
            if index == 0 {
                assert!(
                    canonical_json_bytes(&record).unwrap().len() + 1 >= 673,
                    "legacy reproduction record must retain the observed production size"
                );
            }
            if let Err(error) = writer.append(&record) {
                failure = Some((index + 1, error.to_string()));
                break;
            }
        }
        let (events, error) = failure.expect("legacy 2 GiB spool cap must fail");
        assert!(events < 3_200_000);
        assert!(error.contains("validation issue spool exceeded bounded capacity"));
        assert!(error.contains("2147483648"));
    }

    #[test]
    #[ignore = "local capacity gate: continuous production-sized affected-root partitions"]
    fn qualified_streaming_affected_root_partition_scale() {
        let multiplier = std::env::var("SCOPE_CLOSURE_SCALE_MULTIPLIER")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(1);
        assert!([1, 2, 5, 10].contains(&multiplier));
        let base_relations = std::env::var("SCOPE_CLOSURE_PRODUCTION_RELATIONS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(3_200_000);
        let relation_count = base_relations.checked_mul(multiplier).unwrap();
        let partition_mib = std::env::var("SCOPE_CLOSURE_PARTITION_CANDIDATE_MIB")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(32);
        assert!([16, 32, 64].contains(&partition_mib));
        let partition_bytes = partition_mib.checked_mul(1024 * 1024).unwrap();
        let graph = production_capacity_graph();
        let root = &graph.roots[0];
        let intermediate = graph
            .documents
            .iter()
            .map(|document| &document.identity)
            .find(|identity| {
                identity.category == DatasetCategory::Processes && !graph.roots.contains(identity)
            })
            .unwrap();
        let witness = vec![root.clone(), intermediate.clone(), graph.sources[0].clone()];
        let temp = Arc::new(TempDir::new().unwrap());
        let mut writer = IssuePartitionAccumulator::with_limits(
            Arc::clone(&temp),
            "affected-roots",
            ISSUE_PARTITION_MAX_RECORDS,
            partition_bytes,
        );
        for index in 0..relation_count {
            let issue_key = format!("{:064x}", index / 7);
            let selected_root = &graph.roots[usize::try_from(index % 7).unwrap()];
            writer
                .push(
                    &issue_key,
                    &affected_root_partition_record(&issue_key, selected_root, &witness),
                )
                .unwrap();
            if index.is_multiple_of(100_000) {
                enforce_scope_closure_memory_budget("qualified_relation_partition_scale").unwrap();
            }
        }
        let (entries, artifacts, _) = writer.finish().unwrap();
        let recovered_records = entries.iter().map(|entry| entry.record_count).sum::<u64>();
        let uncompressed_bytes = entries
            .iter()
            .map(|entry| entry.uncompressed_byte_size)
            .sum::<u64>();
        let compressed_bytes = entries
            .iter()
            .map(|entry| entry.compressed_byte_size)
            .sum::<u64>();
        assert_eq!(recovered_records, relation_count);
        assert_eq!(entries.len(), artifacts.len());
        let summary = json!({
            "schemaVersion": "lcia.scope-closure-relation-scale-result.v1",
            "multiplier": multiplier,
            "partitionCandidateMiB": partition_mib,
            "partitionMaxRecords": ISSUE_PARTITION_MAX_RECORDS,
            "relationCount": relation_count,
            "partitionCount": entries.len(),
            "uncompressedBytes": uncompressed_bytes,
            "compressedBytes": compressed_bytes,
            "manifestEntriesSha256": canonical_json_sha256(&entries).unwrap(),
            "resourceMeasurement": ResourceMeasurement::capture(
                "qualified_relation_partition_scale_complete",
                ResourceCounters {
                    temp_bytes: Some(directory_bytes(temp.path()).unwrap_or(0)),
                    rows: Some(relation_count),
                    ..ResourceCounters::default()
                }
            ),
        });
        if let Ok(output) = std::env::var("SCOPE_CLOSURE_SCALE_OUTPUT") {
            fs::write(output, canonical_json_bytes(&summary).unwrap()).unwrap();
        }
        println!("{}", canonical_value(&summary));
    }

    #[test]
    #[ignore = "requires a real Rust tidas binary selected by TIDAS_BIN"]
    fn release_tidas_non_empty_issue_stream_closes_raw_bytes() {
        let document = ClosureDocument {
            identity: identity(
                DatasetCategory::Sources,
                "dadadada-dada-4ada-8ada-dadadadadada",
            ),
            payload: json!({"sourceDataSet": {}}),
        };
        let validation =
            run_tidas_batch_validation(&[document], json!({"asset_fingerprint": "real-binary"}))
                .expect("real TIDAS batch stream must satisfy the raw-byte hash contract");
        assert!(
            validation.issue_events.event_count > 0,
            "fixture must exercise a non-empty TIDAS issue stream"
        );
    }

    #[test]
    fn file_backed_closure_bundle_v3_references_the_single_tidas_stream() {
        let input: ScopeClosureWorkerInput =
            serde_json::from_value(scope_closure_worker_input_json()).unwrap();
        let event = json!({
            "type": "issue",
            "document_key": "sources:1:01.00.000",
            "issue": {"code": "invalid"},
        });
        let mut spool = JsonlValueSpoolWriter::new("issues.jsonl").unwrap();
        spool.append(&event).unwrap();
        let validation = TidasBatchValidation {
            describe: json!({"asset_fingerprint": "fixture"}),
            final_event: json!({"type": "final", "completed": true}),
            issue_events: spool.finish().unwrap(),
        };
        let scan = ScopeClosureScan {
            schema_version: "lcia.scope-closure-scan.v1".to_owned(),
            complete: true,
            roots: Vec::new(),
            documents: ClosureDocumentSpool::empty().unwrap(),
            edges: JsonlValueSpool::empty("empty-edges.jsonl").unwrap(),
            resolved_references: JsonlValueSpool::empty("empty-resolved.jsonl").unwrap(),
            omitted_version_resolutions: Vec::new(),
            issues: Vec::new(),
            frontier: Vec::new(),
            provider_universe: Vec::new(),
            reference_graph: CompactReferenceGraph::default(),
            tidas_issue_event_count: 1,
            issue_relations: None,
        };
        let expected = json!({
            "schemaVersion": "lcia.scope-closure-bundle.v3",
            "requestedScopeHash": input.requested_scope_hash,
            "policyFingerprint": input.policy_fingerprint,
            "dataSnapshotToken": input.data_snapshot_token,
            "validatorScannerFingerprint": input.expected_validator_scanner_fingerprint,
            "tidasValidation": {
                "describe": validation.describe,
                "finalEvent": validation.final_event,
                "issueStream": {
                    "compression": "zstd",
                    "eventCount": validation.issue_events.event_count,
                    "logicalByteSize": validation.issue_events.byte_size,
                    "logicalSha256": validation.issue_events.sha256,
                    "path": "tidas/issues.ndjson.zst",
                    "schemaVersion": "lcia.scope-closure-tidas-issue-stream.v1",
                },
            },
            "scan": {
                "complete": true,
                "documents": [],
                "edges": [],
                "frontier": [],
                "issueSummary": {
                    "canonical": false,
                    "completeMachineResultClientKey": "manifest.json",
                    "issueCountBeforeTidasCoalescing": 0,
                    "issueSchemaVersion": "lcia.scope-closure-issue.v3",
                    "rawTidasIssueEventCount": 1,
                },
                "omittedVersionResolutions": [],
                "providerUniverse": [],
                "resolvedReferences": [],
                "roots": [],
                "schemaVersion": "lcia.scope-closure-scan.v1",
            },
            "resolutionMap": [],
        });

        let resolution_map =
            build_resolution_map_spool(&scan.edges, &scan.omitted_version_resolutions).unwrap();
        let bundle = build_closure_bundle(&input, &validation, &scan, &resolution_map).unwrap();

        assert_eq!(
            fs::read(&bundle.path).unwrap(),
            canonical_json_bytes(&expected).unwrap()
        );
        assert_eq!(
            bundle.sha256,
            sha256_hex(&canonical_json_bytes(&expected).unwrap())
        );
    }

    #[tokio::test]
    async fn traversal_is_batched_bounded_and_cooperatively_cancelled() {
        let roots = (0..200_u128)
            .map(|index| ExactDatasetIdentity {
                category: DatasetCategory::Processes,
                id: Uuid::from_u128(index + 1),
                version: "01.00.000".to_owned(),
            })
            .collect::<Vec<_>>();
        let documents = roots
            .iter()
            .map(|item| {
                (
                    item.clone(),
                    ClosureDocument {
                        identity: item.clone(),
                        payload: json!({}),
                    },
                )
            })
            .collect();
        let provider = FakeProvider {
            documents,
            ..FakeProvider::default()
        };
        let scan = collect_scope_closure(&provider, &manifest(roots.clone()))
            .await
            .unwrap();
        assert_eq!(scan.documents.len(), 200);
        assert_eq!(provider.fetches.lock().unwrap().len(), 3);

        let cancelled = FakeProvider {
            documents: provider.documents,
            fail_checkpoint: Some(2),
            ..FakeProvider::default()
        };
        let error = collect_scope_closure(&cancelled, &manifest(roots))
            .await
            .unwrap_err();
        assert_eq!(error.to_string(), "cancelled");
    }

    #[tokio::test]
    async fn scan_and_artifact_hashes_are_deterministic_across_fetch_order() {
        let root = identity(
            DatasetCategory::Processes,
            "cccccccc-cccc-cccc-cccc-cccccccccccc",
        );
        let child = identity(
            DatasetCategory::Sources,
            "dddddddd-dddd-dddd-dddd-dddddddddddd",
        );
        let documents = [
            ClosureDocument {
                identity: root.clone(),
                payload: json!({"referenceToSource": reference(
                    "source",
                    child.id,
                    Some("01.00.000")
                )}),
            },
            ClosureDocument {
                identity: child,
                payload: json!({}),
            },
        ]
        .into_iter()
        .map(|document| (document.identity.clone(), document))
        .collect::<BTreeMap<_, _>>();
        let normal = FakeProvider {
            documents: documents.clone(),
            ..FakeProvider::default()
        };
        let reversed = FakeProvider {
            documents,
            reverse_fetch: true,
            ..FakeProvider::default()
        };
        let left = collect_scope_closure(&normal, &manifest(vec![root.clone()]))
            .await
            .unwrap();
        let right = collect_scope_closure(&reversed, &manifest(vec![root]))
            .await
            .unwrap();
        assert_eq!(
            canonical_json_sha256(&left).unwrap(),
            canonical_json_sha256(&right).unwrap()
        );
    }

    #[test]
    fn xlsx_report_is_valid_zip_and_tagged_to_current_run() {
        let closure_check_id = Uuid::new_v4();
        let bytes = build_xlsx_report(closure_check_id, &[]).unwrap();
        let mut archive = ZipArchive::new(Cursor::new(bytes)).unwrap();
        let mut workbook = String::new();
        std::io::Read::read_to_string(
            &mut archive.by_name("xl/workbook.xml").unwrap(),
            &mut workbook,
        )
        .unwrap();
        for name in [
            "Summary",
            "Closure Issues",
            "Occurrences",
            "Affected Datasets",
        ] {
            assert!(workbook.contains(format!("name=\"{name}\"").as_str()));
        }
        let mut worksheet = String::new();
        std::io::Read::read_to_string(
            &mut archive.by_name("xl/worksheets/sheet1.xml").unwrap(),
            &mut worksheet,
        )
        .unwrap();
        assert!(worksheet.contains(closure_check_id.to_string().as_str()));
        for sheet_number in 2..=4 {
            assert!(
                archive
                    .by_name(format!("xl/worksheets/sheet{sheet_number}.xml").as_str())
                    .is_ok()
            );
        }
    }

    #[test]
    fn short_exact_versions_are_normalized_without_changing_omitted_semantics() {
        assert_eq!(normalize_exact_version("01.02").unwrap(), "01.02.000");
        assert_eq!(normalize_exact_version("01.02.003").unwrap(), "01.02.003");
        assert!(normalize_exact_version("01").is_err());
    }

    #[test]
    fn frozen_artifact_freshness_does_not_require_current_membership() {
        assert!(freshness_policy_accepts_current_release(
            "frozen-artifact-reusable-v1",
            false
        ));
        assert!(!freshness_policy_accepts_current_release(
            "current-membership-required-v1",
            false
        ));
        assert!(freshness_policy_accepts_current_release(
            "current-membership-required-v1",
            true
        ));
        assert!(!freshness_policy_accepts_current_release("unknown", true));
    }

    #[test]
    fn coalesced_issue_preserves_each_reference_occurrence() {
        let target = identity(
            DatasetCategory::Flows,
            "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee",
        );
        let mut process_issue = missing_dataset_issue(&target, true);
        process_issue.occurrences = vec![ClosureIssueOccurrence {
            occurrence_key: "process-exchange".to_owned(),
            source: Some(identity(
                DatasetCategory::Processes,
                "ffffffff-ffff-ffff-ffff-ffffffffffff",
            )),
            json_path: Some("$.exchanges[0]".to_owned()),
            reference_role: Some("exchange_flow".to_owned()),
            details: json!({}),
        }];
        let mut method_issue = process_issue.clone();
        method_issue.occurrences = vec![ClosureIssueOccurrence {
            occurrence_key: "lcia-factor".to_owned(),
            source: Some(identity(
                DatasetCategory::Lciamethods,
                "abababab-abab-abab-abab-abababababab",
            )),
            json_path: Some("$.factors[0]".to_owned()),
            reference_role: Some("lcia_factor_flow".to_owned()),
            details: json!({}),
        }];

        let issues = coalesce_issues(vec![process_issue, method_issue]);
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].occurrence_count, 2);
        assert_eq!(
            issues[0]
                .occurrences
                .iter()
                .filter_map(|occurrence| occurrence.reference_role.as_deref())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["exchange_flow", "lcia_factor_flow"])
        );
        assert_eq!(
            issue_rpc_projection(&issues[0])["occurrences"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
    }

    #[test]
    fn scan_claim_parser_distinguishes_acquired_busy_and_completed() {
        assert_eq!(
            parse_scan_execution_claim(&json!({"acquired": true})).unwrap(),
            ScanExecutionClaim::Acquired
        );
        assert_eq!(
            parse_scan_execution_claim(&json!({"acquired": false, "completed": false})).unwrap(),
            ScanExecutionClaim::Busy
        );
        let completed_check_id = Uuid::new_v4();
        assert_eq!(
            parse_scan_execution_claim(&json!({
                "acquired": false,
                "completed": true,
                "completedCheckId": completed_check_id,
            }))
            .unwrap(),
            ScanExecutionClaim::Completed { completed_check_id }
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn blocked_closure_has_no_numerical_snapshot_or_pseudo_snapshot_artifact() {
        let missing = identity(
            DatasetCategory::Processes,
            "91919191-9191-9191-9191-919191919191",
        );
        let mut scan = ScopeClosureScan {
            schema_version: "lcia.scope-closure-scan.v1".to_owned(),
            complete: true,
            roots: vec![missing.clone()],
            documents: ClosureDocumentSpool::empty().unwrap(),
            edges: JsonlValueSpool::empty("empty-edges.jsonl").unwrap(),
            resolved_references: JsonlValueSpool::empty("empty-resolved.jsonl").unwrap(),
            omitted_version_resolutions: Vec::new(),
            issues: vec![missing_dataset_issue(&missing, true)],
            frontier: Vec::new(),
            provider_universe: Vec::new(),
            reference_graph: CompactReferenceGraph::from_references(
                &[],
                std::slice::from_ref(&missing),
            )
            .unwrap(),
            tidas_issue_event_count: 0,
            issue_relations: None,
        };
        assert!(!closure_scan_allows_numerical_snapshot(&scan));

        let evidence = administrative_only_evidence(
            "1".repeat(64),
            "2".repeat(64),
            "3".repeat(64),
            id("90909090-9090-4090-8090-909090909090"),
            "4".repeat(64),
        );
        assert_eq!(evidence.snapshot_id, None);
        assert_eq!(evidence.snapshot_hash, None);
        assert_eq!(evidence.snapshot_artifact_id, None);
        assert_eq!(evidence.snapshot_index_sha256, None);
        assert_eq!(evidence.snapshot_build_contract_hash, None);
        assert_eq!(evidence.evidence_hash, None);

        let temp = Arc::new(TempDir::new().unwrap());
        let path = temp.path().join("closure-bundle-v3.json");
        let bytes = br#"{"schemaVersion":"lcia.scope-closure-bundle.v3"}"#;
        fs::write(&path, bytes).unwrap();
        let validation = TidasBatchValidation {
            describe: json!({"asset_fingerprint": "fixture"}),
            final_event: json!({"type": "final", "completed": true}),
            issue_events: JsonlValueSpool::empty("empty-validation-issues.jsonl").unwrap(),
        };
        build_issue_relation_spools(&mut scan, &validation.issue_events).unwrap();
        let artifacts = prepare_closure_content_artifacts(
            ClosureBundleFile {
                temp,
                path,
                byte_size: u64::try_from(bytes.len()).unwrap(),
                sha256: sha256_hex(bytes),
            },
            id("91919191-9191-4191-8191-919191919191"),
            &scan,
            &validation,
        )
        .unwrap();
        let names = artifacts
            .iter()
            .map(|artifact| artifact.descriptor.file_name.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            names,
            BTreeSet::from([
                "closure-bundle-v3.json",
                "closure-report-v1.xlsx",
                "evidence/frozen-reference-graph-v1.bin.zst",
                "evidence/root-impact-index-v1.bin.zst",
                "issues/part-000000.ndjson.zst",
                "manifest.json",
                "tidas/issues.ndjson.zst",
            ])
        );
        assert!(!names.contains("closure-snapshot-v1.json"));
        let roles = artifacts
            .iter()
            .map(|artifact| {
                (
                    artifact.descriptor.file_name.as_str(),
                    artifact.descriptor.artifact_role,
                )
            })
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            roles["closure-bundle-v3.json"],
            ScopeClosureArtifactRole::ClosureBundle
        );
        assert_eq!(
            roles["closure-report-v1.xlsx"],
            ScopeClosureArtifactRole::ClosureReport
        );
        assert_eq!(
            roles["manifest.json"],
            ScopeClosureArtifactRole::CompleteMachineResult
        );
        assert_eq!(
            roles["issues/part-000000.ndjson.zst"],
            ScopeClosureArtifactRole::CompleteMachineResult
        );
        assert!(artifacts.iter().all(|artifact| {
            artifact.descriptor.artifact_role != ScopeClosureArtifactRole::CompleteMachineResult
                || artifact.descriptor.artifact_type == "closure_complete_machine_result"
        }));
    }

    #[test]
    fn scope_closure_publication_metadata_uses_db_first_manifest_binding() {
        assert_eq!(SCOPE_CLOSURE_ARTIFACT_RETENTION_SECONDS, 604_800);
        assert_eq!(SCOPE_CLOSURE_ARTIFACT_STAGING_SECONDS, 3_600);

        let temp = Arc::new(TempDir::new().unwrap());
        let report = PreparedArtifact {
            descriptor: ArtifactManifestEntry {
                artifact_type: "closure_report_xlsx".to_owned(),
                artifact_role: ScopeClosureArtifactRole::ClosureReport,
                file_name: "closure-report-v1.xlsx".to_owned(),
                content_type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    .to_owned(),
                byte_size: 0,
                checksum_sha256: "a".repeat(64),
            },
            path: temp.path().join("closure-report-v1.xlsx"),
            _temp: temp,
        };
        let report_metadata = closure_artifact_metadata(&report, Uuid::nil(), "manifest");
        assert!(
            report_metadata
                .get("completeMachineResultClientKey")
                .is_none()
        );
        assert!(report_metadata.get("writeSetId").is_none());
        assert!(report_metadata.get("lifecycleState").is_none());
        let mut bundle = report.clone();
        bundle.descriptor.artifact_role = ScopeClosureArtifactRole::ClosureBundle;
        let bundle_metadata = closure_artifact_metadata(&bundle, Uuid::nil(), "manifest");
        assert_eq!(
            bundle_metadata["completeMachineResultClientKey"], "manifest.json",
            "Database finalize resolves the preallocated manifest UUID atomically"
        );
        assert!(
            bundle_metadata
                .get("completeMachineResultArtifactId")
                .is_none()
        );
    }

    #[test]
    fn database_316_descriptor_fixture_digest_and_ordinals_match_exactly() {
        let closure_check_id = "11111111-1111-4111-8111-111111111111";
        let content_hash = "a".repeat(64);
        let descriptors = vec![
            json!({
                "ordinal": 1,
                "clientKey": "closure-bundle-v3.json",
                "artifactType": "closure_bundle",
                "artifactRole": "closure_bundle",
                "bucket": "scope-closure-artifacts",
                "objectPath": "scope-closure/11111111-1111-4111-8111-111111111111/99999999-9999-4999-8999-999999999999/closure-bundle-v3.json",
                "mediaType": "application/json",
                "size": 128,
                "checksumSha256": "1".repeat(64),
                "metadata": {
                    "schemaVersion": "lcia.scope-closure-artifact.v2",
                    "closureCheckId": closure_check_id,
                    "fileName": "closure-bundle-v3.json",
                    "artifactRole": "closure_bundle",
                    "retentionSeconds": 604_800,
                    "contentArtifactManifestHash": content_hash,
                    "completeMachineResultClientKey": "manifest.json",
                },
            }),
            json!({
                "ordinal": 2,
                "clientKey": "closure-report-v3.xlsx",
                "artifactType": "closure_report_xlsx",
                "artifactRole": "closure_report",
                "bucket": "scope-closure-artifacts",
                "objectPath": "scope-closure/11111111-1111-4111-8111-111111111111/99999999-9999-4999-8999-999999999999/closure-report-v3.xlsx",
                "mediaType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "size": 256,
                "checksumSha256": "2".repeat(64),
                "metadata": {
                    "schemaVersion": "lcia.scope-closure-artifact.v2",
                    "closureCheckId": closure_check_id,
                    "fileName": "closure-report-v3.xlsx",
                    "artifactRole": "closure_report",
                    "retentionSeconds": 604_800,
                    "contentArtifactManifestHash": content_hash,
                },
            }),
            json!({
                "ordinal": 3,
                "clientKey": "issues/part-000000.ndjson.zst",
                "artifactType": "closure_complete_machine_result",
                "artifactRole": "complete_machine_result",
                "bucket": "scope-closure-artifacts",
                "objectPath": "scope-closure/11111111-1111-4111-8111-111111111111/99999999-9999-4999-8999-999999999999/issues/part-000000.ndjson.zst",
                "mediaType": "application/x-ndjson+zstd",
                "size": 512,
                "checksumSha256": "3".repeat(64),
                "metadata": {
                    "schemaVersion": "lcia.scope-closure-artifact.v2",
                    "closureCheckId": closure_check_id,
                    "fileName": "issues/part-000000.ndjson.zst",
                    "artifactRole": "complete_machine_result",
                    "retentionSeconds": 604_800,
                    "contentArtifactManifestHash": content_hash,
                },
            }),
            json!({
                "ordinal": 4,
                "clientKey": "manifest.json",
                "artifactType": "closure_complete_machine_result",
                "artifactRole": "complete_machine_result",
                "bucket": "scope-closure-artifacts",
                "objectPath": "scope-closure/11111111-1111-4111-8111-111111111111/99999999-9999-4999-8999-999999999999/manifest.json",
                "mediaType": "application/vnd.tiangong.scope-closure-manifest+json",
                "size": 1024,
                "checksumSha256": "4".repeat(64),
                "metadata": {
                    "schemaVersion": "lcia.scope-closure-artifact.v2",
                    "closureCheckId": closure_check_id,
                    "fileName": "manifest.json",
                    "artifactRole": "complete_machine_result",
                    "retentionSeconds": 604_800,
                    "contentArtifactManifestHash": content_hash,
                },
            }),
        ];
        assert_eq!(
            canonical_descriptor_set_sha256(&descriptors).unwrap(),
            "11723d5becbb3c1c3a9a3c6d7d23f021044f260857558c4520c40614fd14e27f"
        );
        assert_eq!(ARTIFACT_REGISTRATION_BATCH_SIZE, 500);
        assert_eq!(
            descriptors
                .iter()
                .map(|descriptor| descriptor["ordinal"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );
        let request_identity = "scope-closure-write-set-v2:fixture";
        assert_eq!(
            deterministic_contract_uuid(request_identity),
            deterministic_contract_uuid(request_identity)
        );
    }

    #[test]
    fn database_316_registration_retry_uses_exact_status_readback() {
        let expected = test_artifact_write_set_header("registration_open");
        let recovered = resolve_artifact_registration_readback(
            Err(anyhow::anyhow!("simulated response loss after commit")),
            Ok(expected.clone()),
        )
        .unwrap();
        assert_eq!(recovered.write_set_id, expected.write_set_id);
        assert_eq!(recovered.registered_descriptor_count, 1);
        assert_eq!(recovered.registered_batch_count, 1);

        let status_lost = resolve_artifact_registration_readback(
            Ok(()),
            Err(anyhow::anyhow!("simulated status outage")),
        )
        .unwrap_err()
        .to_string();
        assert!(status_lost.contains("artifact registration status readback failed"));

        let both_lost = format!(
            "{:#}",
            resolve_artifact_registration_readback(
                Err(anyhow::anyhow!("simulated batch failure")),
                Err(anyhow::anyhow!("simulated status outage")),
            )
            .unwrap_err()
        );
        assert!(both_lost.contains("simulated batch failure"));
        assert!(both_lost.contains("simulated status outage"));
    }

    #[test]
    fn database_316_seal_retry_never_guesses_upload_eligibility() {
        let staging = test_artifact_write_set_header("staging");
        let (recovered, seal_error) = resolve_artifact_seal_readback(
            Err(anyhow::anyhow!("simulated seal response loss")),
            Ok(staging),
        )
        .unwrap();
        assert_eq!(recovered.status, "staging");
        assert!(recovered.upload_eligible);
        assert!(seal_error.is_some());

        let unknown =
            resolve_artifact_seal_readback(Ok(()), Err(anyhow::anyhow!("simulated status outage")))
                .unwrap_err()
                .to_string();
        assert!(unknown.contains("upload was not started"));

        let registration_open = test_artifact_write_set_header("registration_open");
        let (not_sealed, seal_error) = resolve_artifact_seal_readback(
            Err(anyhow::anyhow!("simulated seal rejection")),
            Ok(registration_open),
        )
        .unwrap();
        assert_eq!(not_sealed.status, "registration_open");
        assert!(!not_sealed.upload_eligible);
        assert!(seal_error.is_some());
    }

    #[test]
    fn database_316_state_fence_blocks_upload_before_atomic_seal() {
        let closure_check_id = id("17717717-0177-4177-8177-177177177177");
        let worker_job_id = id("17717717-0277-4277-8277-177177177177");
        let request_id = id("17717717-0477-4477-8477-177177177177");
        let artifact_id = id("17717717-0577-4577-8577-177177177177");
        let digest = "d".repeat(64);
        let required_primary_roles = closure_artifact_required_primary_roles(None);
        let temp = Arc::new(TempDir::new().unwrap());
        let artifact = PreparedArtifact {
            descriptor: ArtifactManifestEntry {
                artifact_type: "closure_complete_machine_result".to_owned(),
                artifact_role: ScopeClosureArtifactRole::CompleteMachineResult,
                file_name: "manifest.json".to_owned(),
                content_type: "application/vnd.tiangong.scope-closure-manifest+json".to_owned(),
                byte_size: 0,
                checksum_sha256: sha256_hex(&[]),
            },
            path: temp.path().join("manifest.json"),
            _temp: temp,
        };
        let header = |status: &str,
                      upload_eligible: bool,
                      registered_descriptor_count: u64,
                      artifact_map: BTreeMap<String, Uuid>| {
            ScopeClosureArtifactWriteSetHeader {
                write_set_id: id("17717717-0677-4677-8677-177177177177"),
                closure_check_id,
                worker_job_id,
                request_id,
                publication_mode: "fresh".to_owned(),
                reused_from_check_id: None,
                status: status.to_owned(),
                write_token: id("17717717-0777-4777-8777-177177177177"),
                contract_version: "lcia.scope-closure-artifact-write-set.v2".to_owned(),
                expected_descriptor_count: 1,
                registered_descriptor_count,
                registered_batch_count: u64::from(registered_descriptor_count > 0),
                descriptor_set_sha256: digest.clone(),
                required_primary_roles: required_primary_roles.clone(),
                upload_eligible,
                artifact_map,
                batches: if registered_descriptor_count == 0 {
                    Vec::new()
                } else {
                    vec![ScopeClosureArtifactWriteSetBatch {
                        batch_id: id("17717717-0877-4877-8877-177177177177"),
                        item_count: 1,
                        first_ordinal: 1,
                        last_ordinal: 1,
                    }]
                },
            }
        };
        let expectation = ScopeClosureArtifactWriteSetExpectation {
            closure_check_id,
            worker_job_id,
            request_id,
            publication_mode: "fresh",
            reused_from_check_id: None,
            expected_descriptor_count: 1,
            descriptor_set_sha256: &digest,
            required_primary_roles: &required_primary_roles,
        };

        let registration = header("registration_open", false, 1, BTreeMap::new());
        validate_closure_artifact_write_set_header(
            &registration,
            &expectation,
            "registration_open",
            false,
        )
        .unwrap();
        assert!(
            validate_closure_artifact_write_set_header(
                &registration,
                &expectation,
                "staging",
                true,
            )
            .is_err(),
            "registration_open must never satisfy the upload fence"
        );

        let artifact_map = BTreeMap::from([("manifest.json".to_owned(), artifact_id)]);
        let staging = header("staging", true, 1, artifact_map.clone());
        validate_closure_artifact_write_set_header(&staging, &expectation, "staging", true)
            .unwrap();
        validate_closure_artifact_map(&staging, std::slice::from_ref(&artifact)).unwrap();

        let incomplete_map = header("staging", true, 1, BTreeMap::new());
        assert!(
            validate_closure_artifact_map(&incomplete_map, std::slice::from_ref(&artifact))
                .is_err()
        );
        let ready = header("ready", false, 1, artifact_map);
        validate_closure_artifact_write_set_header(&ready, &expectation, "ready", false).unwrap();
        validate_closure_artifact_map(&ready, std::slice::from_ref(&artifact)).unwrap();
    }

    #[tokio::test]
    async fn slow_artifact_operation_renews_lease_across_multiple_periods() {
        let heartbeats = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&heartbeats);
        let result = supervise_cancellable_operation(
            async {
                tokio::time::sleep(Duration::from_millis(110)).await;
                Ok::<_, anyhow::Error>("uploaded")
            },
            CancellationToken::default(),
            Duration::from_millis(25),
            move || {
                observed.fetch_add(1, Ordering::SeqCst);
                async { Ok(()) }
            },
        )
        .await
        .unwrap();

        assert_eq!(result, "uploaded");
        assert!(
            heartbeats.load(Ordering::SeqCst) >= 5,
            "initial heartbeat plus at least four periodic heartbeats are required"
        );
    }

    #[tokio::test]
    async fn artifact_operation_is_really_cancelled_when_lease_is_lost_mid_request() {
        let cancellation = CancellationToken::default();
        let operation_cancellation = cancellation.clone();
        let cleanup_observed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let operation_cleanup = Arc::clone(&cleanup_observed);
        let heartbeats = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&heartbeats);

        let error = supervise_cancellable_operation(
            async move {
                while !operation_cancellation.is_cancelled() {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                operation_cleanup.store(true, Ordering::SeqCst);
                Err::<(), _>(anyhow::anyhow!("multipart aborted"))
            },
            cancellation.clone(),
            Duration::from_millis(20),
            move || {
                let call = observed.fetch_add(1, Ordering::SeqCst);
                async move {
                    if call >= 2 {
                        Err(anyhow::anyhow!("lease lost"))
                    } else {
                        Ok(())
                    }
                }
            },
        )
        .await
        .unwrap_err();

        assert!(format!("{error:#}").contains("lease lost"));
        assert!(cancellation.is_cancelled());
        assert!(cleanup_observed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn final_artifact_lease_loss_blocks_ready_finalize() {
        let finalize_calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&finalize_calls);
        let error = fence_closure_artifact_finalize(
            || async { Err(anyhow::anyhow!("lease lost after final artifact")) },
            move || {
                observed.fetch_add(1, Ordering::SeqCst);
                async { Ok::<_, anyhow::Error>("ready") }
            },
        )
        .await
        .unwrap_err();

        assert!(format!("{error:#}").contains("lease lost after final artifact"));
        assert_eq!(
            finalize_calls.load(Ordering::SeqCst),
            0,
            "a failed final heartbeat must never invoke the only ready-row transition"
        );
    }

    #[test]
    fn passed_evidence_is_bound_to_persisted_snapshot_builder_facts() {
        let facts = ScopeClosureSnapshotFacts {
            snapshot_id: id("92929292-9292-9292-9292-929292929292"),
            snapshot_hash: "5".repeat(64),
            snapshot_artifact_id: id("93939393-9393-9393-9393-939393939393"),
            snapshot_index_sha256: "6".repeat(64),
            snapshot_build_contract_hash: "7".repeat(64),
            artifact_format: "snapshot-hdf5:v1".to_owned(),
        };
        let source_fingerprint = "1".repeat(64);
        let resolution_map_hash = "2".repeat(64);
        let closure_bundle_hash = "3".repeat(64);
        let closure_bundle_artifact_id = id("94949494-9494-4494-8494-949494949494");
        let evidence = evidence_from_snapshot_facts(
            source_fingerprint.clone(),
            resolution_map_hash.clone(),
            closure_bundle_hash.clone(),
            closure_bundle_artifact_id,
            "4".repeat(64),
            &facts,
        );

        assert_eq!(evidence.schema_version, "lcia.scope-closure-evidence.v2");
        assert_eq!(
            evidence.closure_bundle_artifact_id,
            closure_bundle_artifact_id
        );
        assert_eq!(evidence.snapshot_id, Some(facts.snapshot_id));
        assert_eq!(
            evidence.snapshot_hash.as_deref(),
            Some(facts.snapshot_hash.as_str())
        );
        assert_eq!(
            evidence.snapshot_artifact_id,
            Some(facts.snapshot_artifact_id)
        );
        assert_eq!(
            evidence.snapshot_index_sha256.as_deref(),
            Some(facts.snapshot_index_sha256.as_str())
        );
        assert_eq!(
            evidence.snapshot_build_contract_hash.as_deref(),
            Some(facts.snapshot_build_contract_hash.as_str())
        );
        assert_eq!(
            evidence.artifact_format.as_deref(),
            Some("snapshot-hdf5:v1")
        );
        assert_eq!(
            evidence.evidence_hash,
            Some(scope_closure_evidence_hash(
                source_fingerprint.as_str(),
                resolution_map_hash.as_str(),
                closure_bundle_hash.as_str(),
                closure_bundle_artifact_id,
                &facts,
            ))
        );
        assert_ne!(
            evidence.evidence_hash,
            Some(scope_closure_evidence_hash(
                source_fingerprint.as_str(),
                resolution_map_hash.as_str(),
                closure_bundle_hash.as_str(),
                id("95959595-9595-4595-8595-959595959595"),
                &facts,
            ))
        );

        let mut missing_bundle_artifact_id = serde_json::to_value(&evidence).unwrap();
        missing_bundle_artifact_id
            .as_object_mut()
            .unwrap()
            .remove("closureBundleArtifactId");
        assert!(
            serde_json::from_value::<ScopeClosureEvidence>(missing_bundle_artifact_id).is_err()
        );
        ensure_closure_bundle_artifact_projection(&evidence, closure_bundle_artifact_id)
            .expect("evidence and record_result_v3 projection agree");
        assert!(
            ensure_closure_bundle_artifact_projection(
                &evidence,
                id("96969696-9696-4696-8696-969696969696")
            )
            .is_err()
        );
    }

    #[test]
    fn discovered_provider_processes_freeze_the_final_exact_axis() {
        let root = identity(
            DatasetCategory::Processes,
            "94949494-9494-9494-9494-949494949494",
        );
        let provider = identity(
            DatasetCategory::Processes,
            "95959595-9595-9595-9595-959595959595",
        );
        let frozen = freeze_discovered_process_axis(
            &manifest(vec![root.clone()]),
            &[
                ScopeClosureDiscoveredProcess {
                    id: root.id,
                    version: root.version,
                },
                ScopeClosureDiscoveredProcess {
                    id: provider.id,
                    version: provider.version,
                },
            ],
        )
        .unwrap();
        assert_eq!(frozen.processes.len(), 2);
        assert!(frozen.process_manifest_hash.is_some());
        assert_eq!(scope_process_axis(&frozen).len(), 2);
    }

    #[test]
    fn exact_document_query_uses_valid_parameterized_tuple_syntax() {
        let first = identity(
            DatasetCategory::Lciamethods,
            "96969696-9696-9696-9696-969696969696",
        );
        let second = identity(
            DatasetCategory::Lciamethods,
            "97979797-9797-9797-9797-979797979797",
        );

        let mut single = exact_documents_query_builder(
            DatasetCategory::Lciamethods,
            &[(first.clone(), first.id)],
        );
        assert_eq!(
            single.build().sql(),
            "SELECT id, btrim(version::text) AS version, COALESCE(json, json_ordered::jsonb) AS document FROM public.lciamethods WHERE (id, btrim(version::text)) IN (($1, $2)) ORDER BY id, btrim(version::text)"
        );

        let mut multiple = exact_documents_query_builder(
            DatasetCategory::Lciamethods,
            &[(first.clone(), first.id), (second.clone(), second.id)],
        );
        assert_eq!(
            multiple.build().sql(),
            "SELECT id, btrim(version::text) AS version, COALESCE(json, json_ordered::jsonb) AS document FROM public.lciamethods WHERE (id, btrim(version::text)) IN (($1, $2), ($3, $4)) ORDER BY id, btrim(version::text)"
        );
    }

    #[test]
    fn database_issue_projection_normalizes_only_supported_severities() {
        let target = identity(
            DatasetCategory::Processes,
            "98989898-9898-9898-9898-989898989898",
        );
        let mut blocking = missing_dataset_issue(&target, true);
        blocking.severity = "error".to_owned();
        let mut warning = blocking.clone();
        warning.blocking = false;
        warning.severity = "warning".to_owned();
        let mut info = warning.clone();
        info.severity = "info".to_owned();
        let mut issues = vec![blocking, warning, info];

        normalize_database_issue_severities(&mut issues).expect("supported projection");
        assert_eq!(issues[0].severity, "blocker");
        assert_eq!(issues[1].severity, "warning");
        assert_eq!(issues[2].severity, "info");

        issues[1].severity = "error".to_owned();
        assert!(normalize_database_issue_severities(&mut issues).is_err());
        issues[1].severity = "warning".to_owned();
        issues[2].severity = "unknown".to_owned();
        assert!(normalize_database_issue_severities(&mut issues).is_err());
    }

    #[test]
    fn worker_input_requires_the_database_preallocated_snapshot_identity() {
        let value = scope_closure_worker_input_json();
        let input: ScopeClosureWorkerInput =
            serde_json::from_value(value.clone()).expect("exact database worker input");
        assert_eq!(
            input.numerical_snapshot_id,
            id("30303030-3030-4030-8030-303030303030")
        );

        let mut missing = value.clone();
        missing
            .as_object_mut()
            .expect("worker input object")
            .remove("numericalSnapshotId");
        assert!(serde_json::from_value::<ScopeClosureWorkerInput>(missing).is_err());

        let mut unknown = value;
        unknown
            .as_object_mut()
            .expect("worker input object")
            .insert("unexpectedField".to_owned(), json!(true));
        assert!(serde_json::from_value::<ScopeClosureWorkerInput>(unknown).is_err());
    }

    #[test]
    fn final_builder_must_preserve_the_database_preallocated_snapshot_identity() {
        let expected = id("40404040-4040-4040-8040-404040404040");
        ensure_preallocated_snapshot_identity(expected, expected)
            .expect("matching preallocated snapshot identity");

        let error = ensure_preallocated_snapshot_identity(
            expected,
            id("50505050-5050-4050-8050-505050505050"),
        )
        .expect_err("builder identity drift must fail closed");
        assert!(error.to_string().contains("database-preallocated identity"));
    }

    #[test]
    fn cached_canonical_sort_preserves_canonical_order() {
        let mut values = vec![
            json!({"z": 1, "a": 2}),
            json!({"a": 1}),
            json!([2, 1]),
            json!(null),
        ];
        let mut expected = values.clone();
        expected.sort_by_key(canonical_value);

        sort_by_canonical_value(&mut values);

        assert_eq!(values, expected);
    }

    #[test]
    fn issue_heavy_finalization_is_bounded_and_orders_by_stable_issue_key() {
        const ISSUE_COUNT: usize = 50_000;
        let raw_issues = (0..ISSUE_COUNT)
            .rev()
            .map(|index| ClosureIssue {
                issue_key: format!("issue-{index:06}"),
                severity: "warning".to_owned(),
                blocking: false,
                issue_code: "test_issue".to_owned(),
                source: None,
                json_path: None,
                reference_role: None,
                requested_target_type: None,
                requested_target_id: None,
                requested_target_version: None,
                message: "representative issue-heavy finalization fixture".to_owned(),
                suggested_action: None,
                occurrence_count: 0,
                occurrences: Vec::new(),
                affected_root_count: 0,
                affected_roots: Vec::new(),
                affected_root_witness_paths: Vec::new(),
                witness_path: Vec::new(),
            })
            .collect();

        let started = Instant::now();
        let (scan, metrics) = finalize_scope_closure_scan(
            &JsonlValueSpool::empty("empty-finalize-edges.jsonl").unwrap(),
            Vec::new(),
            Vec::new(),
            raw_issues,
            &[],
            true,
            ClosureDocumentSpool::empty().unwrap(),
            BTreeSet::new(),
        )
        .unwrap();

        assert_eq!(scan.issues.len(), ISSUE_COUNT);
        assert_eq!(
            scan.issues.first().map(|issue| issue.issue_key.as_str()),
            Some("issue-000000")
        );
        assert_eq!(
            scan.issues.last().map(|issue| issue.issue_key.as_str()),
            Some("issue-049999")
        );
        assert!(metrics.total <= started.elapsed());
        assert!(
            started.elapsed().as_secs() < 10,
            "issue-heavy finalization took {:?}, expected under 10s",
            started.elapsed()
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn manifest_streaming_reconstructs_complete_relations_beyond_inline_sample() {
        let support = identity(
            DatasetCategory::Sources,
            "abababab-abab-4bab-8bab-abababababab",
        );
        let roots = (0..101_u128)
            .map(|index| ExactDatasetIdentity {
                category: DatasetCategory::Processes,
                id: Uuid::from_u128(index + 1),
                version: "01.00.000".to_owned(),
            })
            .collect::<Vec<_>>();
        let mut documents = BTreeMap::from([(
            support.clone(),
            ClosureDocument {
                identity: support.clone(),
                payload: json!({}),
            },
        )]);
        for root in &roots {
            documents.insert(
                root.clone(),
                ClosureDocument {
                    identity: root.clone(),
                    payload: json!({
                        "referenceToSource": reference("source", support.id, Some("01.00.000"))
                    }),
                },
            );
        }
        let provider = FakeProvider {
            documents,
            ..FakeProvider::default()
        };
        let mut scan = collect_scope_closure(&provider, &manifest(roots.clone()))
            .await
            .unwrap();
        scan.issues = vec![ClosureIssue {
            issue_key: "shared-support-issue".to_owned(),
            severity: "warning".to_owned(),
            blocking: false,
            issue_code: "generated_support_issue".to_owned(),
            source: Some(support.clone()),
            json_path: None,
            reference_role: None,
            requested_target_type: None,
            requested_target_id: None,
            requested_target_version: None,
            message: "generated support issue".to_owned(),
            suggested_action: None,
            occurrence_count: 1,
            occurrences: vec![ClosureIssueOccurrence {
                occurrence_key: "shared-support-occurrence".to_owned(),
                source: None,
                json_path: Some("$.fixture".to_owned()),
                reference_role: None,
                details: json!({"source": "reconstruction-proof"}),
            }],
            affected_root_count: 0,
            affected_roots: Vec::new(),
            affected_root_witness_paths: Vec::new(),
            witness_path: Vec::new(),
        }];
        populate_affected_roots(&mut scan);
        assert_eq!(scan.issues[0].affected_root_count, 101);
        assert_eq!(
            scan.issues[0].affected_roots.len(),
            ISSUE_INLINE_AFFECTED_ROOT_SAMPLE_LIMIT
        );
        let beyond_sample_root = roots[100].clone();
        assert!(
            !scan.issues[0].affected_roots.contains(&beyond_sample_root),
            "fixture root must be outside the bounded inline sample"
        );

        let validation = TidasBatchValidation {
            describe: json!({"asset_fingerprint": "fixture"}),
            final_event: json!({"type": "final", "completed": true}),
            issue_events: JsonlValueSpool::empty("empty-root-partition-issues.jsonl").unwrap(),
        };
        build_issue_relation_spools(&mut scan, &validation.issue_events).unwrap();
        let closure_check_id = id("cdcdcdcd-cdcd-4dcd-8dcd-cdcdcdcdcdcd");
        let artifacts = prepare_issue_partition_artifacts(
            closure_check_id,
            &scan,
            &validation,
            Arc::new(TempDir::new().unwrap()),
        )
        .unwrap();
        let reconstructed =
            reconstruct_complete_machine_result(&artifacts, closure_check_id).unwrap();
        assert_eq!(reconstructed.issue_count, 1);
        assert_eq!(reconstructed.occurrence_count, 1);
        assert_eq!(reconstructed.affected_root_count, 101);
        assert_eq!(reconstructed.expanded_affected_root_record_count, 0);
        assert_eq!(reconstructed.root_impact_record_count, 1);
        let root_records = artifacts
            .iter()
            .filter(|artifact| artifact.descriptor.file_name.starts_with("affected-roots/"))
            .map(|artifact| {
                let decoded =
                    zstd::stream::decode_all(File::open(&artifact.path).unwrap()).unwrap();
                decoded.split(|byte| *byte == b'\n').count() - 1
            })
            .sum::<usize>();
        assert_eq!(root_records, 0);
        let impact_artifact = artifacts
            .iter()
            .find(|artifact| {
                artifact.descriptor.file_name == "evidence/root-impact-index-v1.bin.zst"
            })
            .unwrap();
        let impact_bytes =
            zstd::stream::decode_all(File::open(&impact_artifact.path).unwrap()).unwrap();
        let impact = decode_root_impact_index(&impact_bytes).unwrap();
        assert_eq!(impact.records.len(), 1);
        assert_eq!(impact.records[0].affected_root_count, 101);
        assert!(decoded_impact_contains_root(
            &impact.records[0],
            100,
            impact.root_count
        ));
        let graph_artifact = artifacts
            .iter()
            .find(|artifact| {
                artifact.descriptor.file_name == "evidence/frozen-reference-graph-v1.bin.zst"
            })
            .unwrap();
        let graph_bytes =
            zstd::stream::decode_all(File::open(&graph_artifact.path).unwrap()).unwrap();
        let graph = decode_frozen_reference_graph(&graph_bytes).unwrap();
        let witness = reconstruct_frozen_graph_witness(
            &graph,
            impact.records[0].source_node_ordinal.unwrap(),
            100,
        )
        .unwrap();
        assert_eq!(witness, vec![support, beyond_sample_root]);
    }

    #[test]
    fn all_roots_administrative_evidence_is_explicit_and_bounded() {
        let roots = (0..101_u128)
            .map(|index| ExactDatasetIdentity {
                category: DatasetCategory::Processes,
                id: Uuid::from_u128(index + 1),
                version: "01.00.000".to_owned(),
            })
            .collect::<Vec<_>>();
        let (sample, witnesses) = bounded_all_root_evidence(&roots);
        assert_eq!(sample.len(), ISSUE_INLINE_AFFECTED_ROOT_SAMPLE_LIMIT);
        assert_eq!(witnesses.len(), ISSUE_INLINE_AFFECTED_ROOT_SAMPLE_LIMIT);
        assert_eq!(sample.first(), roots.first());
        assert_eq!(
            sample.last(),
            roots.get(ISSUE_INLINE_AFFECTED_ROOT_SAMPLE_LIMIT - 1)
        );
        assert_eq!(
            witnesses.last(),
            sample.last().map(|root| vec![root.clone()]).as_ref()
        );
    }

    fn clone_prepared_artifacts(artifacts: &[PreparedArtifact]) -> Vec<PreparedArtifact> {
        let temp = Arc::new(TempDir::new().unwrap());
        artifacts
            .iter()
            .map(|artifact| {
                let path = temp.path().join(&artifact.descriptor.file_name);
                fs::create_dir_all(path.parent().unwrap()).unwrap();
                fs::copy(&artifact.path, &path).unwrap();
                PreparedArtifact {
                    descriptor: artifact.descriptor.clone(),
                    path,
                    _temp: Arc::clone(&temp),
                }
            })
            .collect()
    }

    fn read_v3_issue_records(artifacts: &[PreparedArtifact]) -> Vec<Value> {
        let mut partitions = artifacts
            .iter()
            .filter(|artifact| artifact.descriptor.file_name.starts_with("issues/"))
            .collect::<Vec<_>>();
        partitions
            .sort_by(|left, right| left.descriptor.file_name.cmp(&right.descriptor.file_name));
        partitions
            .into_iter()
            .flat_map(|artifact| {
                let decoded =
                    zstd::stream::decode_all(File::open(&artifact.path).unwrap()).unwrap();
                decoded
                    .split(|byte| *byte == b'\n')
                    .filter(|line| !line.is_empty())
                    .map(|line| serde_json::from_slice::<Value>(line).unwrap())
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn canonical_v3_preserves_the_unified_issue_set_without_expanded_relations() {
        let graph = production_capacity_graph();
        let reference_graph =
            CompactReferenceGraph::from_references(&graph.references, &graph.roots).unwrap();
        let mut documents = ClosureDocumentSpoolWriter::new().unwrap();
        for document in &graph.documents {
            documents.append(document).unwrap();
        }
        let mut scan = capacity_scan(
            documents.finish().unwrap(),
            graph.roots.clone(),
            reference_graph,
        );
        let issue =
            |ordinal: usize, code: &str, source: Option<ExactDatasetIdentity>, blocking: bool| {
                ClosureIssue {
                    issue_key: format!("unified-{ordinal:02}-{code}"),
                    severity: if blocking { "blocker" } else { "warning" }.to_owned(),
                    blocking,
                    issue_code: code.to_owned(),
                    source: source.clone(),
                    json_path: Some(format!("$.unified[{ordinal}]")),
                    reference_role: Some("qualification_fixture".to_owned()),
                    requested_target_type: None,
                    requested_target_id: None,
                    requested_target_version: None,
                    message: format!("unified fixture {code}"),
                    suggested_action: Some("repair fixture".to_owned()),
                    occurrence_count: 1,
                    occurrences: vec![ClosureIssueOccurrence {
                        occurrence_key: format!("unified-occurrence-{ordinal:02}"),
                        source,
                        json_path: Some(format!("$.unified[{ordinal}]")),
                        reference_role: Some("qualification_fixture".to_owned()),
                        details: json!({"family": code}),
                    }],
                    affected_root_count: u32::try_from(graph.roots.len()).unwrap(),
                    affected_roots: graph.roots.clone(),
                    affected_root_witness_paths: Vec::new(),
                    witness_path: Vec::new(),
                }
            };
        scan.issues = vec![
            issue(
                1,
                "reference_exact_version_missing",
                Some(graph.sources[0].clone()),
                true,
            ),
            issue(
                2,
                "snapshot_source_drift",
                Some(graph.sources[0].clone()),
                true,
            ),
            issue(
                3,
                "provider_outside_scope_universe",
                Some(graph.sources[0].clone()),
                true,
            ),
            issue(4, "matrix_readiness_matrix_not_ready", None, true),
            issue(5, "matrix_readiness_factorization_failed", None, true),
            issue(6, "lcia_readiness_blocked", None, true),
        ];
        let mut events = JsonlValueSpoolWriter::new("unified-tidas-events.jsonl").unwrap();
        events
            .append(&json!({
                "type": "issue",
                "document_key": graph.sources[0].document_key(),
                "issue": {
                    "issue_code": "schema_invalid",
                    "location": "$.tidas",
                    "message": "TIDAS qualification fixture"
                }
            }))
            .unwrap();
        let validation = TidasBatchValidation {
            describe: json!({"asset_fingerprint": "unified-v3"}),
            final_event: json!({"type": "final", "completed": true}),
            issue_events: events.finish().unwrap(),
        };
        build_issue_relation_spools(&mut scan, &validation.issue_events).unwrap();
        let closure_check_id = id("17717717-0177-4177-8177-177177177177");
        let artifacts = prepare_issue_partition_artifacts(
            closure_check_id,
            &scan,
            &validation,
            Arc::new(TempDir::new().unwrap()),
        )
        .unwrap();
        let reconstructed =
            reconstruct_complete_machine_result(&artifacts, closure_check_id).unwrap();
        assert_eq!(
            reconstructed.schema_version,
            "lcia.scope-closure-issue-manifest.v3"
        );
        assert_eq!(reconstructed.issue_count, 7);
        assert_eq!(reconstructed.occurrence_count, 7);
        assert_eq!(reconstructed.affected_root_count, 49);
        assert_eq!(reconstructed.expanded_affected_root_record_count, 0);
        assert_eq!(reconstructed.root_impact_record_count, 1);
        assert!(!artifacts.iter().any(|artifact| {
            artifact.descriptor.file_name.starts_with("occurrences/")
                || artifact.descriptor.file_name.starts_with("affected-roots/")
        }));

        let records = read_v3_issue_records(&artifacts);
        let codes = records
            .iter()
            .map(|record| record["code"].as_str().unwrap())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            codes,
            BTreeSet::from([
                "lcia_readiness_blocked",
                "matrix_readiness_factorization_failed",
                "matrix_readiness_matrix_not_ready",
                "provider_outside_scope_universe",
                "reference_exact_version_missing",
                "snapshot_source_drift",
                "tidas_schema_invalid",
            ])
        );
        assert!(records.iter().all(|record| {
            record["schemaVersion"] == "lcia.scope-closure-issue.v3"
                && record["blocker"] == true
                && record["occurrenceCount"] == 1
                && record["affectedRootCount"] == 7
        }));
        assert!(
            records
                .iter()
                .filter(|record| record["source"].is_null())
                .all(|record| {
                    record["rootImpact"]["mode"] == "all_roots"
                        && record["rootImpact"].get("impactKey").is_none()
                })
        );
    }

    #[test]
    fn canonical_v3_cancellation_is_cooperative_during_coalesce_and_partition_write() {
        let build_fixture = || {
            let graph = production_capacity_graph();
            let reference_graph =
                CompactReferenceGraph::from_references(&graph.references, &graph.roots).unwrap();
            let mut documents = ClosureDocumentSpoolWriter::new().unwrap();
            for document in &graph.documents {
                documents.append(document).unwrap();
            }
            let scan = capacity_scan(documents.finish().unwrap(), graph.roots, reference_graph);
            let mut events = JsonlValueSpoolWriter::new("cancel-v3-events.jsonl").unwrap();
            for index in 0..4_097_u64 {
                events
                    .append(&json!({
                        "type": "issue",
                        "document_key": graph.sources[0].document_key(),
                        "issue": {
                            "issue_code": "cancel_fixture",
                            "location": format!("$.cancel[{index}]"),
                            "message": format!("cancel fixture {index}")
                        }
                    }))
                    .unwrap();
            }
            (scan, events.finish().unwrap())
        };

        for stage in ["scope_closure_coalesce", "scope_closure_partition_write"] {
            let (mut scan, events) = build_fixture();
            let cancellation = CancellationToken::default();
            cancellation.cancel_at_stage(stage);
            let error =
                build_issue_relation_spools_with_cancellation(&mut scan, &events, &cancellation)
                    .unwrap_err();
            assert!(
                error.to_string().contains(stage),
                "unexpected cancellation error at {stage}: {error:#}"
            );
            assert!(cancellation.is_cancelled());
            assert!(
                scan.issue_relations.is_none(),
                "cancelled v3 preparation must not publish partial relation spools"
            );
        }
    }

    #[test]
    fn version_dispatch_reader_preserves_legacy_v2_migration_support() {
        let closure_check_id = id("17717717-0277-4277-8277-177177177177");
        let issue_key = "legacy-v2-issue";
        let root = identity(
            DatasetCategory::Processes,
            "17717717-0377-4377-8377-177177177177",
        );
        let temp = Arc::new(TempDir::new().unwrap());

        let mut issues = IssuePartitionAccumulator::new(Arc::clone(&temp), "issues");
        issues
            .push(
                issue_key,
                &json!({
                    "schemaVersion": "lcia.scope-closure-issue.v2",
                    "issueKey": issue_key,
                    "code": "legacy_fixture",
                }),
            )
            .unwrap();
        let (issue_entries, issue_artifacts, issue_hash) = issues.finish().unwrap();

        let mut occurrences = IssuePartitionAccumulator::new(Arc::clone(&temp), "occurrences");
        occurrences
            .push(
                issue_key,
                &json!({
                    "schemaVersion": "lcia.scope-closure-occurrence.v1",
                    "issueKey": issue_key,
                    "occurrenceKey": "legacy-v2-occurrence",
                }),
            )
            .unwrap();
        let (occurrence_entries, occurrence_artifacts, occurrence_hash) =
            occurrences.finish().unwrap();

        let mut affected_roots =
            IssuePartitionAccumulator::new(Arc::clone(&temp), "affected-roots");
        affected_roots
            .push(
                issue_key,
                &affected_root_partition_record(issue_key, &root, std::slice::from_ref(&root)),
            )
            .unwrap();
        let (affected_entries, affected_artifacts, affected_hash) =
            affected_roots.finish().unwrap();

        let mut entries = issue_entries
            .into_iter()
            .chain(occurrence_entries)
            .chain(affected_entries)
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| left.path.cmp(&right.path));
        let manifest = IssuePartitionManifestV2 {
            schema_version: "lcia.scope-closure-issue-manifest.v2".to_owned(),
            closure_check_id,
            logical_issue_stream_sha256: sha256_hex(&[]),
            logical_issue_event_count: 0,
            partition_max_records: ISSUE_PARTITION_MAX_RECORDS,
            partition_max_uncompressed_bytes: ISSUE_PARTITION_MAX_UNCOMPRESSED_BYTES,
            issue_count: 1,
            occurrence_count: 1,
            affected_root_count: 1,
            relation_stream_sha256: IssueRelationStreamHashesV2 {
                issues: issue_hash,
                occurrences: occurrence_hash,
                affected_roots: affected_hash,
            },
            rpc_issue_sample_limit: ISSUE_INLINE_ISSUE_SAMPLE_LIMIT,
            rpc_occurrence_sample_limit_per_issue: ISSUE_INLINE_OCCURRENCE_SAMPLE_LIMIT,
            rpc_affected_root_sample_limit_per_issue: ISSUE_INLINE_AFFECTED_ROOT_SAMPLE_LIMIT,
            xlsx_issue_sample_limit: XLSX_ISSUE_SAMPLE_LIMIT,
            xlsx_occurrence_sample_limit: XLSX_OCCURRENCE_SAMPLE_LIMIT,
            xlsx_affected_root_sample_limit: XLSX_AFFECTED_ROOT_SAMPLE_LIMIT,
            partitions: entries,
        };
        let manifest_path = temp.path().join("manifest.json");
        fs::write(&manifest_path, canonical_json_bytes(&manifest).unwrap()).unwrap();
        let manifest_artifact = prepare_file_artifact(
            Arc::clone(&temp),
            "closure_complete_machine_result",
            ScopeClosureArtifactRole::CompleteMachineResult,
            "manifest.json",
            "application/vnd.tiangong.scope-closure-manifest+json",
            manifest_path,
        )
        .unwrap();
        let mut artifacts = issue_artifacts
            .into_iter()
            .chain(occurrence_artifacts)
            .chain(affected_artifacts)
            .collect::<Vec<_>>();
        artifacts.push(manifest_artifact);

        let reconstructed =
            reconstruct_complete_machine_result(&artifacts, closure_check_id).unwrap();
        assert_eq!(
            reconstructed.schema_version,
            "lcia.scope-closure-issue-manifest.v2"
        );
        assert_eq!(reconstructed.issue_count, 1);
        assert_eq!(reconstructed.occurrence_count, 1);
        assert_eq!(reconstructed.affected_root_count, 1);
        assert_eq!(reconstructed.expanded_affected_root_record_count, 1);
        assert!(reconstructed.legacy_relation_stream_sha256.is_some());
    }

    fn mutate_manifest(artifacts: &mut [PreparedArtifact], mutate: impl FnOnce(&mut Value)) {
        let manifest = artifacts
            .iter_mut()
            .find(|artifact| artifact.descriptor.file_name == "manifest.json")
            .unwrap();
        let mut value: Value =
            serde_json::from_reader(BufReader::new(File::open(&manifest.path).unwrap())).unwrap();
        mutate(&mut value);
        let bytes = serde_json::to_vec(&value).unwrap();
        fs::write(&manifest.path, &bytes).unwrap();
        manifest.descriptor.byte_size = bytes.len();
        manifest.descriptor.checksum_sha256 = sha256_hex(&bytes);
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn complete_machine_result_rejects_manifest_and_partition_tampering_matrix() {
        let support = identity(
            DatasetCategory::Sources,
            "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
        );
        let root = ExactDatasetIdentity {
            category: DatasetCategory::Processes,
            id: id("ffffffff-ffff-4fff-8fff-ffffffffffff"),
            version: "01.00.000".to_owned(),
        };
        let provider = FakeProvider {
            documents: BTreeMap::from([
                (
                    support.clone(),
                    ClosureDocument {
                        identity: support.clone(),
                        payload: json!({}),
                    },
                ),
                (
                    root.clone(),
                    ClosureDocument {
                        identity: root.clone(),
                        payload: json!({
                            "referenceToSource": reference(
                                "source",
                                support.id,
                                Some("01.00.000"),
                            )
                        }),
                    },
                ),
            ]),
            ..FakeProvider::default()
        };
        let mut scan = collect_scope_closure(&provider, &manifest(vec![root]))
            .await
            .unwrap();
        scan.issues = vec![ClosureIssue {
            issue_key: "tamper-matrix-issue".to_owned(),
            severity: "warning".to_owned(),
            blocking: false,
            issue_code: "tamper_matrix".to_owned(),
            source: Some(support),
            json_path: None,
            reference_role: None,
            requested_target_type: None,
            requested_target_id: None,
            requested_target_version: None,
            message: "tamper matrix".to_owned(),
            suggested_action: None,
            occurrence_count: 1,
            occurrences: vec![ClosureIssueOccurrence {
                occurrence_key: "tamper-matrix-occurrence".to_owned(),
                source: None,
                json_path: Some("$.fixture".to_owned()),
                reference_role: None,
                details: json!({}),
            }],
            affected_root_count: 0,
            affected_roots: Vec::new(),
            affected_root_witness_paths: Vec::new(),
            witness_path: Vec::new(),
        }];
        populate_affected_roots(&mut scan);
        let validation = TidasBatchValidation {
            describe: json!({"asset_fingerprint": "fixture"}),
            final_event: json!({"type": "final", "completed": true}),
            issue_events: JsonlValueSpool::empty("empty-tamper-matrix-issues.jsonl").unwrap(),
        };
        build_issue_relation_spools(&mut scan, &validation.issue_events).unwrap();
        let closure_check_id = id("dddddddd-dddd-4ddd-8ddd-dddddddddddd");
        let artifacts = prepare_issue_partition_artifacts(
            closure_check_id,
            &scan,
            &validation,
            Arc::new(TempDir::new().unwrap()),
        )
        .unwrap();
        reconstruct_complete_machine_result(&artifacts, closure_check_id).unwrap();

        let assert_rejected = |case: &str, tampered: &[PreparedArtifact]| {
            assert!(
                reconstruct_complete_machine_result(tampered, closure_check_id).is_err(),
                "{case} tampering must be rejected"
            );
        };

        let mut missing = clone_prepared_artifacts(&artifacts);
        let missing_index = missing
            .iter()
            .position(|artifact| artifact.descriptor.file_name.starts_with("issues/"))
            .unwrap();
        missing.remove(missing_index);
        assert_rejected("missing partition", &missing);

        let mut extra = clone_prepared_artifacts(&artifacts);
        let mut extra_partition = extra
            .iter()
            .find(|artifact| artifact.descriptor.file_name.starts_with("issues/"))
            .unwrap()
            .clone();
        extra_partition.descriptor.file_name = "extra/part-000000.ndjson.zst".to_owned();
        extra.push(extra_partition);
        assert_rejected("extra partition", &extra);

        let mut duplicate = clone_prepared_artifacts(&artifacts);
        duplicate.push(
            duplicate
                .iter()
                .find(|artifact| artifact.descriptor.file_name.starts_with("issues/"))
                .unwrap()
                .clone(),
        );
        assert_rejected("duplicate partition", &duplicate);

        let mut renamed = clone_prepared_artifacts(&artifacts);
        renamed
            .iter_mut()
            .find(|artifact| artifact.descriptor.file_name.starts_with("issues/"))
            .unwrap()
            .descriptor
            .file_name = "issues/part-999999.ndjson.zst".to_owned();
        assert_rejected("renamed partition", &renamed);

        let corrupted = clone_prepared_artifacts(&artifacts);
        let partition = corrupted
            .iter()
            .find(|artifact| artifact.descriptor.file_name.starts_with("issues/"))
            .unwrap();
        let mut bytes = fs::read(&partition.path).unwrap();
        let midpoint = bytes.len() / 2;
        bytes[midpoint] ^= 0x01;
        fs::write(&partition.path, bytes).unwrap();
        assert_rejected("partition bit corruption", &corrupted);

        let mut wrong_count = clone_prepared_artifacts(&artifacts);
        mutate_manifest(&mut wrong_count, |manifest| {
            manifest["partitions"][0]["recordCount"] = json!(999);
        });
        assert_rejected("partition count", &wrong_count);

        let mut wrong_hash = clone_prepared_artifacts(&artifacts);
        mutate_manifest(&mut wrong_hash, |manifest| {
            manifest["relationStreamSha256"]["issues"] = json!("0".repeat(64));
        });
        assert_rejected("global hash", &wrong_hash);

        let mut wrong_order = clone_prepared_artifacts(&artifacts);
        mutate_manifest(&mut wrong_order, |manifest| {
            manifest["evidence"].as_array_mut().unwrap().swap(0, 1);
        });
        assert_rejected("manifest order", &wrong_order);

        let mut wrong_schema = clone_prepared_artifacts(&artifacts);
        mutate_manifest(&mut wrong_schema, |manifest| {
            manifest["schemaVersion"] = json!("lcia.scope-closure-issue-manifest.v999");
        });
        assert_rejected("schemaVersion", &wrong_schema);

        let mut wrong_check = clone_prepared_artifacts(&artifacts);
        mutate_manifest(&mut wrong_check, |manifest| {
            manifest["closureCheckId"] = json!(Uuid::nil());
        });
        assert_rejected("closureCheckId", &wrong_check);

        let mut wrong_role = clone_prepared_artifacts(&artifacts);
        wrong_role
            .iter_mut()
            .find(|artifact| artifact.descriptor.file_name.ends_with(".ndjson.zst"))
            .unwrap()
            .descriptor
            .artifact_role = ScopeClosureArtifactRole::ClosureReport;
        assert_rejected("artifact role", &wrong_role);
    }

    #[tokio::test]
    async fn large_root_set_completes_within_time_budget() {
        let num_roots: u128 = 5605;
        let num_support: u128 = 2000;
        let num_issues: u128 = 1000;

        let roots = (0..num_roots)
            .map(|i| ExactDatasetIdentity {
                category: DatasetCategory::Processes,
                id: Uuid::from_u128(i + 1),
                version: "01.00.000".to_owned(),
            })
            .collect::<Vec<_>>();

        let support_docs = (0..num_support)
            .map(|i| ExactDatasetIdentity {
                category: DatasetCategory::Sources,
                id: Uuid::from_u128(num_roots + i + 1),
                version: "01.00.000".to_owned(),
            })
            .collect::<Vec<_>>();

        let mut documents = BTreeMap::new();
        for root in &roots {
            let target = &support_docs[root.id.as_u128() as usize % num_support as usize];
            documents.insert(
                root.clone(),
                ClosureDocument {
                    identity: root.clone(),
                    payload: json!({
                        "referenceToSource": reference("source", target.id, Some("01.00.000"))
                    }),
                },
            );
        }
        for support in &support_docs {
            documents.insert(
                support.clone(),
                ClosureDocument {
                    identity: support.clone(),
                    payload: json!({}),
                },
            );
        }

        let provider = FakeProvider {
            documents,
            ..FakeProvider::default()
        };

        let start = std::time::Instant::now();
        let scan = collect_scope_closure(&provider, &manifest(roots.clone()))
            .await
            .expect("scan must complete");
        let elapsed = start.elapsed();

        assert!(scan.documents.len() >= num_roots as usize);
        assert!(
            elapsed.as_secs() < 30,
            "scan took {elapsed:?}, expected under 30s"
        );

        let mut issues: Vec<ClosureIssue> = (0..num_issues)
            .map(|i| {
                let source = &roots[i as usize % num_roots as usize];
                ClosureIssue {
                    issue_key: format!("test_issue_{i}"),
                    severity: "warning".to_owned(),
                    blocking: false,
                    issue_code: "test_missing".to_owned(),
                    source: Some(source.clone()),
                    json_path: None,
                    reference_role: None,
                    requested_target_type: None,
                    requested_target_id: None,
                    requested_target_version: None,
                    message: "test issue".to_owned(),
                    suggested_action: None,
                    occurrence_count: 0,
                    occurrences: Vec::new(),
                    affected_root_count: 0,
                    affected_roots: Vec::new(),
                    affected_root_witness_paths: Vec::new(),
                    witness_path: Vec::new(),
                }
            })
            .collect();

        let start = std::time::Instant::now();
        compute_affected_roots_batch(&mut issues, &roots, &scan.reference_graph);
        let elapsed = start.elapsed();

        assert!(
            elapsed.as_secs() < 10,
            "affected_roots took {elapsed:?}, expected under 10s"
        );
        for issue in &issues {
            assert!(!issue.affected_roots.is_empty());
        }
    }

    #[tokio::test]
    #[ignore = "local capacity gate: constructs the qualified 112,032-document reference graph"]
    async fn qualified_reference_graph_completes_within_five_minutes() {
        const ROOT_COUNT: usize = 5_605;
        const DOCUMENT_COUNT: usize = 112_032;
        let support_count = DOCUMENT_COUNT - ROOT_COUNT;
        let roots = (0..ROOT_COUNT)
            .map(|index| ExactDatasetIdentity {
                category: DatasetCategory::Processes,
                id: Uuid::from_u128(u128::try_from(index).unwrap() + 1),
                version: "01.00.000".to_owned(),
            })
            .collect::<Vec<_>>();
        let support = (0..support_count)
            .map(|index| ExactDatasetIdentity {
                category: DatasetCategory::Sources,
                id: Uuid::from_u128(u128::try_from(ROOT_COUNT + index).unwrap() + 1),
                version: "01.00.000".to_owned(),
            })
            .collect::<Vec<_>>();
        let mut documents = BTreeMap::new();
        for (index, root) in roots.iter().enumerate() {
            let target = &support[index];
            documents.insert(
                root.clone(),
                ClosureDocument {
                    identity: root.clone(),
                    payload: json!({
                        "referenceToSource": reference("source", target.id, Some("01.00.000"))
                    }),
                },
            );
        }
        for (index, identity) in support.iter().enumerate() {
            let next = index + ROOT_COUNT;
            let payload = if next < support.len() {
                json!({
                    "referenceToSource": reference(
                        "source",
                        support[next].id,
                        Some("01.00.000")
                    )
                })
            } else {
                json!({})
            };
            documents.insert(
                identity.clone(),
                ClosureDocument {
                    identity: identity.clone(),
                    payload,
                },
            );
        }
        let provider = FakeProvider {
            documents,
            ..FakeProvider::default()
        };

        let started = Instant::now();
        let scan = collect_scope_closure(&provider, &manifest(roots))
            .await
            .unwrap();

        assert_eq!(scan.documents.len(), DOCUMENT_COUNT);
        assert!(
            started.elapsed().as_secs() <= 300,
            "qualified reference graph exceeded five minutes: {:?}",
            started.elapsed()
        );
    }
}
