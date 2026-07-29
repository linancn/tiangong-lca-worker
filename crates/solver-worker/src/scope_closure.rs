//! Exact-version, non-fail-fast source-closure preflight for data-product builds.

use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet, BinaryHeap, VecDeque},
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

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
    resource::{ResourceCounters, ResourceMeasurement, directory_bytes},
    snapshot_artifacts::ScopeClosureSnapshotBinding,
    storage::ObjectTransferOptions,
    tidas_cli,
    worker_jobs::{WorkerJobProgress, lease_heartbeat_period},
};

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
const XLSX_ISSUE_SAMPLE_LIMIT: usize = 5_000;
const XLSX_OCCURRENCE_SAMPLE_LIMIT: usize = 10_000;
const XLSX_AFFECTED_ROOT_SAMPLE_LIMIT: usize = 10_000;
const XLSX_MAX_WORKSHEET_ROWS: usize = 1_048_576;
const XLSX_MAX_WORKSHEET_UNCOMPRESSED_BYTES: u64 = 64 * 1024 * 1024;
const XLSX_MAX_TOTAL_UNCOMPRESSED_BYTES: u64 = 128 * 1024 * 1024;
const XLSX_MAX_ARCHIVE_BYTES: u64 = 64 * 1024 * 1024;
const SCOPE_CLOSURE_ARTIFACT_RETENTION_SECONDS: i64 = 7 * 24 * 60 * 60;
const SCOPE_CLOSURE_ARTIFACT_MAX_UPLOAD_BYTES: u64 = 8 * 1024 * 1024 * 1024;
const INSERT_SCOPE_CLOSURE_ARTIFACT_SQL: &str = r"
    INSERT INTO public.worker_job_artifacts (
        id, job_id, artifact_type, artifact_role, lifecycle_state,
        storage_bucket, storage_path, content_type, byte_size,
        checksum_sha256, metadata, visibility, expires_at
    ) VALUES (
        $1, $2, $3, $4, 'ready',
        $5, $6, $7, $8, $9, $10::jsonb, 'operator',
        transaction_timestamp() + make_interval(secs => $11::integer)
    )
    RETURNING id
    ";

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
    issues: SortedJsonlRuns,
    occurrences: SortedJsonlRuns,
    affected_roots: SortedJsonlRuns,
    stats: IssueRelationStats,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ScopeClosureArtifactRole {
    ClosureReport,
    CompleteMachineResult,
    ClosureBundle,
}

impl ScopeClosureArtifactRole {
    const fn as_str(self) -> &'static str {
        match self {
            Self::ClosureReport => "closure_report",
            Self::CompleteMachineResult => "complete_machine_result",
            Self::ClosureBundle => "closure_bundle",
        }
    }
}

#[derive(Debug, Clone)]
struct PreparedArtifact {
    descriptor: ArtifactManifestEntry,
    path: PathBuf,
    _temp: Arc<TempDir>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
struct IssuePartitionManifest {
    schema_version: String,
    closure_check_id: Uuid,
    logical_issue_stream_sha256: String,
    logical_issue_event_count: u64,
    partition_max_records: u64,
    partition_max_uncompressed_bytes: u64,
    issue_count: u64,
    occurrence_count: u64,
    affected_root_count: u64,
    rpc_issue_sample_limit: usize,
    rpc_occurrence_sample_limit_per_issue: usize,
    rpc_affected_root_sample_limit_per_issue: usize,
    xlsx_issue_sample_limit: usize,
    xlsx_occurrence_sample_limit: usize,
    xlsx_affected_root_sample_limit: usize,
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
}

#[derive(Debug)]
struct SortedJsonlRuns {
    temp: TempDir,
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
            temp: self.temp,
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

    fn storage_bytes(&self) -> u64 {
        directory_bytes(self.temp.path()).unwrap_or(0)
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
        })
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
        if self.byte_size > VALIDATION_ISSUE_SPOOL_MAX_BYTES
            || self.event_count > VALIDATION_ISSUE_SPOOL_MAX_EVENTS
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
    ensure_temp_free_space(
        temp.path(),
        validation.issue_events.byte_size.saturating_mul(2),
    )?;
    let path = temp.path().join("closure-bundle-v1.json");
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
    write_scope_closure_scan(&mut writer, scan)?;
    write_canonical_field(
        &mut writer,
        "schemaVersion",
        &"lcia.scope-closure-bundle.v1",
        true,
    )?;
    writer.write_all(b",\"tidasValidation\":")?;
    write_tidas_validation(&mut writer, validation)?;
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

fn write_scope_closure_scan<W: Write>(
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
    writer.write_all(b",\"issues\":")?;
    if let Some(relations) = &scan.issue_relations {
        write_relation_payload_json_array(writer, &relations.issues, 1)?;
    } else {
        write_canonical_array(writer, &scan.issues)?;
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

fn write_relation_payload_json_array<W: Write>(
    writer: &mut W,
    spool: &SortedJsonlRuns,
    payload_index: usize,
) -> anyhow::Result<()> {
    writer.write_all(b"[")?;
    let mut comma = false;
    spool.visit(|record| {
        let payload = record
            .as_array()
            .and_then(|fields| fields.get(payload_index))
            .ok_or_else(|| anyhow::anyhow!("relation spool record omitted payload"))?;
        if comma {
            writer.write_all(b",")?;
        }
        writer.write_all(&canonical_json_bytes(payload)?)?;
        comma = true;
        Ok(())
    })?;
    writer.write_all(b"]")?;
    Ok(())
}

fn write_tidas_validation<W: Write>(
    writer: &mut W,
    validation: &TidasBatchValidation,
) -> anyhow::Result<()> {
    writer.write_all(b"{")?;
    write_canonical_field(writer, "describe", &validation.describe, false)?;
    write_canonical_field(writer, "finalEvent", &validation.final_event, true)?;
    writer.write_all(b",\"issueEvents\":[")?;
    let mut comma = false;
    validation.issue_events.visit(|event| {
        if comma {
            writer.write_all(b",")?;
        }
        writer.write_all(&canonical_json_bytes(&event)?)?;
        comma = true;
        Ok(())
    })?;
    writer.write_all(b"]}")?;
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
        affected_roots: scan.roots.clone(),
        affected_root_witness_paths: scan.roots.iter().map(|root| vec![root.clone()]).collect(),
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
            affected_roots: scan.roots.clone(),
            affected_root_witness_paths: scan.roots.iter().map(|root| vec![root.clone()]).collect(),
            witness_path: Vec::new(),
        });
    }
    if readiness.status == ReadinessStatus::Failed && readiness.blockers.is_empty() {
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
            affected_roots: scan.roots.clone(),
            affected_root_witness_paths: scan.roots.iter().map(|root| vec![root.clone()]).collect(),
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
    let prepare_artifacts = tokio::task::spawn_blocking(move || {
        build_issue_relation_spools(&mut scan, &validation.issue_events)?;
        let resolution_map =
            build_resolution_map_spool(&scan.edges, &scan.omitted_version_resolutions)?;
        let resolution_map_hash = spooled_json_array_sha256(&resolution_map)?;
        let closure_bundle =
            build_closure_bundle(&input_for_artifacts, &validation, &scan, &resolution_map)?;
        let closure_bundle_hash = closure_bundle.sha256.clone();
        let source_fingerprint = source_fingerprint(&scan.documents)?;
        let mut artifacts = prepare_closure_content_artifacts(
            closure_bundle,
            closure_check_id,
            &scan,
            &validation,
        )?;
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
                progress
                    .heartbeat(
                        "prepare_closure_artifacts",
                        0.82,
                        Some(json!({
                            "closureCheckId": closure_check_id,
                            "longRunningOperation": true,
                        })),
                    )
                    .await?;
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
        None,
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
    ) -> anyhow::Result<(Vec<IssuePartitionManifestEntry>, Vec<PreparedArtifact>)> {
        self.flush()?;
        Ok((self.entries, self.artifacts))
    }
}

fn issue_partition_record(issue: &ClosureIssue) -> Value {
    json!({
        "schemaVersion": "lcia.scope-closure-issue.v2",
        "issueKey": issue.issue_key,
        "severity": issue.severity,
        "blocking": issue.blocking,
        "issueCode": issue.issue_code,
        "source": issue.source,
        "jsonPath": issue.json_path,
        "referenceRole": issue.reference_role,
        "requestedTargetType": issue.requested_target_type,
        "requestedTargetId": issue.requested_target_id,
        "requestedTargetVersion": issue.requested_target_version,
        "message": issue.message,
        "suggestedAction": issue.suggested_action,
        "occurrenceCount": issue.occurrence_count,
        "affectedRootCount": issue.affected_root_count,
    })
}

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
fn prepare_issue_partition_artifacts(
    closure_check_id: Uuid,
    scan: &ScopeClosureScan,
    validation: &TidasBatchValidation,
    temp: Arc<TempDir>,
) -> anyhow::Result<Vec<PreparedArtifact>> {
    let mut issue_writer = IssuePartitionAccumulator::new(Arc::clone(&temp), "issues");
    let mut occurrence_writer = IssuePartitionAccumulator::new(Arc::clone(&temp), "occurrences");
    let mut affected_root_writer =
        IssuePartitionAccumulator::new(Arc::clone(&temp), "affected-roots");
    let relations = scan
        .issue_relations
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("issue relation spools were not prepared"))?;
    let mut observed = 0_u64;
    relations.issues.visit(|record| {
        observed = observed.saturating_add(1);
        if observed.is_multiple_of(1_024) {
            enforce_scope_closure_memory_budget("write_issue_partitions")?;
        }
        let fields = record
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("issue relation record must be an array"))?;
        let issue_key = fields
            .first()
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("issue relation omitted issue key"))?;
        let issue: ClosureIssue = serde_json::from_value(
            fields
                .get(1)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("issue relation omitted payload"))?,
        )?;
        issue_writer.push(issue_key, &issue_partition_record(&issue))
    })?;
    relations.occurrences.visit(|record| {
        let fields = record
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("occurrence relation record must be an array"))?;
        let issue_key = fields
            .first()
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("occurrence relation omitted issue key"))?;
        let occurrence: ClosureIssueOccurrence = serde_json::from_value(
            fields
                .get(2)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("occurrence relation omitted payload"))?,
        )?;
        occurrence_writer.push(
            issue_key,
            &json!({
                "schemaVersion": "lcia.scope-closure-issue-occurrence.v1",
                "issueKey": issue_key,
                "occurrenceKey": occurrence.occurrence_key,
                "source": occurrence.source,
                "jsonPath": occurrence.json_path,
                "referenceRole": occurrence.reference_role,
                "details": occurrence.details,
            }),
        )
    })?;
    relations.affected_roots.visit(|record| {
        let fields = record
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("affected-root relation record must be an array"))?;
        let issue_key = fields
            .first()
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("affected-root relation omitted issue key"))?;
        let payload = fields
            .get(2)
            .ok_or_else(|| anyhow::anyhow!("affected-root relation omitted payload"))?;
        affected_root_writer.push(issue_key, payload)
    })?;

    let (mut entries, mut artifacts) = issue_writer.finish()?;
    let (occurrence_entries, occurrence_artifacts) = occurrence_writer.finish()?;
    entries.extend(occurrence_entries);
    artifacts.extend(occurrence_artifacts);
    let (affected_root_entries, affected_root_artifacts) = affected_root_writer.finish()?;
    entries.extend(affected_root_entries);
    artifacts.extend(affected_root_artifacts);
    let partition_bytes = artifacts.iter().fold(0_u64, |total, artifact| {
        total.saturating_add(u64::try_from(artifact.descriptor.byte_size).unwrap_or(u64::MAX))
    });
    record_scope_closure_resources(
        "write_issue_partitions_complete",
        Some(partition_bytes),
        Some(relations.stats.affected_root_count),
    );
    entries.sort_by(|left, right| left.path.cmp(&right.path));
    artifacts.sort_by(|left, right| left.descriptor.file_name.cmp(&right.descriptor.file_name));

    let manifest = IssuePartitionManifest {
        schema_version: "lcia.scope-closure-issue-manifest.v2".to_owned(),
        closure_check_id,
        logical_issue_stream_sha256: validation.issue_events.sha256.clone(),
        logical_issue_event_count: validation.issue_events.event_count,
        partition_max_records: ISSUE_PARTITION_MAX_RECORDS,
        partition_max_uncompressed_bytes: ISSUE_PARTITION_MAX_UNCOMPRESSED_BYTES,
        issue_count: relations.stats.issue_count,
        occurrence_count: relations.stats.occurrence_count,
        affected_root_count: relations.stats.affected_root_count,
        rpc_issue_sample_limit: ISSUE_INLINE_ISSUE_SAMPLE_LIMIT,
        rpc_occurrence_sample_limit_per_issue: ISSUE_INLINE_OCCURRENCE_SAMPLE_LIMIT,
        rpc_affected_root_sample_limit_per_issue: ISSUE_INLINE_AFFECTED_ROOT_SAMPLE_LIMIT,
        xlsx_issue_sample_limit: XLSX_ISSUE_SAMPLE_LIMIT,
        xlsx_occurrence_sample_limit: XLSX_OCCURRENCE_SAMPLE_LIMIT,
        xlsx_affected_root_sample_limit: XLSX_AFFECTED_ROOT_SAMPLE_LIMIT,
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

fn prepare_closure_content_artifacts(
    closure_bundle: ClosureBundleFile,
    closure_check_id: Uuid,
    scan: &ScopeClosureScan,
    validation: &TidasBatchValidation,
) -> anyhow::Result<Vec<PreparedArtifact>> {
    let ClosureBundleFile {
        temp,
        path: bundle_path,
        byte_size: bundle_byte_size,
        sha256: bundle_sha256,
    } = closure_bundle;
    let xlsx_path = temp.path().join("closure-report-v1.xlsx");
    build_scan_xlsx_report_file(&xlsx_path, closure_check_id, scan)?;

    let mut artifacts = vec![
        PreparedArtifact {
            descriptor: ArtifactManifestEntry {
                artifact_type: "closure_bundle".to_owned(),
                artifact_role: ScopeClosureArtifactRole::ClosureBundle,
                file_name: "closure-bundle-v1.json".to_owned(),
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
    artifacts.extend(prepare_issue_partition_artifacts(
        closure_check_id,
        scan,
        validation,
        Arc::clone(&temp),
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
    write_set_id: Uuid,
    content_artifact_manifest_hash: &str,
    complete_machine_result_artifact_id: Option<Uuid>,
) -> anyhow::Result<Value> {
    let mut metadata = json!({
        "schemaVersion": "lcia.scope-closure-artifact.v2",
        "closureCheckId": closure_check_id,
        "writeSetId": write_set_id,
        "fileName": artifact.descriptor.file_name,
        "artifactRole": artifact.descriptor.artifact_role,
        "lifecycleState": "ready",
        "retentionSeconds": SCOPE_CLOSURE_ARTIFACT_RETENTION_SECONDS,
        "contentArtifactManifestHash": content_artifact_manifest_hash,
    });
    if artifact.descriptor.artifact_role == ScopeClosureArtifactRole::ClosureBundle {
        metadata["completeMachineResultArtifactId"] = json!(
            complete_machine_result_artifact_id
                .ok_or_else(|| anyhow::anyhow!("closure bundle omitted machine-result manifest"))?
        );
    }
    Ok(metadata)
}

fn preallocated_artifact_id(
    artifact_ids: &BTreeMap<String, Uuid>,
    artifact: &PreparedArtifact,
) -> anyhow::Result<Uuid> {
    artifact_ids
        .get(&artifact.descriptor.file_name)
        .copied()
        .ok_or_else(|| anyhow::anyhow!("closure artifact ID was not preallocated"))
}

fn is_semantic_closure_artifact(artifact: &PreparedArtifact) -> bool {
    artifact.descriptor.artifact_role != ScopeClosureArtifactRole::CompleteMachineResult
        || artifact.descriptor.file_name == "manifest.json"
}

async fn persist_closure_artifacts(
    state: &AppState,
    worker_job_id: Uuid,
    closure_check_id: Uuid,
    artifacts: &[PreparedArtifact],
    content_artifact_manifest_hash: &str,
    progress: Option<&WorkerJobProgress<'_>>,
) -> anyhow::Result<BTreeMap<String, Uuid>> {
    let write_set_id = Uuid::new_v4();
    let mut uploaded = Vec::<String>::new();
    let mut staged = Vec::<(&PreparedArtifact, String)>::new();
    for (index, artifact) in artifacts.iter().enumerate() {
        if let Err(error) =
            heartbeat_closure_artifact_upload(progress, closure_check_id, index, artifacts.len())
                .await
        {
            cleanup_uploaded_artifacts(state, &uploaded).await;
            return Err(error);
        }
        let relative_key = format!(
            "scope-closure/{closure_check_id}/{write_set_id}/{}",
            artifact.descriptor.file_name
        );
        let object_key = state.object_store.prefixed_object_key(&relative_key)?;
        if let Err(error) = state
            .object_store
            .upload_object_key_file_bounded(
                object_key.as_str(),
                artifact.descriptor.content_type.as_str(),
                &artifact.path,
                ObjectTransferOptions::new(SCOPE_CLOSURE_ARTIFACT_MAX_UPLOAD_BYTES)
                    .with_expected_sha256(artifact.descriptor.checksum_sha256.clone()),
            )
            .await
        {
            cleanup_uploaded_artifacts(state, &uploaded).await;
            return Err(error.context("failed to upload closure artifact write set"));
        }
        uploaded.push(object_key.clone());
        staged.push((artifact, object_key));
    }

    let artifact_ids = staged
        .iter()
        .map(|(artifact, _)| (artifact.descriptor.file_name.clone(), Uuid::new_v4()))
        .collect::<BTreeMap<_, _>>();
    let complete_machine_result_artifact_id = artifact_ids.get("manifest.json").copied();
    let mut transaction = match state.pool.begin().await {
        Ok(transaction) => transaction,
        Err(error) => {
            cleanup_uploaded_artifacts(state, &uploaded).await;
            return Err(anyhow::anyhow!(
                "failed to begin closure artifact metadata transaction: {error}"
            ));
        }
    };
    let mut persisted = BTreeMap::new();
    for (artifact, object_key) in &staged {
        let byte_size = i64::try_from(artifact.descriptor.byte_size)?;
        let artifact_id = preallocated_artifact_id(&artifact_ids, artifact)?;
        let metadata = closure_artifact_metadata(
            artifact,
            closure_check_id,
            write_set_id,
            content_artifact_manifest_hash,
            complete_machine_result_artifact_id,
        )?;
        let row = match sqlx::query(INSERT_SCOPE_CLOSURE_ARTIFACT_SQL)
            .bind(artifact_id)
            .bind(worker_job_id)
            .bind(artifact.descriptor.artifact_type.as_str())
            .bind(artifact.descriptor.artifact_role.as_str())
            .bind(state.object_store.bucket_name())
            .bind(object_key)
            .bind(artifact.descriptor.content_type.as_str())
            .bind(byte_size)
            .bind(artifact.descriptor.checksum_sha256.as_str())
            .bind(metadata)
            .bind(i32::try_from(SCOPE_CLOSURE_ARTIFACT_RETENTION_SECONDS)?)
            .fetch_one(&mut *transaction)
            .await
        {
            Ok(row) => row,
            Err(error) => {
                let _ = transaction.rollback().await;
                cleanup_uploaded_artifacts(state, &uploaded).await;
                return Err(anyhow::anyhow!(
                    "failed to persist closure artifact metadata write set: {error}"
                ));
            }
        };
        let persisted_id = row.try_get::<Uuid, _>("id")?;
        if is_semantic_closure_artifact(artifact) {
            persisted.insert(artifact.descriptor.artifact_type.clone(), persisted_id);
        }
    }
    if let Err(error) = transaction.commit().await {
        cleanup_uploaded_artifacts(state, &uploaded).await;
        return Err(anyhow::anyhow!(
            "failed to commit closure artifact metadata write set: {error}"
        ));
    }
    Ok(persisted)
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
        writer: &mut SortedJsonlRunWriter,
        stats: &mut IssueRelationStats,
    ) -> anyhow::Result<()> {
        let Some(occurrence) = occurrence else {
            return Ok(());
        };
        if self.last_occurrence_key.as_deref() == Some(occurrence.occurrence_key.as_str()) {
            return Ok(());
        }
        self.last_occurrence_key = Some(occurrence.occurrence_key.clone());
        self.issue.occurrence_count = self.issue.occurrence_count.saturating_add(1);
        stats.occurrence_count = stats.occurrence_count.saturating_add(1);
        writer.append(&json!([
            self.issue.issue_key,
            occurrence.occurrence_key,
            occurrence
        ]))?;
        if self.issue.occurrences.len() < ISSUE_INLINE_OCCURRENCE_SAMPLE_LIMIT {
            self.issue.occurrences.push(occurrence);
        }
        Ok(())
    }
}

#[derive(Default)]
struct SourceReachability {
    source_key: String,
    visited: Vec<bool>,
    parent: Vec<Option<u32>>,
}

fn load_source_reachability(
    cache: &mut SourceReachability,
    source_key: &str,
    source: Option<&ExactDatasetIdentity>,
    graph: &CompactReferenceGraph,
) {
    if cache.source_key == source_key {
        return;
    }
    source_key.clone_into(&mut cache.source_key);
    cache.visited.clear();
    cache.parent.clear();
    let Some(source_id) = source.and_then(|source| graph.identity_ids.get(source).copied()) else {
        return;
    };
    cache.visited.resize(graph.identities.len(), false);
    cache.parent.resize(graph.identities.len(), None);
    let source_index = usize::try_from(source_id).expect("u32 identity index fits usize");
    cache.visited[source_index] = true;
    let mut queue = VecDeque::from([source_id]);
    while let Some(node) = queue.pop_front() {
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
}

#[allow(clippy::too_many_arguments)]
fn finalize_coalesced_issue(
    mut pending: CoalescedIssueState,
    roots: &[ExactDatasetIdentity],
    root_ids: &[Option<u32>],
    graph: &CompactReferenceGraph,
    reachability: &mut SourceReachability,
    issue_writer: &mut SortedJsonlRunWriter,
    affected_writer: &mut SortedJsonlRunWriter,
    relation_stats: &mut IssueRelationStats,
) -> anyhow::Result<()> {
    load_source_reachability(
        reachability,
        &pending.source_key,
        pending.issue.source.as_ref(),
        graph,
    );
    if !reachability.visited.is_empty() {
        pending.issue.affected_root_count = 0;
        pending.issue.affected_roots.clear();
        pending.issue.affected_root_witness_paths.clear();
        pending.issue.witness_path.clear();
        for (&root_id, root) in root_ids.iter().zip(roots) {
            let Some(root_id) = root_id else {
                continue;
            };
            let root_index = usize::try_from(root_id).expect("u32 identity index fits usize");
            if !reachability.visited[root_index] {
                continue;
            }
            let witness =
                reconstruct_witness_path(root_id, &reachability.parent, &graph.identities);
            pending.issue.affected_root_count = pending.issue.affected_root_count.saturating_add(1);
            relation_stats.affected_root_count =
                relation_stats.affected_root_count.saturating_add(1);
            affected_writer.append(&json!([
                pending.issue.issue_key,
                canonical_json_sha256(root)?,
                affected_root_partition_record(&pending.issue.issue_key, root, &witness)
            ]))?;
            if pending.issue.affected_roots.len() < ISSUE_INLINE_AFFECTED_ROOT_SAMPLE_LIMIT {
                pending.issue.affected_roots.push(root.clone());
                pending
                    .issue
                    .affected_root_witness_paths
                    .push(witness.clone());
                if pending.issue.witness_path.is_empty() {
                    pending.issue.witness_path = witness;
                }
            }
        }
    } else if pending.issue.source.is_none()
        && usize::try_from(pending.issue.affected_root_count).ok() == Some(roots.len())
    {
        pending.issue.affected_roots.clear();
        pending.issue.affected_root_witness_paths.clear();
        for root in roots {
            relation_stats.affected_root_count =
                relation_stats.affected_root_count.saturating_add(1);
            affected_writer.append(&json!([
                pending.issue.issue_key,
                canonical_json_sha256(root)?,
                affected_root_partition_record(
                    &pending.issue.issue_key,
                    root,
                    std::slice::from_ref(root)
                )
            ]))?;
            if pending.issue.affected_roots.len() < ISSUE_INLINE_AFFECTED_ROOT_SAMPLE_LIMIT {
                pending.issue.affected_roots.push(root.clone());
                pending
                    .issue
                    .affected_root_witness_paths
                    .push(vec![root.clone()]);
            }
        }
    } else {
        for (index, root) in pending.issue.affected_roots.iter().enumerate() {
            let witness = pending
                .issue
                .affected_root_witness_paths
                .get(index)
                .unwrap_or(&pending.issue.witness_path);
            relation_stats.affected_root_count =
                relation_stats.affected_root_count.saturating_add(1);
            affected_writer.append(&json!([
                pending.issue.issue_key,
                canonical_json_sha256(root)?,
                affected_root_partition_record(&pending.issue.issue_key, root, witness)
            ]))?;
        }
    }

    relation_stats.issue_count = relation_stats.issue_count.saturating_add(1);
    if pending.issue.blocking {
        relation_stats.blocker_count = relation_stats.blocker_count.saturating_add(1);
        relation_stats
            .blocker_codes
            .insert(pending.issue.issue_code.clone());
    }
    issue_writer.append(&json!([
        pending.issue.issue_key,
        serde_json::to_value(pending.issue)?
    ]))?;
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn build_issue_relation_spools(
    scan: &mut ScopeClosureScan,
    events: &JsonlValueSpool,
) -> anyhow::Result<()> {
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
            enforce_scope_closure_memory_budget("prepare_issue_merge_runs")?;
        }
        append_issue_merge_records(
            &mut merge_input,
            tidas_event_issue(&scan.documents, &event)?,
        )
    })?;
    let sorted_input = merge_input.finish()?;
    let mut issues = SortedJsonlRunWriter::new("coalesced-issues")?;
    let mut occurrences = SortedJsonlRunWriter::new("coalesced-occurrences")?;
    let mut affected_roots = SortedJsonlRunWriter::new("coalesced-affected-roots")?;
    let mut stats = IssueRelationStats::default();
    let root_ids = scan
        .roots
        .iter()
        .map(|root| scan.reference_graph.identity_ids.get(root).copied())
        .collect::<Vec<_>>();
    let mut reachability = SourceReachability::default();
    let mut current = None::<CoalescedIssueState>;
    let mut observed = 0_u64;
    sorted_input.visit(|record| {
        observed = observed.saturating_add(1);
        if observed.is_multiple_of(4_096) {
            enforce_scope_closure_memory_budget("coalesce_sorted_issue_runs")?;
        }
        let (source_key, issue, occurrence) = issue_merge_record(&record)?;
        let starts_new_issue = current
            .as_ref()
            .is_some_and(|state| state.issue.issue_key != issue.issue_key);
        if starts_new_issue {
            finalize_coalesced_issue(
                current.take().expect("current issue exists"),
                &scan.roots,
                &root_ids,
                &scan.reference_graph,
                &mut reachability,
                &mut issues,
                &mut affected_roots,
                &mut stats,
            )?;
        }
        let pending = current.get_or_insert_with(|| CoalescedIssueState::new(source_key, issue));
        pending.push_occurrence(occurrence, &mut occurrences, &mut stats)
    })?;
    if let Some(state) = current {
        finalize_coalesced_issue(
            state,
            &scan.roots,
            &root_ids,
            &scan.reference_graph,
            &mut reachability,
            &mut issues,
            &mut affected_roots,
            &mut stats,
        )?;
    }
    let issues = issues.finish()?;
    let occurrences = occurrences.finish()?;
    let affected_roots = affected_roots.finish()?;
    if issues.event_count != stats.issue_count
        || occurrences.event_count != stats.occurrence_count
        || affected_roots.event_count != stats.affected_root_count
    {
        return Err(anyhow::anyhow!(
            "derived relation run counts diverged: issues={}/{}, occurrences={}/{}, affected_roots={}/{}",
            issues.event_count,
            stats.issue_count,
            occurrences.event_count,
            stats.occurrence_count,
            affected_roots.event_count,
            stats.affected_root_count
        ));
    }
    let relation_bytes = issues
        .storage_bytes()
        .saturating_add(occurrences.storage_bytes())
        .saturating_add(affected_roots.storage_bytes());
    let logical_relation_bytes = issues
        .byte_size
        .saturating_add(occurrences.byte_size)
        .saturating_add(affected_roots.byte_size);
    tracing::info!(
        logical_relation_bytes,
        relation_run_storage_bytes = relation_bytes,
        issue_run_count = issues.run_paths.len(),
        occurrence_run_count = occurrences.run_paths.len(),
        affected_root_run_count = affected_roots.run_paths.len(),
        "scope closure derived relation runs completed"
    );
    record_scope_closure_resources(
        "build_issue_relation_runs_complete",
        Some(relation_bytes),
        Some(
            stats
                .issue_count
                .saturating_add(stats.occurrence_count)
                .saturating_add(stats.affected_root_count),
        ),
    );
    let relations = IssueRelationSpools {
        issues,
        occurrences,
        affected_roots,
        stats,
    };
    relations.issues.visit(|record| {
        if scan.issues.len() < ISSUE_INLINE_ISSUE_SAMPLE_LIMIT {
            let issue = record
                .as_array()
                .and_then(|fields| fields.get(1))
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("coalesced issue record omitted payload"))?;
            scan.issues.push(serde_json::from_value(issue)?);
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
            let (entries, artifacts) = writer.finish().unwrap();
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
    #[allow(clippy::too_many_lines)]
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
        let (documents, roots, reference_graph) =
            if let Ok(package_dir) = std::env::var("SCOPE_CLOSURE_REAL_PACKAGE_DIR") {
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
                let base_events = std::env::var("SCOPE_CLOSURE_SCALE_BASE_EVENTS")
                    .ok()
                    .and_then(|value| value.parse::<u64>().ok())
                    .unwrap_or(50_000);
                let event_count = base_events.checked_mul(multiplier).unwrap();
                let details = "x".repeat(480);
                let sources = (0..64_u128)
                    .map(|index| ExactDatasetIdentity {
                        category: DatasetCategory::Sources,
                        id: Uuid::from_u128(10_000 + index),
                        version: "01.00.000".to_owned(),
                    })
                    .collect::<Vec<_>>();
                let roots = (0..4_u128)
                    .map(|index| ExactDatasetIdentity {
                        category: DatasetCategory::Processes,
                        id: Uuid::from_u128(20_000 + index),
                        version: "01.00.000".to_owned(),
                    })
                    .collect::<Vec<_>>();
                let references = roots
                    .iter()
                    .flat_map(|root| {
                        sources.iter().map(move |source| ResolvedReference {
                            source: root.clone(),
                            target: source.clone(),
                            json_path: "$.generated".to_owned(),
                            reference_role: "generated_capacity".to_owned(),
                            requested_version_state: "explicit".to_owned(),
                        })
                    })
                    .collect::<Vec<_>>();
                let reference_graph =
                    CompactReferenceGraph::from_references(&references, &roots).unwrap();
                let mut documents = ClosureDocumentSpoolWriter::new().unwrap();
                for identity in roots.iter().chain(&sources) {
                    documents
                        .append(&ClosureDocument {
                            identity: identity.clone(),
                            payload: json!({}),
                        })
                        .unwrap();
                }
                for index in 0..event_count {
                    let source = &sources[usize::try_from(index).unwrap() % sources.len()];
                    event_writer
                    .append(&json!({
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
                    }))
                    .unwrap();
                }
                (documents.finish().unwrap(), roots, reference_graph)
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
        let relations = scan.issue_relations.as_ref().unwrap();
        let mut partition_bytes = 0_u64;
        let mut total_artifact_bytes = 0_u64;
        let mut artifact_manifest = Vec::new();
        let mut recovered_issue_count = 0_u64;
        let mut recovered_occurrence_count = 0_u64;
        let mut recovered_affected_root_count = 0_u64;
        let mut partition_uncompressed_bytes = 0_u64;
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
            if artifact.descriptor.file_name == "closure-bundle-v1.json" {
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
            if artifact.descriptor.file_name == "manifest.json"
                || artifact.descriptor.file_name.ends_with(".ndjson.zst")
            {
                partition_bytes = partition_bytes.saturating_add(artifact_bytes);
            }
            if artifact.descriptor.file_name == "manifest.json" {
                let manifest: Value =
                    serde_json::from_slice(&fs::read(&artifact.path).unwrap()).unwrap();
                partition_uncompressed_bytes = manifest["partitions"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter_map(|entry| entry["uncompressedByteSize"].as_u64())
                    .sum();
            }
            if artifact.descriptor.file_name.ends_with(".ndjson.zst") {
                let decoded =
                    zstd::stream::decode_all(File::open(&artifact.path).unwrap()).unwrap();
                let records = u64::try_from(
                    decoded
                        .split(|byte| *byte == b'\n')
                        .count()
                        .saturating_sub(1),
                )
                .unwrap();
                if artifact.descriptor.file_name.starts_with("issues/") {
                    recovered_issue_count = recovered_issue_count.saturating_add(records);
                } else if artifact.descriptor.file_name.starts_with("occurrences/") {
                    recovered_occurrence_count = recovered_occurrence_count.saturating_add(records);
                } else if artifact.descriptor.file_name.starts_with("affected-roots/") {
                    recovered_affected_root_count =
                        recovered_affected_root_count.saturating_add(records);
                }
            }
            artifact_manifest.push(artifact.descriptor.clone());
        }
        assert_eq!(recovered_issue_count, relations.stats.issue_count);
        assert_eq!(recovered_occurrence_count, relations.stats.occurrence_count);
        assert_eq!(
            recovered_affected_root_count,
            relations.stats.affected_root_count
        );
        if let Ok(target) = std::env::var("SCOPE_CLOSURE_PRODUCTION_RELATIONS") {
            let target = target.parse::<u64>().unwrap();
            assert_eq!(relations.stats.affected_root_count, target);
            if target >= 3_191_153 {
                assert!(
                    relations.affected_roots.byte_size > VALIDATION_ISSUE_SPOOL_MAX_BYTES,
                    "production-shaped affected-root relation stream must exceed the legacy 2 GiB cap: {}",
                    relations.affected_roots.byte_size
                );
            }
        }
        let after_artifacts = ResourceMeasurement::capture(
            "capacity_after_artifacts",
            ResourceCounters {
                temp_bytes: Some(
                    relations
                        .issues
                        .storage_bytes()
                        .saturating_add(relations.occurrences.storage_bytes())
                        .saturating_add(relations.affected_roots.storage_bytes())
                        .saturating_add(total_artifact_bytes),
                ),
                rows: Some(
                    relations
                        .stats
                        .issue_count
                        .saturating_add(relations.stats.occurrence_count)
                        .saturating_add(relations.stats.affected_root_count),
                ),
                ..ResourceCounters::default()
            },
        );
        let summary = json!({
            "schemaVersion": "lcia.scope-closure-capacity-result.v1",
            "documentCount": document_count,
            "inputEventCount": input_event_count,
            "inputSpoolBytes": input_spool_bytes,
            "inputSpoolSha256": input_spool_sha256,
            "issueCount": relations.stats.issue_count,
            "occurrenceCount": relations.stats.occurrence_count,
            "affectedRootCount": relations.stats.affected_root_count,
            "recoveredRelationCounts": {
                "issues": recovered_issue_count,
                "occurrences": recovered_occurrence_count,
                "affectedRoots": recovered_affected_root_count,
            },
            "relationSpoolBytes": {
                "issues": relations.issues.byte_size,
                "occurrences": relations.occurrences.byte_size,
                "affectedRoots": relations.affected_roots.byte_size,
            },
            "relationRunStorageBytes": {
                "issues": relations.issues.storage_bytes(),
                "occurrences": relations.occurrences.storage_bytes(),
                "affectedRoots": relations.affected_roots.storage_bytes(),
            },
            "relationRunCounts": {
                "issues": relations.issues.run_paths.len(),
                "occurrences": relations.occurrences.run_paths.len(),
                "affectedRoots": relations.affected_roots.run_paths.len(),
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
            "partitionAndManifestBytes": partition_bytes,
            "partitionUncompressedBytes": partition_uncompressed_bytes,
            "closureBundleBytes": closure_bundle_bytes,
            "closureBundleSha256": closure_bundle_sha256,
            "xlsxBytes": xlsx_bytes,
            "xlsxSha256": xlsx_sha256,
            "resourceMeasurements": {
                "afterRelationRuns": after_relation_runs,
                "afterArtifacts": after_artifacts,
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
        let (entries, artifacts) = writer.finish().unwrap();
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
    fn file_backed_closure_bundle_preserves_v1_canonical_bytes() {
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
            tidas_issue_event_count: 0,
            issue_relations: None,
        };
        let expected = json!({
            "schemaVersion": "lcia.scope-closure-bundle.v1",
            "requestedScopeHash": input.requested_scope_hash,
            "policyFingerprint": input.policy_fingerprint,
            "dataSnapshotToken": input.data_snapshot_token,
            "validatorScannerFingerprint": input.expected_validator_scanner_fingerprint,
            "tidasValidation": {
                "describe": validation.describe,
                "finalEvent": validation.final_event,
                "issueEvents": [event],
            },
            "scan": scan,
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
            reference_graph: CompactReferenceGraph::default(),
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
        let path = temp.path().join("closure-bundle-v1.json");
        let bytes = br#"{"schemaVersion":"lcia.scope-closure-bundle.v1"}"#;
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
                "closure-bundle-v1.json",
                "closure-report-v1.xlsx",
                "issues/part-000000.ndjson.zst",
                "manifest.json",
                "occurrences/part-000000.ndjson.zst",
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
            roles["closure-bundle-v1.json"],
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
    fn scope_closure_publication_uses_complete_storage_identity_and_trusted_expiry() {
        assert!(INSERT_SCOPE_CLOSURE_ARTIFACT_SQL.contains("storage_bucket"));
        assert!(INSERT_SCOPE_CLOSURE_ARTIFACT_SQL.contains("storage_path"));
        assert!(INSERT_SCOPE_CLOSURE_ARTIFACT_SQL.contains("artifact_role"));
        assert!(INSERT_SCOPE_CLOSURE_ARTIFACT_SQL.contains("lifecycle_state"));
        assert!(INSERT_SCOPE_CLOSURE_ARTIFACT_SQL.contains("checksum_sha256"));
        assert!(INSERT_SCOPE_CLOSURE_ARTIFACT_SQL.contains("content_type"));
        assert!(INSERT_SCOPE_CLOSURE_ARTIFACT_SQL.contains("byte_size"));
        assert!(INSERT_SCOPE_CLOSURE_ARTIFACT_SQL.contains("expires_at"));
        assert!(INSERT_SCOPE_CLOSURE_ARTIFACT_SQL.contains("transaction_timestamp()"));
        assert_eq!(SCOPE_CLOSURE_ARTIFACT_RETENTION_SECONDS, 604_800);

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
        assert!(
            closure_artifact_metadata(&report, Uuid::nil(), Uuid::nil(), "manifest", None).is_ok(),
            "reused scans publish a current report without a new machine-result manifest"
        );
        let mut bundle = report.clone();
        bundle.descriptor.artifact_role = ScopeClosureArtifactRole::ClosureBundle;
        assert!(
            closure_artifact_metadata(&bundle, Uuid::nil(), Uuid::nil(), "manifest", None).is_err(),
            "fresh closure bundles must bind their machine-result manifest row"
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
    async fn affected_root_partitions_preserve_complete_relations_beyond_inline_sample() {
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
            source: Some(support),
            json_path: None,
            reference_role: None,
            requested_target_type: None,
            requested_target_id: None,
            requested_target_version: None,
            message: "generated support issue".to_owned(),
            suggested_action: None,
            occurrence_count: 0,
            occurrences: Vec::new(),
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

        let validation = TidasBatchValidation {
            describe: json!({"asset_fingerprint": "fixture"}),
            final_event: json!({"type": "final", "completed": true}),
            issue_events: JsonlValueSpool::empty("empty-root-partition-issues.jsonl").unwrap(),
        };
        build_issue_relation_spools(&mut scan, &validation.issue_events).unwrap();
        let artifacts = prepare_issue_partition_artifacts(
            id("cdcdcdcd-cdcd-4dcd-8dcd-cdcdcdcdcdcd"),
            &scan,
            &validation,
            Arc::new(TempDir::new().unwrap()),
        )
        .unwrap();
        let root_records = artifacts
            .iter()
            .filter(|artifact| artifact.descriptor.file_name.starts_with("affected-roots/"))
            .map(|artifact| {
                let decoded =
                    zstd::stream::decode_all(File::open(&artifact.path).unwrap()).unwrap();
                decoded.split(|byte| *byte == b'\n').count() - 1
            })
            .sum::<usize>();
        assert_eq!(root_records, roots.len());
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
