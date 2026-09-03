use std::collections::BTreeMap;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

use hdf5::File;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use solver_core::ModelSparseData;
use tempfile::{Builder, NamedTempFile};
use uuid::Uuid;

use crate::calculation_evidence::LcaMethodFactorSourceSnapshot;
use crate::compiled_graph::{self, CompiledGraph, CompiledReleaseEvidence};
use crate::graph_types::{RequestRootProcess, SnapshotSelectionMode};

const SCHEMA_VERSION: u8 = 1;
const DATASET_SCHEMA_VERSION: &str = "schema_version";
const DATASET_FORMAT: &str = "format";
const DATASET_ENVELOPE_JSON: &str = "envelope_json";
const HDF5_DEFLATE_LEVEL: u8 = 4;
const HDF5_CHUNK_TARGET_BYTES: usize = 256 * 1024;

/// Snapshot matrix artifact format identifier.
pub const SNAPSHOT_ARTIFACT_FORMAT: &str = "snapshot-hdf5:v1";
/// Snapshot artifact file extension.
pub const SNAPSHOT_ARTIFACT_EXTENSION: &str = "h5";
/// Snapshot artifact content type.
pub const SNAPSHOT_ARTIFACT_CONTENT_TYPE: &str = "application/x-hdf5";
/// Purpose-specific Calculation Bundle evidence format identifier.
pub const SNAPSHOT_RELEASE_EVIDENCE_FORMAT: &str = "snapshot-release-evidence-json-zstd:v2";
const SNAPSHOT_RELEASE_EVIDENCE_LEGACY_FORMAT: &str = "snapshot-release-evidence-json-zstd:v1";
/// Purpose-specific Calculation Bundle evidence file extension.
pub const SNAPSHOT_RELEASE_EVIDENCE_EXTENSION: &str = "json.zst";
/// Purpose-specific Calculation Bundle evidence content type.
pub const SNAPSHOT_RELEASE_EVIDENCE_CONTENT_TYPE: &str =
    "application/vnd.tiangong.snapshot-release-evidence+json+zstd";
/// Content-addressed immutable source-closure format used by Calculation Bundles.
pub const SNAPSHOT_SOURCE_CLOSURE_FORMAT: &str = "snapshot-source-closure-json-zstd:v1";
/// Source-closure file extension.
pub const SNAPSHOT_SOURCE_CLOSURE_EXTENSION: &str = "json.zst";
/// Source-closure content type.
pub const SNAPSHOT_SOURCE_CLOSURE_CONTENT_TYPE: &str =
    "application/vnd.tiangong.snapshot-source-closure+json+zstd";
/// Snapshot coverage JSON schema identifier.
pub const SNAPSHOT_COVERAGE_SCHEMA_VERSION: &str = "snapshot_coverage.v3";

/// Certificate-grade scope binding embedded inside a numerical snapshot artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScopeClosureSnapshotBinding {
    pub schema_version: String,
    pub effective_scope_hash: String,
    pub data_snapshot_token: String,
    pub closure_bundle_hash: String,
}

/// Snapshot build options persisted in artifact metadata.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotBuildConfig {
    /// `state_code` selection used in builder.
    pub process_states: String,
    /// Optional `user_id` inclusion in process selection.
    #[serde(default)]
    pub include_user_id: Option<Uuid>,
    /// Named versioned visibility scope, when present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_scope: Option<String>,
    /// Canonical visibility manifest binding, when present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope_manifest_sha256: Option<String>,
    /// Immutable scope-closure evidence consumed by package Build V2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope_closure_binding: Option<ScopeClosureSnapshotBinding>,
    /// Exact database method/factor snapshot proof used by the build.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lcia_method_factor_source: Option<LcaMethodFactorSourceSnapshot>,
    /// Snapshot selection mode (`filtered_library` / `request_roots_closure`).
    #[serde(default)]
    pub selection_mode: SnapshotSelectionMode,
    /// Explicit request roots for request-scoped graph builds.
    #[serde(default)]
    pub request_roots: Vec<RequestRootProcess>,
    /// Process cap (`0` means unlimited).
    pub process_limit: i32,
    /// Provider matching mode.
    pub provider_rule: String,
    /// Provider candidate eligibility mode.
    #[serde(default)]
    pub provider_candidate_eligibility_mode: String,
    /// Versioned lineage gate applied before provider routing.
    #[serde(default = "default_provider_lineage_policy")]
    pub provider_lineage_policy: String,
    #[serde(default)]
    pub provider_lineage_source_sha256: Option<String>,
    /// Quantitative reference normalization mode (`strict`/`lenient`).
    #[serde(default = "default_strict_mode")]
    pub reference_normalization_mode: String,
    /// Allocation fraction mode (`strict`/`lenient`).
    #[serde(default = "default_strict_mode")]
    pub allocation_fraction_mode: String,
    /// Versioned TIDAS allocation/reference semantics used by matrix compilation.
    #[serde(default = "default_legacy_allocation_semantics_version")]
    pub allocation_semantics_version: String,
    /// Versioned signed-flow balance/link semantics used to derive activity requirements.
    #[serde(default = "default_legacy_link_semantics_version")]
    pub link_semantics_version: String,
    /// Explicit policy for unresolved non-zero technosphere balance coefficients.
    #[serde(default = "default_closed_boundary_policy")]
    pub technosphere_boundary_policy: String,
    /// Versioned exact flow identity and unit compatibility policy.
    #[serde(default = "default_flow_identity_policy")]
    pub flow_identity_policy: String,
    /// Versioned policy for freezing LCIA-factor Flow documents into source closure.
    #[serde(default = "default_source_closure_policy")]
    pub source_closure_policy: String,
    /// Versioned path-aware role × artifact-purpose policy.
    #[serde(default = "default_legacy_source_reference_policy")]
    pub source_reference_policy: String,
    /// Biosphere sign convention (`signed`/`gross`).
    #[serde(default = "default_biosphere_sign_mode")]
    pub biosphere_sign_mode: String,
    /// Self-loop cutoff for technosphere diagonal filtering.
    pub self_loop_cutoff: f64,
    /// Near-singular epsilon.
    pub singular_eps: f64,
    /// Whether LCIA factors were enabled.
    pub has_lcia: bool,
    /// Optional lifecycle / caller purpose for source-hash isolation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_purpose: Option<String>,
    /// Optional dependency surface fingerprint for review-submit baseline reuse.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_dependency_fingerprint: Option<String>,
    /// Optional authoritative root revision checksum for review-submit overlay reuse.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_revision_checksum: Option<String>,
    /// Optional LCIA method id.
    pub method_id: Option<Uuid>,
    /// Optional LCIA method version.
    pub method_version: Option<String>,
}

fn default_provider_lineage_policy() -> String {
    "legacy-flow-compatible-v0".to_owned()
}

/// Matching coverage diagnostics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct SnapshotProviderDecisionDiagnostics {
    #[serde(default)]
    pub resolved_strategy_counts: BTreeMap<String, i64>,
    #[serde(default)]
    pub unresolved_reason_counts: BTreeMap<String, i64>,
    #[serde(default)]
    pub candidate_eligibility_counts: BTreeMap<String, i64>,
    #[serde(default)]
    pub rejected_non_reference_output_count: i64,
    #[serde(default)]
    pub volume_fallback_to_one_count: i64,
    #[serde(default)]
    pub geography_tier_counts: BTreeMap<String, i64>,
    #[serde(default)]
    pub supply_region_source_counts: BTreeMap<String, i64>,
}

/// Provider candidate distribution diagnostics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct SnapshotCandidateSummary {
    #[serde(default)]
    pub candidate_count_histogram: BTreeMap<String, i64>,
}

/// Provider resolution diagnostics in the canonical v2 summary layout.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct SnapshotResolutionSummary {
    #[serde(default)]
    pub resolved_strategy_counts: BTreeMap<String, i64>,
    #[serde(default)]
    pub unresolved_reason_counts: BTreeMap<String, i64>,
}

/// Geography and supply-region diagnostics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct SnapshotGeographySummary {
    #[serde(default)]
    pub tier_counts: BTreeMap<String, i64>,
    #[serde(default)]
    pub tier_counts_by_strategy: BTreeMap<String, BTreeMap<String, i64>>,
    #[serde(default)]
    pub supply_region_source_counts: BTreeMap<String, i64>,
    #[serde(default)]
    pub supply_region_source_counts_by_strategy: BTreeMap<String, BTreeMap<String, i64>>,
    #[serde(default)]
    pub exchange_location_present_count: i64,
    #[serde(default)]
    pub exchange_location_present_count_by_strategy: BTreeMap<String, i64>,
    #[serde(default)]
    pub requested_location_granularity_counts: BTreeMap<String, i64>,
    #[serde(default)]
    pub requested_location_granularity_counts_by_strategy: BTreeMap<String, BTreeMap<String, i64>>,
}

/// Annual supply / production volume weight quality diagnostics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct SnapshotVolumeWeightSummary {
    #[serde(default)]
    pub candidate_total: i64,
    #[serde(default)]
    pub valid_volume_count: i64,
    #[serde(default)]
    pub fallback_to_one_count: i64,
    #[serde(default)]
    pub decisions_total: i64,
    #[serde(default)]
    pub decisions_all_valid_count: i64,
    #[serde(default)]
    pub decisions_partial_missing_count: i64,
    #[serde(default)]
    pub decisions_all_missing_count: i64,
}

/// Top unmatched flow entry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotUnmatchedFlowEntry {
    pub flow_id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flow_name: Option<String>,
    pub count: i64,
}

/// Top process gap entry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotProcessGapEntry {
    pub process_id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_name: Option<String>,
    pub input_edges_total: i64,
    pub unmatched_no_provider: i64,
    pub a_write_pct: f64,
}

/// No-provider gap diagnostics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct SnapshotGapSummary {
    #[serde(default)]
    pub unmatched_top_flows: Vec<SnapshotUnmatchedFlowEntry>,
    #[serde(default)]
    pub process_gap_top: Vec<SnapshotProcessGapEntry>,
}

/// Matching coverage diagnostics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotMatchingCoverage {
    pub input_edges_total: i64,
    pub matched_unique_provider: i64,
    pub matched_multi_provider: i64,
    pub unmatched_no_provider: i64,
    #[serde(default)]
    pub matched_multi_resolved: i64,
    #[serde(default)]
    pub matched_multi_unresolved: i64,
    #[serde(default)]
    pub matched_multi_fallback_equal: i64,
    #[serde(default)]
    pub a_input_edges_written: i64,
    #[serde(default)]
    pub residual_edges_total: i64,
    #[serde(default)]
    pub a_balance_edges_written: i64,
    #[serde(default)]
    pub a_write_pct: f64,
    #[serde(default)]
    pub provider_present_resolved_pct: f64,
    pub unique_provider_match_pct: f64,
    pub any_provider_match_pct: f64,
    #[serde(default)]
    pub provider_decision_diagnostics: SnapshotProviderDecisionDiagnostics,
    #[serde(default)]
    pub candidate_summary: SnapshotCandidateSummary,
    #[serde(default)]
    pub resolution_summary: SnapshotResolutionSummary,
    #[serde(default)]
    pub geography_summary: SnapshotGeographySummary,
    #[serde(default)]
    pub volume_weight_summary: SnapshotVolumeWeightSummary,
    #[serde(default)]
    pub gap_summary: SnapshotGapSummary,
}

/// Quantitative reference diagnostics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct SnapshotReferenceCoverage {
    pub process_total: i64,
    pub normalized_process_count: i64,
    pub missing_reference_count: i64,
    pub invalid_reference_count: i64,
}

/// Allocation diagnostics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct SnapshotAllocationCoverage {
    pub exchange_total: i64,
    pub allocation_fraction_present_pct: f64,
    pub allocation_fraction_missing_count: i64,
    pub allocation_fraction_invalid_count: i64,
    #[serde(default)]
    pub legacy_empty_allocation_as_undeclared_count: i64,
    #[serde(default)]
    pub legacy_single_output_target_inferred_count: i64,
    #[serde(default)]
    pub legacy_single_reference_target_inferred_count: i64,
}

/// Singular risk diagnostics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotSingularRisk {
    pub risk_level: String,
    pub prefilter_diag_abs_ge_cutoff: i64,
    pub postfilter_a_diag_abs_ge_cutoff: i64,
    pub m_zero_diagonal_count: i64,
    pub m_min_abs_diagonal: f64,
}

/// Matrix scale diagnostics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotMatrixScale {
    pub process_count: i64,
    pub flow_count: i64,
    pub impact_count: i64,
    pub a_nnz: i64,
    pub b_nnz: i64,
    pub c_nnz: i64,
    pub m_nnz_estimated: i64,
    pub m_sparsity_estimated: f64,
}

/// Snapshot coverage report persisted beside payload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotCoverageReport {
    #[serde(default = "default_coverage_schema_version")]
    pub schema_version: String,
    pub matching: SnapshotMatchingCoverage,
    #[serde(default)]
    pub reference: SnapshotReferenceCoverage,
    #[serde(default)]
    pub allocation: SnapshotAllocationCoverage,
    pub singular_risk: SnapshotSingularRisk,
    pub matrix_scale: SnapshotMatrixScale,
}

fn default_coverage_schema_version() -> String {
    SNAPSHOT_COVERAGE_SCHEMA_VERSION.to_owned()
}

fn default_strict_mode() -> String {
    "strict".to_owned()
}

fn default_legacy_allocation_semantics_version() -> String {
    "legacy-unscoped-v0".to_owned()
}

fn default_legacy_link_semantics_version() -> String {
    "legacy-directional-link-v0".to_owned()
}

fn default_closed_boundary_policy() -> String {
    "closed".to_owned()
}

fn default_flow_identity_policy() -> String {
    "exact-flow-version-reference-unit-v1".to_owned()
}

fn default_source_closure_policy() -> String {
    "snapshot-exchange-flows-only-v0".to_owned()
}

fn default_legacy_source_reference_policy() -> String {
    "source-reference-policy.legacy-unclassified-v1".to_owned()
}

fn default_biosphere_sign_mode() -> String {
    "signed".to_owned()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotArtifactEnvelope {
    version: u8,
    format: String,
    snapshot_id: Uuid,
    config: SnapshotBuildConfig,
    coverage: SnapshotCoverageReport,
    payload: ModelSparseData,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    compiled_graph: Option<CompiledGraph>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    release_evidence_artifact: Option<SnapshotLinkedArtifact>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    review_baseline: Option<SnapshotReviewBaseline>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    review_gate_evidence: Option<SnapshotReviewGateEvidence>,
}

#[derive(Debug, Deserialize)]
struct SnapshotArtifactEnvelopeRead {
    version: u8,
    format: String,
    snapshot_id: Uuid,
    config: SnapshotBuildConfig,
    coverage: SnapshotCoverageReport,
    payload: ModelSparseData,
    #[serde(default)]
    compiled_graph: Option<serde_json::Value>,
    #[serde(default)]
    release_evidence_artifact: Option<SnapshotLinkedArtifact>,
    #[serde(default)]
    review_baseline: Option<SnapshotReviewBaseline>,
    #[serde(default)]
    review_gate_evidence: Option<SnapshotReviewGateEvidence>,
}

/// Integrity-bound reference to a purpose-specific snapshot sidecar.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotLinkedArtifact {
    pub format: String,
    pub object_url: String,
    pub sha256: String,
    pub byte_size: u64,
    pub content_type: String,
}

/// Stable Review Submit baseline state. This is a consumer-owned projection rather than
/// serialized compiler IR; release-only evidence is deliberately absent.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotReviewBaseline {
    pub processes: Vec<compiled_graph::CompiledProcess>,
    pub flows: Vec<compiled_graph::CompiledFlow>,
    pub reference_ports: Vec<compiled_graph::CompiledReferencePort>,
    pub balance_resolutions: Vec<compiled_graph::CompiledBalanceResolution>,
    pub unresolved_balances: Vec<compiled_graph::CompiledUnresolvedBalance>,
    pub provider_outputs: Vec<compiled_graph::CompiledProviderOutput>,
    pub provider_decisions: Vec<compiled_graph::CompiledProviderDecision>,
    pub technosphere_edges: Vec<compiled_graph::CompiledTechnosphereEdge>,
    pub biosphere_edges: Vec<compiled_graph::CompiledBiosphereEdge>,
    pub reference_stats: compiled_graph::CompiledReferenceStats,
    pub allocation_stats: compiled_graph::CompiledAllocationStats,
    pub matching_stats: compiled_graph::CompiledMatchingStats,
}

impl From<&CompiledGraph> for SnapshotReviewBaseline {
    fn from(graph: &CompiledGraph) -> Self {
        Self {
            processes: graph.processes.clone(),
            flows: graph.flows.clone(),
            reference_ports: graph.reference_ports.clone(),
            balance_resolutions: graph.balance_resolutions.clone(),
            unresolved_balances: graph.unresolved_balances.clone(),
            provider_outputs: graph.provider_outputs.clone(),
            provider_decisions: graph.provider_decisions.clone(),
            technosphere_edges: graph.technosphere_edges.clone(),
            biosphere_edges: graph.biosphere_edges.clone(),
            reference_stats: graph.reference_stats,
            allocation_stats: graph.allocation_stats,
            matching_stats: graph.matching_stats,
        }
    }
}

impl SnapshotReviewBaseline {
    /// Restores transient compiler state for one Review Submit overlay build.
    #[must_use]
    pub fn into_compiled_graph(self) -> CompiledGraph {
        CompiledGraph {
            processes: self.processes,
            flows: self.flows,
            reference_ports: self.reference_ports,
            balance_resolutions: self.balance_resolutions,
            unresolved_balances: self.unresolved_balances,
            provider_outputs: self.provider_outputs,
            provider_decisions: self.provider_decisions,
            technosphere_edges: self.technosphere_edges,
            biosphere_edges: self.biosphere_edges,
            reference_stats: self.reference_stats,
            allocation_stats: self.allocation_stats,
            matching_stats: self.matching_stats,
            release_evidence: None,
        }
    }
}

/// Minimal Review Submit fast-gate evidence persisted with an overlay snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotReviewGateEvidence {
    pub flows: Vec<compiled_graph::CompiledFlow>,
    pub provider_decisions: Vec<compiled_graph::CompiledProviderDecision>,
    pub technosphere_edges: Vec<compiled_graph::CompiledTechnosphereEdge>,
    pub biosphere_edges: Vec<compiled_graph::CompiledBiosphereEdge>,
}

impl From<&CompiledGraph> for SnapshotReviewGateEvidence {
    fn from(graph: &CompiledGraph) -> Self {
        Self {
            flows: graph.flows.clone(),
            provider_decisions: graph.provider_decisions.clone(),
            technosphere_edges: graph.technosphere_edges.clone(),
            biosphere_edges: graph.biosphere_edges.clone(),
        }
    }
}

impl SnapshotReviewGateEvidence {
    /// Adapts the stable projection to the legacy in-memory gate input without persisting IR.
    #[must_use]
    pub fn into_compiled_graph(self) -> CompiledGraph {
        CompiledGraph {
            processes: Vec::new(),
            flows: self.flows,
            reference_ports: Vec::new(),
            balance_resolutions: Vec::new(),
            unresolved_balances: Vec::new(),
            provider_outputs: Vec::new(),
            provider_decisions: self.provider_decisions,
            technosphere_edges: self.technosphere_edges,
            biosphere_edges: self.biosphere_edges,
            reference_stats: compiled_graph::CompiledReferenceStats::default(),
            allocation_stats: compiled_graph::CompiledAllocationStats::default(),
            matching_stats: compiled_graph::CompiledMatchingStats::default(),
            release_evidence: None,
        }
    }
}

/// Purpose-specific non-source metadata consumed by Calculation Bundle generation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotReleaseEvidence {
    pub processes: Vec<compiled_graph::CompiledReleaseProcess>,
    pub inventory_exchanges: Vec<compiled_graph::CompiledReleaseInventoryExchange>,
    pub technosphere_edges: Vec<compiled_graph::CompiledReleaseTechnosphereEdge>,
    pub biosphere_edges: Vec<compiled_graph::CompiledReleaseInventoryExchange>,
    pub source_reference_provenance: Option<compiled_graph::CompiledSourceReferenceProvenance>,
    pub source_dataset_count: u64,
    pub source_closure_artifact: SnapshotLinkedArtifact,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SnapshotReleaseEvidenceRef<'a> {
    processes: &'a [compiled_graph::CompiledReleaseProcess],
    inventory_exchanges: &'a [compiled_graph::CompiledReleaseInventoryExchange],
    technosphere_edges: &'a [compiled_graph::CompiledReleaseTechnosphereEdge],
    biosphere_edges: &'a [compiled_graph::CompiledReleaseInventoryExchange],
    source_reference_provenance: &'a Option<compiled_graph::CompiledSourceReferenceProvenance>,
    source_dataset_count: u64,
    source_closure_artifact: &'a SnapshotLinkedArtifact,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SnapshotReleaseEvidenceEnvelopeRef<'a> {
    version: u8,
    format: &'static str,
    snapshot_id: Uuid,
    release_evidence: SnapshotReleaseEvidenceRef<'a>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SnapshotReleaseEvidenceEnvelope {
    version: u8,
    format: String,
    snapshot_id: Uuid,
    release_evidence: SnapshotReleaseEvidence,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LegacySnapshotReleaseEvidenceEnvelope {
    version: u8,
    format: String,
    snapshot_id: Uuid,
    release_evidence: CompiledReleaseEvidence,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SnapshotSourceClosureEnvelopeRef<'a> {
    version: u8,
    format: &'static str,
    source_datasets: &'a [compiled_graph::CompiledReleaseSourceDataset],
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SnapshotSourceClosureEnvelope {
    version: u8,
    format: String,
    source_datasets: Vec<compiled_graph::CompiledReleaseSourceDataset>,
}

/// Decoded release sidecar, including the temporary v1 compatibility shape.
#[derive(Debug)]
pub enum DecodedSnapshotReleaseEvidence {
    Linked(SnapshotReleaseEvidence),
    Legacy(CompiledReleaseEvidence),
}

/// Encoded snapshot artifact bytes and metadata.
#[derive(Debug, Clone)]
pub struct EncodedSnapshotArtifact {
    pub bytes: Vec<u8>,
    pub sha256: String,
    pub byte_size: usize,
    pub format: &'static str,
    pub content_type: &'static str,
    pub extension: &'static str,
}

/// Decoded snapshot artifact payload.
#[derive(Debug, Clone)]
pub struct DecodedSnapshotArtifact {
    pub snapshot_id: Uuid,
    pub config: SnapshotBuildConfig,
    pub coverage: SnapshotCoverageReport,
    pub payload: ModelSparseData,
    pub compiled_graph: Option<CompiledGraph>,
    /// Legacy compiler metadata may be unreadable after schema evolution; numerical payload
    /// decoding remains available and purpose-specific consumers fail with rebuild guidance.
    pub compiled_graph_decode_error: Option<String>,
    pub release_evidence_artifact: Option<SnapshotLinkedArtifact>,
    pub review_baseline: Option<SnapshotReviewBaseline>,
    pub review_gate_evidence: Option<SnapshotReviewGateEvidence>,
}

/// File-backed, compressed Calculation Bundle evidence and its integrity metadata.
#[derive(Debug)]
pub struct EncodedSnapshotReleaseEvidenceArtifact {
    file: NamedTempFile,
    pub sha256: String,
    pub byte_size: u64,
    pub format: &'static str,
    pub content_type: &'static str,
    pub extension: &'static str,
}

/// File-backed, content-addressed source closure and its integrity metadata.
#[derive(Debug)]
pub struct EncodedSnapshotSourceClosureArtifact {
    file: NamedTempFile,
    pub sha256: String,
    pub byte_size: u64,
    pub format: &'static str,
    pub content_type: &'static str,
    pub extension: &'static str,
}

impl EncodedSnapshotSourceClosureArtifact {
    /// Local file path retained until the encoded artifact is dropped.
    #[must_use]
    pub fn path(&self) -> &Path {
        self.file.path()
    }
}

impl EncodedSnapshotReleaseEvidenceArtifact {
    /// Local file path retained until the encoded artifact is dropped.
    #[must_use]
    pub fn path(&self) -> &Path {
        self.file.path()
    }
}

/// Encodes one snapshot matrix payload into `HDF5`.
pub fn encode_snapshot_artifact(
    snapshot_id: Uuid,
    config: SnapshotBuildConfig,
    coverage: SnapshotCoverageReport,
    payload: &ModelSparseData,
) -> anyhow::Result<EncodedSnapshotArtifact> {
    encode_snapshot_artifact_with_graph(snapshot_id, config, coverage, payload, None)
}

/// Encodes one snapshot matrix payload plus optional compiled graph metadata into `HDF5`.
pub fn encode_snapshot_artifact_with_graph(
    snapshot_id: Uuid,
    config: SnapshotBuildConfig,
    coverage: SnapshotCoverageReport,
    payload: &ModelSparseData,
    compiled_graph: Option<CompiledGraph>,
) -> anyhow::Result<EncodedSnapshotArtifact> {
    encode_snapshot_artifact_with_links(
        snapshot_id,
        config,
        coverage,
        payload,
        compiled_graph,
        None,
    )
}

/// Encodes one numerical snapshot with optional legacy graph and linked business artifacts.
pub fn encode_snapshot_artifact_with_links(
    snapshot_id: Uuid,
    config: SnapshotBuildConfig,
    coverage: SnapshotCoverageReport,
    payload: &ModelSparseData,
    compiled_graph: Option<CompiledGraph>,
    release_evidence_artifact: Option<SnapshotLinkedArtifact>,
) -> anyhow::Result<EncodedSnapshotArtifact> {
    encode_snapshot_artifact_with_purpose_artifacts(
        snapshot_id,
        config,
        coverage,
        payload,
        compiled_graph,
        release_evidence_artifact,
        None,
        None,
    )
}

/// Encodes a numerical snapshot plus explicit purpose-specific projections.
#[allow(clippy::too_many_arguments)]
pub fn encode_snapshot_artifact_with_purpose_artifacts(
    snapshot_id: Uuid,
    config: SnapshotBuildConfig,
    coverage: SnapshotCoverageReport,
    payload: &ModelSparseData,
    compiled_graph: Option<CompiledGraph>,
    release_evidence_artifact: Option<SnapshotLinkedArtifact>,
    review_baseline: Option<SnapshotReviewBaseline>,
    review_gate_evidence: Option<SnapshotReviewGateEvidence>,
) -> anyhow::Result<EncodedSnapshotArtifact> {
    let envelope = SnapshotArtifactEnvelope {
        version: SCHEMA_VERSION,
        format: SNAPSHOT_ARTIFACT_FORMAT.to_owned(),
        snapshot_id,
        config,
        coverage,
        payload: payload.clone(),
        compiled_graph,
        release_evidence_artifact,
        review_baseline,
        review_gate_evidence,
    };

    let json = serde_json::to_vec(&envelope)?;
    let temp = Builder::new()
        .prefix("lca-snapshot-artifact-")
        .suffix(".h5")
        .tempfile()?;
    write_hdf5_file(temp.path(), json.as_slice())?;
    let bytes = std::fs::read(temp.path())?;

    let mut hasher = Sha256::new();
    hasher.update(bytes.as_slice());
    let sha256 = format!("{:x}", hasher.finalize());

    Ok(EncodedSnapshotArtifact {
        byte_size: bytes.len(),
        bytes,
        sha256,
        format: SNAPSHOT_ARTIFACT_FORMAT,
        content_type: SNAPSHOT_ARTIFACT_CONTENT_TYPE,
        extension: SNAPSHOT_ARTIFACT_EXTENSION,
    })
}

/// Decodes snapshot artifact bytes into sparse payload.
pub fn decode_snapshot_artifact(bytes: &[u8]) -> anyhow::Result<DecodedSnapshotArtifact> {
    let temp = Builder::new()
        .prefix("lca-snapshot-artifact-read-")
        .suffix(".h5")
        .tempfile()?;
    std::fs::write(temp.path(), bytes)?;

    let file = File::open(temp.path())?;
    let format_bytes = file
        .dataset(DATASET_FORMAT)?
        .read_1d::<u8>()?
        .into_raw_vec();
    let format = String::from_utf8(format_bytes)?;
    if format != SNAPSHOT_ARTIFACT_FORMAT {
        return Err(anyhow::anyhow!(
            "unsupported snapshot artifact format: {format}"
        ));
    }

    let envelope_bytes = file
        .dataset(DATASET_ENVELOPE_JSON)?
        .read_1d::<u8>()?
        .into_raw_vec();
    let envelope: SnapshotArtifactEnvelopeRead = serde_json::from_slice(&envelope_bytes)?;
    if envelope.version != SCHEMA_VERSION || envelope.format != SNAPSHOT_ARTIFACT_FORMAT {
        return Err(anyhow::anyhow!(
            "unsupported snapshot envelope: version={} format={}",
            envelope.version,
            envelope.format
        ));
    }
    if envelope.payload.model_version != envelope.snapshot_id {
        return Err(anyhow::anyhow!(
            "snapshot payload model_version mismatch: payload={} envelope={}",
            envelope.payload.model_version,
            envelope.snapshot_id
        ));
    }

    let (compiled_graph, compiled_graph_decode_error) = match envelope.compiled_graph {
        Some(value) => match serde_json::from_value(value) {
            Ok(graph) => (Some(graph), None),
            Err(error) => (None, Some(error.to_string())),
        },
        None => (None, None),
    };

    Ok(DecodedSnapshotArtifact {
        snapshot_id: envelope.snapshot_id,
        config: envelope.config,
        coverage: envelope.coverage,
        payload: envelope.payload,
        compiled_graph,
        compiled_graph_decode_error,
        release_evidence_artifact: envelope.release_evidence_artifact,
        review_baseline: envelope.review_baseline,
        review_gate_evidence: envelope.review_gate_evidence,
    })
}

/// Encodes Calculation Bundle release evidence directly to a compressed temporary file.
pub fn encode_snapshot_release_evidence_artifact(
    snapshot_id: Uuid,
    release_evidence: &CompiledReleaseEvidence,
    source_closure_artifact: &SnapshotLinkedArtifact,
) -> anyhow::Result<EncodedSnapshotReleaseEvidenceArtifact> {
    let source_dataset_count = u64::try_from(release_evidence.source_datasets.len())?;
    let file = Builder::new()
        .prefix("lca-snapshot-release-evidence-")
        .suffix(".json.zst")
        .tempfile()?;
    let output = std::fs::File::create(file.path())?;
    let buffered = BufWriter::new(output);
    let mut encoder = zstd::stream::write::Encoder::new(buffered, 3)?;
    serde_json::to_writer(
        &mut encoder,
        &SnapshotReleaseEvidenceEnvelopeRef {
            version: SCHEMA_VERSION,
            format: SNAPSHOT_RELEASE_EVIDENCE_FORMAT,
            snapshot_id,
            release_evidence: SnapshotReleaseEvidenceRef {
                processes: &release_evidence.processes,
                inventory_exchanges: &release_evidence.inventory_exchanges,
                technosphere_edges: &release_evidence.technosphere_edges,
                biosphere_edges: &release_evidence.biosphere_edges,
                source_reference_provenance: &release_evidence.source_reference_provenance,
                source_dataset_count,
                source_closure_artifact,
            },
        },
    )?;
    let mut buffered = encoder.finish()?;
    buffered.flush()?;

    let (byte_size, sha256) = hash_file(file.path())?;
    Ok(EncodedSnapshotReleaseEvidenceArtifact {
        file,
        sha256,
        byte_size,
        format: SNAPSHOT_RELEASE_EVIDENCE_FORMAT,
        content_type: SNAPSHOT_RELEASE_EVIDENCE_CONTENT_TYPE,
        extension: SNAPSHOT_RELEASE_EVIDENCE_EXTENSION,
    })
}

/// Decodes and validates one file-backed Calculation Bundle evidence sidecar.
pub fn decode_snapshot_release_evidence_artifact(
    path: &Path,
    expected_snapshot_id: Uuid,
) -> anyhow::Result<DecodedSnapshotReleaseEvidence> {
    let input = std::fs::File::open(path)?;
    let decoder = zstd::stream::read::Decoder::new(BufReader::new(input))?;
    let value: serde_json::Value = serde_json::from_reader(decoder)?;
    let format = value
        .get("format")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    if format == SNAPSHOT_RELEASE_EVIDENCE_LEGACY_FORMAT {
        let envelope: LegacySnapshotReleaseEvidenceEnvelope = serde_json::from_value(value)?;
        if envelope.version != SCHEMA_VERSION
            || envelope.format != SNAPSHOT_RELEASE_EVIDENCE_LEGACY_FORMAT
            || envelope.snapshot_id != expected_snapshot_id
        {
            return Err(anyhow::anyhow!(
                "legacy snapshot release evidence mismatch: expected={} got={}",
                expected_snapshot_id,
                envelope.snapshot_id
            ));
        }
        return Ok(DecodedSnapshotReleaseEvidence::Legacy(
            envelope.release_evidence,
        ));
    }
    let envelope: SnapshotReleaseEvidenceEnvelope = serde_json::from_value(value)?;
    if envelope.version != SCHEMA_VERSION || envelope.format != SNAPSHOT_RELEASE_EVIDENCE_FORMAT {
        return Err(anyhow::anyhow!(
            "unsupported snapshot release evidence format: version={} format={}",
            envelope.version,
            envelope.format
        ));
    }
    if envelope.snapshot_id != expected_snapshot_id {
        return Err(anyhow::anyhow!(
            "snapshot release evidence mismatch: expected={} got={}",
            expected_snapshot_id,
            envelope.snapshot_id
        ));
    }
    Ok(DecodedSnapshotReleaseEvidence::Linked(
        envelope.release_evidence,
    ))
}

/// Encodes immutable source documents separately from Calculation Bundle metadata.
pub fn encode_snapshot_source_closure_artifact(
    source_datasets: &[compiled_graph::CompiledReleaseSourceDataset],
) -> anyhow::Result<EncodedSnapshotSourceClosureArtifact> {
    let file = Builder::new()
        .prefix("lca-snapshot-source-closure-")
        .suffix(".json.zst")
        .tempfile()?;
    let output = std::fs::File::create(file.path())?;
    let buffered = BufWriter::new(output);
    let mut encoder = zstd::stream::write::Encoder::new(buffered, 3)?;
    serde_json::to_writer(
        &mut encoder,
        &SnapshotSourceClosureEnvelopeRef {
            version: SCHEMA_VERSION,
            format: SNAPSHOT_SOURCE_CLOSURE_FORMAT,
            source_datasets,
        },
    )?;
    let mut buffered = encoder.finish()?;
    buffered.flush()?;
    let (byte_size, sha256) = hash_file(file.path())?;
    Ok(EncodedSnapshotSourceClosureArtifact {
        file,
        sha256,
        byte_size,
        format: SNAPSHOT_SOURCE_CLOSURE_FORMAT,
        content_type: SNAPSHOT_SOURCE_CLOSURE_CONTENT_TYPE,
        extension: SNAPSHOT_SOURCE_CLOSURE_EXTENSION,
    })
}

/// Decodes and validates a content-addressed source closure.
pub fn decode_snapshot_source_closure_artifact(
    path: &Path,
) -> anyhow::Result<Vec<compiled_graph::CompiledReleaseSourceDataset>> {
    let input = std::fs::File::open(path)?;
    let decoder = zstd::stream::read::Decoder::new(BufReader::new(input))?;
    let envelope: SnapshotSourceClosureEnvelope = serde_json::from_reader(decoder)?;
    if envelope.version != SCHEMA_VERSION || envelope.format != SNAPSHOT_SOURCE_CLOSURE_FORMAT {
        return Err(anyhow::anyhow!(
            "unsupported snapshot source closure format: version={} format={}",
            envelope.version,
            envelope.format
        ));
    }
    Ok(envelope.source_datasets)
}

fn hash_file(path: &Path) -> anyhow::Result<(u64, String)> {
    let mut input = std::fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    let mut byte_size = 0_u64;
    loop {
        let count = input.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        byte_size = byte_size
            .checked_add(u64::try_from(count)?)
            .ok_or_else(|| anyhow::anyhow!("snapshot release evidence size overflow"))?;
        hasher.update(&buffer[..count]);
    }
    Ok((byte_size, format!("{:x}", hasher.finalize())))
}

fn write_hdf5_file(path: &Path, envelope_json: &[u8]) -> anyhow::Result<()> {
    let file = File::create(path)?;
    file.new_dataset_builder()
        .with_data(&[SCHEMA_VERSION])
        .create(DATASET_SCHEMA_VERSION)?;
    file.new_dataset_builder()
        .with_data(SNAPSHOT_ARTIFACT_FORMAT.as_bytes())
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
    use serde_json::json;
    use solver_core::{ModelSparseData, SparseTriplet};
    use std::collections::BTreeMap;
    use tempfile::Builder;

    use crate::compiled_graph::{
        CompiledAllocationStats, CompiledFlow, CompiledFlowKind, CompiledGraph,
        CompiledMatchingStats, CompiledReferenceStats, CompiledReleaseEvidence,
        CompiledReleaseSourceDataset, CompiledReleaseSourceDatasetRole,
        CompiledReleaseSourceDatasetType,
    };

    use super::{
        DATASET_ENVELOPE_JSON, DecodedSnapshotReleaseEvidence, HDF5_DEFLATE_LEVEL,
        SNAPSHOT_ARTIFACT_FORMAT, SNAPSHOT_COVERAGE_SCHEMA_VERSION,
        SNAPSHOT_RELEASE_EVIDENCE_FORMAT, SnapshotAllocationCoverage, SnapshotBuildConfig,
        SnapshotCandidateSummary, SnapshotCoverageReport, SnapshotGapSummary,
        SnapshotGeographySummary, SnapshotLinkedArtifact, SnapshotMatchingCoverage,
        SnapshotMatrixScale, SnapshotProviderDecisionDiagnostics, SnapshotReferenceCoverage,
        SnapshotResolutionSummary, SnapshotReviewBaseline, SnapshotReviewGateEvidence,
        SnapshotSelectionMode, SnapshotSingularRisk, SnapshotVolumeWeightSummary,
        decode_snapshot_artifact, decode_snapshot_release_evidence_artifact,
        decode_snapshot_source_closure_artifact, encode_snapshot_artifact,
        encode_snapshot_artifact_with_graph, encode_snapshot_artifact_with_links,
        encode_snapshot_artifact_with_purpose_artifacts, encode_snapshot_release_evidence_artifact,
        encode_snapshot_source_closure_artifact,
    };

    #[test]
    #[allow(clippy::too_many_lines)]
    fn encode_decode_snapshot_artifact_roundtrip() {
        let snapshot_id = uuid::Uuid::new_v4();
        let config = SnapshotBuildConfig {
            process_states: crate::default_snapshot_process_states_arg(),
            include_user_id: None,
            data_scope: None,
            scope_manifest_sha256: None,
            scope_closure_binding: None,
            lcia_method_factor_source: None,
            selection_mode: SnapshotSelectionMode::FilteredLibrary,
            request_roots: Vec::new(),
            process_limit: 0,
            provider_rule: "strict_unique_provider".to_owned(),
            provider_candidate_eligibility_mode: "reference_output_only".to_owned(),
            provider_lineage_policy: "legacy-flow-compatible-v0".to_owned(),
            provider_lineage_source_sha256: None,
            reference_normalization_mode: "strict".to_owned(),
            allocation_fraction_mode: "strict".to_owned(),
            allocation_semantics_version: "tidas-quantitative-reference-v2".to_owned(),
            link_semantics_version: "legacy-directional-link-v0".to_owned(),
            technosphere_boundary_policy: "closed".to_owned(),
            flow_identity_policy: "exact-flow-version-reference-unit-v1".to_owned(),
            source_closure_policy: "snapshot-exchange-flows-only-v0".to_owned(),
            source_reference_policy: "source-reference-policy.legacy-unclassified-v1".to_owned(),
            biosphere_sign_mode: "gross".to_owned(),
            self_loop_cutoff: 0.999_999,
            singular_eps: 1e-12,
            has_lcia: true,
            artifact_purpose: None,
            root_dependency_fingerprint: None,
            root_revision_checksum: None,
            method_id: Some(uuid::Uuid::new_v4()),
            method_version: Some("01.00.000".to_owned()),
        };
        let coverage = SnapshotCoverageReport {
            schema_version: SNAPSHOT_COVERAGE_SCHEMA_VERSION.to_owned(),
            matching: SnapshotMatchingCoverage {
                input_edges_total: 10,
                matched_unique_provider: 7,
                matched_multi_provider: 2,
                unmatched_no_provider: 1,
                matched_multi_resolved: 1,
                matched_multi_unresolved: 1,
                matched_multi_fallback_equal: 0,
                a_input_edges_written: 8,
                residual_edges_total: 10,
                a_balance_edges_written: 8,
                a_write_pct: 80.0,
                provider_present_resolved_pct: 88.888_888_888_888_89,
                unique_provider_match_pct: 70.0,
                any_provider_match_pct: 90.0,
                provider_decision_diagnostics: SnapshotProviderDecisionDiagnostics {
                    resolved_strategy_counts: BTreeMap::from([
                        ("unique_provider".to_owned(), 7),
                        ("split_by_evidence".to_owned(), 1),
                    ]),
                    unresolved_reason_counts: BTreeMap::from([(
                        "rule_requires_unique_provider".to_owned(),
                        1,
                    )]),
                    candidate_eligibility_counts: BTreeMap::new(),
                    rejected_non_reference_output_count: 0,
                    volume_fallback_to_one_count: 0,
                    geography_tier_counts: BTreeMap::new(),
                    supply_region_source_counts: BTreeMap::new(),
                },
                candidate_summary: SnapshotCandidateSummary::default(),
                resolution_summary: SnapshotResolutionSummary::default(),
                geography_summary: SnapshotGeographySummary::default(),
                volume_weight_summary: SnapshotVolumeWeightSummary::default(),
                gap_summary: SnapshotGapSummary::default(),
            },
            reference: SnapshotReferenceCoverage {
                process_total: 2,
                normalized_process_count: 2,
                missing_reference_count: 0,
                invalid_reference_count: 0,
            },
            allocation: SnapshotAllocationCoverage {
                exchange_total: 4,
                allocation_fraction_present_pct: 100.0,
                allocation_fraction_missing_count: 0,
                allocation_fraction_invalid_count: 0,
                legacy_empty_allocation_as_undeclared_count: 2,
                legacy_single_output_target_inferred_count: 1,
                legacy_single_reference_target_inferred_count: 0,
            },
            singular_risk: SnapshotSingularRisk {
                risk_level: "low".to_owned(),
                prefilter_diag_abs_ge_cutoff: 0,
                postfilter_a_diag_abs_ge_cutoff: 0,
                m_zero_diagonal_count: 0,
                m_min_abs_diagonal: 1.0,
            },
            matrix_scale: SnapshotMatrixScale {
                process_count: 2,
                flow_count: 2,
                impact_count: 1,
                a_nnz: 2,
                b_nnz: 2,
                c_nnz: 1,
                m_nnz_estimated: 4,
                m_sparsity_estimated: 0.0,
            },
        };
        let payload = ModelSparseData {
            model_version: snapshot_id,
            process_count: 2,
            flow_count: 2,
            impact_count: 1,
            technosphere_entries: vec![
                SparseTriplet {
                    row: 0,
                    col: 1,
                    value: 0.1,
                },
                SparseTriplet {
                    row: 1,
                    col: 0,
                    value: 0.2,
                },
            ],
            biosphere_entries: vec![
                SparseTriplet {
                    row: 0,
                    col: 0,
                    value: 1.0,
                },
                SparseTriplet {
                    row: 1,
                    col: 1,
                    value: -2.0,
                },
            ],
            characterization_factors: vec![SparseTriplet {
                row: 0,
                col: 1,
                value: 3.5,
            }],
        };

        let encoded =
            encode_snapshot_artifact(snapshot_id, config.clone(), coverage.clone(), &payload)
                .expect("encode");
        assert_eq!(encoded.format, SNAPSHOT_ARTIFACT_FORMAT);
        assert_eq!(encoded.byte_size, encoded.bytes.len());
        let file = write_and_open_hdf5(encoded.bytes.as_slice());
        let envelope_ds = file
            .dataset(DATASET_ENVELOPE_JSON)
            .expect("envelope_json dataset");
        assert!(envelope_ds.is_chunked());
        let filters = envelope_ds.filters();
        assert!(filters.iter().any(
            |filter| matches!(filter, Filter::Deflate(level) if *level == HDF5_DEFLATE_LEVEL)
        ));

        let decoded = decode_snapshot_artifact(encoded.bytes.as_slice()).expect("decode");
        assert_eq!(decoded.snapshot_id, snapshot_id);
        assert_eq!(decoded.config, config);
        assert_eq!(decoded.coverage, coverage);
        assert_eq!(decoded.payload, payload);
        assert!(decoded.compiled_graph.is_none());

        let product_flow_id = uuid::Uuid::new_v4();
        let graph = CompiledGraph {
            processes: Vec::new(),
            flows: vec![CompiledFlow {
                flow_idx: 0,
                flow_id: product_flow_id,
                flow_version: "01.00.000".to_owned(),
                kind: CompiledFlowKind::Product,
                space: crate::compiled_graph::CompiledFlowSpace::Technosphere,
                source_type: crate::compiled_graph::CompiledSourceFlowType::Product,
            }],
            reference_ports: Vec::new(),
            balance_resolutions: Vec::new(),
            unresolved_balances: Vec::new(),
            provider_outputs: Vec::new(),
            provider_decisions: Vec::new(),
            technosphere_edges: Vec::new(),
            biosphere_edges: Vec::new(),
            reference_stats: CompiledReferenceStats::default(),
            allocation_stats: CompiledAllocationStats {
                legacy_empty_allocation_as_undeclared_count: 2,
                legacy_single_output_target_inferred_count: 1,
                ..CompiledAllocationStats::default()
            },
            matching_stats: CompiledMatchingStats::default(),
            release_evidence: None,
        };
        let encoded_with_graph = encode_snapshot_artifact_with_graph(
            snapshot_id,
            config.clone(),
            coverage.clone(),
            &payload,
            Some(graph.clone()),
        )
        .expect("encode with graph");
        let decoded_with_graph =
            decode_snapshot_artifact(encoded_with_graph.bytes.as_slice()).expect("decode graph");
        let decoded_graph = decoded_with_graph.compiled_graph.expect("compiled graph");
        assert_eq!(decoded_graph.flows.len(), 1);
        assert_eq!(decoded_graph.flows[0].flow_id, product_flow_id);
        assert_eq!(decoded_graph.flows[0].kind, CompiledFlowKind::Product);
        assert_eq!(
            decoded_graph
                .allocation_stats
                .legacy_empty_allocation_as_undeclared_count,
            2
        );
        assert_eq!(
            decoded_graph
                .allocation_stats
                .legacy_single_output_target_inferred_count,
            1
        );

        let encoded_graph_file = Builder::new()
            .suffix(".h5")
            .tempfile()
            .expect("legacy graph source tempfile");
        std::fs::write(encoded_graph_file.path(), &encoded_with_graph.bytes)
            .expect("write legacy graph source");
        let file = File::open(encoded_graph_file.path()).expect("open legacy graph source");
        let envelope_bytes = file
            .dataset(DATASET_ENVELOPE_JSON)
            .expect("legacy envelope dataset")
            .read_1d::<u8>()
            .expect("read legacy envelope")
            .into_raw_vec();
        let mut incompatible_envelope: serde_json::Value =
            serde_json::from_slice(&envelope_bytes).expect("parse legacy envelope");
        incompatible_envelope["compiled_graph"]["flows"][0]["kind"] =
            json!("removed_legacy_variant");
        let incompatible_file = Builder::new()
            .suffix(".h5")
            .tempfile()
            .expect("incompatible graph tempfile");
        super::write_hdf5_file(
            incompatible_file.path(),
            &serde_json::to_vec(&incompatible_envelope).expect("encode incompatible envelope"),
        )
        .expect("write incompatible graph artifact");
        let incompatible = decode_snapshot_artifact(
            &std::fs::read(incompatible_file.path()).expect("read incompatible graph artifact"),
        )
        .expect("numerical payload survives incompatible compiler metadata");
        assert!(incompatible.compiled_graph.is_none());
        assert!(incompatible.compiled_graph_decode_error.is_some());
        assert_eq!(incompatible.payload.model_version, snapshot_id);

        let encoded_review = encode_snapshot_artifact_with_purpose_artifacts(
            snapshot_id,
            config,
            coverage,
            &payload,
            None,
            None,
            Some(SnapshotReviewBaseline::from(&graph)),
            Some(SnapshotReviewGateEvidence::from(&graph)),
        )
        .expect("encode review projections");
        let decoded_review =
            decode_snapshot_artifact(&encoded_review.bytes).expect("decode review");
        assert!(decoded_review.compiled_graph.is_none());
        assert_eq!(decoded_review.review_baseline.unwrap().processes.len(), 0);
        assert_eq!(decoded_review.review_gate_evidence.unwrap().flows.len(), 1);

        let release_evidence = CompiledReleaseEvidence {
            processes: Vec::new(),
            inventory_exchanges: Vec::new(),
            technosphere_edges: Vec::new(),
            biosphere_edges: Vec::new(),
            source_datasets: Vec::new(),
            source_reference_provenance: None,
        };
        let source_closure_artifact = SnapshotLinkedArtifact {
            format: super::SNAPSHOT_SOURCE_CLOSURE_FORMAT.to_owned(),
            object_url: "s3://bucket/source.json.zst".to_owned(),
            sha256: "a".repeat(64),
            byte_size: 42,
            content_type: super::SNAPSHOT_SOURCE_CLOSURE_CONTENT_TYPE.to_owned(),
        };
        let encoded_evidence = encode_snapshot_release_evidence_artifact(
            snapshot_id,
            &release_evidence,
            &source_closure_artifact,
        )
        .expect("encode release evidence");
        assert_eq!(encoded_evidence.format, SNAPSHOT_RELEASE_EVIDENCE_FORMAT);
        assert!(encoded_evidence.byte_size > 0);
        assert_eq!(encoded_evidence.sha256.len(), 64);
        let decoded_evidence =
            decode_snapshot_release_evidence_artifact(encoded_evidence.path(), snapshot_id)
                .expect("decode release evidence");
        let DecodedSnapshotReleaseEvidence::Linked(decoded_evidence) = decoded_evidence else {
            panic!("expected linked release evidence");
        };
        assert!(decoded_evidence.processes.is_empty());
        assert_eq!(decoded_evidence.source_closure_artifact.byte_size, 42);

        let legacy_file = Builder::new()
            .suffix(".json.zst")
            .tempfile()
            .expect("legacy evidence tempfile");
        let mut legacy_encoder = zstd::stream::write::Encoder::new(
            std::fs::File::create(legacy_file.path()).expect("legacy evidence file"),
            3,
        )
        .expect("legacy evidence encoder");
        serde_json::to_writer(
            &mut legacy_encoder,
            &super::LegacySnapshotReleaseEvidenceEnvelope {
                version: super::SCHEMA_VERSION,
                format: super::SNAPSHOT_RELEASE_EVIDENCE_LEGACY_FORMAT.to_owned(),
                snapshot_id,
                release_evidence,
            },
        )
        .expect("write legacy evidence");
        legacy_encoder.finish().expect("finish legacy evidence");
        assert!(matches!(
            decode_snapshot_release_evidence_artifact(legacy_file.path(), snapshot_id)
                .expect("decode legacy evidence"),
            DecodedSnapshotReleaseEvidence::Legacy(_)
        ));

        let source_dataset = CompiledReleaseSourceDataset {
            dataset_type: CompiledReleaseSourceDatasetType::Process,
            role: CompiledReleaseSourceDatasetRole::UnitProcess,
            dataset_id: uuid::Uuid::new_v4(),
            dataset_version: "01.00.000".to_owned(),
            document_sha256: "b".repeat(64),
            document: json!({"marker": "stored-only-in-source-closure"}),
        };
        let encoded_source =
            encode_snapshot_source_closure_artifact(std::slice::from_ref(&source_dataset))
                .expect("encode source closure");
        let encoded_source_again =
            encode_snapshot_source_closure_artifact(std::slice::from_ref(&source_dataset))
                .expect("encode source closure deterministically");
        assert_eq!(encoded_source.sha256, encoded_source_again.sha256);
        let decoded_source = decode_snapshot_source_closure_artifact(encoded_source.path())
            .expect("decode source closure");
        assert_eq!(decoded_source.len(), 1);
        assert_eq!(decoded_source[0].document, source_dataset.document);
    }

    #[test]
    fn snapshot_build_config_defaults_legacy_biosphere_sign_mode() {
        let legacy = json!({
            "process_states": "100",
            "process_limit": 0,
            "provider_rule": "strict_unique_provider",
            "reference_normalization_mode": "strict",
            "allocation_fraction_mode": "strict",
            "self_loop_cutoff": 0.999_999,
            "singular_eps": 1e-12,
            "has_lcia": true,
            "method_id": null,
            "method_version": null
        });
        let parsed: SnapshotBuildConfig = serde_json::from_value(legacy).expect("parse legacy");
        assert_eq!(parsed.biosphere_sign_mode, "signed");
        assert_eq!(parsed.allocation_semantics_version, "legacy-unscoped-v0");
        assert_eq!(parsed.include_user_id, None);
        assert_eq!(
            parsed.source_reference_policy,
            "source-reference-policy.legacy-unclassified-v1"
        );
        assert_eq!(
            parsed.selection_mode,
            SnapshotSelectionMode::FilteredLibrary
        );
        assert!(parsed.request_roots.is_empty());
    }

    #[test]
    #[ignore = "qualification gate: requires SNAPSHOT_ARTIFACT_QUALIFICATION_PATH"]
    fn qualified_legacy_snapshot_projection_sizes() {
        let path = std::env::var("SNAPSHOT_ARTIFACT_QUALIFICATION_PATH")
            .expect("set SNAPSHOT_ARTIFACT_QUALIFICATION_PATH to a read-only production artifact");
        let legacy_bytes = std::fs::read(&path).expect("read qualification artifact");
        let decoded = decode_snapshot_artifact(&legacy_bytes).expect("decode legacy snapshot");
        let file = File::open(&path).expect("open qualification HDF5");
        let envelope_bytes = file
            .dataset(DATASET_ENVELOPE_JSON)
            .expect("qualification envelope dataset")
            .read_1d::<u8>()
            .expect("read qualification envelope")
            .into_raw_vec();
        let raw: serde_json::Value =
            serde_json::from_slice(&envelope_bytes).expect("parse qualification envelope JSON");
        let raw_compiled_graph_bytes = raw
            .get("compiled_graph")
            .map(|value| {
                serde_json::to_vec(value)
                    .expect("serialize raw compiled graph")
                    .len()
            })
            .unwrap_or_default();
        let raw_release_evidence_bytes = raw
            .pointer("/compiled_graph/release_evidence")
            .map(|value| {
                serde_json::to_vec(value)
                    .expect("serialize raw release evidence")
                    .len()
            })
            .unwrap_or_default();
        let source_dataset_count = raw
            .pointer("/compiled_graph/release_evidence/source_datasets")
            .and_then(serde_json::Value::as_array)
            .map_or(0, Vec::len);

        let (release_descriptor, release_bytes, source_bytes) = decoded
            .compiled_graph
            .as_ref()
            .and_then(|graph| graph.release_evidence.as_ref())
            .map_or((None, None, None), |release_evidence| {
                let source =
                    encode_snapshot_source_closure_artifact(&release_evidence.source_datasets)
                        .expect("encode source closure");
                let source_descriptor = SnapshotLinkedArtifact {
                    format: source.format.to_owned(),
                    object_url: format!("qualification://source-closure/{}", source.sha256),
                    sha256: source.sha256.clone(),
                    byte_size: source.byte_size,
                    content_type: source.content_type.to_owned(),
                };
                let release = encode_snapshot_release_evidence_artifact(
                    decoded.snapshot_id,
                    release_evidence,
                    &source_descriptor,
                )
                .expect("encode release metadata");
                (
                    Some(SnapshotLinkedArtifact {
                        format: release.format.to_owned(),
                        object_url: format!("qualification://release-evidence/{}", release.sha256),
                        sha256: release.sha256.clone(),
                        byte_size: release.byte_size,
                        content_type: release.content_type.to_owned(),
                    }),
                    Some(release.byte_size),
                    Some(source.byte_size),
                )
            });
        let numerical = encode_snapshot_artifact_with_links(
            decoded.snapshot_id,
            decoded.config,
            decoded.coverage,
            &decoded.payload,
            None,
            release_descriptor,
        )
        .expect("encode numerical snapshot");

        assert!(numerical.byte_size < legacy_bytes.len());
        assert!(
            decode_snapshot_artifact(&numerical.bytes)
                .expect("decode projected numerical snapshot")
                .compiled_graph
                .is_none()
        );
        println!(
            "{}",
            json!({
                "legacyHdf5Bytes": legacy_bytes.len(),
                "numericalHdf5Bytes": numerical.byte_size,
                "rawCompiledGraphJsonBytes": raw_compiled_graph_bytes,
                "rawReleaseEvidenceJsonBytes": raw_release_evidence_bytes,
                "releaseMetadataCompressedBytes": release_bytes,
                "sourceClosureCompressedBytes": source_bytes,
                "sourceDatasetCount": source_dataset_count,
                "legacyCompilerDecodeError": decoded.compiled_graph_decode_error,
            })
        );
    }

    #[test]
    fn allocation_coverage_defaults_legacy_fallback_counts_to_zero() {
        let parsed: SnapshotAllocationCoverage = serde_json::from_value(json!({
            "exchange_total": 4,
            "allocation_fraction_present_pct": 50.0,
            "allocation_fraction_missing_count": 2,
            "allocation_fraction_invalid_count": 0
        }))
        .expect("parse legacy allocation coverage");

        assert_eq!(parsed.legacy_empty_allocation_as_undeclared_count, 0);
        assert_eq!(parsed.legacy_single_output_target_inferred_count, 0);
    }

    fn write_and_open_hdf5(bytes: &[u8]) -> File {
        let temp = Builder::new()
            .prefix("lca-snapshot-artifact-test-")
            .suffix(".h5")
            .tempfile()
            .expect("create tempfile");
        std::fs::write(temp.path(), bytes).expect("write hdf5 bytes");
        File::open(temp.path()).expect("open hdf5 file")
    }
}
