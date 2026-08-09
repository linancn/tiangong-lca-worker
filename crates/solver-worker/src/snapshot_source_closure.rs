use std::collections::BTreeSet;

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    compiled_graph::{
        CompiledReleaseSourceDataset, CompiledReleaseSourceDatasetType,
        CompiledSourceReferenceProvenance, CompiledSourceReferenceSample,
    },
    scope_closure::{DatasetCategory, extract_scope_closure_references},
    source_reference_policy::{
        ArtifactPurpose, SOURCE_REFERENCE_POLICY_VERSION, SourceReferenceAction,
        SourceReferenceRole, classify_malformed_reference_role, classify_reference,
    },
};

pub const DEFAULT_SOURCE_CLOSURE_MAX_REFERENCES: usize = 1_000_000;
pub const DEFAULT_SOURCE_CLOSURE_MAX_EDGE_BYTES: usize = 256 * 1024 * 1024;
pub const DEFAULT_SOURCE_CLOSURE_EVIDENCE_SAMPLES: usize = 64;

#[derive(Debug, Clone, Copy)]
pub struct SourceClosureLimits {
    pub max_references: usize,
    pub max_edge_bytes: usize,
    pub max_evidence_samples: usize,
}

impl Default for SourceClosureLimits {
    fn default() -> Self {
        Self {
            max_references: DEFAULT_SOURCE_CLOSURE_MAX_REFERENCES,
            max_edge_bytes: DEFAULT_SOURCE_CLOSURE_MAX_EDGE_BYTES,
            max_evidence_samples: DEFAULT_SOURCE_CLOSURE_EVIDENCE_SAMPLES,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ClassifiedSourceReference {
    pub source_identity: String,
    pub target_type: CompiledReleaseSourceDatasetType,
    pub target_uuid: String,
    pub requested_version: Option<String>,
    pub json_path: String,
    pub role: SourceReferenceRole,
    pub action: SourceReferenceAction,
}

#[derive(Debug, Clone, Default)]
pub struct SourceClosureClassification {
    pub references: Vec<ClassifiedSourceReference>,
    pub extraction_issues: Vec<Value>,
}

#[derive(Debug, thiserror::Error)]
pub enum SnapshotSourceClosureError {
    #[error("snapshot_source_closure_operator_error: {message}")]
    Operator { message: String },
    #[error("snapshot_source_closure_blocked: {code}")]
    Blocked { code: String, issues: Vec<Value> },
}

pub fn classify_source_document(
    source: &CompiledReleaseSourceDataset,
    purpose: ArtifactPurpose,
) -> Result<SourceClosureClassification, SnapshotSourceClosureError> {
    classify_source_document_with_lcia_flow_axis(source, purpose, None)
}

#[allow(clippy::too_many_lines)]
pub fn classify_source_document_with_lcia_flow_axis(
    source: &CompiledReleaseSourceDataset,
    purpose: ArtifactPurpose,
    active_lcia_flow_ids: Option<&BTreeSet<Uuid>>,
) -> Result<SourceClosureClassification, SnapshotSourceClosureError> {
    let source_category = dataset_category(source.dataset_type);
    let source_identity = format!(
        "{}:{}@{}",
        source.dataset_type.as_str(),
        source.dataset_id,
        source.dataset_version
    );
    let extraction =
        extract_scope_closure_references(&source_identity, source_category, &source.document);
    let mut references = Vec::with_capacity(extraction.edges.len());
    for edge in extraction.edges {
        if !lcia_factor_reference_is_active(
            source.dataset_type,
            edge.json_path.as_str(),
            edge.target_uuid.as_str(),
            active_lcia_flow_ids,
        ) {
            continue;
        }
        let classified = classify_reference(&edge, purpose).map_err(|error| {
            SnapshotSourceClosureError::Operator {
                message: error.to_string(),
            }
        })?;
        let target_type = dataset_type(edge.target_category.as_str()).ok_or_else(|| {
            SnapshotSourceClosureError::Operator {
                message: format!(
                    "unsupported_reference_target_type: {} at {}",
                    edge.target_category, edge.json_path
                ),
            }
        })?;
        references.push(ClassifiedSourceReference {
            source_identity: source_identity.clone(),
            target_type,
            target_uuid: edge.target_uuid,
            requested_version: edge.requested_version,
            json_path: edge.json_path,
            role: classified.role,
            action: classified.action,
        });
    }
    let extraction_issues = extraction
        .issues
        .into_iter()
        .filter_map(|issue| {
            if !lcia_factor_reference_is_active(
                source.dataset_type,
                issue.json_path.as_str(),
                issue
                    .details
                    .get("raw_ref_object_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
                active_lcia_flow_ids,
            ) {
                return None;
            }
            if purpose != ArtifactPurpose::CertificateClosure
                && is_external_digital_file_path(issue.json_path.as_str())
            {
                return None;
            }
            let role = classify_malformed_reference_role(
                issue.source_category.as_str(),
                issue.json_path.as_str(),
            );
            let evidence_only = matches!(
                role,
                Some(
                    SourceReferenceRole::AdministrativeSupport
                        | SourceReferenceRole::Lineage
                        | SourceReferenceRole::ModelComposition
                )
            ) && purpose != ArtifactPurpose::CertificateClosure;
            if evidence_only {
                if !references
                    .iter()
                    .any(|reference| reference.json_path == issue.json_path)
                {
                    let role = role.expect("evidence-only role is present");
                    references.push(ClassifiedSourceReference {
                        source_identity: source_identity.clone(),
                        target_type: malformed_evidence_target_type(
                            role,
                            issue.json_path.as_str(),
                            source.dataset_type,
                        ),
                        target_uuid: issue
                            .details
                            .get("raw_ref_object_id")
                            .and_then(Value::as_str)
                            .unwrap_or("<missing>")
                            .to_owned(),
                        requested_version: None,
                        json_path: issue.json_path,
                        role,
                        action: SourceReferenceAction::RecordEvidence,
                    });
                }
                return None;
            }
            Some(json!({
                "code": "source_reference_invalid",
                "sourceIdentity": issue.document_key,
                "jsonPath": issue.json_path,
                "referenceRole": role.map_or("required_support", SourceReferenceRole::as_str),
                "extractionIssueCode": issue.issue_code,
                "message": issue.message,
                "details": issue.details,
            }))
        })
        .collect();
    references.sort();
    Ok(SourceClosureClassification {
        references,
        extraction_issues,
    })
}

fn lcia_factor_reference_is_active(
    source_type: CompiledReleaseSourceDatasetType,
    json_path: &str,
    target_uuid: &str,
    active_lcia_flow_ids: Option<&BTreeSet<Uuid>>,
) -> bool {
    let Some(active_lcia_flow_ids) = active_lcia_flow_ids else {
        return true;
    };
    if source_type != CompiledReleaseSourceDatasetType::LciaMethod
        || !json_path
            .to_ascii_lowercase()
            .contains("characterisationfactors")
    {
        return true;
    }
    Uuid::parse_str(target_uuid)
        .ok()
        .is_some_and(|flow_id| active_lcia_flow_ids.contains(&flow_id))
}

fn is_external_digital_file_path(json_path: &str) -> bool {
    json_path
        .to_ascii_lowercase()
        .ends_with("referencetodigitalfile")
}

fn malformed_evidence_target_type(
    role: SourceReferenceRole,
    json_path: &str,
    source_type: CompiledReleaseSourceDatasetType,
) -> CompiledReleaseSourceDatasetType {
    if role == SourceReferenceRole::ModelComposition {
        return CompiledReleaseSourceDatasetType::Process;
    }
    let path = json_path.to_ascii_lowercase();
    if path.contains("contact")
        || path.contains("commissioner")
        || path.contains("ownership")
        || path.contains("personorentity")
        || path.contains("registrationauthority")
        || path.contains("entitieswithexclusiveaccess")
    {
        return CompiledReleaseSourceDatasetType::Contact;
    }
    if path.contains("source")
        || path.contains("datasetformat")
        || path.contains("compliancesystem")
        || path.contains("logo")
        || path.contains("technology")
    {
        return CompiledReleaseSourceDatasetType::Source;
    }
    source_type
}

pub fn validate_resource_limits(
    references: &[ClassifiedSourceReference],
    limits: SourceClosureLimits,
) -> Result<(), SnapshotSourceClosureError> {
    if references.len() > limits.max_references {
        return Err(SnapshotSourceClosureError::Operator {
            message: format!(
                "source_reference_limit_exceeded: actual={} limit={}",
                references.len(),
                limits.max_references
            ),
        });
    }
    let edge_bytes = references.iter().try_fold(0_usize, |total, reference| {
        total
            .checked_add(reference.source_identity.len())
            .and_then(|value| value.checked_add(reference.json_path.len()))
            .and_then(|value| value.checked_add(reference.target_uuid.len()))
            .ok_or_else(|| SnapshotSourceClosureError::Operator {
                message: "source_reference_edge_bytes_overflow".to_owned(),
            })
    })?;
    if edge_bytes > limits.max_edge_bytes {
        return Err(SnapshotSourceClosureError::Operator {
            message: format!(
                "source_reference_edge_bytes_exceeded: actual={edge_bytes} limit={}",
                limits.max_edge_bytes
            ),
        });
    }
    Ok(())
}

#[must_use]
pub fn source_dependency_issue(reference: &ClassifiedSourceReference, message: &str) -> Value {
    json!({
        "code": "source_dependency_unavailable",
        "sourceIdentity": reference.source_identity,
        "targetType": reference.target_type.as_str(),
        "targetId": reference.target_uuid,
        "targetVersion": reference.requested_version,
        "jsonPath": reference.json_path,
        "referenceRole": reference.role.as_str(),
        "message": message,
    })
}

pub fn provenance_summary(
    references: &[ClassifiedSourceReference],
    limits: SourceClosureLimits,
) -> Result<Option<CompiledSourceReferenceProvenance>, SnapshotSourceClosureError> {
    let mut evidence = references
        .iter()
        .filter(|reference| {
            matches!(
                reference.action,
                SourceReferenceAction::FetchOptionalSupport | SourceReferenceAction::RecordEvidence
            )
        })
        .map(|reference| CompiledSourceReferenceSample {
            source_identity: reference.source_identity.clone(),
            json_path: reference.json_path.clone(),
            target_category: reference.target_type.as_str().to_owned(),
            target_uuid: reference.target_uuid.clone(),
            requested_version: reference.requested_version.clone(),
            role: reference.role.as_str().to_owned(),
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if evidence.is_empty() {
        return Ok(None);
    }
    let canonical =
        serde_json::to_vec(&evidence).map_err(|error| SnapshotSourceClosureError::Operator {
            message: format!("source_reference_evidence_encode_failed: {error}"),
        })?;
    let evidence_sha256 = format!("{:x}", Sha256::digest(canonical));
    let reference_count =
        u64::try_from(evidence.len()).map_err(|_| SnapshotSourceClosureError::Operator {
            message: "source_reference_count_overflow".to_owned(),
        })?;
    let truncated = evidence.len() > limits.max_evidence_samples;
    evidence.truncate(limits.max_evidence_samples);
    Ok(Some(CompiledSourceReferenceProvenance {
        policy_version: SOURCE_REFERENCE_POLICY_VERSION.to_owned(),
        reference_count,
        evidence_sha256,
        samples: evidence,
        truncated,
    }))
}

pub fn parse_target_uuid(
    reference: &ClassifiedSourceReference,
) -> Result<Uuid, SnapshotSourceClosureError> {
    Uuid::parse_str(reference.target_uuid.as_str()).map_err(|_| {
        SnapshotSourceClosureError::Blocked {
            code: "source_dependency_unavailable".to_owned(),
            issues: vec![source_dependency_issue(
                reference,
                "Required source reference has an invalid target UUID.",
            )],
        }
    })
}

const fn dataset_category(dataset_type: CompiledReleaseSourceDatasetType) -> DatasetCategory {
    match dataset_type {
        CompiledReleaseSourceDatasetType::Contact => DatasetCategory::Contacts,
        CompiledReleaseSourceDatasetType::Flow => DatasetCategory::Flows,
        CompiledReleaseSourceDatasetType::FlowProperty => DatasetCategory::Flowproperties,
        CompiledReleaseSourceDatasetType::LciaMethod => DatasetCategory::Lciamethods,
        CompiledReleaseSourceDatasetType::Process => DatasetCategory::Processes,
        CompiledReleaseSourceDatasetType::Source => DatasetCategory::Sources,
        CompiledReleaseSourceDatasetType::UnitGroup => DatasetCategory::Unitgroups,
    }
}

fn dataset_type(value: &str) -> Option<CompiledReleaseSourceDatasetType> {
    match value {
        "contacts" => Some(CompiledReleaseSourceDatasetType::Contact),
        "flows" => Some(CompiledReleaseSourceDatasetType::Flow),
        "flowproperties" => Some(CompiledReleaseSourceDatasetType::FlowProperty),
        "lciamethods" => Some(CompiledReleaseSourceDatasetType::LciaMethod),
        "processes" => Some(CompiledReleaseSourceDatasetType::Process),
        "sources" => Some(CompiledReleaseSourceDatasetType::Source),
        "unitgroups" => Some(CompiledReleaseSourceDatasetType::UnitGroup),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compiled_graph::CompiledReleaseSourceDatasetRole;

    fn source(
        dataset_type: CompiledReleaseSourceDatasetType,
        document: Value,
    ) -> CompiledReleaseSourceDataset {
        CompiledReleaseSourceDataset {
            dataset_type,
            role: CompiledReleaseSourceDatasetRole::Support,
            dataset_id: Uuid::new_v4(),
            dataset_version: "01.01.000".to_owned(),
            document_sha256: "a".repeat(64),
            document,
        }
    }

    #[test]
    fn evidence_summary_is_stable_bounded_and_target_lookup_free() {
        let document = serde_json::from_str(include_str!(
            "../tests/fixtures/source_reference_policy/review_submit_lineage_flow_does_not_expand_axis.json"
        ))
        .unwrap();
        let classified = classify_source_document(
            &source(CompiledReleaseSourceDatasetType::Flow, document),
            ArtifactPurpose::ReviewSubmit,
        )
        .unwrap();
        let first = provenance_summary(&classified.references, SourceClosureLimits::default())
            .unwrap()
            .unwrap();
        let second = provenance_summary(&classified.references, SourceClosureLimits::default())
            .unwrap()
            .unwrap();
        assert_eq!(first, second);
        assert_eq!(first.reference_count, 1);
        assert_eq!(first.samples.len(), 1);
        assert_eq!(first.samples[0].role, "lineage");
    }

    #[test]
    fn cumulative_reference_count_and_edge_bytes_are_bounded() {
        let reference = ClassifiedSourceReference {
            source_identity: "process:source@01.00.000".to_owned(),
            target_type: CompiledReleaseSourceDatasetType::Flow,
            target_uuid: Uuid::new_v4().to_string(),
            requested_version: Some("01.00.000".to_owned()),
            json_path: "$.exchange.referenceToFlowDataSet".to_owned(),
            role: SourceReferenceRole::ExchangeFlow,
            action: SourceReferenceAction::ValidateExchangeAxis,
        };
        assert!(
            validate_resource_limits(
                &[reference.clone(), reference.clone()],
                SourceClosureLimits {
                    max_references: 1,
                    max_edge_bytes: usize::MAX,
                    max_evidence_samples: 1,
                }
            )
            .is_err()
        );
        assert!(
            validate_resource_limits(
                &[reference],
                SourceClosureLimits {
                    max_references: 1,
                    max_edge_bytes: 1,
                    max_evidence_samples: 1,
                }
            )
            .is_err()
        );
    }

    #[test]
    fn calculation_bundle_ignores_lcia_factor_references_outside_the_active_c_axis() {
        let active_flow = Uuid::new_v4();
        let unrelated_flow = Uuid::new_v4();
        let document = json!({
            "LCIAMethodDataSet": {
                "characterisationFactors": {
                    "factor": [
                        {"referenceToFlowDataSet": {
                            "@type": "flow data set",
                            "@refObjectId": active_flow,
                            "@version": "01.00.000"
                        }},
                        {"referenceToFlowDataSet": {
                            "@type": "flow data set",
                            "@refObjectId": unrelated_flow,
                            "@version": "01.00.000"
                        }},
                        {"referenceToFlowDataSet": {
                            "@type": "flow data set",
                            "@refObjectId": "not-a-uuid",
                            "@version": "01.00.000"
                        }}
                    ]
                }
            }
        });
        let classified = classify_source_document_with_lcia_flow_axis(
            &source(CompiledReleaseSourceDatasetType::LciaMethod, document),
            ArtifactPurpose::CalculationBundle,
            Some(&BTreeSet::from([active_flow])),
        )
        .unwrap();

        assert!(classified.extraction_issues.is_empty());
        assert_eq!(classified.references.len(), 1);
        assert_eq!(
            classified.references[0].target_uuid,
            active_flow.to_string()
        );
    }

    #[test]
    fn certificate_closure_ignores_historical_process_lcia_results() {
        let exchange_flow = Uuid::new_v4();
        let classified = classify_source_document(
            &source(
                CompiledReleaseSourceDatasetType::Process,
                json!({
                    "processDataSet": {
                        "exchanges": {
                            "exchange": {
                                "referenceToFlowDataSet": {
                                    "@type": "flow data set",
                                    "@refObjectId": exchange_flow,
                                    "@version": "01.00.000"
                                }
                            }
                        },
                        "LCIAResults": {
                            "LCIAResult": {
                                "referenceToLCIAMethodDataSet": {
                                    "@type": "LCIA method data set",
                                    "@version": "invalid"
                                },
                                "meanAmount": 1.0
                            }
                        }
                    }
                }),
            ),
            ArtifactPurpose::CertificateClosure,
        )
        .unwrap();

        assert!(classified.extraction_issues.is_empty());
        assert_eq!(classified.references.len(), 1);
        assert_eq!(
            classified.references[0].target_uuid,
            exchange_flow.to_string()
        );
        assert_eq!(
            classified.references[0].role,
            SourceReferenceRole::ExchangeFlow
        );
    }

    #[test]
    fn malformed_lineage_is_evidence_only_but_required_reference_is_blocking() {
        let lineage_cases = [
            json!({
                "referenceToReplacedDataSet": {
                    "@type": "flow data set",
                    "@version": "01.00.000"
                }
            }),
            json!({
                "referenceToOriginalDataSet": {
                    "@type": "flow data set",
                    "@refObjectId": "not-a-uuid",
                    "@version": "01.00.000"
                }
            }),
            json!({
                "referenceToPrecedingDataSetVersion": {
                    "@type": "flow data set",
                    "@refObjectId": Uuid::new_v4(),
                    "@version": "1.0"
                }
            }),
        ];
        for document in lineage_cases {
            let classified = classify_source_document(
                &source(CompiledReleaseSourceDatasetType::Flow, document),
                ArtifactPurpose::ReviewSubmit,
            )
            .unwrap();
            assert!(
                classified.extraction_issues.is_empty(),
                "lineage validation must remain evidence-only"
            );
            assert!(classified.references.iter().all(|reference| {
                reference.role == SourceReferenceRole::Lineage
                    && reference.action == SourceReferenceAction::RecordEvidence
            }));
        }

        let required = classify_source_document(
            &source(
                CompiledReleaseSourceDatasetType::Process,
                json!({
                    "exchanges": {
                        "exchange": [{
                            "referenceToFlowDataSet": {
                                "@type": "flow data set",
                                "@version": "01.00.000"
                            }
                        }]
                    }
                }),
            ),
            ArtifactPurpose::ReviewSubmit,
        )
        .unwrap();
        assert_eq!(required.extraction_issues.len(), 1);
        assert_eq!(
            required.extraction_issues[0]["code"],
            "source_reference_invalid"
        );
        assert_eq!(
            required.extraction_issues[0]["referenceRole"],
            "exchange_flow"
        );
    }

    #[test]
    fn empty_administrative_placeholder_is_evidence_only_for_numeric_artifacts() {
        let document = json!({
            "flowDataSet": {
                "administrativeInformation": {
                    "dataEntryBy": {
                        "common:referenceToPersonOrEntityEnteringTheData": {}
                    }
                }
            }
        });
        for purpose in [
            ArtifactPurpose::ReviewSubmit,
            ArtifactPurpose::CalculationBundle,
        ] {
            let classified = classify_source_document(
                &source(CompiledReleaseSourceDatasetType::Flow, document.clone()),
                purpose,
            )
            .unwrap();
            assert!(classified.extraction_issues.is_empty());
            assert_eq!(classified.references.len(), 1);
            assert_eq!(
                classified.references[0].role,
                SourceReferenceRole::AdministrativeSupport
            );
            assert_eq!(
                classified.references[0].action,
                SourceReferenceAction::RecordEvidence
            );
        }

        let certificate = classify_source_document(
            &source(CompiledReleaseSourceDatasetType::Flow, document),
            ArtifactPurpose::CertificateClosure,
        )
        .unwrap();
        assert!(!certificate.extraction_issues.is_empty());
    }

    #[test]
    fn external_digital_file_uri_does_not_block_numeric_source_closure() {
        let document = json!({
            "sourceDataSet": {
                "sourceInformation": {
                    "dataSetInformation": {
                        "referenceToDigitalFile": {
                            "@uri": "http://lca.jrc.ec.europa.eu"
                        }
                    }
                }
            }
        });

        for purpose in [
            ArtifactPurpose::ReviewSubmit,
            ArtifactPurpose::CalculationBundle,
        ] {
            let classified = classify_source_document(
                &source(CompiledReleaseSourceDatasetType::Source, document.clone()),
                purpose,
            )
            .unwrap();
            assert!(classified.references.is_empty());
            assert!(classified.extraction_issues.is_empty());
        }

        let certificate = classify_source_document(
            &source(CompiledReleaseSourceDatasetType::Source, document),
            ArtifactPurpose::CertificateClosure,
        )
        .unwrap();
        assert!(certificate.references.is_empty());
        assert_eq!(certificate.extraction_issues.len(), 2);
        assert!(certificate.extraction_issues.iter().all(|issue| {
            issue["jsonPath"]
                .as_str()
                .is_some_and(|path| path.ends_with("referenceToDigitalFile"))
        }));
    }
}
