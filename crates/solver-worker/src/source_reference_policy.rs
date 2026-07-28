use serde::{Deserialize, Serialize};

use crate::scope_closure::ReferenceEdge;

pub const SOURCE_REFERENCE_POLICY_VERSION: &str = "source-reference-policy.v2";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactPurpose {
    ReviewSubmit,
    CalculationBundle,
    CertificateClosure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceReferenceRole {
    ExchangeFlow,
    ProviderProcess,
    RequiredSupport,
    Lineage,
    ModelComposition,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceReferenceAction {
    ValidateExchangeAxis,
    ValidateProviderInvariant,
    FetchRequiredSupport,
    RecordEvidence,
    TraverseAdministrative,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClassifiedSourceReference {
    pub policy_version: String,
    pub role: SourceReferenceRole,
    pub action: SourceReferenceAction,
}

#[derive(Debug, thiserror::Error)]
pub enum SourceReferencePolicyError {
    #[error(
        "unknown_flow_or_process_reference_path: source={source_category} target={target_category} path={json_path}"
    )]
    UnknownFlowOrProcessPath {
        source_category: String,
        target_category: String,
        json_path: String,
    },
}

pub fn classify_reference(
    edge: &ReferenceEdge,
    purpose: ArtifactPurpose,
) -> Result<ClassifiedSourceReference, SourceReferencePolicyError> {
    let role = classify_role(edge)?;
    let action = match (purpose, role) {
        (ArtifactPurpose::CertificateClosure, _) => SourceReferenceAction::TraverseAdministrative,
        (_, SourceReferenceRole::ExchangeFlow) => SourceReferenceAction::ValidateExchangeAxis,
        (_, SourceReferenceRole::ProviderProcess) => {
            SourceReferenceAction::ValidateProviderInvariant
        }
        (_, SourceReferenceRole::RequiredSupport) => SourceReferenceAction::FetchRequiredSupport,
        (_, SourceReferenceRole::Lineage | SourceReferenceRole::ModelComposition) => {
            SourceReferenceAction::RecordEvidence
        }
    };
    Ok(ClassifiedSourceReference {
        policy_version: SOURCE_REFERENCE_POLICY_VERSION.to_owned(),
        role,
        action,
    })
}

fn classify_role(edge: &ReferenceEdge) -> Result<SourceReferenceRole, SourceReferencePolicyError> {
    let path = edge.json_path.to_ascii_lowercase();
    let source = edge.source_category.as_str();
    let target = edge.target_category.as_str();

    if source == "processes"
        && target == "flows"
        && path.contains("exchange")
        && path.ends_with("referencetoflowdataset")
    {
        return Ok(SourceReferenceRole::ExchangeFlow);
    }
    if source == "lciamethods"
        && target == "flows"
        && (path.contains("characterisation") || path.contains("characterization"))
    {
        return Ok(SourceReferenceRole::RequiredSupport);
    }
    if path.contains("referencetoprecedingdatasetversion")
        || path.contains("referencetoreplaceddataset")
        || path.contains("referencetooriginaldataset")
    {
        return Ok(SourceReferenceRole::Lineage);
    }
    if path.contains("referencetoincludedprocesses")
        || path.contains("referencetoincludedprocess")
        || (source == "lifecyclemodels" && target == "processes")
    {
        return Ok(SourceReferenceRole::ModelComposition);
    }
    if source == "processes"
        && target == "processes"
        && (path.contains("provider") || path.contains("referenceprocess"))
    {
        return Ok(SourceReferenceRole::ProviderProcess);
    }
    if matches!(
        target,
        "contacts" | "flowproperties" | "lciamethods" | "sources" | "unitgroups"
    ) {
        return Ok(SourceReferenceRole::RequiredSupport);
    }
    if matches!(target, "flows" | "processes") {
        return Err(SourceReferencePolicyError::UnknownFlowOrProcessPath {
            source_category: edge.source_category.clone(),
            target_category: edge.target_category.clone(),
            json_path: edge.json_path.clone(),
        });
    }
    Ok(SourceReferenceRole::RequiredSupport)
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::*;
    use crate::scope_closure::{DatasetCategory, extract_references};

    fn one_edge(category: DatasetCategory, payload: &str) -> ReferenceEdge {
        let payload: Value = serde_json::from_str(payload).unwrap();
        let extraction = extract_references("fixture", category, &payload);
        assert!(extraction.issues.is_empty());
        assert_eq!(extraction.edges.len(), 1);
        extraction.edges.into_iter().next().unwrap()
    }

    #[test]
    fn lineage_and_model_composition_are_evidence_only_for_numeric_artifacts() {
        let lineage = one_edge(
            DatasetCategory::Flows,
            include_str!(
                "../tests/fixtures/source_reference_policy/review_submit_lineage_flow_does_not_expand_axis.json"
            ),
        );
        let composition = one_edge(
            DatasetCategory::Processes,
            include_str!(
                "../tests/fixtures/source_reference_policy/review_submit_model_composition_does_not_expand_axis.json"
            ),
        );
        for purpose in [
            ArtifactPurpose::ReviewSubmit,
            ArtifactPurpose::CalculationBundle,
        ] {
            assert_eq!(
                classify_reference(&lineage, purpose).unwrap().action,
                SourceReferenceAction::RecordEvidence
            );
            assert_eq!(
                classify_reference(&composition, purpose).unwrap().action,
                SourceReferenceAction::RecordEvidence
            );
        }
        assert_eq!(
            classify_reference(&lineage, ArtifactPurpose::CertificateClosure)
                .unwrap()
                .action,
            SourceReferenceAction::TraverseAdministrative
        );
    }

    #[test]
    fn exchange_flow_is_axis_validation_and_unknown_flow_path_is_operator_error() {
        let exchange = one_edge(
            DatasetCategory::Processes,
            include_str!(
                "../tests/fixtures/source_reference_policy/review_submit_exchange_control.json"
            ),
        );
        assert_eq!(
            classify_reference(&exchange, ArtifactPurpose::ReviewSubmit)
                .unwrap()
                .action,
            SourceReferenceAction::ValidateExchangeAxis
        );
        let mut unknown = exchange;
        unknown.json_path = "$.processDataSet.unknownFlowPointer".to_owned();
        assert!(matches!(
            classify_reference(&unknown, ArtifactPurpose::ReviewSubmit),
            Err(SourceReferencePolicyError::UnknownFlowOrProcessPath { .. })
        ));
    }
}
