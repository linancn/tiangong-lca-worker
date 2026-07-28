use serde_json::Value;
use solver_worker::scope_closure::{DatasetCategory, extract_references};

fn fixture(name: &str) -> Value {
    serde_json::from_str(match name {
        "lineage" => include_str!(
            "fixtures/source_reference_policy/review_submit_lineage_flow_does_not_expand_axis.json"
        ),
        "composition" => include_str!(
            "fixtures/source_reference_policy/review_submit_model_composition_does_not_expand_axis.json"
        ),
        "exchange" => include_str!(
            "fixtures/source_reference_policy/review_submit_exchange_control.json"
        ),
        _ => panic!("unknown fixture"),
    })
    .expect("fixture must be valid JSON")
}

#[test]
fn raw_extractor_preserves_lineage_identity_and_stable_path() {
    let result = extract_references(
        "flows:11111111-1111-4111-8111-111111111111:01.01.002",
        DatasetCategory::Flows,
        &fixture("lineage"),
    );
    assert!(result.issues.is_empty());
    assert_eq!(result.edges.len(), 1);
    let edge = &result.edges[0];
    assert_eq!(edge.target_category, "flows");
    assert_eq!(edge.target_uuid, "22222222-2222-4222-8222-222222222222");
    assert_eq!(edge.requested_version.as_deref(), Some("01.01.001"));
    assert!(
        edge.json_path
            .ends_with("common:referenceToPrecedingDataSetVersion")
    );
}

#[test]
fn raw_extractor_preserves_model_composition_identity_and_stable_path() {
    let result = extract_references(
        "processes:33333333-3333-4333-8333-333333333333:01.01.000",
        DatasetCategory::Processes,
        &fixture("composition"),
    );
    assert!(result.issues.is_empty());
    assert_eq!(result.edges.len(), 1);
    let edge = &result.edges[0];
    assert_eq!(edge.target_category, "processes");
    assert_eq!(edge.target_uuid, "44444444-4444-4444-8444-444444444444");
    assert!(edge.json_path.ends_with("referenceToIncludedProcesses"));
}

#[test]
fn raw_extractor_characterizes_exchange_flow_role_without_axis_logic() {
    let result = extract_references(
        "processes:55555555-5555-4555-8555-555555555555:01.01.000",
        DatasetCategory::Processes,
        &fixture("exchange"),
    );
    assert!(result.issues.is_empty());
    assert_eq!(result.edges.len(), 1);
    let edge = &result.edges[0];
    assert_eq!(edge.reference_role, "process_exchange_flow");
    assert_eq!(edge.target_category, "flows");
    assert_eq!(edge.requested_version.as_deref(), Some("01.01.000"));
}
