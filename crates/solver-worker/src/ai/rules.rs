use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::tidas_cli;

use super::tidas_suggestion::TidasDatasetType;

pub const PROCESS_AUTHORING_RULESET: &str = "process-authoring/strict";
pub const FLOW_AUTHORING_RULESET: &str = "flow-authoring/strict";

#[derive(Debug, Clone)]
pub struct AiRulesets {
    by_type: BTreeMap<TidasDatasetType, AiRuleset>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AiRuleset {
    pub id: String,
    pub ruleset_version: String,
    pub catalog_sha256: String,
    pub tidas_version: String,
    pub rules: Vec<AiRule>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AiRule {
    pub id: String,
    pub dataset_type: TidasDatasetType,
    pub summary: String,
    pub severity: String,
    pub phases: Vec<String>,
    pub default_blocker: bool,
    pub field_paths: Vec<String>,
    #[serde(default)]
    pub source_rule_refs: Vec<Value>,
}

#[derive(Debug, Deserialize)]
struct RulesetDescription {
    schema_version: String,
    ruleset_version: String,
    catalog_sha256: String,
    ruleset_count: u64,
    rule_count: u64,
    ruleset_ids: Vec<String>,
    methodology_file_count: u64,
    #[serde(rename = "methodology_warning_count")]
    _methodology_warning_count: u64,
}

impl AiRulesets {
    pub fn load_from_tidas() -> anyhow::Result<Self> {
        let handshake = tidas_cli::handshake()?;
        let process = load_one(
            PROCESS_AUTHORING_RULESET,
            TidasDatasetType::Process,
            &handshake.binary_version,
        )?;
        let flow = load_one(
            FLOW_AUTHORING_RULESET,
            TidasDatasetType::Flow,
            &handshake.binary_version,
        )?;
        if process.ruleset_version != flow.ruleset_version
            || process.catalog_sha256 != flow.catalog_sha256
        {
            anyhow::bail!(
                "tidas_ruleset_mismatch: Process and Flow catalogs do not share a binding"
            );
        }
        Ok(Self {
            by_type: BTreeMap::from([
                (TidasDatasetType::Process, process),
                (TidasDatasetType::Flow, flow),
            ]),
        })
    }

    pub fn from_rulesets(
        rulesets: impl IntoIterator<Item = (TidasDatasetType, AiRuleset)>,
    ) -> Self {
        Self {
            by_type: rulesets.into_iter().collect(),
        }
    }

    pub fn for_type(&self, data_type: TidasDatasetType) -> anyhow::Result<&AiRuleset> {
        self.by_type.get(&data_type).ok_or_else(|| {
            anyhow::anyhow!("ai_ruleset_missing: no ruleset loaded for {data_type:?}")
        })
    }
}

fn load_one(
    ruleset_id: &str,
    data_type: TidasDatasetType,
    tidas_version: &str,
) -> anyhow::Result<AiRuleset> {
    let output = tidas_cli::run_json(&[
        "ruleset",
        "--id",
        ruleset_id,
        "--format",
        "json",
        "--progress",
        "never",
    ])?;
    parse_ruleset_report(&output.report, ruleset_id, data_type, tidas_version)
}

fn parse_ruleset_report(
    report: &Value,
    ruleset_id: &str,
    data_type: TidasDatasetType,
    tidas_version: &str,
) -> anyhow::Result<AiRuleset> {
    if report.get("status").and_then(Value::as_str) != Some("succeeded")
        || report.get("exit_class").and_then(Value::as_str) != Some("success")
        || report.get("completeness").and_then(Value::as_str) != Some("complete")
    {
        anyhow::bail!("tidas_ruleset_failed: {ruleset_id}");
    }
    let reported_id = report
        .pointer("/summary/ruleset_id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("tidas_ruleset_invalid: missing ruleset_id"))?;
    if reported_id != ruleset_id {
        anyhow::bail!("tidas_ruleset_invalid: expected ruleset {ruleset_id}, got {reported_id}");
    }
    let description: RulesetDescription = serde_json::from_value(
        report
            .pointer("/summary/ruleset_description")
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("tidas_ruleset_invalid: missing ruleset_description"))?,
    )?;
    if description.schema_version != "tidas.ruleset-description.v1"
        || description.ruleset_count == 0
        || description.rule_count == 0
        || description.methodology_file_count == 0
        || !description.ruleset_ids.iter().any(|id| id == ruleset_id)
    {
        anyhow::bail!("tidas_ruleset_invalid: incomplete catalog description");
    }
    let rules: Vec<AiRule> = serde_json::from_value(
        report
            .pointer("/summary/rules")
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("tidas_ruleset_invalid: missing rules"))?,
    )?;
    if rules.is_empty()
        || rules.iter().any(|rule| {
            rule.dataset_type != data_type
                || rule.field_paths.is_empty()
                || rule.phases.is_empty()
                || rule.source_rule_refs.is_empty()
                || rule.summary.trim().is_empty()
                || rule.id.trim().is_empty()
                || !matches!(rule.severity.as_str(), "info" | "warning" | "blocker")
        })
    {
        anyhow::bail!("tidas_ruleset_invalid: rules do not match requested dataset type");
    }
    Ok(AiRuleset {
        id: ruleset_id.to_owned(),
        ruleset_version: description.ruleset_version,
        catalog_sha256: description.catalog_sha256,
        tidas_version: tidas_version.to_owned(),
        rules,
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::parse_ruleset_report;
    use crate::ai::tidas_suggestion::TidasDatasetType;

    #[test]
    fn parses_integrity_bound_ruleset_report() {
        let report = json!({
            "status": "succeeded",
            "exit_class": "success",
            "completeness": "complete",
            "summary": {
                "ruleset_id": "process-authoring/strict",
                "ruleset_description": {
                    "schema_version": "tidas.ruleset-description.v1",
                    "ruleset_version": "2026.05.23",
                    "catalog_sha256": "abc",
                    "ruleset_count": 7,
                    "rule_count": 10,
                    "ruleset_ids": ["process-authoring/strict"],
                    "methodology_file_count": 2,
                    "methodology_warning_count": 0
                },
                "rules": [{
                    "id": "tidas.process.name",
                    "dataset_type": "process",
                    "summary": "Use a technical name.",
                    "severity": "blocker",
                    "phases": ["save-draft"],
                    "default_blocker": true,
                    "field_paths": ["processDataSet.name"],
                    "source_rule_refs": [{"asset": "rules.yaml", "path": "name"}]
                }]
            }
        });
        let parsed = parse_ruleset_report(
            &report,
            "process-authoring/strict",
            TidasDatasetType::Process,
            "0.2.0",
        )
        .unwrap();
        assert_eq!(parsed.ruleset_version, "2026.05.23");
        assert_eq!(parsed.rules.len(), 1);
    }

    #[test]
    fn rejects_cross_type_rules() {
        let report = json!({
            "status": "succeeded",
            "exit_class": "success",
            "completeness": "complete",
            "summary": {
                "ruleset_id": "process-authoring/strict",
                "ruleset_description": {
                    "schema_version": "tidas.ruleset-description.v1",
                    "ruleset_version": "1",
                    "catalog_sha256": "abc",
                    "ruleset_count": 1,
                    "rule_count": 1,
                    "ruleset_ids": ["process-authoring/strict"],
                    "methodology_file_count": 2,
                    "methodology_warning_count": 0
                },
                "rules": [{
                    "id": "wrong",
                    "dataset_type": "flow",
                    "summary": "Wrong type.",
                    "severity": "blocker",
                    "phases": ["save-draft"],
                    "default_blocker": true,
                    "field_paths": ["flowDataSet.name"],
                    "source_rule_refs": [{"asset": "rules.yaml", "path": "name"}]
                }]
            }
        });
        assert!(
            parse_ruleset_report(
                &report,
                "process-authoring/strict",
                TidasDatasetType::Process,
                "0.2.0",
            )
            .is_err()
        );
    }
}
