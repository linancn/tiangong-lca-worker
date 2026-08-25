use std::{collections::BTreeMap, sync::Arc};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::{sync::Semaphore, task::JoinSet};

use super::{
    client::{AiClientError, AiModelClient},
    rules::{AiRule, AiRuleset, AiRulesets},
};

pub const AI_TIDAS_SUGGESTION_JOB_KIND: &str = "ai.tidas_suggestion";
pub const AI_TIDAS_SUGGESTION_REQUEST_SCHEMA_VERSION: &str = "ai.tidas_suggestion.request.v1";
pub const AI_TIDAS_SUGGESTION_RESULT_SCHEMA_VERSION: &str = "ai.tidas_suggestion.result.v1";

const SYSTEM_PROMPT: &str = "You improve one existing field in a TIDAS/LCA JSON dataset. Follow only the supplied authoritative rule. Return only valid JSON for the field value. Preserve the original JSON shape exactly: do not add or remove object keys or array items, and do not change JSON value types.";

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "lowercase")]
pub enum TidasDatasetType {
    Process,
    Flow,
}

impl TidasDatasetType {
    #[must_use]
    pub const fn root_key(self) -> &'static str {
        match self {
            Self::Process => "processDataSet",
            Self::Flow => "flowDataSet",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AiTidasSuggestionRequest {
    pub data_type: TidasDatasetType,
    pub data: Value,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AiTidasSuggestionResult {
    pub schema_version: &'static str,
    pub status: AiSuggestionStatus,
    pub data_type: TidasDatasetType,
    pub data: Value,
    pub input_sha256: String,
    pub ruleset: RulesetResultBinding,
    pub model: ModelResultBinding,
    pub summary: AiSuggestionSummary,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub failures: Vec<AiPathFailure>,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AiSuggestionStatus {
    Complete,
    Partial,
    Failed,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RulesetResultBinding {
    pub id: String,
    pub version: String,
    pub catalog_sha256: String,
    pub tidas_version: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelResultBinding {
    pub model: String,
    pub config_version: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AiSuggestionSummary {
    pub matched_path_count: usize,
    pub processed_path_count: usize,
    pub changed_path_count: usize,
    pub failed_path_count: usize,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AiPathFailure {
    pub path: String,
    pub rule_id: String,
    pub code: String,
    pub retryable: bool,
}

#[derive(Clone)]
pub struct AiTidasSuggestionRuntime {
    client: Arc<dyn AiModelClient>,
    rulesets: AiRulesets,
    max_concurrency: usize,
    max_input_bytes: usize,
}

impl AiTidasSuggestionRuntime {
    pub fn new(
        client: Arc<dyn AiModelClient>,
        rulesets: AiRulesets,
        max_concurrency: usize,
        max_input_bytes: usize,
    ) -> anyhow::Result<Self> {
        if !(1..=64).contains(&max_concurrency) {
            anyhow::bail!("ai_configuration_invalid: max concurrency must be between 1 and 64");
        }
        if !(1_024..=16 * 1024 * 1024).contains(&max_input_bytes) {
            anyhow::bail!("ai_configuration_invalid: input byte limit is out of bounds");
        }
        Ok(Self {
            client,
            rulesets,
            max_concurrency,
            max_input_bytes,
        })
    }

    pub async fn execute(
        &self,
        request: AiTidasSuggestionRequest,
    ) -> anyhow::Result<AiTidasSuggestionResult> {
        validate_request(&request, self.max_input_bytes)?;
        let input_bytes = serde_json::to_vec(&request.data)?;
        let input_sha256 = hex::encode(Sha256::digest(&input_bytes));
        let ruleset = self.rulesets.for_type(request.data_type)?.clone();
        let targets = collect_targets(&request.data, &ruleset);
        let matched_path_count = targets.len();
        let semaphore = Arc::new(Semaphore::new(self.max_concurrency));
        let mut tasks = JoinSet::new();
        for target in targets {
            let client = Arc::clone(&self.client);
            let semaphore = Arc::clone(&semaphore);
            let data_type = request.data_type;
            tasks.spawn(async move {
                let _permit = semaphore.acquire_owned().await.map_err(|_| {
                    PathExecutionFailure::runtime(target.path.clone(), &target.rule)
                })?;
                execute_target(client.as_ref(), data_type, target).await
            });
        }

        let mut improved = request.data.clone();
        let mut processed_path_count = 0_usize;
        let mut changed_path_count = 0_usize;
        let mut failures = Vec::new();
        while let Some(joined) = tasks.join_next().await {
            match joined {
                Ok(Ok(success)) => {
                    processed_path_count += 1;
                    if success.original != success.suggested {
                        set_value_at_path(&mut improved, &success.tokens, success.suggested)?;
                        changed_path_count += 1;
                    }
                }
                Ok(Err(failure)) => failures.push(failure.into_public()),
                Err(_) => failures.push(AiPathFailure {
                    path: "<runtime>".to_owned(),
                    rule_id: "<runtime>".to_owned(),
                    code: "ai_task_join_failed".to_owned(),
                    retryable: true,
                }),
            }
        }
        failures.sort_by(|left, right| left.path.cmp(&right.path));
        let status = if failures.is_empty() {
            AiSuggestionStatus::Complete
        } else if processed_path_count == 0 && matched_path_count > 0 {
            AiSuggestionStatus::Failed
        } else {
            AiSuggestionStatus::Partial
        };
        Ok(AiTidasSuggestionResult {
            schema_version: AI_TIDAS_SUGGESTION_RESULT_SCHEMA_VERSION,
            status,
            data_type: request.data_type,
            data: improved,
            input_sha256,
            ruleset: ruleset_binding(&ruleset),
            model: ModelResultBinding {
                model: self.client.model().to_owned(),
                config_version: self.client.config_version().to_owned(),
            },
            summary: AiSuggestionSummary {
                matched_path_count,
                processed_path_count,
                changed_path_count,
                failed_path_count: failures.len(),
            },
            failures,
        })
    }
}

#[derive(Debug, Clone)]
struct SuggestionTarget {
    path: String,
    tokens: Vec<PathToken>,
    original: Value,
    rule: AiRule,
}

#[derive(Debug)]
struct TargetSuccess {
    tokens: Vec<PathToken>,
    original: Value,
    suggested: Value,
}

#[derive(Debug)]
struct PathExecutionFailure {
    path: String,
    rule_id: String,
    code: String,
    retryable: bool,
}

impl PathExecutionFailure {
    fn runtime(path: String, rule: &AiRule) -> Self {
        Self {
            path,
            rule_id: rule.id.clone(),
            code: "ai_runtime_unavailable".to_owned(),
            retryable: true,
        }
    }

    fn into_public(self) -> AiPathFailure {
        AiPathFailure {
            path: self.path,
            rule_id: self.rule_id,
            code: self.code,
            retryable: self.retryable,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum PathToken {
    Key(String),
    Index(usize),
}

async fn execute_target(
    client: &dyn AiModelClient,
    data_type: TidasDatasetType,
    target: SuggestionTarget,
) -> Result<TargetSuccess, PathExecutionFailure> {
    let prompt = serde_json::to_string(&json!({
        "task": "improve_existing_tidas_field",
        "datasetType": data_type,
        "path": &target.path,
        "rule": &target.rule,
        "originalValue": &target.original,
        "requiredOutput": {
            "format": "json_value_only",
            "preserveShape": true
        }
    }))
    .map_err(|_| PathExecutionFailure::runtime(target.path.clone(), &target.rule))?;
    let output = client
        .complete(SYSTEM_PROMPT, &prompt)
        .await
        .map_err(|error| map_client_error(&target, &error))?;
    let suggested = parse_json_output(&output).map_err(|code| PathExecutionFailure {
        path: target.path.clone(),
        rule_id: target.rule.id.clone(),
        code: code.to_owned(),
        retryable: false,
    })?;
    if !same_json_shape(&target.original, &suggested) {
        return Err(PathExecutionFailure {
            path: target.path,
            rule_id: target.rule.id,
            code: "ai_output_shape_mismatch".to_owned(),
            retryable: false,
        });
    }
    Ok(TargetSuccess {
        tokens: target.tokens,
        original: target.original,
        suggested,
    })
}

fn map_client_error(target: &SuggestionTarget, error: &AiClientError) -> PathExecutionFailure {
    let code = match error {
        AiClientError::Configuration(_) | AiClientError::ConfigurationOwned(_) => {
            "ai_provider_configuration_invalid"
        }
        AiClientError::Timeout => "ai_provider_timeout",
        AiClientError::Transport { .. } => "ai_provider_transport_failed",
        AiClientError::Http { status, .. } if *status == 429 => "ai_provider_rate_limited",
        AiClientError::Http { .. } => "ai_provider_http_failed",
        AiClientError::ResponseTooLarge { .. } => "ai_provider_response_too_large",
        AiClientError::MalformedResponse(_) => "ai_provider_response_invalid",
    };
    PathExecutionFailure {
        path: target.path.clone(),
        rule_id: target.rule.id.clone(),
        code: code.to_owned(),
        retryable: error.retryable(),
    }
}

fn validate_request(request: &AiTidasSuggestionRequest, max_bytes: usize) -> anyhow::Result<()> {
    let object = request
        .data
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("ai_tidas_input_invalid: data must be a JSON object"))?;
    if !object.contains_key(request.data_type.root_key()) {
        anyhow::bail!(
            "ai_tidas_input_invalid: data does not contain {}",
            request.data_type.root_key()
        );
    }
    let encoded = serde_json::to_vec(&request.data)?;
    if encoded.len() > max_bytes {
        anyhow::bail!(
            "ai_tidas_input_too_large: {} bytes exceeds {max_bytes}",
            encoded.len()
        );
    }
    Ok(())
}

fn collect_targets(data: &Value, ruleset: &AiRuleset) -> Vec<SuggestionTarget> {
    let mut by_path = BTreeMap::<String, SuggestionTarget>::new();
    for rule in &ruleset.rules {
        for pattern in &rule.field_paths {
            for (path, tokens, value) in expand_existing_path(data, pattern) {
                by_path
                    .entry(path.clone())
                    .or_insert_with(|| SuggestionTarget {
                        path,
                        tokens,
                        original: value,
                        rule: rule.clone(),
                    });
            }
        }
    }
    by_path.into_values().collect()
}

fn expand_existing_path(data: &Value, pattern: &str) -> Vec<(String, Vec<PathToken>, Value)> {
    let segments = pattern
        .split('.')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    let mut results = Vec::new();
    expand_path_recursive(
        data,
        &segments,
        0,
        &mut Vec::new(),
        &mut Vec::new(),
        &mut results,
    );
    results
}

fn expand_path_recursive(
    current: &Value,
    segments: &[&str],
    offset: usize,
    tokens: &mut Vec<PathToken>,
    display: &mut Vec<String>,
    results: &mut Vec<(String, Vec<PathToken>, Value)>,
) {
    if offset == segments.len() {
        results.push((display.join("."), tokens.clone(), current.clone()));
        return;
    }
    let segment = segments[offset];
    if let Some(key) = segment.strip_suffix("[*]") {
        let Some(array) = current.get(key).and_then(Value::as_array) else {
            return;
        };
        tokens.push(PathToken::Key(key.to_owned()));
        for (index, value) in array.iter().enumerate() {
            tokens.push(PathToken::Index(index));
            display.push(format!("{key}[{index}]"));
            expand_path_recursive(value, segments, offset + 1, tokens, display, results);
            display.pop();
            tokens.pop();
        }
        tokens.pop();
        return;
    }
    let Some(value) = current.get(segment) else {
        return;
    };
    tokens.push(PathToken::Key(segment.to_owned()));
    display.push(segment.to_owned());
    expand_path_recursive(value, segments, offset + 1, tokens, display, results);
    display.pop();
    tokens.pop();
}

fn set_value_at_path(root: &mut Value, tokens: &[PathToken], value: Value) -> anyhow::Result<()> {
    let (last, parents) = tokens
        .split_last()
        .ok_or_else(|| anyhow::anyhow!("ai_path_invalid: empty path"))?;
    let mut current = root;
    for token in parents {
        current = match token {
            PathToken::Key(key) => current
                .get_mut(key)
                .ok_or_else(|| anyhow::anyhow!("ai_path_invalid: missing key {key}"))?,
            PathToken::Index(index) => current
                .get_mut(*index)
                .ok_or_else(|| anyhow::anyhow!("ai_path_invalid: missing index {index}"))?,
        };
    }
    match last {
        PathToken::Key(key) => {
            *current
                .get_mut(key)
                .ok_or_else(|| anyhow::anyhow!("ai_path_invalid: missing key {key}"))? = value;
        }
        PathToken::Index(index) => {
            *current
                .get_mut(*index)
                .ok_or_else(|| anyhow::anyhow!("ai_path_invalid: missing index {index}"))? = value;
        }
    }
    Ok(())
}

fn parse_json_output(output: &str) -> Result<Value, &'static str> {
    let trimmed = output.trim();
    let normalized = trimmed
        .strip_prefix("```json")
        .or_else(|| trimmed.strip_prefix("```JSON"))
        .or_else(|| trimmed.strip_prefix("```"))
        .and_then(|value| value.strip_suffix("```"))
        .map_or(trimmed, str::trim);
    serde_json::from_str(normalized).map_err(|_| "ai_output_json_invalid")
}

fn same_json_shape(original: &Value, suggested: &Value) -> bool {
    match (original, suggested) {
        (Value::Null, Value::Null)
        | (Value::Bool(_), Value::Bool(_))
        | (Value::Number(_), Value::Number(_))
        | (Value::String(_), Value::String(_)) => true,
        (Value::Array(left), Value::Array(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right)
                    .all(|(left, right)| same_json_shape(left, right))
        }
        (Value::Object(left), Value::Object(right)) => {
            left.len() == right.len()
                && left.iter().all(|(key, value)| {
                    right
                        .get(key)
                        .is_some_and(|candidate| same_json_shape(value, candidate))
                })
        }
        _ => false,
    }
}

fn ruleset_binding(ruleset: &AiRuleset) -> RulesetResultBinding {
    RulesetResultBinding {
        id: ruleset.id.clone(),
        version: ruleset.ruleset_version.clone(),
        catalog_sha256: ruleset.catalog_sha256.clone(),
        tidas_version: ruleset.tidas_version.clone(),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use serde_json::json;

    use super::{
        AI_TIDAS_SUGGESTION_RESULT_SCHEMA_VERSION, AiSuggestionStatus, AiTidasSuggestionRequest,
        AiTidasSuggestionRuntime, TidasDatasetType, expand_existing_path, parse_json_output,
        same_json_shape,
    };
    use crate::ai::{
        client::{AiClientError, AiModelClient, CompletionFuture},
        rules::{AiRule, AiRuleset, AiRulesets},
    };

    #[derive(Clone)]
    struct MockClient {
        active: Arc<AtomicUsize>,
        max_active: Arc<AtomicUsize>,
        response: String,
    }

    impl AiModelClient for MockClient {
        fn complete<'a>(&'a self, _system: &'a str, _user: &'a str) -> CompletionFuture<'a> {
            Box::pin(async move {
                let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
                self.max_active.fetch_max(active, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(10)).await;
                self.active.fetch_sub(1, Ordering::SeqCst);
                Ok(self.response.clone())
            })
        }

        fn model(&self) -> &'static str {
            "test-model"
        }

        fn config_version(&self) -> &'static str {
            "test-v1"
        }
    }

    fn rulesets(paths: &[&str]) -> AiRulesets {
        let process = AiRuleset {
            id: "process-authoring/strict".to_owned(),
            ruleset_version: "1".to_owned(),
            catalog_sha256: "abc".to_owned(),
            tidas_version: "0.2.0".to_owned(),
            rules: vec![AiRule {
                id: "process.rule".to_owned(),
                dataset_type: TidasDatasetType::Process,
                summary: "Improve the field.".to_owned(),
                severity: "warning".to_owned(),
                phases: vec!["save-draft".to_owned()],
                default_blocker: false,
                field_paths: paths.iter().map(|path| (*path).to_owned()).collect(),
                source_rule_refs: Vec::new(),
            }],
        };
        AiRulesets::from_rulesets([(TidasDatasetType::Process, process)])
    }

    #[test]
    fn expands_array_wildcards_to_existing_paths() {
        let data = json!({"items": [{"name": "a"}, {"name": "b"}]});
        let paths = expand_existing_path(&data, "items[*].name");
        assert_eq!(paths.len(), 2);
        assert_eq!(paths[0].0, "items[0].name");
        assert_eq!(paths[1].0, "items[1].name");
    }

    #[test]
    fn validates_exact_json_shape() {
        assert!(same_json_shape(
            &json!({"a": ["x", 1]}),
            &json!({"a": ["y", 2]})
        ));
        assert!(!same_json_shape(
            &json!({"a": ["x"]}),
            &json!({"a": ["x", "y"]})
        ));
        assert!(!same_json_shape(&json!({"a": 1}), &json!({"a": "1"})));
    }

    #[test]
    fn parses_fenced_json_only() {
        assert_eq!(
            parse_json_output("```json\n{\"a\":1}\n```").unwrap(),
            json!({"a": 1})
        );
        assert!(parse_json_output("answer: {\"a\":1}").is_err());
    }

    #[tokio::test]
    async fn executes_existing_fields_with_bounded_concurrency() {
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let client = MockClient {
            active,
            max_active: Arc::clone(&max_active),
            response: "\"improved\"".to_owned(),
        };
        let runtime = AiTidasSuggestionRuntime::new(
            Arc::new(client),
            rulesets(&["processDataSet.a", "processDataSet.b", "processDataSet.c"]),
            2,
            1024 * 1024,
        )
        .unwrap();
        let result = runtime
            .execute(AiTidasSuggestionRequest {
                data_type: TidasDatasetType::Process,
                data: json!({"processDataSet": {"a": "a", "b": "b", "c": "c"}}),
            })
            .await
            .unwrap();
        assert_eq!(
            result.schema_version,
            AI_TIDAS_SUGGESTION_RESULT_SCHEMA_VERSION
        );
        assert_eq!(result.status, AiSuggestionStatus::Complete);
        assert_eq!(result.summary.changed_path_count, 3);
        assert!(max_active.load(Ordering::SeqCst) <= 2);
    }

    #[derive(Clone)]
    struct FailingClient;

    impl AiModelClient for FailingClient {
        fn complete<'a>(&'a self, _system: &'a str, _user: &'a str) -> CompletionFuture<'a> {
            let future: Pin<Box<dyn Future<Output = Result<String, AiClientError>> + Send + 'a>> =
                Box::pin(async { Err(AiClientError::Timeout) });
            future
        }

        fn model(&self) -> &'static str {
            "test-model"
        }

        fn config_version(&self) -> &'static str {
            "test-v1"
        }
    }

    #[tokio::test]
    async fn reports_failed_without_replacing_data_with_empty_object() {
        let runtime = AiTidasSuggestionRuntime::new(
            Arc::new(FailingClient),
            rulesets(&["processDataSet.name"]),
            1,
            1024 * 1024,
        )
        .unwrap();
        let input = json!({"processDataSet": {"name": "original"}});
        let result = runtime
            .execute(AiTidasSuggestionRequest {
                data_type: TidasDatasetType::Process,
                data: input.clone(),
            })
            .await
            .unwrap();
        assert_eq!(result.status, AiSuggestionStatus::Failed);
        assert_eq!(result.data, input);
        assert_eq!(result.failures[0].code, "ai_provider_timeout");
    }
}
