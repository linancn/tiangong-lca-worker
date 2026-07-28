use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use uuid::Uuid;

pub const SNAPSHOT_BUILDER_TERMINAL_SCHEMA_VERSION: &str = "snapshot_builder_terminal.v1";
pub const SNAPSHOT_BUILDER_TERMINAL_PREFIX: &str = "[snapshot_builder_terminal] ";
pub const SNAPSHOT_BUILDER_TERMINAL_MAX_BYTES: usize = 32 * 1024;
pub const SNAPSHOT_BUILDER_BLOCKED_EXIT_CODE: i32 = 42;
const SNAPSHOT_BUILDER_BLOCKING_REASON_MAX_COUNT: usize = 16;
const SNAPSHOT_BUILDER_BLOCKING_REASON_MAX_BYTES: usize = 1024;
const SNAPSHOT_BUILDER_BLOCKING_SAMPLE_MAX_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum SnapshotBuilderTerminal {
    Succeeded {
        schema_version: String,
        resolved_snapshot_id: Uuid,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        build_timing_sec: Option<Value>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        scope_closure_discovery: Option<Value>,
    },
    Blocked {
        schema_version: String,
        code: String,
        blocking_reasons: Vec<Value>,
        blocking_reason_count: u64,
        blocking_reasons_sha256: String,
        blocking_reasons_truncated: bool,
    },
}

impl SnapshotBuilderTerminal {
    #[must_use]
    pub fn succeeded(
        resolved_snapshot_id: Uuid,
        build_timing_sec: Option<Value>,
        scope_closure_discovery: Option<Value>,
    ) -> Self {
        Self::Succeeded {
            schema_version: SNAPSHOT_BUILDER_TERMINAL_SCHEMA_VERSION.to_owned(),
            resolved_snapshot_id,
            build_timing_sec,
            scope_closure_discovery,
        }
    }

    #[must_use]
    pub fn blocked(code: impl Into<String>, blocking_reasons: Vec<Value>) -> Self {
        let canonical_reasons = canonicalize_value(Value::Array(blocking_reasons));
        let canonical_bytes =
            serde_json::to_vec(&canonical_reasons).expect("JSON value serialization cannot fail");
        let blocking_reasons_sha256 = format!("{:x}", Sha256::digest(canonical_bytes));
        let Value::Array(all_reasons) = canonical_reasons else {
            unreachable!("canonicalized reasons remain an array");
        };
        let blocking_reason_count = u64::try_from(all_reasons.len()).unwrap_or(u64::MAX);
        let mut sample = Vec::new();
        let mut sample_bytes = 0_usize;
        let mut summarized = false;
        for reason in all_reasons
            .iter()
            .take(SNAPSHOT_BUILDER_BLOCKING_REASON_MAX_COUNT)
        {
            let encoded = serde_json::to_vec(reason).expect("JSON value serialization cannot fail");
            let bounded = if encoded.len() <= SNAPSHOT_BUILDER_BLOCKING_REASON_MAX_BYTES {
                reason.clone()
            } else {
                summarized = true;
                json!({
                    "code": reason.get("code").and_then(Value::as_str).unwrap_or("source_reference_invalid"),
                    "truncated": true,
                    "byteCount": encoded.len(),
                    "sha256": format!("{:x}", Sha256::digest(&encoded)),
                })
            };
            let bounded_len = serde_json::to_vec(&bounded)
                .expect("JSON value serialization cannot fail")
                .len();
            if sample_bytes.saturating_add(bounded_len) > SNAPSHOT_BUILDER_BLOCKING_SAMPLE_MAX_BYTES
            {
                break;
            }
            sample_bytes += bounded_len;
            sample.push(bounded);
        }
        Self::Blocked {
            schema_version: SNAPSHOT_BUILDER_TERMINAL_SCHEMA_VERSION.to_owned(),
            code: code.into(),
            blocking_reasons_truncated: summarized
                || u64::try_from(sample.len()).unwrap_or(u64::MAX) < blocking_reason_count,
            blocking_reasons: sample,
            blocking_reason_count,
            blocking_reasons_sha256,
        }
    }

    pub fn to_line(&self) -> anyhow::Result<String> {
        let line = format!(
            "{SNAPSHOT_BUILDER_TERMINAL_PREFIX}{}",
            serde_json::to_string(self)?
        );
        if line.len() > SNAPSHOT_BUILDER_TERMINAL_MAX_BYTES {
            return Err(anyhow::anyhow!(
                "snapshot_builder_protocol_terminal_too_large: actual={} limit={SNAPSHOT_BUILDER_TERMINAL_MAX_BYTES}",
                line.len()
            ));
        }
        Ok(line)
    }

    fn schema_version(&self) -> &str {
        match self {
            Self::Succeeded { schema_version, .. } | Self::Blocked { schema_version, .. } => {
                schema_version
            }
        }
    }
}

pub fn parse_terminal(stdout: &str) -> anyhow::Result<SnapshotBuilderTerminal> {
    let frames = stdout
        .lines()
        .filter_map(|line| line.strip_prefix(SNAPSHOT_BUILDER_TERMINAL_PREFIX))
        .collect::<Vec<_>>();
    if frames.len() != 1 {
        return Err(anyhow::anyhow!(
            "snapshot_builder_protocol_terminal_count: expected=1 actual={}",
            frames.len()
        ));
    }
    if SNAPSHOT_BUILDER_TERMINAL_PREFIX.len() + frames[0].len()
        > SNAPSHOT_BUILDER_TERMINAL_MAX_BYTES
    {
        return Err(anyhow::anyhow!(
            "snapshot_builder_protocol_terminal_too_large: actual={} limit={SNAPSHOT_BUILDER_TERMINAL_MAX_BYTES}",
            SNAPSHOT_BUILDER_TERMINAL_PREFIX.len() + frames[0].len()
        ));
    }
    let terminal: SnapshotBuilderTerminal = serde_json::from_str(frames[0])
        .map_err(|error| anyhow::anyhow!("snapshot_builder_protocol_terminal_invalid: {error}"))?;
    if terminal.schema_version() != SNAPSHOT_BUILDER_TERMINAL_SCHEMA_VERSION {
        return Err(anyhow::anyhow!(
            "snapshot_builder_protocol_schema_unsupported: {}",
            terminal.schema_version()
        ));
    }
    match &terminal {
        SnapshotBuilderTerminal::Succeeded {
            resolved_snapshot_id,
            ..
        } if resolved_snapshot_id.is_nil() => {
            Err(anyhow::anyhow!("snapshot_builder_protocol_snapshot_id_nil"))
        }
        SnapshotBuilderTerminal::Blocked {
            code,
            blocking_reasons,
            blocking_reason_count,
            blocking_reasons_sha256,
            blocking_reasons_truncated,
            ..
        } if code.trim().is_empty()
            || blocking_reasons.is_empty()
            || *blocking_reason_count
                < u64::try_from(blocking_reasons.len()).unwrap_or(u64::MAX)
            || blocking_reasons_sha256.len() != 64
            || !blocking_reasons_sha256
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
            || !blocked_metadata_is_consistent(
                blocking_reasons,
                *blocking_reason_count,
                *blocking_reasons_truncated,
            ) =>
        {
            Err(anyhow::anyhow!(
                "snapshot_builder_protocol_blocked_payload_invalid"
            ))
        }
        _ => Ok(terminal),
    }
}

fn blocked_metadata_is_consistent(
    blocking_reasons: &[Value],
    blocking_reason_count: u64,
    truncated: bool,
) -> bool {
    let sample_count = u64::try_from(blocking_reasons.len()).unwrap_or(u64::MAX);
    let has_truncated_summary = blocking_reasons
        .iter()
        .any(|reason| reason.get("truncated").and_then(Value::as_bool) == Some(true));
    if truncated {
        blocking_reason_count > sample_count || has_truncated_summary
    } else {
        blocking_reason_count == sample_count && !has_truncated_summary
    }
}

fn canonicalize_value(value: Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.into_iter().map(canonicalize_value).collect()),
        Value::Object(values) => {
            let mut entries = values.into_iter().collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key, canonicalize_value(value)))
                    .collect::<Map<_, _>>(),
            )
        }
        scalar => scalar,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn terminal_round_trip_is_versioned_and_exactly_once() {
        let snapshot_id = Uuid::new_v4();
        let line =
            SnapshotBuilderTerminal::succeeded(snapshot_id, Some(json!({"total_sec": 1.25})), None)
                .to_line()
                .unwrap();
        assert_eq!(
            parse_terminal(&line).unwrap(),
            SnapshotBuilderTerminal::succeeded(snapshot_id, Some(json!({"total_sec": 1.25})), None)
        );
        assert!(parse_terminal("").is_err());
        assert!(parse_terminal(format!("{line}\n{line}").as_str()).is_err());
    }

    #[test]
    fn blocked_terminal_requires_stable_code_and_reasons() {
        let terminal = SnapshotBuilderTerminal::blocked(
            "source_dependency_unavailable",
            vec![json!({
                "sourceIdentity": "processes:1",
                "jsonPath": "$.exchanges.exchange[0].referenceToFlowDataSet"
            })],
        );
        assert_eq!(
            parse_terminal(&terminal.to_line().unwrap()).unwrap(),
            terminal
        );
        assert!(
            parse_terminal(
                SnapshotBuilderTerminal::blocked("", Vec::new())
                    .to_line()
                    .unwrap()
                    .as_str()
            )
            .is_err()
        );
    }

    #[test]
    fn terminal_rejects_unknown_schema_and_truncated_json() {
        let unknown_schema = format!(
            "{SNAPSHOT_BUILDER_TERMINAL_PREFIX}{}",
            json!({
                "status": "succeeded",
                "schema_version": "snapshot_builder_terminal.v2",
                "resolved_snapshot_id": Uuid::new_v4()
            })
        );
        let unknown_error = parse_terminal(&unknown_schema).unwrap_err().to_string();
        assert!(unknown_error.contains("snapshot_builder_protocol_schema_unsupported"));

        let truncated = format!("{SNAPSHOT_BUILDER_TERMINAL_PREFIX}{{\"status\":\"succeeded\"");
        let truncated_error = parse_terminal(&truncated).unwrap_err().to_string();
        assert!(truncated_error.contains("snapshot_builder_protocol_terminal_invalid"));
    }

    #[test]
    fn oversized_blocked_payload_is_deterministically_bounded_below_capture_limit() {
        let reasons = (0..128)
            .map(|index| {
                json!({
                    "code": "source_reference_invalid",
                    "index": index,
                    "payload": "x".repeat(2048),
                })
            })
            .collect::<Vec<_>>();
        assert!(serde_json::to_vec(&reasons).unwrap().len() > 64 * 1024);

        let terminal =
            SnapshotBuilderTerminal::blocked("source_reference_invalid", reasons.clone());
        let line = terminal.to_line().unwrap();
        assert!(line.len() <= SNAPSHOT_BUILDER_TERMINAL_MAX_BYTES);
        let parsed = parse_terminal(&line).unwrap();
        let SnapshotBuilderTerminal::Blocked {
            blocking_reasons,
            blocking_reason_count,
            blocking_reasons_sha256,
            blocking_reasons_truncated,
            ..
        } = parsed
        else {
            panic!("expected blocked terminal");
        };
        assert_eq!(blocking_reason_count, 128);
        assert!(blocking_reasons.len() <= SNAPSHOT_BUILDER_BLOCKING_REASON_MAX_COUNT);
        assert!(blocking_reasons_truncated);
        assert_eq!(blocking_reasons_sha256.len(), 64);
        assert_eq!(
            terminal,
            SnapshotBuilderTerminal::blocked("source_reference_invalid", reasons)
        );
    }

    #[test]
    fn blocked_terminal_rejects_inconsistent_truncation_metadata() {
        let reason = json!({"code": "source_reference_invalid"});
        let hash = "a".repeat(64);
        for terminal in [
            SnapshotBuilderTerminal::Blocked {
                schema_version: SNAPSHOT_BUILDER_TERMINAL_SCHEMA_VERSION.to_owned(),
                code: "source_reference_invalid".to_owned(),
                blocking_reasons: vec![reason.clone()],
                blocking_reason_count: 2,
                blocking_reasons_sha256: hash.clone(),
                blocking_reasons_truncated: false,
            },
            SnapshotBuilderTerminal::Blocked {
                schema_version: SNAPSHOT_BUILDER_TERMINAL_SCHEMA_VERSION.to_owned(),
                code: "source_reference_invalid".to_owned(),
                blocking_reasons: vec![json!({
                    "code": "source_reference_invalid",
                    "truncated": true
                })],
                blocking_reason_count: 1,
                blocking_reasons_sha256: hash.clone(),
                blocking_reasons_truncated: false,
            },
            SnapshotBuilderTerminal::Blocked {
                schema_version: SNAPSHOT_BUILDER_TERMINAL_SCHEMA_VERSION.to_owned(),
                code: "source_reference_invalid".to_owned(),
                blocking_reasons: vec![reason],
                blocking_reason_count: 1,
                blocking_reasons_sha256: hash,
                blocking_reasons_truncated: true,
            },
        ] {
            let error = parse_terminal(&terminal.to_line().unwrap())
                .unwrap_err()
                .to_string();
            assert!(error.contains("snapshot_builder_protocol_blocked_payload_invalid"));
        }
    }
}
