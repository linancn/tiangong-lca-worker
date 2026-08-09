use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;

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
pub const SNAPSHOT_BUILDER_BLOCKING_REASONS_MAX_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotBuilderBlockingReasonsFile {
    pub record_count: u64,
    pub byte_size: u64,
    pub sha256: String,
    pub collection_complete: bool,
}

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
        #[serde(default, skip_serializing_if = "Option::is_none")]
        blocking_reasons_file: Option<SnapshotBuilderBlockingReasonsFile>,
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
        Self::blocked_with_file(code, blocking_reasons, None)
    }

    #[must_use]
    pub fn blocked_with_file(
        code: impl Into<String>,
        blocking_reasons: Vec<Value>,
        blocking_reasons_file: Option<SnapshotBuilderBlockingReasonsFile>,
    ) -> Self {
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
            blocking_reasons_file,
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

pub fn write_blocking_reasons_file(
    path: &Path,
    blocking_reasons: &[Value],
) -> anyhow::Result<SnapshotBuilderBlockingReasonsFile> {
    let mut writer = BufWriter::new(File::create(path)?);
    let mut digest = Sha256::new();
    let mut byte_size = 0_u64;
    for reason in blocking_reasons {
        let bytes = serde_json::to_vec(&canonicalize_value(reason.clone()))?;
        let record_bytes = u64::try_from(bytes.len().saturating_add(1))?;
        byte_size = byte_size
            .checked_add(record_bytes)
            .ok_or_else(|| anyhow::anyhow!("snapshot_builder_blocking_reasons_size_overflow"))?;
        if byte_size > SNAPSHOT_BUILDER_BLOCKING_REASONS_MAX_BYTES {
            return Err(anyhow::anyhow!(
                "snapshot_builder_blocking_reasons_too_large: actual={byte_size} limit={SNAPSHOT_BUILDER_BLOCKING_REASONS_MAX_BYTES}"
            ));
        }
        writer.write_all(&bytes)?;
        writer.write_all(b"\n")?;
        digest.update(&bytes);
        digest.update(b"\n");
    }
    writer.flush()?;
    Ok(SnapshotBuilderBlockingReasonsFile {
        record_count: u64::try_from(blocking_reasons.len())?,
        byte_size,
        sha256: format!("{:x}", digest.finalize()),
        collection_complete: true,
    })
}

pub fn validate_blocking_reasons_file(
    path: &Path,
    descriptor: &SnapshotBuilderBlockingReasonsFile,
    expected_array_sha256: &str,
) -> anyhow::Result<()> {
    if !descriptor.collection_complete {
        return Err(anyhow::anyhow!(
            "snapshot_builder_blocking_reasons_collection_incomplete"
        ));
    }
    if descriptor.byte_size > SNAPSHOT_BUILDER_BLOCKING_REASONS_MAX_BYTES {
        return Err(anyhow::anyhow!(
            "snapshot_builder_blocking_reasons_too_large: actual={} limit={SNAPSHOT_BUILDER_BLOCKING_REASONS_MAX_BYTES}",
            descriptor.byte_size
        ));
    }
    let actual_size = fs::metadata(path)?.len();
    if actual_size != descriptor.byte_size {
        return Err(anyhow::anyhow!(
            "snapshot_builder_blocking_reasons_size_mismatch: expected={} actual={actual_size}",
            descriptor.byte_size
        ));
    }
    let mut file_digest = Sha256::new();
    let mut array_digest = Sha256::new();
    array_digest.update(b"[");
    let mut record_count = 0_u64;
    for line in BufReader::new(File::open(path)?).split(b'\n') {
        let line = line?;
        if line.is_empty() {
            continue;
        }
        let value: Value = serde_json::from_slice(&line).map_err(|error| {
            anyhow::anyhow!("snapshot_builder_blocking_reasons_invalid_json: {error}")
        })?;
        let canonical = serde_json::to_vec(&canonicalize_value(value))?;
        if canonical != line {
            return Err(anyhow::anyhow!(
                "snapshot_builder_blocking_reasons_noncanonical_json"
            ));
        }
        if record_count > 0 {
            array_digest.update(b",");
        }
        array_digest.update(&canonical);
        file_digest.update(&canonical);
        file_digest.update(b"\n");
        record_count = record_count
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("snapshot_builder_blocking_reasons_count_overflow"))?;
    }
    array_digest.update(b"]");
    let actual_file_sha256 = format!("{:x}", file_digest.finalize());
    if actual_file_sha256 != descriptor.sha256 {
        return Err(anyhow::anyhow!(
            "snapshot_builder_blocking_reasons_sha256_mismatch: expected={} actual={actual_file_sha256}",
            descriptor.sha256
        ));
    }
    if record_count != descriptor.record_count {
        return Err(anyhow::anyhow!(
            "snapshot_builder_blocking_reasons_count_mismatch: expected={} actual={record_count}",
            descriptor.record_count
        ));
    }
    let actual_array_sha256 = format!("{:x}", array_digest.finalize());
    if actual_array_sha256 != expected_array_sha256 {
        return Err(anyhow::anyhow!(
            "snapshot_builder_blocking_reasons_array_sha256_mismatch: expected={expected_array_sha256} actual={actual_array_sha256}"
        ));
    }
    Ok(())
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
                blocking_reasons_file: None,
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
                blocking_reasons_file: None,
            },
            SnapshotBuilderTerminal::Blocked {
                schema_version: SNAPSHOT_BUILDER_TERMINAL_SCHEMA_VERSION.to_owned(),
                code: "source_reference_invalid".to_owned(),
                blocking_reasons: vec![reason],
                blocking_reason_count: 1,
                blocking_reasons_sha256: hash,
                blocking_reasons_truncated: true,
                blocking_reasons_file: None,
            },
        ] {
            let error = parse_terminal(&terminal.to_line().unwrap())
                .unwrap_err()
                .to_string();
            assert!(error.contains("snapshot_builder_protocol_blocked_payload_invalid"));
        }
    }

    #[test]
    fn blocked_sidecar_preserves_all_records_while_terminal_stays_bounded() {
        let reasons = (0..39)
            .map(|index| {
                json!({
                    "code": "source_reference_invalid",
                    "sourceIdentity": format!("process:{}@01.00.000", Uuid::from_u128(index + 1)),
                    "jsonPath": format!("$.exchanges.exchange[{index}].referenceToFlowDataSet"),
                })
            })
            .collect::<Vec<_>>();
        let file = tempfile::NamedTempFile::new().unwrap();
        let descriptor = write_blocking_reasons_file(file.path(), &reasons).unwrap();
        let terminal = SnapshotBuilderTerminal::blocked_with_file(
            "source_reference_invalid",
            reasons,
            Some(descriptor.clone()),
        );
        let SnapshotBuilderTerminal::Blocked {
            blocking_reasons,
            blocking_reason_count,
            blocking_reasons_sha256,
            blocking_reasons_file,
            ..
        } = &terminal
        else {
            unreachable!()
        };
        assert!(blocking_reasons.len() <= 16);
        assert_eq!(*blocking_reason_count, 39);
        assert_eq!(blocking_reasons_file.as_ref(), Some(&descriptor));
        validate_blocking_reasons_file(file.path(), &descriptor, blocking_reasons_sha256).unwrap();
        assert_eq!(
            parse_terminal(&terminal.to_line().unwrap()).unwrap(),
            terminal
        );
    }

    #[test]
    fn blocked_sidecar_rejects_incomplete_or_tampered_collections() {
        let reasons = vec![json!({"code": "source_dependency_unavailable", "n": 1})];
        let file = tempfile::NamedTempFile::new().unwrap();
        let descriptor = write_blocking_reasons_file(file.path(), &reasons).unwrap();
        let array_sha256 = format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&canonicalize_value(Value::Array(reasons))).unwrap())
        );

        let mut incomplete = descriptor.clone();
        incomplete.collection_complete = false;
        assert!(validate_blocking_reasons_file(file.path(), &incomplete, &array_sha256).is_err());

        let mut wrong_count = descriptor.clone();
        wrong_count.record_count += 1;
        assert!(validate_blocking_reasons_file(file.path(), &wrong_count, &array_sha256).is_err());

        let mut wrong_hash = descriptor.clone();
        wrong_hash.sha256 = "0".repeat(64);
        assert!(validate_blocking_reasons_file(file.path(), &wrong_hash, &array_sha256).is_err());

        std::fs::write(file.path(), b"{invalid json}\n").unwrap();
        let mut invalid_json = descriptor;
        invalid_json.byte_size = std::fs::metadata(file.path()).unwrap().len();
        assert!(validate_blocking_reasons_file(file.path(), &invalid_json, &array_sha256).is_err());
    }
}
