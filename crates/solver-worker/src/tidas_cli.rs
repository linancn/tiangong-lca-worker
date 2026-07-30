//! Coarse-grained, bounded integration with the unified Rust `tidas` binary.

use std::{
    fs::File,
    io::{BufRead, BufReader, Read},
    path::Path,
    process::{Child, Command, ExitStatus, Stdio},
    thread,
    time::{Duration, Instant},
};

use anyhow::Context;
use serde_json::Value;
use sha2::{Digest, Sha256};

pub const TIDAS_OPERATION_REPORT_SCHEMA: &str = "tidas.operation-report.v1";
pub const TIDAS_BATCH_PROTOCOL: &str = "document-validation-batch.v1";
pub const TIDAS_BATCH_PROFILE: &str = "tidas-document-conformance.v1";
pub const DEFAULT_TIDAS_VERSION: &str = "0.1.2";
const DEFAULT_TIDAS_TIMEOUT_SECONDS: u64 = 1_800;
const PROCESS_POLL_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Debug)]
pub struct TidasCommandOutput {
    pub report: Value,
    pub status: ExitStatus,
}

#[derive(Debug, Clone)]
pub struct TidasHandshake {
    pub binary_version: String,
    pub validation_describe: Value,
}

#[must_use]
pub fn binary_path() -> String {
    std::env::var("TIDAS_BIN")
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "tidas".to_owned())
}

#[must_use]
pub fn expected_version() -> String {
    std::env::var("TIDAS_EXPECTED_VERSION")
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_TIDAS_VERSION.to_owned())
}

fn timeout() -> Duration {
    let seconds = std::env::var("TIDAS_TIMEOUT_SECONDS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_TIDAS_TIMEOUT_SECONDS);
    Duration::from_secs(seconds)
}

pub fn handshake() -> anyhow::Result<TidasHandshake> {
    let version = run_json(&["version", "--format", "json", "--progress", "never"])?;
    require_success(&version.report, "version")?;
    let binary_version = version
        .report
        .pointer("/summary/binary_version")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("tidas_handshake_invalid: version omitted binary_version"))?
        .to_owned();
    let expected = expected_version();
    if binary_version != expected {
        return Err(anyhow::anyhow!(
            "tidas_version_mismatch: expected {expected}, got {binary_version}"
        ));
    }

    let describe = run_json(&[
        "validate",
        "--describe",
        "--format",
        "json",
        "--progress",
        "never",
    ])?;
    require_success(&describe.report, "validate --describe")?;
    let validation_describe = describe
        .report
        .pointer("/summary/validation_describe")
        .cloned()
        .ok_or_else(|| {
            anyhow::anyhow!(
                "tidas_handshake_invalid: validate --describe omitted validation_describe"
            )
        })?;
    let described_version = validation_describe
        .pointer("/package/version")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            anyhow::anyhow!("tidas_handshake_invalid: validation describe omitted package version")
        })?;
    if described_version != binary_version {
        return Err(anyhow::anyhow!(
            "tidas_version_mismatch: version command reported {binary_version}, validation reported {described_version}"
        ));
    }
    if !validation_describe
        .get("protocols")
        .and_then(Value::as_array)
        .is_some_and(|protocols| {
            protocols
                .iter()
                .any(|protocol| protocol.as_str() == Some(TIDAS_BATCH_PROTOCOL))
        })
        || !validation_describe
            .get("profiles")
            .and_then(Value::as_array)
            .is_some_and(|profiles| {
                profiles
                    .iter()
                    .any(|profile| profile.as_str() == Some(TIDAS_BATCH_PROFILE))
            })
    {
        return Err(anyhow::anyhow!(
            "tidas_protocol_mismatch: {binary_version} does not advertise {TIDAS_BATCH_PROTOCOL}/{TIDAS_BATCH_PROFILE}"
        ));
    }

    Ok(TidasHandshake {
        binary_version,
        validation_describe,
    })
}

pub fn run_json(args: &[&str]) -> anyhow::Result<TidasCommandOutput> {
    let program = binary_path();
    let mut child = Command::new(&program)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| {
            format!(
                "tidas_binary_unavailable: failed to start unified Rust tidas binary at {program}"
            )
        })?;
    let started = Instant::now();
    let status = wait_with_timeout(&mut child, timeout(), started)?;
    let output = child
        .wait_with_output()
        .context("tidas_execution_failed: collect tidas process output")?;
    debug_assert_eq!(status, output.status);
    let stdout = String::from_utf8(output.stdout)
        .context("tidas_report_invalid: tidas stdout was not UTF-8")?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    let report: Value = serde_json::from_str(stdout.trim()).with_context(|| {
        format!(
            "tidas_report_invalid: parse tidas operation report (stderr: {})",
            stderr.trim()
        )
    })?;
    validate_operation_report(&report)?;
    if !status.success() && status.code() != Some(2) {
        return Err(anyhow::anyhow!(
            "tidas_execution_failed: exit status {status}; {}",
            diagnostic_summary(&report, stderr.trim())
        ));
    }

    Ok(TidasCommandOutput { report, status })
}

fn wait_with_timeout(
    child: &mut Child,
    timeout: Duration,
    started: Instant,
) -> anyhow::Result<ExitStatus> {
    loop {
        if let Some(status) = child
            .try_wait()
            .context("tidas_execution_failed: poll tidas process")?
        {
            return Ok(status);
        }
        if started.elapsed() >= timeout {
            let _ = child.kill();
            let _ = child.wait();
            return Err(anyhow::anyhow!(
                "tidas_timeout: unified tidas process exceeded {} seconds",
                timeout.as_secs()
            ));
        }
        thread::sleep(PROCESS_POLL_INTERVAL);
    }
}

fn validate_operation_report(report: &Value) -> anyhow::Result<()> {
    if report.get("schema_version").and_then(Value::as_str) != Some(TIDAS_OPERATION_REPORT_SCHEMA) {
        return Err(anyhow::anyhow!(
            "tidas_report_invalid: expected {TIDAS_OPERATION_REPORT_SCHEMA}"
        ));
    }
    for field in [
        "command",
        "status",
        "exit_class",
        "completeness",
        "summary",
        "diagnostics",
        "artifacts",
        "next_actions",
    ] {
        if report.get(field).is_none() {
            return Err(anyhow::anyhow!(
                "tidas_report_invalid: operation report omitted {field}"
            ));
        }
    }
    Ok(())
}

fn require_success(report: &Value, operation: &str) -> anyhow::Result<()> {
    if report.get("status").and_then(Value::as_str) != Some("succeeded")
        || report.get("exit_class").and_then(Value::as_str) != Some("success")
        || report.get("completeness").and_then(Value::as_str) != Some("complete")
    {
        return Err(anyhow::anyhow!(
            "tidas_handshake_failed: {operation}: {}",
            diagnostic_summary(report, "")
        ));
    }
    Ok(())
}

fn diagnostic_summary(report: &Value, stderr: &str) -> String {
    let diagnostics = report
        .get("diagnostics")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| {
                    let code = item.get("code").and_then(Value::as_str)?;
                    let message = item.get("message").and_then(Value::as_str)?;
                    Some(format!("{code}: {message}"))
                })
                .collect::<Vec<_>>()
                .join("; ")
        })
        .unwrap_or_default();
    if diagnostics.is_empty() {
        stderr.to_owned()
    } else if stderr.is_empty() {
        diagnostics
    } else {
        format!("{diagnostics}; stderr: {stderr}")
    }
}

pub fn read_jsonl(path: &Path) -> anyhow::Result<Vec<Value>> {
    let mut values = Vec::new();
    visit_jsonl(path, |value| {
        values.push(value);
        Ok(())
    })?;
    Ok(values)
}

pub fn visit_jsonl(
    path: &Path,
    mut visit: impl FnMut(Value) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    visit_jsonl_raw(path, |value, _raw_line| visit(value))
}

pub fn visit_jsonl_raw(
    path: &Path,
    mut visit: impl FnMut(Value, &[u8]) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let file = File::open(path)
        .with_context(|| format!("tidas_spool_missing: open {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut raw_line = Vec::new();
    let mut index = 0_usize;
    loop {
        raw_line.clear();
        if reader.read_until(b'\n', &mut raw_line).with_context(|| {
            format!(
                "tidas_spool_invalid: read line {} from {}",
                index + 1,
                path.display()
            )
        })? == 0
        {
            break;
        }
        index += 1;
        let json_bytes = raw_line.strip_suffix(b"\n").unwrap_or(raw_line.as_slice());
        let json_bytes = json_bytes.strip_suffix(b"\r").unwrap_or(json_bytes);
        if json_bytes.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let value = serde_json::from_slice(json_bytes).with_context(|| {
            format!(
                "tidas_spool_invalid: parse line {} from {}",
                index,
                path.display()
            )
        })?;
        visit(value, &raw_line)?;
    }
    Ok(())
}

pub fn read_verified_jsonl(path: &Path, expected: &Value) -> anyhow::Result<Vec<Value>> {
    let mut events = Vec::new();
    visit_verified_jsonl(path, expected, |event| {
        events.push(event);
        Ok(())
    })?;
    Ok(events)
}

pub fn visit_verified_jsonl(
    path: &Path,
    expected: &Value,
    mut visit: impl FnMut(Value) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let expected_hash = expected
        .get("sha256")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("tidas_report_invalid: spool summary omitted sha256"))?;
    let expected_bytes = expected
        .get("bytes")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow::anyhow!("tidas_report_invalid: spool summary omitted bytes"))?;
    let expected_events = expected
        .get("event_count")
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            anyhow::anyhow!("tidas_report_invalid: spool summary omitted event_count")
        })?;
    let file = File::open(path)
        .with_context(|| format!("tidas_spool_missing: open {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut digest = Sha256::new();
    let mut actual_bytes = 0_u64;
    let mut actual_events = 0_u64;
    let mut line = Vec::new();
    loop {
        line.clear();
        let bytes_read = reader
            .read_until(b'\n', &mut line)
            .with_context(|| format!("tidas_spool_invalid: read {}", path.display()))?;
        if bytes_read == 0 {
            break;
        }
        digest.update(&line);
        actual_bytes = actual_bytes
            .checked_add(
                u64::try_from(bytes_read)
                    .context("tidas_spool_invalid: line byte length does not fit u64")?,
            )
            .ok_or_else(|| anyhow::anyhow!("tidas_spool_invalid: spool byte length overflow"))?;
        let text = std::str::from_utf8(&line)
            .with_context(|| format!("tidas_spool_invalid: non-UTF-8 line in {}", path.display()))?
            .trim();
        if text.is_empty() {
            continue;
        }
        let event = serde_json::from_str(text).with_context(|| {
            format!(
                "tidas_spool_invalid: parse line {} from {}",
                actual_events + 1,
                path.display()
            )
        })?;
        visit(event)?;
        actual_events = actual_events
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("tidas_spool_invalid: event count overflow"))?;
    }
    let actual_hash = format!("{:x}", digest.finalize());
    if actual_hash != expected_hash || actual_bytes != expected_bytes {
        return Err(anyhow::anyhow!(
            "tidas_spool_hash_mismatch: expected {expected_hash}/{expected_bytes}, got {actual_hash}/{actual_bytes}"
        ));
    }
    if actual_events != expected_events {
        return Err(anyhow::anyhow!(
            "tidas_spool_count_mismatch: expected {expected_events}, got {actual_events}"
        ));
    }
    Ok(())
}

pub fn verify_artifact(path: &Path, expected: &Value) -> anyhow::Result<()> {
    let mut file = File::open(path)
        .with_context(|| format!("tidas_spool_missing: open {}", path.display()))?;
    let mut digest = Sha256::new();
    let mut actual_bytes = 0_u64;
    let mut buffer = vec![0_u8; 64 * 1024].into_boxed_slice();
    loop {
        let bytes_read = file
            .read(&mut buffer)
            .with_context(|| format!("tidas_spool_invalid: read {}", path.display()))?;
        if bytes_read == 0 {
            break;
        }
        digest.update(&buffer[..bytes_read]);
        actual_bytes = actual_bytes
            .checked_add(
                u64::try_from(bytes_read)
                    .context("tidas_spool_invalid: artifact byte length does not fit u64")?,
            )
            .ok_or_else(|| anyhow::anyhow!("tidas_spool_invalid: artifact byte length overflow"))?;
    }
    let actual_hash = format!("{:x}", digest.finalize());
    let expected_hash = expected
        .get("sha256")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            anyhow::anyhow!("tidas_report_invalid: artifact reference omitted sha256")
        })?;
    let expected_bytes = expected
        .get("bytes")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow::anyhow!("tidas_report_invalid: artifact reference omitted bytes"))?;
    if actual_hash != expected_hash || actual_bytes != expected_bytes {
        return Err(anyhow::anyhow!(
            "tidas_spool_hash_mismatch: expected {expected_hash}/{expected_bytes}, got {actual_hash}/{actual_bytes}"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jsonl_visitor_streams_non_empty_values_in_order() {
        let temp = tempfile::TempDir::new().unwrap();
        let path = temp.path().join("events.jsonl");
        std::fs::write(&path, b"{\"index\":1}\n\n{\"index\":2}\n").unwrap();
        let mut indexes = Vec::new();
        visit_jsonl(&path, |value| {
            indexes.push(value.get("index").and_then(Value::as_u64).unwrap());
            Ok(())
        })
        .unwrap();
        assert_eq!(indexes, vec![1, 2]);
    }

    #[test]
    fn raw_jsonl_visitor_preserves_field_order_and_line_framing() {
        let temp = tempfile::TempDir::new().unwrap();
        let path = temp.path().join("events.jsonl");
        let bytes = b"{\"z\":1,\"a\":2}\n";
        std::fs::write(&path, bytes).unwrap();
        let mut observed = Vec::new();
        visit_jsonl_raw(&path, |value, raw_line| {
            assert_eq!(value.get("z").and_then(Value::as_u64), Some(1));
            observed.extend_from_slice(raw_line);
            Ok(())
        })
        .unwrap();
        assert_eq!(observed, bytes);
    }

    #[test]
    fn operation_report_requires_stable_schema_and_fields() {
        let report = serde_json::json!({
            "schema_version": TIDAS_OPERATION_REPORT_SCHEMA,
            "command": "version",
            "status": "succeeded",
            "exit_class": "success",
            "completeness": "complete",
            "summary": {},
            "diagnostics": [],
            "artifacts": [],
            "next_actions": []
        });
        validate_operation_report(&report).unwrap();
        let mut invalid = report;
        invalid["schema_version"] = Value::String("future".to_owned());
        assert!(validate_operation_report(&invalid).is_err());
    }

    #[test]
    fn binary_defaults_to_unified_command() {
        if std::env::var_os("TIDAS_BIN").is_none() {
            assert_eq!(binary_path(), "tidas");
        }
    }

    #[test]
    fn governed_release_version_is_the_runtime_default() {
        assert_eq!(DEFAULT_TIDAS_VERSION, "0.1.2");
        if std::env::var_os("TIDAS_EXPECTED_VERSION").is_none() {
            assert_eq!(expected_version(), DEFAULT_TIDAS_VERSION);
        }
    }

    #[test]
    #[ignore = "requires an installed release tidas binary selected by TIDAS_BIN"]
    fn release_binary_completes_version_and_protocol_handshake() {
        let handshake = handshake().expect("release tidas handshake");
        assert_eq!(handshake.binary_version, expected_version());
        assert!(
            handshake
                .validation_describe
                .get("asset_fingerprint")
                .and_then(Value::as_str)
                .is_some()
        );
    }

    #[test]
    #[ignore = "requires TIDAS_VALIDATION_REPORT and TIDAS_ISSUE_SPOOL from a real validation run"]
    fn release_issue_spool_verifies_with_streaming_memory() {
        let report_path =
            std::env::var("TIDAS_VALIDATION_REPORT").expect("TIDAS_VALIDATION_REPORT");
        let spool_path = std::env::var("TIDAS_ISSUE_SPOOL").expect("TIDAS_ISSUE_SPOOL");
        let report: Value = serde_json::from_reader(
            File::open(&report_path).expect("open release validation report"),
        )
        .expect("parse release validation report");
        let expected = report
            .pointer("/summary/validation/issue_spool")
            .expect("release validation issue spool summary");
        let mut count = 0_u64;
        visit_verified_jsonl(Path::new(&spool_path), expected, |_| {
            count += 1;
            Ok(())
        })
        .expect("stream-verify release issue spool");
        assert_eq!(
            Some(count),
            expected.get("event_count").and_then(Value::as_u64)
        );
    }
}
