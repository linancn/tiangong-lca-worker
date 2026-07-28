use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

pub const SNAPSHOT_BUILDER_TERMINAL_SCHEMA_VERSION: &str = "snapshot_builder_terminal.v1";
pub const SNAPSHOT_BUILDER_TERMINAL_PREFIX: &str = "[snapshot_builder_terminal] ";

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
        Self::Blocked {
            schema_version: SNAPSHOT_BUILDER_TERMINAL_SCHEMA_VERSION.to_owned(),
            code: code.into(),
            blocking_reasons,
        }
    }

    pub fn to_line(&self) -> anyhow::Result<String> {
        Ok(format!(
            "{SNAPSHOT_BUILDER_TERMINAL_PREFIX}{}",
            serde_json::to_string(self)?
        ))
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
            ..
        } if code.trim().is_empty() || blocking_reasons.is_empty() => Err(anyhow::anyhow!(
            "snapshot_builder_protocol_blocked_payload_invalid"
        )),
        _ => Ok(terminal),
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
}
