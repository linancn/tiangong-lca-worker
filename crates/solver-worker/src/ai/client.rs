use std::{future::Future, pin::Pin, time::Duration};

use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::time::sleep;

const DEFAULT_REQUEST_TIMEOUT_SECONDS: u64 = 120;
const DEFAULT_MAX_RESPONSE_BYTES: usize = 2 * 1024 * 1024;
const DEFAULT_MAX_ATTEMPTS: u32 = 1;

pub type CompletionFuture<'a> =
    Pin<Box<dyn Future<Output = Result<String, AiClientError>> + Send + 'a>>;

/// Minimal model boundary used by reusable AI job handlers.
pub trait AiModelClient: Send + Sync {
    fn complete<'a>(&'a self, system: &'a str, user: &'a str) -> CompletionFuture<'a>;

    fn model(&self) -> &str;

    fn config_version(&self) -> &str;
}

#[derive(Clone)]
pub struct OpenAiCompatibleClient {
    http: reqwest::Client,
    endpoint: String,
    api_key: String,
    model: String,
    config_version: String,
    max_response_bytes: usize,
    max_attempts: u32,
}

#[derive(Clone)]
pub struct AiClientConfig {
    pub base_url: String,
    pub api_key: String,
    pub model: String,
    pub config_version: String,
    pub request_timeout: Duration,
    pub max_response_bytes: usize,
    pub max_attempts: u32,
}

impl AiClientConfig {
    pub fn from_env() -> Result<Self, AiClientError> {
        Ok(Self {
            base_url: required_env("AI_PROVIDER_BASE_URL")?,
            api_key: required_env("AI_PROVIDER_API_KEY")?,
            model: required_env("AI_PROVIDER_MODEL")?,
            config_version: required_env("AI_MODEL_CONFIG_VERSION")?,
            request_timeout: Duration::from_secs(env_u64(
                "AI_REQUEST_TIMEOUT_SECONDS",
                DEFAULT_REQUEST_TIMEOUT_SECONDS,
                1,
                900,
            )?),
            max_response_bytes: usize::try_from(env_u64(
                "AI_MAX_RESPONSE_BYTES",
                u64::try_from(DEFAULT_MAX_RESPONSE_BYTES)
                    .expect("default response byte limit fits u64"),
                1_024,
                16 * 1024 * 1024,
            )?)
            .map_err(|_| AiClientError::Configuration("AI_MAX_RESPONSE_BYTES is too large"))?,
            max_attempts: u32::try_from(env_u64(
                "AI_PROVIDER_MAX_ATTEMPTS",
                u64::from(DEFAULT_MAX_ATTEMPTS),
                1,
                5,
            )?)
            .map_err(|_| AiClientError::Configuration("AI_PROVIDER_MAX_ATTEMPTS is too large"))?,
        })
    }
}

impl OpenAiCompatibleClient {
    pub fn new(config: AiClientConfig) -> Result<Self, AiClientError> {
        let endpoint = chat_completions_endpoint(&config.base_url)?;
        let http = reqwest::Client::builder()
            .timeout(config.request_timeout)
            .build()
            .map_err(|error| {
                AiClientError::ConfigurationOwned(format!(
                    "failed to construct AI HTTP client: {error}"
                ))
            })?;
        Ok(Self {
            http,
            endpoint,
            api_key: config.api_key,
            model: config.model,
            config_version: config.config_version,
            max_response_bytes: config.max_response_bytes,
            max_attempts: config.max_attempts,
        })
    }

    async fn complete_once(&self, system: &str, user: &str) -> Result<String, AiClientError> {
        let request = ChatCompletionRequest {
            model: &self.model,
            messages: [
                ChatMessage {
                    role: "system",
                    content: system,
                },
                ChatMessage {
                    role: "user",
                    content: user,
                },
            ],
            temperature: 0.0,
        };
        let response = self
            .http
            .post(&self.endpoint)
            .bearer_auth(&self.api_key)
            .json(&request)
            .send()
            .await
            .map_err(|error| AiClientError::from_reqwest(&error))?;
        let status = response.status();
        if response.content_length().is_some_and(|length| {
            length > u64::try_from(self.max_response_bytes).unwrap_or(u64::MAX)
        }) {
            return Err(AiClientError::ResponseTooLarge {
                max_bytes: self.max_response_bytes,
            });
        }
        let bytes = response
            .bytes()
            .await
            .map_err(|error| AiClientError::from_reqwest(&error))?;
        if bytes.len() > self.max_response_bytes {
            return Err(AiClientError::ResponseTooLarge {
                max_bytes: self.max_response_bytes,
            });
        }
        if !status.is_success() {
            return Err(AiClientError::Http {
                status: status.as_u16(),
                retryable: retryable_status(status),
            });
        }
        let payload: Value = serde_json::from_slice(&bytes)
            .map_err(|_| AiClientError::MalformedResponse("response is not valid JSON"))?;
        extract_output_text(&payload).ok_or(AiClientError::MalformedResponse(
            "response omitted output text",
        ))
    }

    async fn complete_with_retry(&self, system: &str, user: &str) -> Result<String, AiClientError> {
        for attempt in 1..=self.max_attempts {
            match self.complete_once(system, user).await {
                Ok(output) => return Ok(output),
                Err(error) if error.retryable() && attempt < self.max_attempts => {
                    sleep(retry_delay(attempt)).await;
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("AI retry loop always returns")
    }
}

impl AiModelClient for OpenAiCompatibleClient {
    fn complete<'a>(&'a self, system: &'a str, user: &'a str) -> CompletionFuture<'a> {
        Box::pin(self.complete_with_retry(system, user))
    }

    fn model(&self) -> &str {
        &self.model
    }

    fn config_version(&self) -> &str {
        &self.config_version
    }
}

#[derive(Debug, Error)]
pub enum AiClientError {
    #[error("AI client configuration is invalid: {0}")]
    Configuration(&'static str),
    #[error("AI client configuration is invalid: {0}")]
    ConfigurationOwned(String),
    #[error("AI provider request timed out")]
    Timeout,
    #[error("AI provider transport failed")]
    Transport { retryable: bool },
    #[error("AI provider returned HTTP {status}")]
    Http { status: u16, retryable: bool },
    #[error("AI provider response exceeded {max_bytes} bytes")]
    ResponseTooLarge { max_bytes: usize },
    #[error("AI provider response is malformed: {0}")]
    MalformedResponse(&'static str),
}

impl AiClientError {
    #[must_use]
    pub const fn retryable(&self) -> bool {
        match self {
            Self::Timeout => true,
            Self::Transport { retryable } | Self::Http { retryable, .. } => *retryable,
            Self::Configuration(_)
            | Self::ConfigurationOwned(_)
            | Self::ResponseTooLarge { .. }
            | Self::MalformedResponse(_) => false,
        }
    }

    fn from_reqwest(error: &reqwest::Error) -> Self {
        if error.is_timeout() {
            Self::Timeout
        } else {
            Self::Transport {
                retryable: error.is_connect() || error.is_request() || error.is_body(),
            }
        }
    }
}

#[derive(Serialize)]
struct ChatCompletionRequest<'a> {
    model: &'a str,
    messages: [ChatMessage<'a>; 2],
    temperature: f64,
}

#[derive(Serialize)]
struct ChatMessage<'a> {
    role: &'static str,
    content: &'a str,
}

#[derive(Deserialize)]
struct ContentPart {
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    content: Option<String>,
}

fn required_env(name: &'static str) -> Result<String, AiClientError> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .ok_or(AiClientError::Configuration(name))
}

fn env_u64(name: &'static str, default: u64, min: u64, max: u64) -> Result<u64, AiClientError> {
    let value = std::env::var(name)
        .ok()
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|_| AiClientError::Configuration(name))
        })
        .transpose()?
        .unwrap_or(default);
    if !(min..=max).contains(&value) {
        return Err(AiClientError::Configuration(name));
    }
    Ok(value)
}

fn chat_completions_endpoint(base_url: &str) -> Result<String, AiClientError> {
    let trimmed = base_url.trim().trim_end_matches('/');
    if trimmed.is_empty() || !(trimmed.starts_with("https://") || trimmed.starts_with("http://")) {
        return Err(AiClientError::Configuration(
            "AI_PROVIDER_BASE_URL must be an HTTP(S) URL",
        ));
    }
    if trimmed.ends_with("/chat/completions") {
        Ok(trimmed.to_owned())
    } else {
        Ok(format!("{trimmed}/chat/completions"))
    }
}

fn retryable_status(status: StatusCode) -> bool {
    status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
}

fn retry_delay(attempt: u32) -> Duration {
    Duration::from_millis(200_u64.saturating_mul(1_u64 << attempt.saturating_sub(1).min(4)))
}

fn extract_output_text(payload: &Value) -> Option<String> {
    if let Some(text) = payload
        .get("output_text")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
    {
        return Some(text.to_owned());
    }
    let content = payload
        .pointer("/choices/0/message/content")
        .or_else(|| payload.pointer("/choices/0/text"))?;
    match content {
        Value::String(text) if !text.trim().is_empty() => Some(text.trim().to_owned()),
        Value::Array(parts) => {
            let joined = parts
                .iter()
                .filter_map(|part| serde_json::from_value::<ContentPart>(part.clone()).ok())
                .filter_map(|part| part.text.or(part.content))
                .collect::<String>();
            (!joined.trim().is_empty()).then(|| joined.trim().to_owned())
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use reqwest::StatusCode;
    use serde_json::json;

    use super::{AiClientError, chat_completions_endpoint, extract_output_text, retryable_status};

    #[test]
    fn builds_chat_completion_endpoint() {
        assert_eq!(
            chat_completions_endpoint("https://model.example/v1/").unwrap(),
            "https://model.example/v1/chat/completions"
        );
        assert_eq!(
            chat_completions_endpoint("https://model.example/v1/chat/completions").unwrap(),
            "https://model.example/v1/chat/completions"
        );
        assert!(chat_completions_endpoint("model.example").is_err());
    }

    #[test]
    fn extracts_string_and_part_content() {
        assert_eq!(
            extract_output_text(&json!({"choices": [{"message": {"content": " ok "}}]})),
            Some("ok".to_owned())
        );
        assert_eq!(
            extract_output_text(&json!({
                "choices": [{"message": {"content": [{"text": "a"}, {"text": "b"}]}}]
            })),
            Some("ab".to_owned())
        );
    }

    #[test]
    fn classifies_retryable_failures() {
        assert!(retryable_status(StatusCode::TOO_MANY_REQUESTS));
        assert!(retryable_status(StatusCode::BAD_GATEWAY));
        assert!(!retryable_status(StatusCode::BAD_REQUEST));
        assert!(AiClientError::Timeout.retryable());
        assert!(!AiClientError::MalformedResponse("bad").retryable());
    }
}
