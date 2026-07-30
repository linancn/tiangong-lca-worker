//! Shared resource admission, cancellation, and phase telemetry primitives.

use std::{
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

#[cfg(test)]
use std::sync::Mutex;

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const RESOURCE_PROFILE_SCHEMA: &str = "worker.resource-profile.v1";

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceLimits {
    pub memory_soft_bytes: Option<u64>,
    pub memory_hard_bytes: Option<u64>,
    pub temp_bytes: Option<u64>,
    pub object_download_bytes: Option<u64>,
    pub object_upload_bytes: Option<u64>,
    pub cache_bytes: Option<u64>,
    pub stage_window_bytes: Option<u64>,
    pub max_concurrency: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceProfile {
    pub schema: String,
    pub job_family: String,
    pub limits: ResourceLimits,
}

impl ResourceProfile {
    #[must_use]
    pub fn new(job_family: impl Into<String>, limits: ResourceLimits) -> Self {
        Self {
            schema: RESOURCE_PROFILE_SCHEMA.to_owned(),
            job_family: job_family.into(),
            limits,
        }
    }

    pub fn admit(&self, demand: &ResourceDemand) -> Result<ResourceAdmission, ResourceError> {
        self.validate()?;
        demand.validate()?;
        check_limit(
            "ownedEstimateBytes",
            demand.owned_estimate_bytes,
            self.limits.memory_hard_bytes,
        )?;
        check_limit("tempBytes", demand.temp_bytes, self.limits.temp_bytes)?;
        check_limit(
            "objectDownloadBytes",
            demand.object_download_bytes,
            self.limits.object_download_bytes,
        )?;
        check_limit(
            "objectUploadBytes",
            demand.object_upload_bytes,
            self.limits.object_upload_bytes,
        )?;
        check_limit("cacheBytes", demand.cache_bytes, self.limits.cache_bytes)?;
        check_limit(
            "stageWindowBytes",
            demand.stage_window_bytes,
            self.limits.stage_window_bytes,
        )?;
        if let (Some(requested), Some(limit)) = (demand.concurrency, self.limits.max_concurrency)
            && requested > limit
        {
            return Err(ResourceError::AdmissionRejected {
                resource: "concurrency",
                requested: u64::from(requested),
                limit: u64::from(limit),
            });
        }
        Ok(ResourceAdmission {
            memory_soft_limit_exceeded: matches!(
                (
                    demand.owned_estimate_bytes,
                    self.limits.memory_soft_bytes
                ),
                (Some(requested), Some(limit)) if requested > limit
            ),
        })
    }

    fn validate(&self) -> Result<(), ResourceError> {
        if self.schema != RESOURCE_PROFILE_SCHEMA {
            return Err(ResourceError::InvalidProfile(format!(
                "unsupported resource profile schema: {}",
                self.schema
            )));
        }
        if let (Some(soft), Some(hard)) =
            (self.limits.memory_soft_bytes, self.limits.memory_hard_bytes)
            && soft > hard
        {
            return Err(ResourceError::InvalidProfile(format!(
                "memory soft limit must not exceed hard limit: soft={soft} hard={hard}"
            )));
        }
        if self.limits.max_concurrency == Some(0) {
            return Err(ResourceError::InvalidProfile(
                "resource profile max concurrency must be greater than zero".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceAdmission {
    pub memory_soft_limit_exceeded: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceDemand {
    pub owned_estimate_bytes: Option<u64>,
    pub temp_bytes: Option<u64>,
    pub object_download_bytes: Option<u64>,
    pub object_upload_bytes: Option<u64>,
    pub cache_bytes: Option<u64>,
    pub stage_window_bytes: Option<u64>,
    pub concurrency: Option<u32>,
}

impl ResourceDemand {
    fn validate(&self) -> Result<(), ResourceError> {
        if self.concurrency == Some(0) {
            return Err(ResourceError::InvalidProfile(
                "resource demand concurrency must be greater than zero".to_owned(),
            ));
        }
        Ok(())
    }
}

fn check_limit(
    resource: &'static str,
    requested: Option<u64>,
    limit: Option<u64>,
) -> Result<(), ResourceError> {
    if let (Some(requested), Some(limit)) = (requested, limit)
        && requested > limit
    {
        return Err(ResourceError::AdmissionRejected {
            resource,
            requested,
            limit,
        });
    }
    Ok(())
}

#[derive(Debug, Error)]
pub enum ResourceError {
    #[error("resource admission rejected for {resource}: requested={requested} limit={limit}")]
    AdmissionRejected {
        resource: &'static str,
        requested: u64,
        limit: u64,
    },
    #[error("artifact limit exceeded for {resource}: observed={observed} limit={limit}")]
    ArtifactLimitExceeded {
        resource: &'static str,
        observed: u64,
        limit: u64,
    },
    #[error("operation cancelled during {stage}")]
    Cancelled { stage: &'static str },
    #[error("invalid resource profile: {0}")]
    InvalidProfile(String),
}

impl ResourceError {
    #[must_use]
    pub const fn error_code(&self) -> &'static str {
        match self {
            Self::AdmissionRejected { .. } | Self::InvalidProfile(_) => {
                "resource_admission_rejected"
            }
            Self::ArtifactLimitExceeded { .. } => "artifact_limit_exceeded",
            Self::Cancelled { .. } => "operation_cancelled",
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct CancellationToken {
    cancelled: Arc<AtomicBool>,
    #[cfg(test)]
    test_cancel_stage: Arc<Mutex<Option<&'static str>>>,
}

impl CancellationToken {
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub fn cancel_at_stage(&self, stage: &'static str) {
        *self
            .test_cancel_stage
            .lock()
            .expect("cancellation stage lock poisoned") = Some(stage);
    }

    pub fn check(&self, stage: &'static str) -> Result<(), ResourceError> {
        #[cfg(test)]
        if self
            .test_cancel_stage
            .lock()
            .expect("cancellation stage lock poisoned")
            .is_some_and(|target| target == stage)
        {
            self.cancel();
        }
        if self.is_cancelled() {
            Err(ResourceError::Cancelled { stage })
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceCounters {
    pub owned_estimate_bytes: Option<u64>,
    pub temp_bytes: Option<u64>,
    pub object_bytes: Option<u64>,
    pub cache_bytes: Option<u64>,
    pub rows: Option<u64>,
    pub nnz: Option<u64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CgroupMemory {
    pub anon_bytes: Option<u64>,
    pub file_bytes: Option<u64>,
    pub current_bytes: Option<u64>,
    pub peak_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceMeasurement {
    pub schema: String,
    pub phase: String,
    pub process_rss_bytes: Option<u64>,
    pub cgroup: CgroupMemory,
    pub counters: ResourceCounters,
}

impl ResourceMeasurement {
    #[must_use]
    pub fn capture(phase: impl Into<String>, counters: ResourceCounters) -> Self {
        Self {
            schema: RESOURCE_PROFILE_SCHEMA.to_owned(),
            phase: phase.into(),
            process_rss_bytes: process_rss_bytes(),
            cgroup: cgroup_memory(),
            counters,
        }
    }
}

#[must_use]
pub fn directory_bytes(path: &Path) -> Option<u64> {
    let metadata = fs::symlink_metadata(path).ok()?;
    if metadata.is_file() {
        return Some(metadata.len());
    }
    if !metadata.is_dir() {
        return Some(0);
    }
    let mut total = 0_u64;
    for entry in fs::read_dir(path).ok()? {
        let entry = entry.ok()?;
        total = total.checked_add(directory_bytes(&entry.path())?)?;
    }
    Some(total)
}

fn process_rss_bytes() -> Option<u64> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    let kib = status.lines().find_map(|line| {
        line.strip_prefix("VmRSS:")?
            .split_whitespace()
            .next()?
            .parse::<u64>()
            .ok()
    })?;
    kib.checked_mul(1024)
}

fn cgroup_memory() -> CgroupMemory {
    let Some(base) = cgroup_v2_path() else {
        return CgroupMemory::default();
    };
    let current_bytes = read_u64(base.join("memory.current"));
    let peak_bytes = read_u64(base.join("memory.peak"));
    let memory_stat = fs::read_to_string(base.join("memory.stat")).ok();
    let stat_value = |name: &str| {
        memory_stat.as_deref()?.lines().find_map(|line| {
            let (key, value) = line.split_once(' ')?;
            (key == name).then(|| value.parse::<u64>().ok()).flatten()
        })
    };
    CgroupMemory {
        anon_bytes: stat_value("anon"),
        file_bytes: stat_value("file"),
        current_bytes,
        peak_bytes,
    }
}

fn cgroup_v2_path() -> Option<PathBuf> {
    let cgroup = fs::read_to_string("/proc/self/cgroup").ok()?;
    let relative = cgroup.lines().find_map(|line| line.strip_prefix("0::"))?;
    Some(Path::new("/sys/fs/cgroup").join(relative.trim_start_matches('/')))
}

fn read_u64(path: PathBuf) -> Option<u64> {
    fs::read_to_string(path).ok()?.trim().parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admission_rejects_before_allocation_with_stable_code() {
        let profile = ResourceProfile::new(
            "test",
            ResourceLimits {
                memory_hard_bytes: Some(1024),
                ..ResourceLimits::default()
            },
        );
        let err = profile
            .admit(&ResourceDemand {
                owned_estimate_bytes: Some(1025),
                ..ResourceDemand::default()
            })
            .unwrap_err();
        assert_eq!(err.error_code(), "resource_admission_rejected");
        assert!(matches!(
            err,
            ResourceError::AdmissionRejected {
                resource: "ownedEstimateBytes",
                requested: 1025,
                limit: 1024
            }
        ));
    }

    #[test]
    fn cancellation_has_stable_code() {
        let token = CancellationToken::default();
        token.cancel();
        let err = token.check("test").unwrap_err();
        assert_eq!(err.error_code(), "operation_cancelled");
    }

    #[test]
    fn admission_reports_soft_memory_pressure_without_hiding_hard_capacity() {
        let profile = ResourceProfile::new(
            "test",
            ResourceLimits {
                memory_soft_bytes: Some(512),
                memory_hard_bytes: Some(1024),
                ..ResourceLimits::default()
            },
        );
        let admission = profile
            .admit(&ResourceDemand {
                owned_estimate_bytes: Some(768),
                ..ResourceDemand::default()
            })
            .unwrap();
        assert!(admission.memory_soft_limit_exceeded);
    }

    #[test]
    fn directory_measurement_counts_nested_files() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("one"), [0_u8; 3]).unwrap();
        fs::create_dir(temp.path().join("nested")).unwrap();
        fs::write(temp.path().join("nested/two"), [0_u8; 5]).unwrap();
        assert_eq!(directory_bytes(temp.path()), Some(8));
    }
}
