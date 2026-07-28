use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::SystemTime,
};

use blake3::Hasher;
use serde::{Deserialize, Serialize};
use suitesparse_ffi::{CscMatrix, UmfpackFactorization};
use thiserror::Error;
use uuid::Uuid;

use crate::validator::ValidationReport;

/// Solver backend options.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SolverBackend {
    /// `SuiteSparse` UMFPACK backend.
    Umfpack,
}

/// Cache state for one factorization key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FactorizationState {
    /// Job is queued but not started.
    Pending,
    /// Factorization in progress.
    Building,
    /// Factorization is ready for solve.
    Ready,
    /// Factorization failed.
    Failed,
    /// Cache entry is stale and must be rebuilt.
    Stale,
}

/// Cache key driven by model version, backend and options hash.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FactorizationKey {
    /// Model version.
    pub model_version: Uuid,
    /// Backend id.
    pub backend: SolverBackend,
    /// Hash of numeric options.
    pub options_hash: String,
}

impl FactorizationKey {
    /// Constructs cache key.
    #[must_use]
    pub fn new(model_version: Uuid, backend: SolverBackend, options_bytes: &[u8]) -> Self {
        let mut hasher = Hasher::new();
        hasher.update(options_bytes);
        let hash = hasher.finalize();
        Self {
            model_version,
            backend,
            options_hash: hash.to_hex().to_string(),
        }
    }

    /// Stable id string.
    #[must_use]
    pub fn factorization_id(&self) -> String {
        format!(
            "{}:{}:{}",
            self.model_version, self.options_hash, self.backend as u8
        )
    }
}

/// Prepared model bundle stored in cache.
#[derive(Debug)]
pub struct PreparedModel {
    /// UMFPACK factorization handle.
    pub factorization: Arc<UmfpackFactorization>,
    /// B matrix.
    pub b: CscMatrix,
    /// C matrix.
    pub c: CscMatrix,
    /// Validation report.
    pub validation: ValidationReport,
}

impl PreparedModel {
    /// Deterministic estimate of retained owned bytes.
    ///
    /// The UMFPACK portion uses the factorization's actual workload-derived
    /// symbolic/numeric object sizes, so fill-in is reflected after build.
    #[must_use]
    pub fn estimated_owned_bytes(&self) -> usize {
        self.factorization
            .estimated_owned_bytes()
            .saturating_add(self.b.estimated_owned_bytes())
            .saturating_add(self.c.estimated_owned_bytes())
    }
}

#[derive(Debug, Clone)]
struct CacheMeta {
    state: FactorizationState,
    updated_at: SystemTime,
    error_message: Option<String>,
    estimated_bytes: usize,
    last_access: u64,
}

#[derive(Debug)]
struct CacheEntry {
    prepared: Option<Arc<PreparedModel>>,
    meta: CacheMeta,
}

/// Snapshot of factorization-cache capacity and activity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactorizationCacheTelemetry {
    /// Configured hard retained-byte capacity.
    pub capacity_bytes: usize,
    /// Estimated retained bytes currently admitted.
    pub resident_bytes: usize,
    /// Total metadata entries, including building/failed entries.
    pub entry_count: usize,
    /// Ready entries that own prepared factorization state.
    pub ready_entry_count: usize,
    /// Ready entries evicted to make space.
    pub eviction_count: u64,
    /// Entries invalidated and released.
    pub invalidation_count: u64,
    /// Ready entries rejected because one item exceeded capacity.
    pub admission_rejection_count: u64,
    /// Successful ready-cache lookups.
    pub hit_count: u64,
    /// Ready-cache lookup misses.
    pub miss_count: u64,
}

/// Error returned when a prepared entry cannot fit within the hard capacity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error(
    "factorization cache admission rejected: estimated_bytes={estimated_bytes} capacity_bytes={capacity_bytes}"
)]
pub struct FactorizationCacheAdmissionError {
    /// Workload-derived retained-byte estimate.
    pub estimated_bytes: usize,
    /// Configured hard cache capacity.
    pub capacity_bytes: usize,
}

#[derive(Debug)]
struct CacheInner {
    entries: HashMap<FactorizationKey, CacheEntry>,
    capacity_bytes: usize,
    resident_bytes: usize,
    access_clock: u64,
    eviction_count: u64,
    invalidation_count: u64,
    admission_rejection_count: u64,
    hit_count: u64,
    miss_count: u64,
}

/// Byte-bounded in-memory factorization cache with deterministic LRU eviction.
#[derive(Debug)]
pub struct FactorizationCache {
    inner: Mutex<CacheInner>,
}

impl Default for FactorizationCache {
    fn default() -> Self {
        Self::new()
    }
}

impl FactorizationCache {
    /// Default retained-byte capacity (1 GiB).
    pub const DEFAULT_CAPACITY_BYTES: usize = 1024 * 1024 * 1024;

    /// Creates cache with the default hard capacity.
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacity(Self::DEFAULT_CAPACITY_BYTES)
    }

    /// Creates cache with an explicit hard retained-byte capacity.
    #[must_use]
    pub fn with_capacity(capacity_bytes: usize) -> Self {
        Self {
            inner: Mutex::new(CacheInner {
                entries: HashMap::new(),
                capacity_bytes,
                resident_bytes: 0,
                access_clock: 0,
                eviction_count: 0,
                invalidation_count: 0,
                admission_rejection_count: 0,
                hit_count: 0,
                miss_count: 0,
            }),
        }
    }

    fn next_access(inner: &mut CacheInner) -> u64 {
        inner.access_clock = inner.access_clock.saturating_add(1);
        inner.access_clock
    }

    fn remove_entry(inner: &mut CacheInner, key: &FactorizationKey) -> Option<CacheEntry> {
        let removed = inner.entries.remove(key);
        if let Some(entry) = &removed {
            inner.resident_bytes = inner
                .resident_bytes
                .saturating_sub(entry.meta.estimated_bytes);
        }
        removed
    }

    /// Marks key as building and releases any older prepared state for the key.
    pub fn set_building(&self, key: FactorizationKey) {
        let mut inner = self.inner.lock().expect("factorization cache poisoned");
        Self::remove_entry(&mut inner, &key);
        let access = Self::next_access(&mut inner);
        inner.entries.insert(
            key,
            CacheEntry {
                prepared: None,
                meta: CacheMeta {
                    state: FactorizationState::Building,
                    updated_at: SystemTime::now(),
                    error_message: None,
                    estimated_bytes: 0,
                    last_access: access,
                },
            },
        );
    }

    /// Inserts a ready factorization after evicting least-recently-used ready entries.
    pub fn set_ready(
        &self,
        key: FactorizationKey,
        prepared: Arc<PreparedModel>,
    ) -> Result<usize, FactorizationCacheAdmissionError> {
        let estimated_bytes = prepared.estimated_owned_bytes();
        let mut inner = self.inner.lock().expect("factorization cache poisoned");
        Self::remove_entry(&mut inner, &key);

        if estimated_bytes > inner.capacity_bytes {
            inner.admission_rejection_count = inner.admission_rejection_count.saturating_add(1);
            let access = Self::next_access(&mut inner);
            let capacity_bytes = inner.capacity_bytes;
            inner.entries.insert(
                key,
                CacheEntry {
                    prepared: None,
                    meta: CacheMeta {
                        state: FactorizationState::Failed,
                        updated_at: SystemTime::now(),
                        error_message: Some(format!(
                            "cache admission rejected: estimated_bytes={estimated_bytes} capacity_bytes={capacity_bytes}"
                        )),
                        estimated_bytes: 0,
                        last_access: access,
                    },
                },
            );
            return Err(FactorizationCacheAdmissionError {
                estimated_bytes,
                capacity_bytes,
            });
        }

        while inner.resident_bytes.saturating_add(estimated_bytes) > inner.capacity_bytes {
            let eviction_key = inner
                .entries
                .iter()
                .filter(|(_, entry)| entry.meta.estimated_bytes > 0)
                .min_by(|(left_key, left), (right_key, right)| {
                    left.meta
                        .last_access
                        .cmp(&right.meta.last_access)
                        .then_with(|| {
                            left_key
                                .factorization_id()
                                .cmp(&right_key.factorization_id())
                        })
                })
                .map(|(candidate, _)| candidate.clone());
            let Some(eviction_key) = eviction_key else {
                break;
            };
            Self::remove_entry(&mut inner, &eviction_key);
            inner.eviction_count = inner.eviction_count.saturating_add(1);
        }

        let access = Self::next_access(&mut inner);
        inner.resident_bytes = inner.resident_bytes.saturating_add(estimated_bytes);
        inner.entries.insert(
            key,
            CacheEntry {
                prepared: Some(prepared),
                meta: CacheMeta {
                    state: FactorizationState::Ready,
                    updated_at: SystemTime::now(),
                    error_message: None,
                    estimated_bytes,
                    last_access: access,
                },
            },
        );
        Ok(estimated_bytes)
    }

    /// Marks failure for key and releases any prepared state.
    pub fn set_failed(&self, key: FactorizationKey, error_message: String) {
        let mut inner = self.inner.lock().expect("factorization cache poisoned");
        Self::remove_entry(&mut inner, &key);
        let access = Self::next_access(&mut inner);
        inner.entries.insert(
            key,
            CacheEntry {
                prepared: None,
                meta: CacheMeta {
                    state: FactorizationState::Failed,
                    updated_at: SystemTime::now(),
                    error_message: Some(error_message),
                    estimated_bytes: 0,
                    last_access: access,
                },
            },
        );
    }

    /// Invalidates all entries for a model and immediately releases prepared state.
    #[must_use]
    pub fn invalidate_model(&self, model_version: Uuid) -> usize {
        let mut inner = self.inner.lock().expect("factorization cache poisoned");
        let keys = inner
            .entries
            .keys()
            .filter(|key| key.model_version == model_version)
            .cloned()
            .collect::<Vec<_>>();
        for key in &keys {
            Self::remove_entry(&mut inner, key);
            let access = Self::next_access(&mut inner);
            inner.entries.insert(
                key.clone(),
                CacheEntry {
                    prepared: None,
                    meta: CacheMeta {
                        state: FactorizationState::Stale,
                        updated_at: SystemTime::now(),
                        error_message: None,
                        estimated_bytes: 0,
                        last_access: access,
                    },
                },
            );
        }
        inner.invalidation_count = inner
            .invalidation_count
            .saturating_add(u64::try_from(keys.len()).unwrap_or(u64::MAX));
        keys.len()
    }

    /// Returns prepared model if ready and refreshes LRU access order.
    #[must_use]
    pub fn get_ready(&self, key: &FactorizationKey) -> Option<Arc<PreparedModel>> {
        let mut inner = self.inner.lock().expect("factorization cache poisoned");
        let prepared = inner
            .entries
            .get(key)
            .filter(|entry| entry.meta.state == FactorizationState::Ready)
            .and_then(|entry| entry.prepared.clone());
        if prepared.is_some() {
            let access = Self::next_access(&mut inner);
            if let Some(entry) = inner.entries.get_mut(key) {
                entry.meta.last_access = access;
                entry.meta.updated_at = SystemTime::now();
            }
            inner.hit_count = inner.hit_count.saturating_add(1);
        } else {
            inner.miss_count = inner.miss_count.saturating_add(1);
        }
        prepared
    }

    /// Returns state for key.
    #[must_use]
    pub fn state(&self, key: &FactorizationKey) -> Option<FactorizationState> {
        self.inner
            .lock()
            .expect("factorization cache poisoned")
            .entries
            .get(key)
            .map(|entry| entry.meta.state)
    }

    /// Returns optional error string for key.
    #[must_use]
    pub fn error(&self, key: &FactorizationKey) -> Option<String> {
        self.inner
            .lock()
            .expect("factorization cache poisoned")
            .entries
            .get(key)
            .and_then(|entry| entry.meta.error_message.clone())
    }

    /// Returns current byte-capacity and cache activity telemetry.
    #[must_use]
    pub fn telemetry(&self) -> FactorizationCacheTelemetry {
        let inner = self.inner.lock().expect("factorization cache poisoned");
        FactorizationCacheTelemetry {
            capacity_bytes: inner.capacity_bytes,
            resident_bytes: inner.resident_bytes,
            entry_count: inner.entries.len(),
            ready_entry_count: inner
                .entries
                .values()
                .filter(|entry| entry.meta.state == FactorizationState::Ready)
                .count(),
            eviction_count: inner.eviction_count,
            invalidation_count: inner.invalidation_count,
            admission_rejection_count: inner.admission_rejection_count,
            hit_count: inner.hit_count,
            miss_count: inner.miss_count,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::validator::{MatrixStats, ValidationReport, ValidationStatus};
    use suitesparse_ffi::{CscMatrix, UmfpackNumericOptions};

    fn prepared(scale: f64) -> Arc<PreparedModel> {
        let matrix = CscMatrix::new(1, 1, vec![0, 1], vec![0], vec![scale]).expect("valid matrix");
        Arc::new(PreparedModel {
            factorization: Arc::new(
                UmfpackFactorization::factorize(matrix, UmfpackNumericOptions::default())
                    .expect("factorization"),
            ),
            b: CscMatrix::new(1, 1, vec![0, 1], vec![0], vec![1.0]).expect("B"),
            c: CscMatrix::new(1, 1, vec![0, 1], vec![0], vec![1.0]).expect("C"),
            validation: ValidationReport {
                status: ValidationStatus::Ok,
                stats: MatrixStats {
                    nrows: 1,
                    ncols: 1,
                    nnz: 1,
                    sparsity: 1.0,
                    empty_columns: 0,
                    empty_rows: 0,
                    near_zero_diagonal: 0,
                },
                messages: Vec::new(),
            },
        })
    }

    fn key(model_version: Uuid) -> FactorizationKey {
        FactorizationKey::new(model_version, SolverBackend::Umfpack, &[0])
    }

    #[test]
    fn rejects_single_entry_over_hard_capacity() {
        let cache = FactorizationCache::with_capacity(1);
        let model = Uuid::new_v4();
        cache.set_building(key(model));
        let error = cache.set_ready(key(model), prepared(1.0)).unwrap_err();
        assert_eq!(error.capacity_bytes, 1);
        assert_eq!(cache.telemetry().resident_bytes, 0);
        assert_eq!(cache.telemetry().admission_rejection_count, 1);
    }

    #[test]
    fn evicts_lru_entry_and_invalidation_releases_bytes() {
        let first_model = Uuid::new_v4();
        let second_model = Uuid::new_v4();
        let first = prepared(1.0);
        let one_entry_bytes = first.estimated_owned_bytes();
        let cache = FactorizationCache::with_capacity(one_entry_bytes);
        cache.set_ready(key(first_model), first).unwrap();
        cache.set_ready(key(second_model), prepared(2.0)).unwrap();

        let telemetry = cache.telemetry();
        assert_eq!(telemetry.ready_entry_count, 1);
        assert!(telemetry.resident_bytes <= telemetry.capacity_bytes);
        assert_eq!(telemetry.eviction_count, 1);
        assert!(cache.get_ready(&key(first_model)).is_none());
        assert!(cache.get_ready(&key(second_model)).is_some());

        assert_eq!(cache.invalidate_model(second_model), 1);
        assert_eq!(cache.telemetry().resident_bytes, 0);
        assert_eq!(
            cache.state(&key(second_model)),
            Some(FactorizationState::Stale)
        );
    }
}
