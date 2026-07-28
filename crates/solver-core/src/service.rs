use std::sync::Arc;
use std::time::Instant;

use serde::{Deserialize, Serialize};
use suitesparse_ffi::{UmfpackError, UmfpackFactorization, UmfpackNumericOptions};
use thiserror::Error;
use tracing::instrument;
use uuid::Uuid;

use crate::{
    cache::{
        FactorizationCache, FactorizationCacheAdmissionError, FactorizationCacheTelemetry,
        FactorizationKey, FactorizationState, PreparedModel, SolverBackend,
    },
    data_builder::{BuiltMatrices, DataBuilder, DataBuilderError, ModelSparseData},
    validator::{ValidationReport, ValidationStatus, validate_factorization_matrix},
};

/// Numeric solve options included in factorization key.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct NumericOptions {
    /// UMFPACK verbosity.
    pub print_level: f64,
}

impl Default for NumericOptions {
    fn default() -> Self {
        Self { print_level: 0.0 }
    }
}

/// Solve output selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SolveOptions {
    /// Return x.
    pub return_x: bool,
    /// Return g = Bx.
    pub return_g: bool,
    /// Return h = Cg.
    pub return_h: bool,
}

impl Default for SolveOptions {
    fn default() -> Self {
        Self {
            return_x: true,
            return_g: true,
            return_h: true,
        }
    }
}

/// Prepare diagnostics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FactorizationDiagnostics {
    /// Validation report for matrix M.
    pub validation: ValidationReport,
    /// Backend.
    pub backend: SolverBackend,
    /// Matrix nnz stats.
    pub m_nnz: usize,
    /// Matrix nnz stats.
    pub b_nnz: usize,
    /// Matrix nnz stats.
    pub c_nnz: usize,
    /// Workload-derived retained-byte estimate admitted to the cache.
    pub cache_entry_estimated_bytes: usize,
    /// Peak bytes reported by UMFPACK for symbolic/numeric factorization.
    pub factorization_reported_peak_bytes: Option<usize>,
    /// Cache capacity and activity after admission.
    pub cache: FactorizationCacheTelemetry,
}

/// Result of prepare stage.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PrepareResult {
    /// Factorization id.
    pub factorization_id: String,
    /// State in cache.
    pub status: FactorizationState,
    /// Diagnostics.
    pub diagnostics: FactorizationDiagnostics,
}

/// Single RHS solve result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SolveResult {
    /// Optional x.
    pub x: Option<Vec<f64>>,
    /// Optional g = Bx.
    pub g: Option<Vec<f64>>,
    /// Optional h = Cg.
    pub h: Option<Vec<f64>>,
    /// Factorization state at solve time.
    pub factorization_state: FactorizationState,
}

/// Detailed compute timing for one solve call, excluding queue/DB/object-storage I/O.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SolveComputationTiming {
    /// Time spent in sparse back-substitution `Mx=y`.
    pub solve_mx_sec: f64,
    /// Time spent computing `g = Bx` (if needed by options).
    pub bx_sec: f64,
    /// Time spent computing `h = Cg` (if requested).
    pub cg_sec: f64,
    /// Sum of the comparable compute stages.
    pub comparable_compute_sec: f64,
}

/// Solve output plus timing breakdown.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TimedSolveResult {
    /// Functional solve output.
    pub result: SolveResult,
    /// Compute timing breakdown.
    pub timing: SolveComputationTiming,
}

/// Multi-RHS solve result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SolveBatchResult {
    /// Solve outputs for each RHS.
    pub items: Vec<SolveResult>,
}

/// Solver layer errors.
#[derive(Debug, Error)]
pub enum SolverError {
    #[error("data builder failed: {0}")]
    DataBuilder(#[from] DataBuilderError),
    #[error("factorization failed: {0}")]
    Factorization(#[from] UmfpackError),
    #[error("factorization key not prepared for model {model_version}")]
    NotPrepared { model_version: Uuid },
    #[error("matrix validation failed: {0:?}")]
    ValidationFailed(ValidationReport),
    #[error("rhs dimension {rhs_len} does not match process count {process_count}")]
    RhsDimensionMismatch {
        rhs_len: usize,
        process_count: usize,
    },
    #[error(
        "factorization workload admission rejected: estimated_bytes={estimated_bytes} capacity_bytes={capacity_bytes} fill_in_multiplier={fill_in_multiplier}"
    )]
    WorkloadAdmissionRejected {
        estimated_bytes: usize,
        capacity_bytes: usize,
        fill_in_multiplier: f64,
    },
    #[error(transparent)]
    CacheAdmission(#[from] FactorizationCacheAdmissionError),
}

/// Orchestrates prepare/solve/invalidate with in-memory factorization cache.
#[derive(Debug, Default)]
pub struct SolverService {
    cache: FactorizationCache,
    builder: DataBuilder,
    admission_fill_in_multiplier: f64,
}

impl SolverService {
    /// Creates service with default builder.
    #[must_use]
    pub fn new() -> Self {
        Self {
            cache: FactorizationCache::new(),
            builder: DataBuilder::default(),
            admission_fill_in_multiplier: 8.0,
        }
    }

    /// Creates service with explicit retained-cache capacity and workload admission multiplier.
    ///
    /// The multiplier is a configurable workload policy, not a universal memory
    /// bound. Actual UMFPACK fill-in is measured after factorization and the
    /// hard cache capacity is enforced again using that observed estimate.
    #[must_use]
    pub fn with_cache_policy(capacity_bytes: usize, admission_fill_in_multiplier: f64) -> Self {
        Self {
            cache: FactorizationCache::with_capacity(capacity_bytes),
            builder: DataBuilder::default(),
            admission_fill_in_multiplier: admission_fill_in_multiplier.max(1.0),
        }
    }

    /// Returns cache handle.
    #[must_use]
    pub fn cache(&self) -> &FactorizationCache {
        &self.cache
    }

    /// Prepares and caches factorization for one model version.
    #[instrument(skip_all, fields(model_version = %data.model_version))]
    pub fn prepare(
        &self,
        data: &ModelSparseData,
        options: NumericOptions,
    ) -> Result<PrepareResult, SolverError> {
        let key = Self::factorization_key(data.model_version, options);
        self.cache.set_building(key.clone());

        let matrices = self.builder.build(data)?;
        let validation = validate_factorization_matrix(&matrices.m, 1e-12);

        if validation.status == ValidationStatus::FailedInvalidStructure {
            self.cache.set_failed(
                key.clone(),
                "matrix structural validation failed".to_owned(),
            );
            return Err(SolverError::ValidationFailed(validation));
        }

        let m_nnz = matrices.m_nnz;
        let b_nnz = matrices.b_nnz;
        let c_nnz = matrices.c_nnz;
        let matrix_bytes = matrices
            .m
            .estimated_owned_bytes()
            .saturating_add(matrices.b.estimated_owned_bytes())
            .saturating_add(matrices.c.estimated_owned_bytes());
        let m_owned_bytes = matrices.m.estimated_owned_bytes();
        let m_owned_bytes_f64 = m_owned_bytes
            .to_string()
            .parse::<f64>()
            .unwrap_or(f64::INFINITY);
        let scaled_factorization_bytes =
            (m_owned_bytes_f64 * self.admission_fill_in_multiplier).ceil();
        let factorization_working_estimate = format!("{scaled_factorization_bytes:.0}")
            .parse::<usize>()
            .unwrap_or(usize::MAX);
        let workload_estimated_bytes = matrix_bytes.saturating_add(factorization_working_estimate);
        let capacity_bytes = self.cache.telemetry().capacity_bytes;
        if workload_estimated_bytes > capacity_bytes {
            self.cache.set_failed(
                key.clone(),
                format!(
                    "workload admission rejected: estimated_bytes={workload_estimated_bytes} capacity_bytes={capacity_bytes}"
                ),
            );
            return Err(SolverError::WorkloadAdmissionRejected {
                estimated_bytes: workload_estimated_bytes,
                capacity_bytes,
                fill_in_multiplier: self.admission_fill_in_multiplier,
            });
        }

        let factorization = match UmfpackFactorization::factorize(
            matrices.m,
            UmfpackNumericOptions {
                print_level: options.print_level,
            },
        ) {
            Ok(factorization) => Arc::new(factorization),
            Err(error) => {
                self.cache
                    .set_failed(key.clone(), format!("factorization failed: {error}"));
                return Err(SolverError::Factorization(error));
            }
        };
        let factorization_reported_peak_bytes = factorization.reported_peak_bytes();

        let prepared = Arc::new(PreparedModel {
            factorization,
            b: matrices.b,
            c: matrices.c,
            validation: validation.clone(),
        });

        let cache_entry_estimated_bytes = self.cache.set_ready(key.clone(), prepared)?;
        let cache = self.cache.telemetry();

        Ok(PrepareResult {
            factorization_id: key.factorization_id(),
            status: FactorizationState::Ready,
            diagnostics: FactorizationDiagnostics {
                validation,
                backend: SolverBackend::Umfpack,
                m_nnz,
                b_nnz,
                c_nnz,
                cache_entry_estimated_bytes,
                factorization_reported_peak_bytes,
                cache,
            },
        })
    }

    /// Returns factorization state for model+options.
    #[must_use]
    pub fn factorization_status(
        &self,
        model_version: Uuid,
        options: NumericOptions,
    ) -> Option<FactorizationState> {
        let key = Self::factorization_key(model_version, options);
        self.cache.state(&key)
    }

    /// Solves one RHS using prepared factorization.
    #[instrument(skip_all, fields(model_version = %model_version))]
    pub fn solve_one(
        &self,
        model_version: Uuid,
        options: NumericOptions,
        rhs: &[f64],
        solve_options: SolveOptions,
    ) -> Result<SolveResult, SolverError> {
        Ok(self
            .solve_one_timed(model_version, options, rhs, solve_options)?
            .result)
    }

    /// Solves one RHS and returns per-stage compute timing.
    #[instrument(skip_all, fields(model_version = %model_version))]
    pub fn solve_one_timed(
        &self,
        model_version: Uuid,
        options: NumericOptions,
        rhs: &[f64],
        solve_options: SolveOptions,
    ) -> Result<TimedSolveResult, SolverError> {
        let key = Self::factorization_key(model_version, options);
        let prepared = self
            .cache
            .get_ready(&key)
            .ok_or(SolverError::NotPrepared { model_version })?;

        let process_count =
            usize::try_from(prepared.factorization.matrix().ncols).unwrap_or_default();
        if rhs.len() != process_count {
            return Err(SolverError::RhsDimensionMismatch {
                rhs_len: rhs.len(),
                process_count,
            });
        }

        let solve_started = Instant::now();
        let x = prepared.factorization.solve(rhs)?;
        let solve_mx_sec = solve_started.elapsed().as_secs_f64();

        let mut bx_sec = 0.0_f64;
        let g = if solve_options.return_g || solve_options.return_h {
            let bx_started = Instant::now();
            let g_vec = prepared.b.mul_vector(&x);
            bx_sec = bx_started.elapsed().as_secs_f64();
            Some(g_vec)
        } else {
            None
        };

        let mut cg_sec = 0.0_f64;
        let h = if solve_options.return_h {
            let cg_started = Instant::now();
            let h_vec = g.as_ref().map(|g_vec| prepared.c.mul_vector(g_vec));
            cg_sec = cg_started.elapsed().as_secs_f64();
            h_vec
        } else {
            None
        };

        let x_out = if solve_options.return_x {
            Some(x)
        } else {
            None
        };
        let g_out = if solve_options.return_g { g } else { None };

        let result = SolveResult {
            x: x_out,
            g: g_out,
            h,
            factorization_state: FactorizationState::Ready,
        };

        Ok(TimedSolveResult {
            result,
            timing: SolveComputationTiming {
                solve_mx_sec,
                bx_sec,
                cg_sec,
                comparable_compute_sec: solve_mx_sec + bx_sec + cg_sec,
            },
        })
    }

    /// Solves multiple RHS vectors by repeated back-substitution.
    pub fn solve_batch(
        &self,
        model_version: Uuid,
        options: NumericOptions,
        rhs_batch: &[Vec<f64>],
        solve_options: SolveOptions,
    ) -> Result<SolveBatchResult, SolverError> {
        let mut items = Vec::with_capacity(rhs_batch.len());
        for rhs in rhs_batch {
            items.push(self.solve_one(model_version, options, rhs, solve_options)?);
        }
        Ok(SolveBatchResult { items })
    }

    /// Invalidates all cache entries for the model version.
    #[must_use]
    pub fn invalidate(&self, model_version: Uuid) -> usize {
        self.cache.invalidate_model(model_version)
    }

    /// Builds factorization key from inputs.
    #[must_use]
    pub fn factorization_key(model_version: Uuid, options: NumericOptions) -> FactorizationKey {
        let options_bytes = options.print_level.to_le_bytes();
        FactorizationKey::new(model_version, SolverBackend::Umfpack, &options_bytes)
    }

    #[allow(dead_code)]
    fn _matrices_for_test(&self, data: &ModelSparseData) -> Result<BuiltMatrices, SolverError> {
        Ok(self.builder.build(data)?)
    }
}

#[cfg(test)]
mod tests {
    use approx::assert_relative_eq;
    use uuid::Uuid;

    use crate::{
        data_builder::{ModelSparseData, SparseTriplet},
        service::{NumericOptions, SolveOptions, SolverService},
    };

    #[test]
    fn prepare_and_solve_pipeline() {
        let model_version = Uuid::new_v4();
        let data = ModelSparseData {
            model_version,
            process_count: 2,
            flow_count: 1,
            impact_count: 1,
            technosphere_entries: vec![
                SparseTriplet {
                    row: 0,
                    col: 1,
                    value: 0.5,
                },
                SparseTriplet {
                    row: 1,
                    col: 0,
                    value: 0.25,
                },
            ],
            biosphere_entries: vec![
                SparseTriplet {
                    row: 0,
                    col: 0,
                    value: 2.0,
                },
                SparseTriplet {
                    row: 0,
                    col: 1,
                    value: 3.0,
                },
            ],
            characterization_factors: vec![SparseTriplet {
                row: 0,
                col: 0,
                value: 0.1,
            }],
        };

        let service = SolverService::new();
        let _prepare = service
            .prepare(&data, NumericOptions::default())
            .expect("prepare factorization");

        let solved = service
            .solve_one(
                model_version,
                NumericOptions::default(),
                &[1.0, 2.0],
                SolveOptions::default(),
            )
            .expect("solve");

        let x = solved.x.expect("x");
        let g = solved.g.expect("g");
        let h = solved.h.expect("h");

        assert_relative_eq!(x[0], 2.285_714_285_7, epsilon = 1e-9);
        assert_relative_eq!(x[1], 2.571_428_571_4, epsilon = 1e-9);
        assert_relative_eq!(g[0], 12.285_714_285_7, epsilon = 1e-9);
        assert_relative_eq!(h[0], 1.228_571_428_57, epsilon = 1e-9);
    }

    #[test]
    fn invalidate_marks_entries_stale() {
        let model_version = Uuid::new_v4();
        let data = ModelSparseData {
            model_version,
            process_count: 1,
            flow_count: 1,
            impact_count: 1,
            technosphere_entries: vec![],
            biosphere_entries: vec![SparseTriplet {
                row: 0,
                col: 0,
                value: 1.0,
            }],
            characterization_factors: vec![SparseTriplet {
                row: 0,
                col: 0,
                value: 1.0,
            }],
        };

        let service = SolverService::new();
        service
            .prepare(&data, NumericOptions::default())
            .expect("prepare factorization");

        let count = service.invalidate(model_version);
        assert_eq!(count, 1);
    }

    #[test]
    fn solve_options_skip_unrequested_vectors() {
        let model_version = Uuid::new_v4();
        let data = ModelSparseData {
            model_version,
            process_count: 2,
            flow_count: 1,
            impact_count: 1,
            technosphere_entries: vec![
                SparseTriplet {
                    row: 0,
                    col: 1,
                    value: 0.5,
                },
                SparseTriplet {
                    row: 1,
                    col: 0,
                    value: 0.25,
                },
            ],
            biosphere_entries: vec![
                SparseTriplet {
                    row: 0,
                    col: 0,
                    value: 2.0,
                },
                SparseTriplet {
                    row: 0,
                    col: 1,
                    value: 3.0,
                },
            ],
            characterization_factors: vec![SparseTriplet {
                row: 0,
                col: 0,
                value: 0.1,
            }],
        };

        let service = SolverService::new();
        service
            .prepare(&data, NumericOptions::default())
            .expect("prepare factorization");

        let solved = service
            .solve_one(
                model_version,
                NumericOptions::default(),
                &[1.0, 2.0],
                SolveOptions {
                    return_x: false,
                    return_g: false,
                    return_h: true,
                },
            )
            .expect("solve");

        assert!(solved.x.is_none());
        assert!(solved.g.is_none());
        let h = solved.h.expect("h");
        assert_relative_eq!(h[0], 1.228_571_428_57, epsilon = 1e-9);
    }
}
