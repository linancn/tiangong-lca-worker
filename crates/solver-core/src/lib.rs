//! Core LCA sparse solve pipeline with matrix build, validation, factorization cache, and solve APIs.

mod cache;
mod data_builder;
mod service;
mod validator;

pub use cache::{
    FactorizationCache, FactorizationCacheAdmissionError, FactorizationCacheTelemetry,
    FactorizationKey, FactorizationState, SolverBackend,
};
pub use data_builder::{DataBuilder, ModelSparseData, SparseTriplet};
pub use service::{
    FactorizationDiagnostics, NumericOptions, PrepareResult, SolveBatchResult,
    SolveComputationTiming, SolveOptions, SolveResult, SolverError, SolverService,
    TimedSolveResult,
};
pub use validator::{MatrixStats, ValidationReport, ValidationStatus};
