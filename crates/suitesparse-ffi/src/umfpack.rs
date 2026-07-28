use std::{
    ffi::c_void,
    ptr::{self, NonNull},
};

use thiserror::Error;

use crate::matrix::{CscMatrix, MatrixError};

const UMFPACK_INFO_LEN: usize = 90;
const UMFPACK_CONTROL_LEN: usize = 20;
const UMFPACK_OK: i32 = 0;
const UMFPACK_WARNING_SINGULAR: i32 = 1;
const UMFPACK_A: i32 = 0;
const UMFPACK_PRL: usize = 0;
const UMFPACK_SIZE_OF_UNIT: usize = 3;
const UMFPACK_SYMBOLIC_SIZE: usize = 14;
const UMFPACK_NUMERIC_SIZE_ESTIMATE: usize = 20;
const UMFPACK_PEAK_MEMORY_ESTIMATE: usize = 21;
const UMFPACK_NUMERIC_SIZE: usize = 40;
const UMFPACK_PEAK_MEMORY: usize = 41;

#[link(name = "umfpack")]
unsafe extern "C" {
    fn umfpack_di_defaults(control: *mut f64);
    fn umfpack_di_symbolic(
        n_row: i32,
        n_col: i32,
        ap: *const i32,
        ai: *const i32,
        ax: *const f64,
        symbolic: *mut *mut c_void,
        control: *const f64,
        info: *mut f64,
    ) -> i32;
    fn umfpack_di_numeric(
        ap: *const i32,
        ai: *const i32,
        ax: *const f64,
        symbolic: *mut c_void,
        numeric: *mut *mut c_void,
        control: *const f64,
        info: *mut f64,
    ) -> i32;
    fn umfpack_di_solve(
        sys: i32,
        ap: *const i32,
        ai: *const i32,
        ax: *const f64,
        x: *mut f64,
        b: *const f64,
        numeric: *mut c_void,
        control: *const f64,
        info: *mut f64,
    ) -> i32;
    fn umfpack_di_free_symbolic(symbolic: *mut *mut c_void);
    fn umfpack_di_free_numeric(numeric: *mut *mut c_void);
}

/// UMFPACK numeric options.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct UmfpackNumericOptions {
    /// UMFPACK print level. `0.0` is silent.
    pub print_level: f64,
}

impl Default for UmfpackNumericOptions {
    fn default() -> Self {
        Self { print_level: 0.0 }
    }
}

/// Snapshot of UMFPACK Info arrays from symbolic/numeric stages.
#[derive(Debug, Clone, PartialEq)]
pub struct FactorizationStats {
    /// Status code from symbolic stage.
    pub symbolic_status: i32,
    /// Status code from numeric stage.
    pub numeric_status: i32,
    /// Raw symbolic info array.
    pub symbolic_info: [f64; UMFPACK_INFO_LEN],
    /// Raw numeric info array.
    pub numeric_info: [f64; UMFPACK_INFO_LEN],
}

/// Errors from UMFPACK wrapper.
#[derive(Debug, Error)]
pub enum UmfpackError {
    #[error("invalid matrix: {0}")]
    Matrix(#[from] MatrixError),
    #[error("RHS length {rhs_len} does not match matrix columns {ncols}")]
    RhsDimensionMismatch { rhs_len: usize, ncols: usize },
    #[error("matrix is singular")]
    SingularMatrix,
    #[error("umfpack symbolic factorization returned status {status}")]
    SymbolicStatus { status: i32 },
    #[error("umfpack numeric factorization returned status {status}")]
    NumericStatus { status: i32 },
    #[error("umfpack solve returned status {status}")]
    SolveStatus { status: i32 },
    #[error("umfpack returned null pointer")]
    NullHandle,
}

/// Prepared UMFPACK factorization for repeated sparse solves.
#[derive(Debug)]
pub struct UmfpackFactorization {
    matrix: CscMatrix,
    symbolic: NonNull<c_void>,
    numeric: NonNull<c_void>,
    control: [f64; UMFPACK_CONTROL_LEN],
    /// Factorization diagnostics.
    pub stats: FactorizationStats,
}

// SAFETY: The UMFPACK numeric object is immutable after factorization in this wrapper.
// External synchronization is still required for concurrent solve calls.
unsafe impl Send for UmfpackFactorization {}
// SAFETY: Same reasoning as Send; no mutable aliasing is exposed without &mut self.
unsafe impl Sync for UmfpackFactorization {}

impl UmfpackFactorization {
    fn info_units_to_bytes(info: &[f64; UMFPACK_INFO_LEN], index: usize) -> Option<usize> {
        let units = info[index];
        let bytes_per_unit = info[UMFPACK_SIZE_OF_UNIT];
        if !units.is_finite() || units < 0.0 || !bytes_per_unit.is_finite() || bytes_per_unit <= 0.0
        {
            return None;
        }
        let bytes = (units.ceil() * bytes_per_unit.ceil()).ceil();
        format!("{bytes:.0}").parse::<usize>().ok()
    }

    /// Actual retained bytes reported by UMFPACK plus the owned factorized CSC matrix.
    ///
    /// UMFPACK reports the symbolic and numeric object sizes in implementation
    /// units together with the unit size. The returned estimate is therefore
    /// workload-derived and includes factorization fill-in observed for this
    /// matrix, rather than assuming a constant multiplier.
    #[must_use]
    pub fn estimated_owned_bytes(&self) -> usize {
        let symbolic = Self::info_units_to_bytes(&self.stats.symbolic_info, UMFPACK_SYMBOLIC_SIZE)
            .unwrap_or_default();
        let numeric = Self::info_units_to_bytes(&self.stats.numeric_info, UMFPACK_NUMERIC_SIZE)
            .unwrap_or_default();
        self.matrix
            .estimated_owned_bytes()
            .saturating_add(symbolic)
            .saturating_add(numeric)
    }

    /// Peak factorization bytes reported by UMFPACK for telemetry.
    #[must_use]
    pub fn reported_peak_bytes(&self) -> Option<usize> {
        Self::info_units_to_bytes(&self.stats.numeric_info, UMFPACK_PEAK_MEMORY).or_else(|| {
            Self::info_units_to_bytes(&self.stats.symbolic_info, UMFPACK_PEAK_MEMORY_ESTIMATE)
        })
    }

    /// Symbolic-stage numeric-object estimate reported before numeric factorization.
    #[must_use]
    pub fn reported_numeric_estimate_bytes(&self) -> Option<usize> {
        Self::info_units_to_bytes(&self.stats.symbolic_info, UMFPACK_NUMERIC_SIZE_ESTIMATE)
    }

    /// Performs symbolic and numeric factorization.
    pub fn factorize(
        matrix: CscMatrix,
        options: UmfpackNumericOptions,
    ) -> Result<Self, UmfpackError> {
        matrix.validate()?;
        if matrix.nrows != matrix.ncols {
            return Err(UmfpackError::Matrix(MatrixError::InvalidDimensions {
                nrows: matrix.nrows,
                ncols: matrix.ncols,
            }));
        }

        let mut control = [0.0_f64; UMFPACK_CONTROL_LEN];
        let mut symbolic_info = [-1.0_f64; UMFPACK_INFO_LEN];
        let mut numeric_info = [-1.0_f64; UMFPACK_INFO_LEN];

        // SAFETY: control has exactly UMFPACK_CONTROL_LEN elements expected by UMFPACK.
        unsafe {
            umfpack_di_defaults(control.as_mut_ptr());
        }
        control[UMFPACK_PRL] = options.print_level;

        let mut symbolic_ptr: *mut c_void = ptr::null_mut();
        // SAFETY: Matrix arrays are valid CSC arrays by construction/validation.
        let symbolic_status = unsafe {
            umfpack_di_symbolic(
                matrix.nrows,
                matrix.ncols,
                matrix.col_ptr.as_ptr(),
                matrix.row_idx.as_ptr(),
                matrix.values.as_ptr(),
                &raw mut symbolic_ptr,
                control.as_ptr(),
                symbolic_info.as_mut_ptr(),
            )
        };

        if symbolic_status == UMFPACK_WARNING_SINGULAR {
            return Err(UmfpackError::SingularMatrix);
        }
        if symbolic_status != UMFPACK_OK {
            return Err(UmfpackError::SymbolicStatus {
                status: symbolic_status,
            });
        }

        let symbolic = NonNull::new(symbolic_ptr).ok_or(UmfpackError::NullHandle)?;
        let mut numeric_ptr: *mut c_void = ptr::null_mut();

        // SAFETY: symbolic is produced by UMFPACK symbolic, and matrix is valid.
        let numeric_status = unsafe {
            umfpack_di_numeric(
                matrix.col_ptr.as_ptr(),
                matrix.row_idx.as_ptr(),
                matrix.values.as_ptr(),
                symbolic.as_ptr(),
                &raw mut numeric_ptr,
                control.as_ptr(),
                numeric_info.as_mut_ptr(),
            )
        };

        if numeric_status == UMFPACK_WARNING_SINGULAR {
            // SAFETY: symbolic came from UMFPACK and can be released now.
            unsafe {
                let mut s = symbolic.as_ptr();
                umfpack_di_free_symbolic(&raw mut s);
            }
            return Err(UmfpackError::SingularMatrix);
        }
        if numeric_status != UMFPACK_OK {
            // SAFETY: symbolic came from UMFPACK and can be released now.
            unsafe {
                let mut s = symbolic.as_ptr();
                umfpack_di_free_symbolic(&raw mut s);
            }
            return Err(UmfpackError::NumericStatus {
                status: numeric_status,
            });
        }

        let numeric = NonNull::new(numeric_ptr).ok_or(UmfpackError::NullHandle)?;

        Ok(Self {
            matrix,
            symbolic,
            numeric,
            control,
            stats: FactorizationStats {
                symbolic_status,
                numeric_status,
                symbolic_info,
                numeric_info,
            },
        })
    }

    /// Solves `Ax = b` for one RHS using cached factorization.
    pub fn solve(&self, rhs: &[f64]) -> Result<Vec<f64>, UmfpackError> {
        let n = usize::try_from(self.matrix.ncols).map_err(|_| UmfpackError::NullHandle)?;
        if rhs.len() != n {
            return Err(UmfpackError::RhsDimensionMismatch {
                rhs_len: rhs.len(),
                ncols: n,
            });
        }

        let mut x = vec![0.0_f64; n];
        let mut info = [-1.0_f64; UMFPACK_INFO_LEN];

        // SAFETY: numeric handle is valid for the lifetime of self, matrix/rhs buffers are valid.
        let status = unsafe {
            umfpack_di_solve(
                UMFPACK_A,
                self.matrix.col_ptr.as_ptr(),
                self.matrix.row_idx.as_ptr(),
                self.matrix.values.as_ptr(),
                x.as_mut_ptr(),
                rhs.as_ptr(),
                self.numeric.as_ptr(),
                self.control.as_ptr(),
                info.as_mut_ptr(),
            )
        };

        if status == UMFPACK_WARNING_SINGULAR {
            return Err(UmfpackError::SingularMatrix);
        }
        if status != UMFPACK_OK {
            return Err(UmfpackError::SolveStatus { status });
        }

        Ok(x)
    }

    /// Solves for multiple RHS vectors by repeated back-substitution.
    pub fn solve_many(&self, rhs_list: &[Vec<f64>]) -> Result<Vec<Vec<f64>>, UmfpackError> {
        rhs_list.iter().map(|rhs| self.solve(rhs)).collect()
    }

    /// Returns a reference to the factorized matrix.
    #[must_use]
    pub fn matrix(&self) -> &CscMatrix {
        &self.matrix
    }
}

impl Drop for UmfpackFactorization {
    fn drop(&mut self) {
        // SAFETY: handles were allocated by UMFPACK and are freed exactly once in Drop.
        unsafe {
            let mut s = self.symbolic.as_ptr();
            umfpack_di_free_symbolic(&raw mut s);
            let mut n = self.numeric.as_ptr();
            umfpack_di_free_numeric(&raw mut n);
        }
    }
}

#[cfg(test)]
mod tests {
    use approx::assert_relative_eq;

    use crate::{CscMatrix, UmfpackFactorization, UmfpackNumericOptions};

    #[test]
    fn solves_small_system() {
        let matrix = CscMatrix::new(
            2,
            2,
            vec![0, 2, 4],
            vec![0, 1, 0, 1],
            vec![4.0, 2.0, 1.0, 3.0],
        )
        .expect("valid matrix");
        let factor = UmfpackFactorization::factorize(matrix, UmfpackNumericOptions::default())
            .expect("factorization");
        let x = factor.solve(&[1.0, 1.0]).expect("solve");

        assert_relative_eq!(x[0], 0.2, epsilon = 1e-10);
        assert_relative_eq!(x[1], 0.2, epsilon = 1e-10);
    }
}
