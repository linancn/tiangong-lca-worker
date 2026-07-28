use std::collections::HashMap;

use thiserror::Error;

/// Sparse triplet entry in COO format.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MatrixTriplet {
    /// Row index.
    pub row: i32,
    /// Column index.
    pub col: i32,
    /// Numeric value.
    pub value: f64,
}

/// Compressed sparse column matrix using 32-bit indices and f64 values.
#[derive(Debug, Clone, PartialEq)]
pub struct CscMatrix {
    /// Number of rows.
    pub nrows: i32,
    /// Number of columns.
    pub ncols: i32,
    /// Column pointers of length `ncols + 1`.
    pub col_ptr: Vec<i32>,
    /// Row indices of each non-zero entry.
    pub row_idx: Vec<i32>,
    /// Non-zero values, aligned with `row_idx`.
    pub values: Vec<f64>,
}

/// Errors for sparse matrix construction and validation.
#[derive(Debug, Error)]
pub enum MatrixError {
    #[error("invalid matrix dimensions ({nrows}, {ncols})")]
    InvalidDimensions { nrows: i32, ncols: i32 },
    #[error("col_ptr length {got} does not match ncols+1 ({expected})")]
    InvalidColPtrLength { got: usize, expected: usize },
    #[error("col_ptr must start at 0")]
    InvalidColPtrStart,
    #[error("col_ptr must be non-decreasing")]
    NonMonotonicColPtr,
    #[error("final col_ptr value {last} does not match nnz {nnz}")]
    InvalidNnz { last: i32, nnz: usize },
    #[error("row index {row} out of range [0, {nrows})")]
    RowOutOfRange { row: i32, nrows: i32 },
    #[error("column index {col} out of range [0, {ncols})")]
    ColOutOfRange { col: i32, ncols: i32 },
    #[error("row indices within each column must be strictly increasing")]
    NonSortedOrDuplicateRows,
    #[error("matrix conversion overflow")]
    IndexOverflow,
}

impl CscMatrix {
    /// Estimated bytes owned by the CSC vectors at their current capacities.
    ///
    /// This excludes the small inline `Vec` headers and the matrix struct itself;
    /// callers can use it as a deterministic lower-level accounting primitive.
    #[must_use]
    pub fn estimated_owned_bytes(&self) -> usize {
        self.col_ptr
            .capacity()
            .saturating_mul(std::mem::size_of::<i32>())
            .saturating_add(
                self.row_idx
                    .capacity()
                    .saturating_mul(std::mem::size_of::<i32>()),
            )
            .saturating_add(
                self.values
                    .capacity()
                    .saturating_mul(std::mem::size_of::<f64>()),
            )
    }

    /// Creates a validated CSC matrix.
    pub fn new(
        nrows: i32,
        ncols: i32,
        col_ptr: Vec<i32>,
        row_idx: Vec<i32>,
        values: Vec<f64>,
    ) -> Result<Self, MatrixError> {
        let matrix = Self {
            nrows,
            ncols,
            col_ptr,
            row_idx,
            values,
        };
        matrix.validate()?;
        Ok(matrix)
    }

    /// Builds a validated CSC matrix from COO triplets.
    pub fn from_triplets(
        nrows: i32,
        ncols: i32,
        triplets: &[MatrixTriplet],
        zero_epsilon: f64,
    ) -> Result<Self, MatrixError> {
        Self::from_triplet_iter(nrows, ncols, triplets.iter().copied(), zero_epsilon)
    }

    /// Builds a validated CSC matrix from an owned triplet iterator.
    ///
    /// This permits callers to transform source entries directly into the
    /// aggregation pass without retaining an intermediate triplet vector.
    pub fn from_triplet_iter(
        nrows: i32,
        ncols: i32,
        triplets: impl IntoIterator<Item = MatrixTriplet>,
        zero_epsilon: f64,
    ) -> Result<Self, MatrixError> {
        if nrows <= 0 || ncols <= 0 {
            return Err(MatrixError::InvalidDimensions { nrows, ncols });
        }

        let mut acc: HashMap<(i32, i32), f64> = HashMap::new();
        for t in triplets {
            if !(0..nrows).contains(&t.row) {
                return Err(MatrixError::RowOutOfRange { row: t.row, nrows });
            }
            if !(0..ncols).contains(&t.col) {
                return Err(MatrixError::ColOutOfRange { col: t.col, ncols });
            }
            if t.value.abs() <= zero_epsilon {
                continue;
            }
            *acc.entry((t.row, t.col)).or_insert(0.0) += t.value;
        }

        let mut entries: Vec<(i32, i32, f64)> = acc
            .into_iter()
            .filter_map(|((row, col), value)| {
                (value.abs() > zero_epsilon).then_some((row, col, value))
            })
            .collect();
        entries.sort_by(|(row_a, col_a, _), (row_b, col_b, _)| {
            col_a.cmp(col_b).then_with(|| row_a.cmp(row_b))
        });

        let mut col_ptr =
            vec![0_i32; usize::try_from(ncols).map_err(|_| MatrixError::IndexOverflow)? + 1];
        for (_, col, _) in &entries {
            let col_usize = usize::try_from(*col).map_err(|_| MatrixError::IndexOverflow)?;
            col_ptr[col_usize + 1] += 1;
        }

        for col in 0..usize::try_from(ncols).map_err(|_| MatrixError::IndexOverflow)? {
            col_ptr[col + 1] += col_ptr[col];
        }

        let mut row_idx = Vec::with_capacity(entries.len());
        let mut values = Vec::with_capacity(entries.len());
        for (row, _, value) in entries {
            row_idx.push(row);
            values.push(value);
        }

        Self::new(nrows, ncols, col_ptr, row_idx, values)
    }

    /// Validates CSC structure and matrix index bounds.
    pub fn validate(&self) -> Result<(), MatrixError> {
        if self.nrows <= 0 || self.ncols <= 0 {
            return Err(MatrixError::InvalidDimensions {
                nrows: self.nrows,
                ncols: self.ncols,
            });
        }

        let expected_col_len =
            usize::try_from(self.ncols).map_err(|_| MatrixError::IndexOverflow)? + 1;
        if self.col_ptr.len() != expected_col_len {
            return Err(MatrixError::InvalidColPtrLength {
                got: self.col_ptr.len(),
                expected: expected_col_len,
            });
        }

        if self.col_ptr.first().copied() != Some(0) {
            return Err(MatrixError::InvalidColPtrStart);
        }

        if self
            .col_ptr
            .windows(2)
            .any(|window| matches!(window, [a, b] if b < a))
        {
            return Err(MatrixError::NonMonotonicColPtr);
        }

        let nnz = self.values.len();
        if self.row_idx.len() != nnz {
            return Err(MatrixError::InvalidNnz {
                last: i32::try_from(self.row_idx.len()).map_err(|_| MatrixError::IndexOverflow)?,
                nnz,
            });
        }

        let last = *self.col_ptr.last().unwrap_or(&-1);
        if usize::try_from(last).ok() != Some(nnz) {
            return Err(MatrixError::InvalidNnz { last, nnz });
        }

        let nrows = self.nrows;
        for &row in &self.row_idx {
            if !(0..nrows).contains(&row) {
                return Err(MatrixError::RowOutOfRange { row, nrows });
            }
        }

        for col in 0..usize::try_from(self.ncols).map_err(|_| MatrixError::IndexOverflow)? {
            let start =
                usize::try_from(self.col_ptr[col]).map_err(|_| MatrixError::IndexOverflow)?;
            let end =
                usize::try_from(self.col_ptr[col + 1]).map_err(|_| MatrixError::IndexOverflow)?;
            if self.row_idx[start..end]
                .windows(2)
                .any(|window| matches!(window, [a, b] if b <= a))
            {
                return Err(MatrixError::NonSortedOrDuplicateRows);
            }
        }

        Ok(())
    }

    /// Matrix dimensions.
    #[must_use]
    pub fn shape(&self) -> (i32, i32) {
        (self.nrows, self.ncols)
    }

    /// Number of non-zero entries.
    #[must_use]
    pub fn nnz(&self) -> usize {
        self.values.len()
    }

    /// Sparse matrix-vector multiplication.
    #[must_use]
    pub fn mul_vector(&self, x: &[f64]) -> Vec<f64> {
        let mut out = vec![0.0; usize::try_from(self.nrows).unwrap_or_default()];
        if x.len() != usize::try_from(self.ncols).unwrap_or_default() {
            return out;
        }

        for (col, xj) in x
            .iter()
            .copied()
            .enumerate()
            .take(usize::try_from(self.ncols).unwrap_or_default())
        {
            let start = usize::try_from(self.col_ptr[col]).unwrap_or_default();
            let end = usize::try_from(self.col_ptr[col + 1]).unwrap_or_default();
            for idx in start..end {
                let row = usize::try_from(self.row_idx[idx]).unwrap_or_default();
                out[row] += self.values[idx] * xj;
            }
        }

        out
    }
}

#[cfg(test)]
mod tests {
    use super::{CscMatrix, MatrixError, MatrixTriplet};

    #[test]
    fn merges_duplicates_from_triplets() {
        let matrix = CscMatrix::from_triplets(
            2,
            2,
            &[
                MatrixTriplet {
                    row: 0,
                    col: 0,
                    value: 1.5,
                },
                MatrixTriplet {
                    row: 0,
                    col: 0,
                    value: 2.5,
                },
                MatrixTriplet {
                    row: 1,
                    col: 1,
                    value: 3.0,
                },
            ],
            1e-12,
        )
        .expect("matrix");

        assert_eq!(matrix.col_ptr, vec![0, 1, 2]);
        assert_eq!(matrix.row_idx, vec![0, 1]);
        assert_eq!(matrix.values, vec![4.0, 3.0]);
    }

    #[test]
    fn rejects_duplicate_rows_in_column() {
        let err =
            CscMatrix::new(2, 1, vec![0, 2], vec![0, 0], vec![1.0, 2.0]).expect_err("should fail");
        assert!(matches!(err, MatrixError::NonSortedOrDuplicateRows));
    }
}
