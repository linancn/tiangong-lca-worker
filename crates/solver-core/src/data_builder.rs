use serde::{Deserialize, Serialize};
use suitesparse_ffi::{CscMatrix, MatrixError, MatrixTriplet};
use thiserror::Error;
use uuid::Uuid;

/// Sparse entry from Supabase tables.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct SparseTriplet {
    /// Row index.
    pub row: i32,
    /// Column index.
    pub col: i32,
    /// Value.
    pub value: f64,
}

/// Sparse input for one model version.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelSparseData {
    /// Model version id.
    pub model_version: Uuid,
    /// Number of process nodes.
    pub process_count: i32,
    /// Number of biosphere flow rows.
    pub flow_count: i32,
    /// Number of impact category rows.
    pub impact_count: i32,
    /// A matrix entries.
    pub technosphere_entries: Vec<SparseTriplet>,
    /// B matrix entries.
    pub biosphere_entries: Vec<SparseTriplet>,
    /// C matrix entries.
    pub characterization_factors: Vec<SparseTriplet>,
}

/// Built matrices used for solve.
#[derive(Debug, Clone, PartialEq)]
pub struct BuiltMatrices {
    /// M = I - A matrix.
    pub m: CscMatrix,
    /// Biosphere matrix B.
    pub b: CscMatrix,
    /// Characterization matrix C.
    pub c: CscMatrix,
    /// M non-zeros.
    pub m_nnz: usize,
    /// B non-zeros.
    pub b_nnz: usize,
    /// C non-zeros.
    pub c_nnz: usize,
}

/// Errors from sparse matrix build stage.
#[derive(Debug, Error)]
pub enum DataBuilderError {
    #[error(
        "invalid dimensions: process={process_count}, flow={flow_count}, impact={impact_count}"
    )]
    InvalidDimensions {
        process_count: i32,
        flow_count: i32,
        impact_count: i32,
    },
    #[error("matrix error: {0}")]
    Matrix(#[from] MatrixError),
}

/// Builds matrix bundle from Supabase sparse entries.
#[derive(Debug)]
pub struct DataBuilder {
    zero_epsilon: f64,
}

impl Default for DataBuilder {
    fn default() -> Self {
        Self::new(1e-15)
    }
}

impl DataBuilder {
    /// Creates a builder with configurable zero threshold.
    #[must_use]
    pub fn new(zero_epsilon: f64) -> Self {
        Self { zero_epsilon }
    }

    /// Builds CSC matrices `M=I-A`, `B`, and `C`.
    pub fn build(&self, data: &ModelSparseData) -> Result<BuiltMatrices, DataBuilderError> {
        if data.process_count <= 0 || data.flow_count <= 0 || data.impact_count <= 0 {
            return Err(DataBuilderError::InvalidDimensions {
                process_count: data.process_count,
                flow_count: data.flow_count,
                impact_count: data.impact_count,
            });
        }

        let m = CscMatrix::from_triplet_iter(
            data.process_count,
            data.process_count,
            data.technosphere_entries
                .iter()
                .map(|entry| MatrixTriplet {
                    row: entry.row,
                    col: entry.col,
                    value: -entry.value,
                })
                .chain((0..data.process_count).map(|index| MatrixTriplet {
                    row: index,
                    col: index,
                    value: 1.0,
                })),
            self.zero_epsilon,
        )?;
        let b = CscMatrix::from_triplet_iter(
            data.flow_count,
            data.process_count,
            data.biosphere_entries
                .iter()
                .copied()
                .map(Self::to_matrix_triplet),
            self.zero_epsilon,
        )?;
        let c = CscMatrix::from_triplet_iter(
            data.impact_count,
            data.flow_count,
            data.characterization_factors
                .iter()
                .copied()
                .map(Self::to_matrix_triplet),
            self.zero_epsilon,
        )?;

        Ok(BuiltMatrices {
            m_nnz: m.nnz(),
            b_nnz: b.nnz(),
            c_nnz: c.nnz(),
            m,
            b,
            c,
        })
    }

    fn to_matrix_triplet(t: SparseTriplet) -> MatrixTriplet {
        MatrixTriplet {
            row: t.row,
            col: t.col,
            value: t.value,
        }
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::{DataBuilder, ModelSparseData, SparseTriplet};

    #[test]
    fn builds_i_minus_a_with_expected_signs() {
        let data = ModelSparseData {
            model_version: Uuid::new_v4(),
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
            biosphere_entries: vec![],
            characterization_factors: vec![],
        };

        let built = DataBuilder::default().build(&data).expect("build matrices");

        // M = [[1, -0.5],[-0.25, 1]] in CSC order
        assert_eq!(built.m.col_ptr, vec![0, 2, 4]);
        assert_eq!(built.m.row_idx, vec![0, 1, 0, 1]);
        assert_eq!(built.m.values, vec![1.0, -0.25, -0.5, 1.0]);
    }
}
