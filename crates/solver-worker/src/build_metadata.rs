/// Exact 40-character Git commit resolved from this crate's checkout at build time.
pub const SOURCE_COMMIT: &str = env!("SOLVER_WORKER_SOURCE_COMMIT");

/// Stable journal marker proving which exact Worker source commit accepted the database contract.
pub const DOCUMENT_VALIDATION_DATABASE_CONTRACT_ACCEPTED_MESSAGE: &str = concat!(
    "document-validation database contract accepted; worker_source_commit=",
    env!("SOLVER_WORKER_SOURCE_COMMIT"),
    "; identity and target omitted"
);

#[cfg(test)]
mod tests {
    use super::{DOCUMENT_VALIDATION_DATABASE_CONTRACT_ACCEPTED_MESSAGE, SOURCE_COMMIT};

    #[test]
    fn source_commit_and_journal_marker_are_exact_and_secret_free() {
        assert_eq!(SOURCE_COMMIT.len(), 40);
        assert!(
            SOURCE_COMMIT
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        );
        assert_eq!(
            DOCUMENT_VALIDATION_DATABASE_CONTRACT_ACCEPTED_MESSAGE,
            format!(
                "document-validation database contract accepted; worker_source_commit={SOURCE_COMMIT}; identity and target omitted"
            )
        );
        for forbidden in [
            "postgres://",
            "postgresql://",
            "DOCUMENT_VALIDATION_DATABASE_URL",
        ] {
            assert!(!DOCUMENT_VALIDATION_DATABASE_CONTRACT_ACCEPTED_MESSAGE.contains(forbidden));
        }
    }
}
