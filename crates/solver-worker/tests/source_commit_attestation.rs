use std::process::Command;

use solver_worker::build_metadata::{
    DOCUMENT_VALIDATION_DATABASE_CONTRACT_ACCEPTED_MESSAGE, SOURCE_COMMIT,
};

#[test]
fn compiled_source_commit_matches_exact_checkout_head() {
    let status = Command::new("git")
        .args([
            "status",
            "--porcelain=v1",
            "--untracked-files=all",
            "--ignore-submodules=none",
        ])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("git must be available for source cleanliness qualification");
    assert!(
        status.status.success(),
        "git could not inspect checkout status"
    );
    assert!(
        status.stdout.is_empty(),
        "source checkout must be clean when qualifying compiled source commit"
    );

    let output = Command::new("git")
        .args(["rev-parse", "--verify", "HEAD^{commit}"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("git must be available for source commit qualification");
    assert!(
        output.status.success(),
        "git could not resolve checkout HEAD"
    );
    let checkout_commit = String::from_utf8(output.stdout)
        .expect("git commit must be UTF-8")
        .strip_suffix('\n')
        .expect("git commit must be LF terminated")
        .to_owned();

    assert_eq!(SOURCE_COMMIT, checkout_commit);
    assert_eq!(checkout_commit.len(), 40);
    assert_eq!(
        DOCUMENT_VALIDATION_DATABASE_CONTRACT_ACCEPTED_MESSAGE,
        format!(
            "document-validation database contract accepted; worker_source_commit={checkout_commit}; identity and target omitted"
        )
    );
}
