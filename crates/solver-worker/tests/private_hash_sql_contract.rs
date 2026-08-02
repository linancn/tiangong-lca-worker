use std::fs;
use std::path::{Path, PathBuf};

const HASH_FUNCTION: &str = "lcia_scope_closure_sha256";
const PRIVATE_HASH_FUNCTION: &str = concat!("private.", "lcia_scope_closure_sha256");
const LEGACY_PUBLIC_HASH_FUNCTION: &str = concat!("public.", "lcia_scope_closure_sha256");
const ESTABLISHED_PRIVATE_CALL_LOWER_BOUND: usize = 8;

#[derive(Debug, Default)]
struct SqlSurfaceReport {
    scanned_files: usize,
    helper_mentions: usize,
    private_calls: usize,
    public_hits: Vec<String>,
}

fn collect_rust_files(directory: &Path, files: &mut Vec<PathBuf>) {
    let mut entries = fs::read_dir(directory)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", directory.display()))
        .map(|entry| entry.expect("failed to read Rust SQL surface entry").path())
        .collect::<Vec<_>>();
    entries.sort();

    for path in entries {
        if path.is_dir() {
            collect_rust_files(&path, files);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path);
        }
    }
}

fn scan_rust_sql_surfaces(roots: &[PathBuf], excluded_contract: &Path) -> SqlSurfaceReport {
    let mut files = Vec::new();
    for root in roots {
        collect_rust_files(root, &mut files);
    }
    files.sort();

    let mut report = SqlSurfaceReport::default();
    for path in files {
        if path == excluded_contract {
            continue;
        }

        let source = fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
        report.scanned_files += 1;
        report.helper_mentions += source.matches(HASH_FUNCTION).count();
        report.private_calls += source.matches(PRIVATE_HASH_FUNCTION).count();
        for (line_index, line) in source.lines().enumerate() {
            for _ in line.match_indices(LEGACY_PUBLIC_HASH_FUNCTION) {
                report
                    .public_hits
                    .push(format!("{}:{}", path.display(), line_index + 1));
            }
        }
    }
    report
}

#[test]
fn all_worker_rust_sql_surfaces_exclude_the_public_hash_wrapper() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let excluded_contract = manifest_dir.join("tests/private_hash_sql_contract.rs");
    let report = scan_rust_sql_surfaces(
        &[manifest_dir.join("src"), manifest_dir.join("tests")],
        &excluded_contract,
    );

    assert!(
        report.public_hits.is_empty(),
        "public hash-wrapper calls remain in governed Rust SQL surfaces: {report:?}"
    );
    assert!(
        report.private_calls >= ESTABLISHED_PRIVATE_CALL_LOWER_BOUND,
        "recursive scan did not retain the established private call surface: {report:?}"
    );
    assert_eq!(
        report.helper_mentions, report.private_calls,
        "every hash-helper mention must be explicitly qualified through private: {report:?}"
    );
}

#[test]
fn recursive_scan_rejects_public_unqualified_and_dynamic_schema_calls_in_new_files() {
    let fixture = tempfile::tempdir().expect("create hostile SQL surface fixture");
    let nested_source = fixture.path().join("src/new_family/nested_consumer.rs");
    let real_db_test = fixture.path().join("tests/new_real_db_contract.rs");
    let unqualified_source = fixture.path().join("src/new_family/unqualified.rs");
    let dynamic_public_source = fixture.path().join("src/new_family/dynamic_public.rs");
    let private_baseline = fixture.path().join("src/existing_private_calls.rs");
    fs::create_dir_all(nested_source.parent().expect("source parent")).expect("create source tree");
    fs::create_dir_all(real_db_test.parent().expect("test parent")).expect("create test tree");
    let public_sql = format!("SELECT {LEGACY_PUBLIC_HASH_FUNCTION}($1::jsonb)");
    let unqualified_sql = format!("SELECT {HASH_FUNCTION}($1::jsonb)");
    let dynamic_public_sql =
        format!("let sql = format!(\"SELECT {{}}{HASH_FUNCTION}($1::jsonb)\", \"public.\");");
    let private_sql = format!("SELECT {PRIVATE_HASH_FUNCTION}($1::jsonb);\n").repeat(8);
    fs::write(&nested_source, &public_sql).expect("write hostile source");
    fs::write(&real_db_test, &public_sql).expect("write hostile real-DB test");
    fs::write(&unqualified_source, &unqualified_sql).expect("write unqualified source");
    fs::write(&dynamic_public_source, &dynamic_public_sql).expect("write dynamic public source");
    fs::write(&private_baseline, &private_sql).expect("write private baseline source");

    let report = scan_rust_sql_surfaces(
        &[fixture.path().join("src"), fixture.path().join("tests")],
        Path::new("/contract-is-not-inside-hostile-fixture.rs"),
    );

    assert_eq!(report.scanned_files, 5, "hostile fixture discovery drifted");
    assert_eq!(
        report.public_hits.len(),
        2,
        "every newly introduced public call must be rejected: {report:?}"
    );
    assert_eq!(
        report.private_calls, ESTABLISHED_PRIVATE_CALL_LOWER_BOUND,
        "hostile fixture must satisfy the private lower bound"
    );
    assert!(
        report.helper_mentions > report.private_calls,
        "unqualified and dynamically schema-qualified helper mentions must fail the explicit-private equality: {report:?}"
    );
}
