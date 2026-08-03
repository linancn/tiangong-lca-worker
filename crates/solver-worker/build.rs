use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};

const SOURCE_COMMIT_ENV: &str = "SOLVER_WORKER_SOURCE_COMMIT";

fn main() {
    if let Err(error) = emit_source_commit() {
        panic!("cannot attest solver-worker source commit: {error}");
    }
}

fn emit_source_commit() -> Result<(), String> {
    let manifest_dir = PathBuf::from(
        env::var_os("CARGO_MANIFEST_DIR")
            .ok_or_else(|| "CARGO_MANIFEST_DIR is unavailable".to_owned())?,
    );
    let repository_root = git_path(&manifest_dir, &["rev-parse", "--show-toplevel"])?;
    let git_dir = git_path(&manifest_dir, &["rev-parse", "--absolute-git-dir"])?;
    let common_dir = git_path(&manifest_dir, &["rev-parse", "--git-common-dir"])?;
    let common_dir = absolute_from(&manifest_dir, &common_dir);

    require_clean_worktree(&repository_root)?;

    let checkout_dot_git = repository_root.join(".git");
    if checkout_dot_git.is_file() {
        track(checkout_dot_git);
    }
    let head_path = git_dir.join("HEAD");
    track(&head_path);
    let common_dir_pointer = git_dir.join("commondir");
    if common_dir_pointer.is_file() {
        track(common_dir_pointer);
    }
    track(common_dir.join("packed-refs"));

    let head = read_one_line(&head_path)?;
    if let Some(reference) = head.strip_prefix("ref: ") {
        validate_reference(reference)?;
        let reference_path = git_path(
            &manifest_dir,
            &[
                "rev-parse",
                "--path-format=absolute",
                "--git-path",
                reference,
            ],
        )?;
        track_reference(&reference_path, &common_dir)?;
    } else {
        validate_commit(&head)?;
    }

    let commit = git_output(&manifest_dir, &["rev-parse", "--verify", "HEAD^{commit}"])?;
    validate_commit(&commit)?;
    println!("cargo:rustc-env={SOURCE_COMMIT_ENV}={commit}");
    Ok(())
}

fn git_path(current_dir: &Path, arguments: &[&str]) -> Result<PathBuf, String> {
    Ok(PathBuf::from(git_output(current_dir, arguments)?))
}

fn git_output(current_dir: &Path, arguments: &[&str]) -> Result<String, String> {
    let output = Command::new("git")
        .args(arguments)
        .current_dir(current_dir)
        .output()
        .map_err(|error| format!("failed to execute git: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "git {} failed with status {}",
            arguments.join(" "),
            output.status
        ));
    }
    let value = String::from_utf8(output.stdout)
        .map_err(|_| format!("git {} returned non-UTF-8 output", arguments.join(" ")))?;
    one_line(&value).map(str::to_owned)
}

fn require_clean_worktree(repository_root: &Path) -> Result<(), String> {
    let output = Command::new("git")
        .args([
            "status",
            "--porcelain=v1",
            "--untracked-files=all",
            "--ignore-submodules=none",
        ])
        .current_dir(repository_root)
        .output()
        .map_err(|error| format!("failed to inspect source worktree: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "git status failed with status {} while attesting source worktree",
            output.status
        ));
    }
    if !output.stdout.is_empty() {
        return Err(
            "source worktree must be completely clean, including untracked files, before building"
                .to_owned(),
        );
    }
    Ok(())
}

fn read_one_line(path: &Path) -> Result<String, String> {
    let value = fs::read_to_string(path)
        .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
    one_line(&value).map(str::to_owned)
}

fn one_line(value: &str) -> Result<&str, String> {
    let value = value.strip_suffix('\n').unwrap_or(value);
    if value.is_empty() || value.contains(['\n', '\r']) {
        return Err("expected exactly one non-empty LF-terminated or unterminated line".to_owned());
    }
    Ok(value)
}

fn validate_commit(commit: &str) -> Result<(), String> {
    if commit.len() != 40
        || !commit
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err("HEAD must resolve to exactly 40 lowercase hexadecimal characters".to_owned());
    }
    Ok(())
}

fn validate_reference(reference: &str) -> Result<(), String> {
    if !reference.starts_with("refs/")
        || reference.ends_with('/')
        || reference.contains("..")
        || reference.contains("//")
        || reference.contains('\\')
        || reference.chars().any(char::is_control)
    {
        return Err("HEAD contains an invalid symbolic reference".to_owned());
    }
    Ok(())
}

fn absolute_from(current_dir: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_owned()
    } else {
        current_dir.join(path)
    }
}

fn track(path: impl AsRef<Path>) {
    println!("cargo:rerun-if-changed={}", path.as_ref().display());
}

fn track_reference(reference_path: &Path, common_dir: &Path) -> Result<(), String> {
    if reference_path.is_file() {
        track(reference_path);
        return Ok(());
    }

    let mut ancestor = reference_path.parent();
    while let Some(path) = ancestor {
        if !path.starts_with(common_dir) {
            break;
        }
        if path.is_dir() {
            track(path);
            return Ok(());
        }
        ancestor = path.parent();
    }
    Err("symbolic HEAD does not resolve inside the Git common directory".to_owned())
}
