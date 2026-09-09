//! Package-local partial import. No business-table reads occur during preparation.
//! The validator command and its effective v1 blocking rules remain unchanged.
use super::{
    AppState, BTreeMap, CompressionMethod, Deserialize, Duration, File,
    PACKAGE_ZIP_ARTIFACT_FORMAT, PackageArtifactInsert, PackageArtifactKind, PackageEntry,
    PackageManifest, PackageRootRef, PackageRootTable, Path, Read, Row, Serialize,
    SimpleFileOptions, TempDir, Uuid, VALIDATION_ISSUE_SAMPLE_LIMIT, Value, VecDeque, Write,
    ZipArchive, ZipWriter, backfill_process_model_ids, copy, dataset_root_key,
    extract_model_submodels, fetch_package_artifact, fs, insert_package_artifact, json,
    normalize_imported_entry, normalize_json_ordered_for_insert,
    normalize_manifest_dataset_payload, normalize_path_dataset_payload, normalize_version_string,
    parse_root_from_package_file_path, parse_table_name, parse_uuid_opt,
    prepare_package_zip_artifact_from_path, sqlx, table_key, table_name, tidas_cli,
};
use crate::{
    package_artifacts::PackageArtifactUploadMeta,
    resource::CancellationToken,
    scope_closure::{DatasetCategory, extract_references},
    storage::ObjectTransferOptions,
    worker_jobs::WorkerJob,
};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    io::{BufRead, BufReader, BufWriter},
    path::PathBuf,
};

const MAX_ZIP_BYTES: u64 = 512 * 1024 * 1024;
const MAX_EXTRACTED_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const MAX_DOCUMENT_BYTES: u64 = 16 * 1024 * 1024;
const MAX_DOCUMENTS: usize = 100_000;
const MAX_EDGES: usize = 1_000_000;
const MAX_GROUP_DOCUMENTS: usize = 50_000;
const MAX_GROUP_BYTES: u64 = 64 * 1024 * 1024;
const ROOT_SAMPLE_LIMIT: usize = 100;
const MAX_ROOTS: usize = 2_000;
const MAX_VALIDATION_DOCUMENT_VISITS: usize = 2_000_000;
const REPORT_FORMAT: &str = "tidas-package-import-report:v2";

#[derive(Debug, Clone, Serialize)]
struct Node {
    identity: PackageRootRef,
    source_paths: Vec<String>,
    entry_path: PathBuf,
    raw_path: PathBuf,
    content_sha256: String,
    blocked: bool,
    #[serde(skip)]
    edges: Vec<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GroupPlan {
    root: usize,
    members: Vec<usize>,
    blocked: bool,
    blocking_path: Vec<usize>,
}

struct Evidence {
    writer: BufWriter<File>,
    errors: usize,
    warnings: usize,
    count: usize,
    samples: Vec<Value>,
    bytes: u64,
    sample_bytes: usize,
    samples_closed: bool,
}

impl Evidence {
    fn new(path: &Path) -> anyhow::Result<Self> {
        Ok(Self {
            writer: BufWriter::new(File::create(path)?),
            errors: 0,
            warnings: 0,
            count: 0,
            samples: Vec::new(),
            bytes: 0,
            sample_bytes: 0,
            samples_closed: false,
        })
    }

    fn issue(&mut self, issue: &Value) -> anyhow::Result<()> {
        let encoded = serde_json::to_vec(issue)?;
        anyhow::ensure!(
            encoded.len() as u64 <= MAX_DOCUMENT_BYTES
                && self.bytes + (encoded.len() as u64) < MAX_ZIP_BYTES,
            "import_issue_evidence_capacity_exceeded"
        );
        self.bytes += encoded.len() as u64 + 1;
        self.count += 1;
        self.errors += usize::from(issue.get("severity").and_then(Value::as_str) == Some("error"));
        self.warnings +=
            usize::from(issue.get("severity").and_then(Value::as_str) == Some("warning"));
        if !self.samples_closed
            && self.samples.len() < VALIDATION_ISSUE_SAMPLE_LIMIT
            && self.sample_bytes + encoded.len() <= 8 * 1024 * 1024
        {
            self.sample_bytes += encoded.len();
            self.samples.push(issue.clone());
        } else {
            self.samples_closed = true;
        }
        self.writer.write_all(&encoded)?;
        self.writer.write_all(b"\n")?;
        Ok(())
    }

    fn blocker(
        &mut self,
        node: &mut Node,
        code: &str,
        location: &str,
        message: &str,
    ) -> anyhow::Result<()> {
        node.blocked = true;
        self.issue(&json!({ "issue_code": code, "severity": "error", "category": node.identity.table,
            "identity": node.identity, "file_path": node.source_paths[0], "location": location, "message": message,
            "stage": "package_closure" }))
    }
}

struct Prepared {
    temp: TempDir,
    nodes: Vec<Node>,
    plan_path: PathBuf,
    plan_sha256: String,
    root_count: usize,
    evidence: Evidence,
}

fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn identity_key(identity: &PackageRootRef) -> String {
    table_key(identity.table, identity.id, &identity.version)
}
fn write_line(writer: &mut impl Write, value: &impl Serialize) -> anyhow::Result<()> {
    serde_json::to_writer(&mut *writer, value)?;
    writer.write_all(b"\n")?;
    Ok(())
}
fn read_json(path: &Path) -> anyhow::Result<Value> {
    anyhow::ensure!(
        fs::metadata(path)?.len() <= MAX_DOCUMENT_BYTES,
        "import_document_capacity_exceeded"
    );
    Ok(serde_json::from_reader(File::open(path)?)?)
}

fn extract(zip_path: &Path, root: &Path, cancel: &CancellationToken) -> anyhow::Result<()> {
    let mut archive = ZipArchive::new(File::open(zip_path)?)?;
    anyhow::ensure!(
        archive.len() <= MAX_DOCUMENTS * 3,
        "import_archive_entry_capacity_exceeded"
    );
    let mut paths = BTreeSet::new();
    let mut total = 0_u64;
    for ordinal in 0..archive.len() {
        cancel.check("import_extract")?;
        let mut entry = archive.by_index(ordinal)?;
        let relative = entry
            .enclosed_name()
            .ok_or_else(|| anyhow::anyhow!("import_unsafe_zip_path"))?;
        anyhow::ensure!(paths.insert(relative.clone()), "import_duplicate_zip_path");
        anyhow::ensure!(
            entry
                .unix_mode()
                .is_none_or(|mode| mode & 0o170_000 != 0o120_000),
            "import_zip_symlink"
        );
        total = total
            .checked_add(entry.size())
            .ok_or_else(|| anyhow::anyhow!("import_archive_size_overflow"))?;
        anyhow::ensure!(
            total <= MAX_EXTRACTED_BYTES,
            "import_archive_capacity_exceeded"
        );
        let output = root.join(relative);
        if entry.is_dir() {
            fs::create_dir_all(output)?;
            continue;
        }
        anyhow::ensure!(
            entry.size() <= MAX_DOCUMENT_BYTES,
            "import_document_capacity_exceeded"
        );
        if let Some(parent) = output.parent() {
            fs::create_dir_all(parent)?;
        }
        let copied = copy(
            &mut (&mut entry).take(MAX_DOCUMENT_BYTES + 1),
            &mut File::create(output)?,
        )?;
        anyhow::ensure!(
            copied <= MAX_DOCUMENT_BYTES,
            "import_document_capacity_exceeded"
        );
    }
    Ok(())
}

fn files_in(root: &Path) -> anyhow::Result<Vec<String>> {
    let mut files = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        for entry in fs::read_dir(dir)? {
            let path = entry?.path();
            if path.is_dir() {
                pending.push(path);
            } else {
                files.push(
                    path.strip_prefix(root)?
                        .to_string_lossy()
                        .replace('\\', "/"),
                );
            }
            anyhow::ensure!(
                files.len() + pending.len() <= MAX_DOCUMENTS * 3,
                "import_archive_entry_capacity_exceeded"
            );
        }
    }
    files.sort();
    Ok(files)
}

#[allow(clippy::case_sensitive_file_extension_comparisons)] // Exact legacy ZIP contract.
fn index(root: &Path, entries_dir: &Path, evidence: &mut Evidence) -> anyhow::Result<Vec<Node>> {
    fs::create_dir_all(entries_dir)?;
    let manifest = fs::read(root.join("manifest.json"))
        .ok()
        .and_then(|bytes| serde_json::from_slice::<PackageManifest>(&bytes).ok());
    let mut metadata = BTreeMap::new();
    if let Some(manifest) = &manifest {
        for entry in &manifest.entries {
            anyhow::ensure!(
                Path::new(&entry.file_path)
                    .components()
                    .all(|component| matches!(component, std::path::Component::Normal(_)))
                    && !entry.file_path.is_empty(),
                "import_unsafe_manifest_path"
            );
            anyhow::ensure!(
                !metadata.contains_key(&entry.file_path),
                "import_ambiguous_manifest_path"
            );
            metadata.insert(entry.file_path.clone(), entry);
        }
    }
    let mut nodes: Vec<Node> = Vec::new();
    let mut by_key = BTreeMap::new();
    let mut paths = files_in(root)?;
    for path in metadata.keys() {
        if !paths.contains(path) {
            paths.push(path.clone());
        }
    }
    paths.sort();
    for path in paths {
        let meta = metadata.get(&path).copied();
        let from_path = parse_root_from_package_file_path(&path);
        let Some((table, id, version)) = meta
            .map(|m| (m.table, m.id, normalize_version_string(&m.version)))
            .or(from_path.clone())
        else {
            // Preserve the v1 legacy reader rather than guessing the identity of
            // an unrelated attachment. Legacy records retain their original payload.
            if path.starts_with("data/") && path.ends_with(".json") {
                let table_raw = path.trim_start_matches("data/").trim_end_matches(".json");
                if let Some(table) = parse_table_name(table_raw) {
                    let raw = read_json(&root.join(&path))?;
                    let items = raw
                        .as_array()
                        .ok_or_else(|| anyhow::anyhow!("import_invalid_legacy_array"))?;
                    for (ordinal, item) in items.iter().enumerate() {
                        let entry =
                            normalize_imported_entry(table, item.clone())?.ok_or_else(|| {
                                anyhow::anyhow!("import_unattributable_legacy_identity")
                            })?;
                        add_node(
                            &mut nodes,
                            &mut by_key,
                            entries_dir,
                            &format!("{path}#{ordinal}"),
                            root.join(&path),
                            &entry,
                            item,
                            evidence,
                            false,
                        )?;
                    }
                }
            }
            continue;
        };
        let raw_path = root.join(&path);
        let raw = if raw_path.is_file() {
            anyhow::ensure!(
                fs::metadata(&raw_path)?.len() <= MAX_DOCUMENT_BYTES,
                "import_document_capacity_exceeded"
            );
            serde_json::from_reader(File::open(&raw_path)?).unwrap_or(Value::Null)
        } else {
            Value::Null
        };
        let entry = meta.map_or_else(
            || normalize_path_dataset_payload(table, id, &version, raw.clone()),
            |meta| normalize_manifest_dataset_payload(table, raw.clone(), meta),
        );
        let mismatch = from_path.is_some_and(|(t, i, v)| t != table || i != id || v != version);
        add_node(
            &mut nodes,
            &mut by_key,
            entries_dir,
            &path,
            raw_path,
            &entry,
            &raw,
            evidence,
            mismatch,
        )?;
    }
    nodes.sort_by_key(|node| identity_key(&node.identity));
    Ok(nodes)
}

#[allow(clippy::too_many_arguments)]
fn add_node(
    nodes: &mut Vec<Node>,
    by_key: &mut BTreeMap<String, usize>,
    entries_dir: &Path,
    source: &str,
    raw_path: PathBuf,
    entry: &PackageEntry,
    raw: &Value,
    evidence: &mut Evidence,
    identity_mismatch: bool,
) -> anyhow::Result<()> {
    let identity = PackageRootRef {
        table: entry.table,
        id: entry.id,
        version: entry.version.clone(),
    };
    let key = identity_key(&identity);
    let content_sha256 = hash(&serde_json::to_vec(&entry)?);
    if let Some(&ordinal) = by_key.get(&key) {
        let node = &mut nodes[ordinal];
        node.source_paths.push(source.to_owned());
        if identity_mismatch {
            evidence.blocker(
                node,
                "package_identity_mismatch",
                "<root>",
                "Manifest and file identity disagree",
            )?;
        }
        if node.content_sha256 != content_sha256 {
            evidence.blocker(
                node,
                "package_duplicate_identity",
                "<root>",
                "Different package contents share one exact dataset identity",
            )?;
        }
        return Ok(());
    }
    anyhow::ensure!(
        nodes.len() < MAX_DOCUMENTS,
        "import_document_count_exceeded"
    );
    let raw_path = if source.starts_with("data/") {
        let path = entries_dir.join(format!("{}.raw.json", nodes.len()));
        serde_json::to_writer(File::create(&path)?, raw)?;
        path
    } else {
        raw_path
    };
    let entry_path = entries_dir.join(format!("{}.json", nodes.len()));
    serde_json::to_writer(File::create(&entry_path)?, &entry)?;
    let mut node = Node {
        identity,
        source_paths: vec![source.to_owned()],
        entry_path,
        raw_path,
        content_sha256,
        blocked: false,
        edges: Vec::new(),
    };
    if !raw.is_object() {
        evidence.blocker(
            &mut node,
            "package_document_unreadable",
            "<root>",
            "Dataset file is missing or is not a JSON object",
        )?;
    }
    if identity_mismatch {
        evidence.blocker(
            &mut node,
            "package_identity_mismatch",
            "<root>",
            "Manifest and file identity disagree",
        )?;
    }
    let dataset = &entry.json_ordered;
    let declared_id = find_identity_field(dataset, entry.table, "common:UUID");
    if declared_id.is_some_and(|value| parse_uuid_opt(value) != Some(entry.id)) {
        evidence.blocker(
            &mut node,
            "package_identity_mismatch",
            "common:UUID",
            "Dataset and package UUID disagree",
        )?;
    }
    let declared_version = find_identity_field(dataset, entry.table, "common:dataSetVersion");
    if declared_version.is_some_and(|value| normalize_version_string(value) != entry.version) {
        evidence.blocker(
            &mut node,
            "package_identity_mismatch",
            "common:dataSetVersion",
            "Dataset and package version disagree",
        )?;
    }
    by_key.insert(key, nodes.len());
    nodes.push(node);
    Ok(())
}

fn find_identity_field<'a>(
    value: &'a Value,
    table: PackageRootTable,
    name: &str,
) -> Option<&'a str> {
    // Only the dataset's own information/admin blocks, never referenced objects.
    let root = value.get(dataset_root_key(table))?;
    if name == "common:dataSetVersion" {
        return root
            .pointer("/administrativeInformation/publicationAndOwnership/common:dataSetVersion")
            .and_then(Value::as_str);
    }
    root.as_object()?
        .iter()
        .filter(|(key, _)| key.ends_with("Information"))
        .find_map(|(_, block)| block.get("dataSetInformation")?.get(name)?.as_str())
}

fn validate(
    root: &Path,
    stage: &str,
    nodes: &mut [Node],
    evidence: &mut Evidence,
    cancel: &CancellationToken,
) -> anyhow::Result<Value> {
    cancel.check("import_validate")?;
    let handshake = tidas_cli::handshake()?;
    let spool_dir = TempDir::new()?;
    let spool_path = spool_dir.path().join("issues.jsonl");
    let output = super::run_tidas_package_command(root, &spool_path)?;
    anyhow::ensure!(
        output.report.get("command").and_then(Value::as_str) == Some("validate")
            && output.report.get("completeness").and_then(Value::as_str) == Some("complete"),
        "tidas_report_invalid"
    );
    let summary = output
        .report
        .pointer("/summary/validation")
        .ok_or_else(|| anyhow::anyhow!("tidas_report_invalid"))?;
    anyhow::ensure!(
        summary.get("asset_fingerprint") == handshake.validation_describe.get("asset_fingerprint"),
        "tidas_handshake_mismatch"
    );
    let spool = summary
        .get("issue_spool")
        .ok_or_else(|| anyhow::anyhow!("tidas_report_invalid"))?;
    let mut path_index = BTreeMap::new();
    for (ordinal, node) in nodes.iter().enumerate() {
        for path in &node.source_paths {
            path_index.insert(path.clone(), ordinal);
        }
        path_index.insert(canonical_path(&node.identity), ordinal);
    }
    let mut seen = 0_u64;
    let mut errors = 0_u64;
    let mut warnings = 0_u64;
    tidas_cli::visit_verified_jsonl(&spool_path, spool, |event| {
        cancel.check("import_validation_evidence")?;
        seen += 1;
        let mut issue = event
            .get("issue")
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("tidas_spool_invalid"))?;
        let is_error = issue.get("severity").and_then(Value::as_str) == Some("error");
        errors += u64::from(is_error);
        warnings += u64::from(issue.get("severity").and_then(Value::as_str) == Some("warning"));
        let path = issue.get("file_path").and_then(Value::as_str).unwrap_or("");
        let relative = Path::new(path)
            .strip_prefix(root)
            .unwrap_or(Path::new(path))
            .to_string_lossy()
            .replace('\\', "/");
        let matched = path_index
            .get(&relative)
            .map(|&ordinal| &mut nodes[ordinal]);
        if let Some(node) = matched {
            node.blocked |= is_error;
            issue["identity"] = json!(node.identity);
            issue["file_path"] = json!(node.source_paths[0]);
        } else {
            anyhow::ensure!(
                !is_error,
                "import_unattributable_validation_error: {relative}"
            );
        }
        issue["stage"] = json!(stage);
        evidence.issue(&issue)
    })?;
    anyhow::ensure!(
        summary.get("issue_count").and_then(Value::as_u64) == Some(seen)
            && summary.get("error_count").and_then(Value::as_u64) == Some(errors)
            && summary.get("warning_count").and_then(Value::as_u64) == Some(warnings),
        "tidas_spool_count_mismatch"
    );
    // Preserve v1's effective gate. Compatibility evidence is retained, not
    // silently promoted into an additional blocking policy.
    cancel.check("import_validation_complete")?;
    Ok(json!({ "binary_version": handshake.binary_version,
        "asset_fingerprint": handshake.validation_describe.get("asset_fingerprint"),
        "operation": output.report }))
}

fn canonical_path(identity: &PackageRootRef) -> String {
    format!(
        "{}/{}_{}.json",
        table_name(identity.table),
        identity.id,
        identity.version
    )
}

#[allow(clippy::too_many_lines)] // Keep each source record and its evidence together.
fn graph(
    nodes: &mut [Node],
    evidence: &mut Evidence,
    writer: &mut impl Write,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    let exact: BTreeMap<_, _> = nodes
        .iter()
        .enumerate()
        .map(|(i, n)| (identity_key(&n.identity), i))
        .collect();
    let mut versions: BTreeMap<(PackageRootTable, Uuid), Vec<usize>> = BTreeMap::new();
    for (i, node) in nodes.iter().enumerate() {
        versions
            .entry((node.identity.table, node.identity.id))
            .or_default()
            .push(i);
    }
    let mut edge_count = 0;
    for source in 0..nodes.len() {
        cancel.check("import_graph")?;
        let entry: PackageEntry = serde_json::from_reader(File::open(&nodes[source].entry_path)?)?;
        let category: DatasetCategory = serde_json::from_value(json!(entry.table))?;
        let extracted = extract_references(
            &identity_key(&nodes[source].identity),
            category,
            &entry.json_ordered,
        );
        for issue in extracted.issues {
            if issue.severity == "error"
                && issue.details.get("raw_type").and_then(Value::as_str)
                    != Some("other external file")
            {
                evidence.blocker(
                    &mut nodes[source],
                    &issue.issue_code,
                    &issue.json_path,
                    &issue.message,
                )?;
            }
        }
        let mut refs = extracted
            .edges
            .into_iter()
            .map(|edge| {
                (
                    parse_table_name(&edge.target_category),
                    parse_uuid_opt(&edge.target_uuid),
                    edge.requested_version.map(|v| normalize_version_string(&v)),
                    edge.json_path,
                )
            })
            .collect::<Vec<_>>();
        if let Some(submodels) = entry.json_tg.as_ref().and_then(|tg| tg.get("submodels"))
            && (!submodels.is_array()
                || submodels
                    .as_array()
                    .is_some_and(|items| items.len() != extract_model_submodels(&entry).len()))
        {
            evidence.blocker(
                &mut nodes[source],
                "package_model_reference_invalid",
                "$.json_tg.submodels",
                "A model submodel reference cannot be resolved",
            )?;
        }
        for reference in extract_model_submodels(&entry) {
            refs.push((
                Some(reference.table),
                Some(reference.id),
                reference.version,
                "$.json_tg.submodels".to_owned(),
            ));
        }
        for (table, id, version, path) in refs {
            edge_count += 1;
            anyhow::ensure!(
                edge_count <= MAX_EDGES,
                "import_reference_capacity_exceeded"
            );
            let target = if let (Some(table), Some(id)) = (table, id) {
                if let Some(version) = &version {
                    exact.get(&table_key(table, id, version)).copied()
                } else {
                    versions
                        .get(&(table, id))
                        .filter(|v| v.len() == 1)
                        .map(|v| v[0])
                }
            } else {
                None
            };
            write_line(
                writer,
                &json!({"source": nodes[source].identity, "target": target.map(|i| &nodes[i].identity),
                "requested": {"table":table,"id":id,"version":version}, "location":path}),
            )?;
            if let Some(target) = target {
                nodes[source].edges.push(target);
            } else {
                evidence.blocker(&mut nodes[source], "package_reference_unresolved", &path, "Exact reference is missing, unsupported or has an ambiguous omitted version in this ZIP")?;
            }
        }
        nodes[source].edges.sort_unstable();
        nodes[source].edges.dedup();
    }
    Ok(())
}

fn closure(nodes: &[Node], root: usize) -> GroupPlan {
    let mut pending = VecDeque::from([root]);
    let mut parents = BTreeMap::from([(root, root)]);
    let mut blocked_node = None;
    while let Some(source) = pending.pop_front() {
        if nodes[source].blocked && blocked_node.is_none() {
            blocked_node = Some(source);
        }
        for &target in &nodes[source].edges {
            if let std::collections::btree_map::Entry::Vacant(entry) = parents.entry(target) {
                entry.insert(source);
                pending.push_back(target);
            }
        }
    }
    let mut path = Vec::new();
    if let Some(mut node) = blocked_node {
        loop {
            path.push(node);
            if node == root {
                break;
            }
            node = parents[&node];
        }
        path.reverse();
    }
    GroupPlan {
        root,
        members: parents.into_keys().collect(),
        blocked: blocked_node.is_some(),
        blocking_path: path,
    }
}

fn materialize(nodes: &[Node], group: &GroupPlan, target: &Path) -> anyhow::Result<()> {
    let mut bytes = 0;
    let mut legacy: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    for &member in &group.members {
        let node = &nodes[member];
        bytes += fs::metadata(&node.entry_path)?.len();
        anyhow::ensure!(
            bytes <= MAX_GROUP_BYTES && group.members.len() <= MAX_GROUP_DOCUMENTS,
            "import_group_capacity_exceeded"
        );
        if node.source_paths[0].starts_with("data/") {
            legacy
                .entry(format!("data/{}.json", table_name(node.identity.table)))
                .or_default()
                .push(read_json(&node.raw_path)?);
        } else {
            let destination = target.join(canonical_path(&node.identity));
            fs::create_dir_all(
                destination
                    .parent()
                    .ok_or_else(|| anyhow::anyhow!("invalid staging path"))?,
            )?;
            // Preserve the uploaded JSON bytes rather than normalizing away errors.
            fs::copy(&node.raw_path, destination)?;
        }
    }
    for (path, items) in legacy {
        let path = target.join(path);
        fs::create_dir_all(
            path.parent()
                .ok_or_else(|| anyhow::anyhow!("invalid staging path"))?,
        )?;
        serde_json::to_writer(File::create(path)?, &items)?;
    }
    Ok(())
}

fn prepare(
    zip_path: &Path,
    source_sha: &str,
    cancel: &CancellationToken,
) -> anyhow::Result<Prepared> {
    let temp = TempDir::new()?;
    let root = temp.path().join("input");
    fs::create_dir(&root)?;
    extract(zip_path, &root, cancel)?;
    let mut evidence = Evidence::new(&temp.path().join("issues.ndjson"))?;
    let mut nodes = index(&root, &temp.path().join("entries"), &mut evidence)?;
    let mut validation = BufWriter::new(File::create(temp.path().join("validation.ndjson"))?);
    let original = validate(&root, "package", &mut nodes, &mut evidence, cancel)?;
    write_line(
        &mut validation,
        &json!({"scope":"package","evidence": original}),
    )?;
    let mut edges = BufWriter::new(File::create(temp.path().join("references.ndjson"))?);
    graph(&mut nodes, &mut evidence, &mut edges, cancel)?;
    edges.flush()?;
    let roots = nodes
        .iter()
        .enumerate()
        .filter(|(_, n)| {
            matches!(
                n.identity.table,
                PackageRootTable::Processes | PackageRootTable::Lifecyclemodels
            )
        })
        .map(|(i, _)| i)
        .collect::<Vec<_>>();
    anyhow::ensure!(roots.len() <= MAX_ROOTS, "import_root_capacity_exceeded");
    let mut validation_document_visits = 0;
    // Finish all candidate checks before fixing the plan. Errors discovered in
    // a closure propagate to every root that uses that same original document.
    for &root in &roots {
        cancel.check("import_closure_validation")?;
        let group = closure(&nodes, root);
        if group.blocked {
            continue;
        }
        validation_document_visits += group.members.len();
        anyhow::ensure!(
            validation_document_visits <= MAX_VALIDATION_DOCUMENT_VISITS,
            "import_validation_capacity_exceeded"
        );
        let directory = TempDir::new()?;
        materialize(&nodes, &group, directory.path())?;
        let checked = validate(
            directory.path(),
            "root_closure",
            &mut nodes,
            &mut evidence,
            cancel,
        )?;
        anyhow::ensure!(
            checked.get("binary_version") == original.get("binary_version")
                && checked.get("asset_fingerprint") == original.get("asset_fingerprint"),
            "tidas_validation_assets_changed_during_import"
        );
        write_line(
            &mut validation,
            &json!({"scope":nodes[root].identity,"evidence":checked}),
        )?;
        validation.flush()?;
        anyhow::ensure!(
            validation.get_ref().metadata()?.len() <= MAX_ZIP_BYTES,
            "import_validation_evidence_capacity_exceeded"
        );
    }
    validation.flush()?;
    evidence.writer.flush()?;
    let plan_path = temp.path().join("plan.ndjson");
    let mut plan = BufWriter::new(File::create(&plan_path)?);
    for &root in &roots {
        cancel.check("import_plan")?;
        write_line(&mut plan, &closure(&nodes, root))?;
        anyhow::ensure!(
            plan.get_ref().metadata()?.len() <= MAX_ZIP_BYTES,
            "import_plan_capacity_exceeded"
        );
    }
    plan.flush()?;
    let meta = prepare_package_zip_artifact_from_path(&plan_path)?;
    let plan_sha256 = hash(&serde_json::to_vec(
        &json!({"policy":"root_closure_v2", "source":source_sha,
        "plan":meta.sha256,"validator": original.get("binary_version"), "assets":original.get("asset_fingerprint")}),
    )?);
    Ok(Prepared {
        temp,
        nodes,
        plan_path,
        plan_sha256,
        root_count: roots.len(),
        evidence,
    })
}

struct CancelOnDrop(CancellationToken);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

async fn publish_preparation_failure(
    state: &AppState,
    job: &WorkerJob,
    package_job_id: Uuid,
    error: &anyhow::Error,
) -> anyhow::Result<()> {
    let row = sqlx::query("select api.svc_tidas_package_read_v2($1,$2) as result")
        .bind(job.requested_by)
        .bind(job.id)
        .fetch_one(&state.pool)
        .await?;
    let result: Value = row.try_get("result")?;
    let summary = result
        .pointer("/data/importProgress")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let temp = TempDir::new()?;
    for name in [
        "issues.ndjson",
        "validation.ndjson",
        "references.ndjson",
        "roots.ndjson",
        "records.ndjson",
        "plan.ndjson",
    ] {
        File::create(temp.path().join(name))?;
    }
    let execution_error = json!({"code":"IMPORT_PREPARATION_FAILED","stage":"package_preparation","message":error.to_string()});
    let report = json!({"report_version":2,"import_policy":"root_closure_v2","ok":false,"code":"IMPORT_PREPARATION_FAILED",
        "outcome":"interrupted","execution_complete":false,"summary":summary,"roots":[],"roots_truncated":false,
        "execution_error":execution_error,"validation_issues":[],"validation_issues_truncated":false,"details_artifact_kind":"import_details"});
    let report_path = temp.path().join("report.json");
    serde_json::to_writer(
        File::create(&report_path)?,
        &json!({"version":2,"format":REPORT_FORMAT,"job_id":package_job_id,"payload":report}),
    )?;
    let bundle_path = temp.path().join("details.zip");
    bundle(temp.path(), &bundle_path)?;
    publish(
        state,
        package_job_id,
        &bundle_path,
        PackageArtifactKind::ImportDetails,
        "tidas-package-import-details:v2",
        "application/zip",
        json!({"filename":"tidas-import-details.zip"}),
    )
    .await?;
    publish(state,package_job_id,&report_path,PackageArtifactKind::ImportReport,REPORT_FORMAT,"application/json",json!({"filename":"tidas-import-report.json","import_policy":"root_closure_v2","outcome":"interrupted","summary":summary,"execution_complete":false})).await
}

/// Execute a v2 job with the caller's lease token. v1 execution is untouched.
pub async fn execute(
    state: &AppState,
    job: &WorkerJob,
    package_job_id: Uuid,
    source_id: Uuid,
) -> anyhow::Result<()> {
    let artifact = fetch_package_artifact(&state.pool, source_id).await?;
    anyhow::ensure!(
        artifact.artifact_kind == PackageArtifactKind::ImportSource
            && artifact.status == "ready"
            && artifact.artifact_format == PACKAGE_ZIP_ARTIFACT_FORMAT,
        "import_source_invalid"
    );
    let row = sqlx::query("select artifact_sha256, artifact_byte_size from private.lca_package_artifacts where id=$1 and worker_job_id=$2")
        .bind(source_id).bind(job.id).fetch_one(&state.pool).await?;
    let expected_sha: String = row.try_get("artifact_sha256")?;
    let expected_bytes: i64 = row.try_get("artifact_byte_size")?;
    let download = TempDir::new()?;
    let zip_path = download.path().join("input.zip");
    let transfer = state
        .object_store
        .download_object_url_to_file(
            &artifact.artifact_url,
            &zip_path,
            ObjectTransferOptions::new(MAX_ZIP_BYTES).with_expected_sha256(&expected_sha),
        )
        .await?;
    anyhow::ensure!(
        i64::try_from(transfer.byte_size)? == expected_bytes,
        "import_source_size_mismatch"
    );
    let cancel = CancellationToken::default();
    let _guard = CancelOnDrop(cancel.clone());
    let prepared =
        match tokio::task::spawn_blocking(move || prepare(&zip_path, &expected_sha, &cancel))
            .await?
        {
            Ok(prepared) => prepared,
            Err(error) => {
                publish_preparation_failure(state, job, package_job_id, &error).await?;
                return Err(error);
            }
        };
    execute_plan(state, job, package_job_id, source_id, prepared).await
}

#[allow(clippy::too_many_lines)] // Sequential commit/report orchestration keeps lease ownership visible.
async fn execute_plan(
    state: &AppState,
    job: &WorkerJob,
    package_job_id: Uuid,
    source_id: Uuid,
    mut prepared: Prepared,
) -> anyhow::Result<()> {
    let mut roots_file = BufWriter::new(File::create(prepared.temp.path().join("roots.ndjson"))?);
    let mut records_file =
        BufWriter::new(File::create(prepared.temp.path().join("records.ndjson"))?);
    let mut inserted = BTreeSet::new();
    let mut existing = BTreeSet::new();
    let mut succeeded = 0;
    let mut blocked = 0;
    let mut write_failed = 0;
    let mut root_samples = Vec::new();
    let mut interruption: Option<String> = None;
    for line in BufReader::new(File::open(&prepared.plan_path)?).lines() {
        let group: GroupPlan = serde_json::from_str(&line?)?;
        let identity = &prepared.nodes[group.root].identity;
        let mut result = json!({"root":identity,"dependency_count":group.members.len().saturating_sub(1),
            "blocking_path":group.blocking_path.iter().map(|&i| &prepared.nodes[i].identity).collect::<Vec<_>>()});
        if interruption.is_some() {
            result["status"] = json!("not_attempted");
        } else if group.blocked {
            blocked += 1;
            result["status"] = json!("blocked");
        } else {
            let mut entries = Vec::with_capacity(group.members.len());
            let mut bytes = 0;
            for &member in &group.members {
                let node = &prepared.nodes[member];
                bytes += fs::metadata(&node.entry_path)?.len();
                anyhow::ensure!(bytes <= MAX_GROUP_BYTES, "import_group_capacity_exceeded");
                let mut entry: PackageEntry =
                    serde_json::from_reader(File::open(&node.entry_path)?)?;
                entry.json_ordered = normalize_json_ordered_for_insert(
                    entry.table,
                    &entry.version,
                    entry.json_ordered,
                );
                entries.push(entry);
            }
            let payload = serde_json::to_value(backfill_process_model_ids(entries))?;
            let mut attempt = 0;
            let receipt = loop {
                let attempt_result = sqlx::query("select private.tidas_import_group_apply_v2($1,$2,$3,$4,$5::jsonb,$6::jsonb) as receipt")
                    .bind(job.id).bind(job.lease_token).bind(source_id).bind(&prepared.plan_sha256)
                    .bind(json!(identity)).bind(&payload).fetch_one(&state.pool).await;
                match attempt_result {
                    Err(ref error) if retryable_transaction(error) && attempt < 2 => {
                        attempt += 1;
                        tokio::time::sleep(Duration::from_millis(50 * attempt)).await;
                    }
                    result => break result,
                }
            };
            match receipt {
                Ok(row) => {
                    let receipt: Value = row.try_get("receipt")?;
                    succeeded += 1;
                    result["status"] = receipt["status"].clone();
                    result["inserted_count"] = receipt["inserted_count"].clone();
                    result["existing_count"] = receipt["existing_count"].clone();
                    for item in receipt["items"]
                        .as_array()
                        .ok_or_else(|| anyhow::anyhow!("import_receipt_invalid"))?
                    {
                        let key = serde_json::to_string(&(
                            item["table"].clone(),
                            item["id"].clone(),
                            item["version"].clone(),
                        ))?;
                        if item["disposition"] == "inserted" {
                            inserted.insert(key);
                        } else {
                            existing.insert(key);
                        }
                    }
                }
                Err(error) => {
                    let code = sqlstate(&error);
                    result["error_code"] = json!(code);
                    if code.starts_with("23") || code.starts_with("22") {
                        write_failed += 1;
                        result["status"] = json!("write_failed");
                    } else {
                        interruption = Some(code);
                        result["status"] = json!("not_attempted");
                    }
                }
            }
        }
        write_line(&mut roots_file, &result)?;
        if root_samples.len() < ROOT_SAMPLE_LIMIT {
            root_samples.push(result);
        }
    }
    for (ordinal, node) in prepared.nodes.iter().enumerate() {
        let key = serde_json::to_string(&(
            json!(node.identity.table),
            json!(node.identity.id),
            json!(node.identity.version),
        ))?;
        let disposition = if inserted.contains(&key) {
            "inserted"
        } else if existing.contains(&key) {
            "existing"
        } else {
            "not_imported"
        };
        write_line(
            &mut records_file,
            &json!({"ordinal":ordinal,"identity":node.identity,"source_paths":node.source_paths,"disposition":disposition,"validation_blocked":node.blocked}),
        )?;
    }
    roots_file.flush()?;
    records_file.flush()?;
    prepared.evidence.writer.flush()?;
    existing.retain(|key| !inserted.contains(key));
    let outcome = if interruption.is_some() {
        "interrupted"
    } else if succeeded == prepared.root_count && succeeded > 0 {
        "success"
    } else if succeeded > 0 {
        "partial"
    } else {
        "none"
    };
    let summary = json!({"total_entries":prepared.nodes.len(),"imported_count":inserted.len(),"existing_count":existing.len(),
        "not_imported_count":prepared.nodes.len().saturating_sub(inserted.len()+existing.len()),
        "root_count":prepared.root_count,"successful_root_count":succeeded,"blocked_root_count":blocked,
        "write_failed_root_count":write_failed,"not_attempted_root_count":prepared.root_count-succeeded-blocked-write_failed,
        "error_count":prepared.evidence.errors,"warning_count":prepared.evidence.warnings,"validation_issue_count":prepared.evidence.count});
    let report = json!({"report_version":2,"import_policy":"root_closure_v2","ok":outcome=="success", "outcome":outcome,
        "execution_complete":interruption.is_none(),"code":if prepared.root_count==0 {"NO_IMPORT_ROOTS"} else {match outcome {"success"=>"IMPORTED","partial"=>"PARTIALLY_IMPORTED","interrupted"=>"IMPORT_INTERRUPTED",_=>"NO_ROOTS_IMPORTED"}},
        "summary":summary,"roots":root_samples,"roots_truncated":prepared.root_count>ROOT_SAMPLE_LIMIT,
        "validation_issues":prepared.evidence.samples,"validation_issues_truncated":prepared.evidence.count>prepared.evidence.samples.len(),
        "plan_sha256":prepared.plan_sha256,"execution_error_code":interruption,"details_artifact_kind":"import_details"});
    let summary_path = prepared.temp.path().join("report.json");
    serde_json::to_writer(
        File::create(&summary_path)?,
        &json!({"version":2,"format":REPORT_FORMAT,"job_id":package_job_id,"payload":report}),
    )?;
    let bundle_path = prepared.temp.path().join("details.zip");
    let bundle_root = prepared.temp.path().to_path_buf();
    let bundle_output = bundle_path.clone();
    tokio::task::spawn_blocking(move || bundle(&bundle_root, &bundle_output)).await??;
    publish(
        state,
        package_job_id,
        &bundle_path,
        PackageArtifactKind::ImportDetails,
        "tidas-package-import-details:v2",
        "application/zip",
        json!({"filename":"tidas-import-details.zip"}),
    )
    .await?;
    publish(state,package_job_id,&summary_path,PackageArtifactKind::ImportReport,REPORT_FORMAT,"application/json",
        json!({"filename":"tidas-import-report.json","import_policy":"root_closure_v2","outcome":outcome,"summary":summary,"execution_complete":interruption.is_none()})).await?;
    Ok(())
}

fn sqlstate(error: &sqlx::Error) -> String {
    match error {
        sqlx::Error::Database(error) => error
            .code()
            .map_or_else(|| "DATABASE_ERROR".to_owned(), std::borrow::Cow::into_owned),
        _ => "DATABASE_UNAVAILABLE".to_owned(),
    }
}
fn retryable_transaction(error: &sqlx::Error) -> bool {
    matches!(sqlstate(error).as_str(), "40001" | "40P01")
}

fn bundle(root: &Path, output: &Path) -> anyhow::Result<()> {
    let mut writer = ZipWriter::new(File::create(output)?);
    let mut manifest = Vec::new();
    for name in [
        "report.json",
        "issues.ndjson",
        "validation.ndjson",
        "references.ndjson",
        "roots.ndjson",
        "records.ndjson",
        "plan.ndjson",
    ] {
        let path = root.join(name);
        let meta = prepare_package_zip_artifact_from_path(&path)?;
        anyhow::ensure!(
            meta.byte_size <= MAX_EXTRACTED_BYTES,
            "import_report_capacity_exceeded"
        );
        writer.start_file(
            name,
            SimpleFileOptions::default().compression_method(CompressionMethod::Deflated),
        )?;
        copy(&mut File::open(path)?, &mut writer)?;
        manifest.push(json!({"path":name,"sha256":meta.sha256,"byte_size":meta.byte_size}));
    }
    writer.start_file("manifest.json", SimpleFileOptions::default())?;
    serde_json::to_writer(
        &mut writer,
        &json!({"format":"tidas-package-import-details:v2","files":manifest}),
    )?;
    writer.finish()?;
    Ok(())
}

async fn publish(
    state: &AppState,
    job_id: Uuid,
    path: &Path,
    kind: PackageArtifactKind,
    format: &'static str,
    content_type: &'static str,
    metadata: Value,
) -> anyhow::Result<()> {
    let name = path
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(|| anyhow::anyhow!("invalid artifact path"))?;
    let uploaded = state
        .object_store
        .upload_object_key_file_bounded(
            &format!("tidas-packages/{job_id}/v2/{name}"),
            content_type,
            path,
            ObjectTransferOptions::new(MAX_EXTRACTED_BYTES),
        )
        .await?;
    let meta = PackageArtifactUploadMeta {
        sha256: uploaded.sha256,
        byte_size: uploaded.byte_size,
        format,
        content_type,
        extension: if kind == PackageArtifactKind::ImportDetails {
            "zip"
        } else {
            "json"
        },
    };
    insert_package_artifact(
        &state.pool,
        PackageArtifactInsert::ready(job_id, kind, uploaded.upload.object_url, meta, metadata),
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn node(table: PackageRootTable, ordinal: u128, blocked: bool, edges: Vec<usize>) -> Node {
        Node {
            identity: PackageRootRef {
                table,
                id: Uuid::from_u128(ordinal),
                version: "01.00.000".to_owned(),
            },
            source_paths: vec!["fixture.json".to_owned()],
            entry_path: PathBuf::new(),
            raw_path: PathBuf::new(),
            content_sha256: String::new(),
            blocked,
            edges,
        }
    }
    #[test]
    fn independent_roots_and_shared_failure() {
        let mut nodes = vec![
            node(PackageRootTable::Processes, 1, false, vec![2]),
            node(PackageRootTable::Processes, 2, false, vec![3]),
            node(PackageRootTable::Flows, 3, false, vec![4]),
            node(PackageRootTable::Flows, 4, true, vec![4]),
            node(PackageRootTable::Unitgroups, 5, false, vec![]),
        ];
        assert!(!closure(&nodes, 0).blocked);
        assert!(closure(&nodes, 1).blocked);
        nodes[4].blocked = true;
        assert_eq!(closure(&nodes, 0).blocking_path, vec![0, 2, 4]);
        assert!(closure(&nodes, 1).blocked);
    }
    #[test]
    fn failed_model_does_not_block_independent_process_and_cycles_terminate() {
        let nodes = vec![
            node(PackageRootTable::Lifecyclemodels, 1, true, vec![1]),
            node(PackageRootTable::Processes, 2, false, vec![2]),
            node(PackageRootTable::Sources, 3, false, vec![1]),
        ];
        assert!(closure(&nodes, 0).blocked);
        assert!(!closure(&nodes, 1).blocked);
        assert_eq!(closure(&nodes, 1).members, vec![1, 2]);
    }
    #[allow(clippy::needless_pass_by_value)] // Tests use inline JSON fixtures.
    fn fixture(root: &Path, table: PackageRootTable, id: u128, version: &str, extra: Value) {
        let identity = PackageRootRef {
            table,
            id: Uuid::from_u128(id),
            version: version.to_owned(),
        };
        let path = root.join(canonical_path(&identity));
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        serde_json::to_writer(File::create(path).unwrap(), &extra).unwrap();
    }
    fn evidence(temp: &TempDir) -> Evidence {
        Evidence::new(&temp.path().join("issues.ndjson")).unwrap()
    }
    fn indexed(temp: &TempDir, evidence: &mut Evidence) -> Vec<Node> {
        index(
            &temp.path().join("input"),
            &temp.path().join("entries"),
            evidence,
        )
        .unwrap()
    }
    #[test]
    fn exact_version_missing_and_ambiguous_reference_fail_without_database() {
        for version in [Some("01.00.001"), None] {
            let temp = TempDir::new().unwrap();
            let root = temp.path().join("input");
            let mut reference = json!({"@type":"flow data set","@refObjectId":Uuid::from_u128(2)});
            if let Some(version) = version {
                reference["@version"] = json!(version);
            }
            fixture(
                &root,
                PackageRootTable::Processes,
                1,
                "01.00.000",
                json!({"processDataSet":{"exchanges":{"exchange":[{"referenceToFlowDataSet":reference}]}}}),
            );
            fixture(
                &root,
                PackageRootTable::Flows,
                2,
                "01.00.000",
                json!({"flowDataSet":{}}),
            );
            fixture(
                &root,
                PackageRootTable::Flows,
                2,
                "02.00.000",
                json!({"flowDataSet":{}}),
            );
            let mut evidence = evidence(&temp);
            let mut nodes = indexed(&temp, &mut evidence);
            graph(
                &mut nodes,
                &mut evidence,
                &mut Vec::new(),
                &CancellationToken::default(),
            )
            .unwrap();
            let process = nodes
                .iter()
                .position(|n| n.identity.table == PackageRootTable::Processes)
                .unwrap();
            assert!(closure(&nodes, process).blocked);
        }
    }
    #[test]
    fn unique_omitted_version_resolves_and_external_attachment_is_not_dependency() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("input");
        fixture(
            &root,
            PackageRootTable::Processes,
            1,
            "01.00.000",
            json!({"processDataSet":{
            "referenceToFlowDataSet":{"@type":"flow data set","@refObjectId":Uuid::from_u128(2)},
            "referenceToExternalFile":{"@type":"other external file","@refObjectId":Uuid::from_u128(3),"@uri":"https://example.com/file.pdf"}}}),
        );
        fixture(
            &root,
            PackageRootTable::Flows,
            2,
            "01.00.000",
            json!({"flowDataSet":{}}),
        );
        let mut evidence = evidence(&temp);
        let mut nodes = indexed(&temp, &mut evidence);
        graph(
            &mut nodes,
            &mut evidence,
            &mut Vec::new(),
            &CancellationToken::default(),
        )
        .unwrap();
        let process = nodes
            .iter()
            .position(|n| n.identity.table == PackageRootTable::Processes)
            .unwrap();
        assert!(!closure(&nodes, process).blocked);
        assert_eq!(closure(&nodes, process).members.len(), 2);
    }
    #[test]
    fn published_state_does_not_bypass_identity_validation() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("input");
        fixture(
            &root,
            PackageRootTable::Processes,
            1,
            "01.00.000",
            json!({"state_code":100,"processDataSet":{"processInformation":{"dataSetInformation":{"common:UUID":Uuid::from_u128(2)}}}}),
        );
        let mut evidence = evidence(&temp);
        let nodes = indexed(&temp, &mut evidence);
        assert!(nodes[0].blocked);
        assert_eq!(evidence.errors, 1);
    }
    #[test]
    fn same_identity_duplicates_coalesce_but_different_content_blocks() {
        for different in [false, true] {
            let temp = TempDir::new().unwrap();
            let root = temp.path().join("input");
            fixture(
                &root,
                PackageRootTable::Processes,
                1,
                "01.00.000",
                json!({"processDataSet":{}}),
            );
            fixture(
                &root,
                PackageRootTable::Processes,
                1,
                "1.0.0",
                if different {
                    json!({"processDataSet":{"changed":true}})
                } else {
                    json!({"processDataSet":{}})
                },
            );
            let mut evidence = evidence(&temp);
            let nodes = indexed(&temp, &mut evidence);
            assert_eq!(nodes.len(), 1);
            assert_eq!(nodes[0].source_paths.len(), 2);
            assert_eq!(nodes[0].blocked, different);
        }
    }
    #[test]
    fn issue_sample_limit_never_limits_blocking_or_complete_evidence() {
        let temp = TempDir::new().unwrap();
        let mut evidence = evidence(&temp);
        for _ in 0..VALIDATION_ISSUE_SAMPLE_LIMIT {
            evidence.issue(&json!({"severity":"warning"})).unwrap();
        }
        let mut node = node(PackageRootTable::Processes, 1, false, vec![]);
        evidence
            .blocker(
                &mut node,
                "schema_error",
                "late.field",
                "Failure after all display samples",
            )
            .unwrap();
        assert!(closure(&[node], 0).blocked);
        assert_eq!(evidence.errors, 1);
        assert_eq!(evidence.samples.len(), VALIDATION_ISSUE_SAMPLE_LIMIT);
        evidence.writer.flush().unwrap();
        assert_eq!(
            BufReader::new(File::open(temp.path().join("issues.ndjson")).unwrap())
                .lines()
                .count(),
            VALIDATION_ISSUE_SAMPLE_LIMIT + 1
        );
    }
    #[test]
    fn closure_materialization_keeps_uploaded_bytes() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("input");
        fixture(
            &root,
            PackageRootTable::Processes,
            1,
            "01.00.000",
            json!({"processDataSet":{},"state_code":100}),
        );
        let mut evidence = evidence(&temp);
        let nodes = indexed(&temp, &mut evidence);
        let destination = temp.path().join("stage");
        materialize(&nodes, &closure(&nodes, 0), &destination).unwrap();
        assert_eq!(
            fs::read(&nodes[0].raw_path).unwrap(),
            fs::read(destination.join(canonical_path(&nodes[0].identity))).unwrap()
        );
    }
    #[test]
    #[ignore = "requires the governed release TIDAS_BIN and bundled assets"]
    fn release_validator_matches_legacy_native_gate_and_issue_details() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("input");
        for (table, id, payload) in [
            (
                PackageRootTable::Processes,
                1,
                json!({"processDataSet":{},"state_code":100}),
            ),
            (
                PackageRootTable::Lifecyclemodels,
                2,
                json!({"lifeCycleModelDataSet":{}}),
            ),
            (PackageRootTable::Sources, 3, json!({"sourceDataSet":{}})),
        ] {
            fixture(&root, table, id, "01.00.000", payload);
        }
        let legacy = super::super::run_tidas_validation(&root).unwrap();
        let mut evidence = evidence(&temp);
        let mut nodes = indexed(&temp, &mut evidence);
        assert_eq!(evidence.errors, 0);
        let result = validate(
            &root,
            "package",
            &mut nodes,
            &mut evidence,
            &CancellationToken::default(),
        )
        .unwrap();
        assert_eq!(evidence.errors, legacy.summary.error_count);
        assert_eq!(evidence.warnings, legacy.summary.warning_count);
        assert!(evidence.errors > 0);
        assert!(nodes.iter().all(|node| node.blocked));
        let mut old_issues = legacy
            .issues
            .iter()
            .map(|issue| serde_json::to_string(issue).unwrap())
            .collect::<Vec<_>>();
        let mut new_issues = evidence
            .samples
            .iter()
            .map(|issue| {
                let projected: super::super::ValidationIssueDetail =
                    serde_json::from_value(issue.clone()).unwrap();
                serde_json::to_string(&projected).unwrap()
            })
            .collect::<Vec<_>>();
        old_issues.sort();
        new_issues.sort();
        assert_eq!(old_issues, new_issues);
        assert_eq!(result["binary_version"], tidas_cli::DEFAULT_TIDAS_VERSION);
    }
}
