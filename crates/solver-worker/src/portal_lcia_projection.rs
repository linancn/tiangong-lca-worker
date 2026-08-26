use std::{
    fs::File,
    io::{BufRead, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use flate2::read::GzDecoder;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use uuid::Uuid;

pub const PORTAL_LCIA_PROJECTION_CONTRACT_VERSION: &str = "portal.lcia-projection.v1";
pub const PORTAL_LCIA_DECIMAL_CONTRACT_VERSION: &str = "ieee754-binary64-shortest-fixed-p38.v1";
pub const PORTAL_LCIA_HASH_CONTRACT_VERSION: &str =
    "portal.lcia-projection.int32be-frame-sha256.v1";
pub const PORTAL_LCIA_PROCESS_SCHEMA_VERSION: &str = "portal.lcia-projection.process.v1";
pub const PORTAL_LCIA_IMPACT_SCHEMA_VERSION: &str = "portal.lcia-projection.impact.v1";
pub const PORTAL_LCIA_VALUE_SCHEMA_VERSION: &str = "portal.lcia-projection.value.v1";
pub const PORTAL_LCIA_MAX_BATCH_RECORDS: usize = 500;
pub const PORTAL_LCIA_MAX_BATCH_ENCODED_BYTES: usize = 1_048_576;
const BATCH_ENVELOPE_RESERVE_BYTES: usize = 4_096;

#[derive(Debug, Clone)]
pub struct PortalProcessSource {
    pub process_index: u64,
    pub process_id: Uuid,
    pub process_version: String,
    pub process_document_sha256: String,
    pub reference_flow_id: Uuid,
    pub reference_flow_version: String,
    pub reference_exchange_internal_id: String,
    pub reference_flow_amount: f64,
    pub reference_flow_direction: String,
    pub functional_unit_amount: f64,
    pub functional_unit_unit: String,
    pub functional_unit_description: Vec<PortalLocalizedText>,
    pub geography_code: String,
    pub geography_precision: String,
    pub reference_year: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PortalLocalizedText {
    pub language: String,
    pub value: String,
}

#[derive(Debug, Clone)]
pub struct PortalImpactSource {
    pub impact_index: u64,
    pub method_id: Uuid,
    pub method_version: String,
    pub method_document_sha256: String,
    pub impact_category_id: String,
    pub impact_name: Vec<PortalLocalizedText>,
    pub result_unit: String,
}

#[derive(Debug, Clone)]
pub struct PortalLciaShard {
    pub chunk_ordinal: u64,
    pub first_process_ordinal: u64,
    pub last_process_ordinal: u64,
    pub sha256: String,
    pub uncompressed_sha256: String,
    pub byte_size: u64,
    pub uncompressed_byte_size: u64,
    pub record_count: u64,
    pub local_path: PathBuf,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PortalProjectionSourceBinding {
    pub input_manifest_hash: String,
    pub closure_certificate_hash: String,
    pub snapshot_hash: String,
    pub closure_bundle_hash: String,
    pub snapshot_index_sha256: String,
    pub snapshot_build_contract_hash: String,
    pub bundle_content_hash: String,
    pub bundle_manifest_sha256: String,
    pub result_artifact_sha256: String,
    pub query_artifact_sha256: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PortalProcessRecord {
    pub process_index: u64,
    pub process_id: Uuid,
    pub process_version: String,
    pub process_document_sha256: String,
    pub reference_flow_id: Uuid,
    pub reference_flow_version: String,
    pub reference_exchange_internal_id: String,
    pub reference_flow_amount: String,
    pub reference_flow_direction: String,
    pub functional_unit_amount: String,
    pub functional_unit_unit: String,
    pub functional_unit_description: Vec<PortalLocalizedText>,
    pub geography_code: String,
    pub geography_precision: String,
    pub reference_year: i32,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PortalImpactRecord {
    pub impact_index: u64,
    pub method_id: Uuid,
    pub method_version: String,
    pub method_document_sha256: String,
    #[serde(rename = "impactId")]
    pub impact_category_id: String,
    pub impact_name: Vec<PortalLocalizedText>,
    #[serde(rename = "unit")]
    pub result_unit: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PortalValueRecord {
    pub ordinal: u64,
    pub process_index: u64,
    pub impact_index: u64,
    #[serde(rename = "value")]
    pub value_text: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SpoolRecord<T> {
    ordinal: u64,
    record_hash: String,
    record: T,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PreparedPortalLciaProjection {
    pub contract_version: &'static str,
    pub decimal_contract_version: &'static str,
    pub hash_contract_version: &'static str,
    pub chunk_descriptor_set_hash: String,
    pub relation_hash: String,
    pub process_relation: PreparedPortalRelation,
    pub impact_relation: PreparedPortalRelation,
    pub value_relation: PreparedPortalRelation,
    #[serde(skip)]
    #[allow(dead_code)]
    directory_guard: Arc<tempfile::TempDir>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BoundPortalLciaProjection {
    pub contract_version: &'static str,
    pub decimal_contract_version: &'static str,
    pub hash_contract_version: &'static str,
    pub content_hash: String,
    pub chunk_descriptor_set_hash: String,
    pub relation_hash: String,
    pub source: PortalProjectionSourceBinding,
    pub process_relation: PreparedPortalRelation,
    pub impact_relation: PreparedPortalRelation,
    pub value_relation: PreparedPortalRelation,
    #[serde(skip)]
    #[allow(dead_code)]
    directory_guard: Arc<tempfile::TempDir>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PreparedPortalRelation {
    pub relation: &'static str,
    pub schema_version: &'static str,
    pub record_count: u64,
    pub encoded_byte_size: u64,
    pub relation_hash: String,
    #[serde(skip)]
    pub local_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct PortalProjectionBatch {
    pub relation: &'static str,
    pub batch_ordinal: u64,
    pub first_ordinal: u64,
    pub last_ordinal: u64,
    pub record_count: usize,
    pub payload: Value,
    pub encoded_bytes: usize,
}

pub struct PortalProjectionBatchReader {
    relation: PreparedPortalRelation,
    reader: BufReader<File>,
    batch_ordinal: u64,
    next_line: Option<Vec<u8>>,
    next_expected_ordinal: u64,
    exhausted: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BundleLciaRecord {
    process_index: u64,
    method: BundleGlobalReference,
    mean_amount: f64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BundleGlobalReference {
    id: Uuid,
    version: String,
}

struct RelationWriter {
    writer: BufWriter<File>,
    path: PathBuf,
    relation_hasher: Sha256,
    expected_count: u64,
    record_count: u64,
    encoded_byte_size: u64,
}

impl RelationWriter {
    fn create(path: PathBuf, relation: &str, expected_count: u64) -> anyhow::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut relation_hasher = Sha256::new();
        update_framed_fields(
            &mut relation_hasher,
            &[
                Some("portal.lcia-projection.relation.v1"),
                Some(PORTAL_LCIA_HASH_CONTRACT_VERSION),
                Some(relation),
                Some(expected_count.to_string().as_str()),
            ],
        )?;
        Ok(Self {
            writer: BufWriter::new(File::create(&path)?),
            path,
            relation_hasher,
            expected_count,
            record_count: 0,
            encoded_byte_size: 0,
        })
    }

    fn write<T: Serialize>(
        &mut self,
        ordinal: u64,
        record_hash: String,
        record: &T,
    ) -> anyhow::Result<()> {
        let expected_ordinal = self
            .record_count
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("Portal projection relation count overflow"))?;
        if ordinal != expected_ordinal {
            return Err(anyhow::anyhow!(
                "Portal projection ordinal gap: expected={expected_ordinal} got={ordinal}"
            ));
        }
        update_framed_fields(
            &mut self.relation_hasher,
            &[
                Some(ordinal.to_string().as_str()),
                Some(record_hash.as_str()),
            ],
        )?;
        let spool = SpoolRecord {
            ordinal,
            record_hash,
            record,
        };
        let spool_bytes = canonical_json_bytes(&spool)?;
        self.writer.write_all(spool_bytes.as_slice())?;
        self.writer.write_all(b"\n")?;
        self.encoded_byte_size = self
            .encoded_byte_size
            .checked_add(u64::try_from(spool_bytes.len() + 1)?)
            .ok_or_else(|| anyhow::anyhow!("Portal projection relation byte size overflow"))?;
        self.record_count = self
            .record_count
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("Portal projection relation count overflow"))?;
        Ok(())
    }

    fn finish(
        mut self,
        relation: &'static str,
        schema_version: &'static str,
    ) -> anyhow::Result<PreparedPortalRelation> {
        if self.record_count != self.expected_count {
            return Err(anyhow::anyhow!(
                "Portal projection relation count mismatch: expected={} observed={}",
                self.expected_count,
                self.record_count
            ));
        }
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        Ok(PreparedPortalRelation {
            relation,
            schema_version,
            record_count: self.record_count,
            encoded_byte_size: self.encoded_byte_size,
            relation_hash: hex::encode(self.relation_hasher.finalize()),
            local_path: self.path,
        })
    }
}

pub fn canonical_portal_decimal(value: f64) -> anyhow::Result<String> {
    if !value.is_finite() {
        return Err(anyhow::anyhow!("Portal LCIA decimal source must be finite"));
    }
    if value == 0.0 {
        return Ok("0".to_owned());
    }
    let shortest = value.to_string();
    let fixed = expand_exponent_notation(shortest.as_str())?;
    validate_canonical_decimal(fixed.as_str())?;
    let parsed = fixed.parse::<f64>()?;
    if parsed.to_bits() != value.to_bits() {
        return Err(anyhow::anyhow!(
            "Portal LCIA decimal does not round-trip to the source binary64"
        ));
    }
    Ok(fixed)
}

pub fn validate_canonical_decimal(value: &str) -> anyhow::Result<()> {
    if value.is_empty()
        || value.contains(['e', 'E', '+'])
        || value.starts_with('.')
        || value.ends_with('.')
        || value == "-0"
    {
        return Err(anyhow::anyhow!("Portal LCIA decimal is not canonical"));
    }
    let unsigned = value.strip_prefix('-').unwrap_or(value);
    let mut parts = unsigned.split('.');
    let integer = parts.next().unwrap_or_default();
    let fraction = parts.next();
    if parts.next().is_some()
        || integer.is_empty()
        || !integer.bytes().all(|byte| byte.is_ascii_digit())
        || fraction.is_some_and(|digits| {
            digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit())
        })
        || (integer.len() > 1 && integer.starts_with('0'))
        || fraction.is_some_and(|digits| digits.ends_with('0'))
    {
        return Err(anyhow::anyhow!("Portal LCIA decimal is not canonical"));
    }
    if integer == "0" && fraction.is_none() && value.starts_with('-') {
        return Err(anyhow::anyhow!("Portal LCIA decimal is negative zero"));
    }
    let digits = integer.len() + fraction.map_or(0, str::len);
    if digits == 0 || digits > 38 {
        return Err(anyhow::anyhow!(
            "Portal LCIA decimal precision exceeds 38 digits"
        ));
    }
    Ok(())
}

fn expand_exponent_notation(value: &str) -> anyhow::Result<String> {
    let (negative, unsigned) = value
        .strip_prefix('-')
        .map_or((false, value), |unsigned| (true, unsigned));
    let Some((mantissa, exponent)) = unsigned.split_once(['e', 'E']) else {
        return Ok(value.to_owned());
    };
    let exponent = exponent.parse::<i32>()?;
    let decimal_index = mantissa.find('.').unwrap_or(mantissa.len());
    let digits = mantissa.replace('.', "");
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(anyhow::anyhow!("invalid binary64 shortest representation"));
    }
    let shifted = i64::try_from(decimal_index)? + i64::from(exponent);
    let mut fixed = if shifted <= 0 {
        let zero_count = usize::try_from(-shifted)?;
        format!("0.{}{}", "0".repeat(zero_count), digits)
    } else if usize::try_from(shifted)? >= digits.len() {
        let zero_count = usize::try_from(shifted)? - digits.len();
        format!("{}{}", digits, "0".repeat(zero_count))
    } else {
        let split = usize::try_from(shifted)?;
        format!("{}.{}", &digits[..split], &digits[split..])
    };
    if let Some(dot) = fixed.find('.') {
        while fixed.ends_with('0') {
            fixed.pop();
        }
        if fixed.len() == dot + 1 {
            fixed.pop();
        }
    }
    if negative {
        fixed.insert(0, '-');
    }
    Ok(fixed)
}

#[allow(clippy::too_many_lines)]
pub fn prepare_portal_lcia_projection(
    processes: &[PortalProcessSource],
    impacts: &[PortalImpactSource],
    shards: &[PortalLciaShard],
) -> anyhow::Result<PreparedPortalLciaProjection> {
    if processes.is_empty() || impacts.is_empty() || shards.is_empty() {
        return Err(anyhow::anyhow!(
            "Portal LCIA projection requires non-empty process, impact, and value axes"
        ));
    }
    validate_axis_indices(processes.iter().map(|item| item.process_index), "process")?;
    validate_axis_indices(impacts.iter().map(|item| item.impact_index), "impact")?;
    let directory = Arc::new(
        tempfile::Builder::new()
            .prefix("portal-lcia-projection-")
            .tempdir()?,
    );
    let spool_root = directory.path();

    let process_count = u64::try_from(processes.len())?;
    let impact_count = u64::try_from(impacts.len())?;
    let expected_value_count = process_count
        .checked_mul(impact_count)
        .ok_or_else(|| anyhow::anyhow!("Portal LCIA Cartesian grid size overflow"))?;
    let mut process_writer = RelationWriter::create(
        spool_root.join("process.ndjson"),
        "process-axis",
        process_count,
    )?;
    for process in processes {
        require_nonempty(&process.process_version, "processVersion")?;
        validate_sha256(&process.process_document_sha256, "processDocumentSha256")?;
        require_nonempty(&process.reference_flow_version, "referenceFlowVersion")?;
        validate_internal_id(
            &process.reference_exchange_internal_id,
            "referenceExchangeInternalId",
        )?;
        let reference_flow_direction = process.reference_flow_direction.trim().to_ascii_lowercase();
        if !matches!(reference_flow_direction.as_str(), "input" | "output") {
            return Err(anyhow::anyhow!(
                "Portal LCIA projection referenceFlowDirection is invalid"
            ));
        }
        require_nonempty(&process.functional_unit_unit, "functionalUnitUnit")?;
        let functional_unit_description = normalize_localized_text(
            &process.functional_unit_description,
            "functionalUnitDescription",
        )?;
        require_nonempty(&process.geography_code, "geographyCode")?;
        let geography_precision = process.geography_precision.trim().to_ascii_lowercase();
        if !matches!(
            geography_precision.as_str(),
            "country" | "province" | "city" | "other" | "unknown"
        ) {
            return Err(anyhow::anyhow!(
                "Portal LCIA projection geographyPrecision is invalid"
            ));
        }
        if !(0..=9999).contains(&process.reference_year) {
            return Err(anyhow::anyhow!(
                "Portal LCIA projection referenceYear is invalid"
            ));
        }
        let record = PortalProcessRecord {
            process_index: process.process_index,
            process_id: process.process_id,
            process_version: process.process_version.clone(),
            process_document_sha256: process.process_document_sha256.clone(),
            reference_flow_id: process.reference_flow_id,
            reference_flow_version: process.reference_flow_version.clone(),
            reference_exchange_internal_id: process.reference_exchange_internal_id.clone(),
            reference_flow_amount: canonical_portal_decimal(process.reference_flow_amount)?,
            reference_flow_direction,
            functional_unit_amount: canonical_portal_decimal(process.functional_unit_amount)?,
            functional_unit_unit: process.functional_unit_unit.trim().to_owned(),
            functional_unit_description,
            geography_code: process.geography_code.trim().to_owned(),
            geography_precision,
            reference_year: process.reference_year,
        };
        process_writer.write(
            record.process_index + 1,
            process_record_hash(&record)?,
            &record,
        )?;
    }
    let process_relation =
        process_writer.finish("process-axis", PORTAL_LCIA_PROCESS_SCHEMA_VERSION)?;

    let mut impact_writer = RelationWriter::create(
        spool_root.join("impact.ndjson"),
        "impact-axis",
        impact_count,
    )?;
    for impact in impacts {
        require_nonempty(&impact.method_version, "methodVersion")?;
        validate_sha256(&impact.method_document_sha256, "methodDocumentSha256")?;
        require_nonempty(&impact.impact_category_id, "impactCategoryId")?;
        let impact_name = normalize_localized_text(&impact.impact_name, "impactName")?;
        require_nonempty(&impact.result_unit, "resultUnit")?;
        let record = PortalImpactRecord {
            impact_index: impact.impact_index,
            method_id: impact.method_id,
            method_version: impact.method_version.clone(),
            method_document_sha256: impact.method_document_sha256.clone(),
            impact_category_id: impact.impact_category_id.trim().to_owned(),
            impact_name,
            result_unit: impact.result_unit.trim().to_owned(),
        };
        impact_writer.write(
            record.impact_index + 1,
            impact_record_hash(&record)?,
            &record,
        )?;
    }
    let impact_relation = impact_writer.finish("impact-axis", PORTAL_LCIA_IMPACT_SCHEMA_VERSION)?;

    let chunk_descriptor_set_hash = chunk_descriptor_set_hash(shards)?;
    let mut value_writer = RelationWriter::create(
        spool_root.join("value.ndjson"),
        "value-grid",
        expected_value_count,
    )?;
    let mut expected_value_ordinal = 0_u64;
    for (expected_chunk_ordinal, shard) in shards.iter().enumerate() {
        if shard.chunk_ordinal != u64::try_from(expected_chunk_ordinal)? {
            return Err(anyhow::anyhow!(
                "Portal LCIA chunk ordinal gap: expected={expected_chunk_ordinal} got={}",
                shard.chunk_ordinal
            ));
        }
        let expected_first = expected_value_ordinal / impact_count;
        if shard.first_process_ordinal != expected_first
            || shard.last_process_ordinal < shard.first_process_ordinal
            || shard.last_process_ordinal >= process_count
        {
            return Err(anyhow::anyhow!(
                "Portal LCIA chunk process range is not contiguous"
            ));
        }
        verify_sha256_file(&shard.local_path, shard.byte_size, shard.sha256.as_str())?;
        visit_verified_lcia_shard(shard, |source| {
            if expected_value_ordinal >= expected_value_count {
                return Err(anyhow::anyhow!("Portal LCIA grid has extra values"));
            }
            let process_ordinal = expected_value_ordinal / impact_count;
            let impact_ordinal = expected_value_ordinal % impact_count;
            let impact = &impacts[usize::try_from(impact_ordinal)?];
            if source.process_index != process_ordinal
                || source.method.id != impact.method_id
                || source.method.version != impact.method_version
            {
                return Err(anyhow::anyhow!(
                    "Portal LCIA grid is missing, duplicated, or reordered at value ordinal {expected_value_ordinal}"
                ));
            }
            let record = PortalValueRecord {
                ordinal: expected_value_ordinal + 1,
                process_index: process_ordinal,
                impact_index: impact_ordinal,
                value_text: canonical_portal_decimal(source.mean_amount)?,
            };
            value_writer.write(record.ordinal, value_record_hash(&record)?, &record)?;
            expected_value_ordinal += 1;
            Ok(())
        })?;
        let expected_last = (expected_value_ordinal - 1) / impact_count;
        if shard.last_process_ordinal != expected_last {
            return Err(anyhow::anyhow!(
                "Portal LCIA chunk last process ordinal does not match its verified records"
            ));
        }
    }
    if expected_value_ordinal != expected_value_count {
        return Err(anyhow::anyhow!(
            "Portal LCIA Cartesian grid is incomplete: expected={expected_value_count} observed={expected_value_ordinal}"
        ));
    }
    let value_relation = value_writer.finish("value-grid", PORTAL_LCIA_VALUE_SCHEMA_VERSION)?;

    let relation_hash = relation_set_hash(&process_relation, &impact_relation, &value_relation)?;
    Ok(PreparedPortalLciaProjection {
        contract_version: PORTAL_LCIA_PROJECTION_CONTRACT_VERSION,
        decimal_contract_version: PORTAL_LCIA_DECIMAL_CONTRACT_VERSION,
        hash_contract_version: PORTAL_LCIA_HASH_CONTRACT_VERSION,
        chunk_descriptor_set_hash,
        relation_hash,
        process_relation,
        impact_relation,
        value_relation,
        directory_guard: directory,
    })
}

fn validate_axis_indices<I>(indices: I, axis: &str) -> anyhow::Result<()>
where
    I: IntoIterator<Item = u64>,
{
    for (expected, observed) in indices.into_iter().enumerate() {
        if observed != u64::try_from(expected)? {
            return Err(anyhow::anyhow!(
                "Portal LCIA {axis} axis is not unique and contiguous: expected={expected} got={observed}"
            ));
        }
    }
    Ok(())
}

fn visit_verified_lcia_shard<F>(shard: &PortalLciaShard, mut visitor: F) -> anyhow::Result<()>
where
    F: FnMut(BundleLciaRecord) -> anyhow::Result<()>,
{
    let decoder = GzDecoder::new(File::open(&shard.local_path)?);
    let mut reader = BufReader::new(decoder);
    let mut line = Vec::new();
    let mut plain_hasher = Sha256::new();
    let mut plain_bytes = 0_u64;
    let mut records = 0_u64;
    loop {
        line.clear();
        let read = reader.read_until(b'\n', &mut line)?;
        if read == 0 {
            break;
        }
        plain_hasher.update(line.as_slice());
        plain_bytes = plain_bytes
            .checked_add(u64::try_from(read)?)
            .ok_or_else(|| anyhow::anyhow!("Portal LCIA shard byte size overflow"))?;
        if line.last() != Some(&b'\n') {
            return Err(anyhow::anyhow!(
                "Portal LCIA shard has an unterminated NDJSON record"
            ));
        }
        line.pop();
        if line.last() == Some(&b'\r') || line.is_empty() {
            return Err(anyhow::anyhow!(
                "Portal LCIA shard contains non-canonical NDJSON framing"
            ));
        }
        visitor(serde_json::from_slice(line.as_slice())?)?;
        records = records
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("Portal LCIA shard record count overflow"))?;
    }
    if records != shard.record_count
        || plain_bytes != shard.uncompressed_byte_size
        || hex::encode(plain_hasher.finalize()) != shard.uncompressed_sha256
    {
        return Err(anyhow::anyhow!(
            "Portal LCIA shard uncompressed count/size/hash verification failed"
        ));
    }
    Ok(())
}

fn verify_sha256_file(path: &Path, expected_size: u64, expected_hash: &str) -> anyhow::Result<()> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut observed_size = 0_u64;
    let mut buffer = vec![0_u8; 64 * 1024].into_boxed_slice();
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        observed_size = observed_size
            .checked_add(u64::try_from(read)?)
            .ok_or_else(|| anyhow::anyhow!("Portal LCIA shard compressed size overflow"))?;
    }
    if observed_size != expected_size || hex::encode(hasher.finalize()) != expected_hash {
        return Err(anyhow::anyhow!(
            "Portal LCIA shard compressed size/hash verification failed"
        ));
    }
    Ok(())
}

fn chunk_descriptor_set_hash(shards: &[PortalLciaShard]) -> anyhow::Result<String> {
    let mut hasher = Sha256::new();
    update_framed_fields(
        &mut hasher,
        &[
            Some("portal.lcia-projection.chunk-descriptor-set.v1"),
            Some(PORTAL_LCIA_HASH_CONTRACT_VERSION),
            Some(shards.len().to_string().as_str()),
        ],
    )?;
    for shard in shards {
        update_framed_fields(
            &mut hasher,
            &[
                Some((shard.chunk_ordinal + 1).to_string().as_str()),
                Some(shard.first_process_ordinal.to_string().as_str()),
                Some(shard.last_process_ordinal.to_string().as_str()),
                Some(shard.sha256.as_str()),
                Some(shard.uncompressed_sha256.as_str()),
                Some(shard.byte_size.to_string().as_str()),
                Some(shard.uncompressed_byte_size.to_string().as_str()),
                Some(shard.record_count.to_string().as_str()),
            ],
        )?;
    }
    Ok(hex::encode(hasher.finalize()))
}

fn relation_set_hash(
    process: &PreparedPortalRelation,
    impact: &PreparedPortalRelation,
    value: &PreparedPortalRelation,
) -> anyhow::Result<String> {
    sha256_fields(&[
        Some("portal.lcia-projection.grid-relation.v1"),
        Some(PORTAL_LCIA_HASH_CONTRACT_VERSION),
        Some(process.record_count.to_string().as_str()),
        Some(impact.record_count.to_string().as_str()),
        Some(value.record_count.to_string().as_str()),
        Some("ordinal=processIndex*impactCount+impactIndex+1"),
        Some(process.relation_hash.as_str()),
        Some(impact.relation_hash.as_str()),
        Some(value.relation_hash.as_str()),
    ])
}

fn content_hash(
    chunk_descriptor_set_hash: &str,
    relation_hash: &str,
    source: &PortalProjectionSourceBinding,
    process: &PreparedPortalRelation,
    impact: &PreparedPortalRelation,
    value: &PreparedPortalRelation,
) -> anyhow::Result<String> {
    sha256_fields(&[
        Some("portal.lcia-projection.content.v1"),
        Some(PORTAL_LCIA_HASH_CONTRACT_VERSION),
        Some(PORTAL_LCIA_PROJECTION_CONTRACT_VERSION),
        Some(source.input_manifest_hash.as_str()),
        Some(source.closure_certificate_hash.as_str()),
        Some(source.snapshot_hash.as_str()),
        Some(source.closure_bundle_hash.as_str()),
        Some(source.snapshot_index_sha256.as_str()),
        Some(source.snapshot_build_contract_hash.as_str()),
        Some(source.bundle_content_hash.as_str()),
        Some(source.bundle_manifest_sha256.as_str()),
        Some(chunk_descriptor_set_hash),
        Some(source.result_artifact_sha256.as_str()),
        Some(source.query_artifact_sha256.as_str()),
        Some(process.record_count.to_string().as_str()),
        Some(impact.record_count.to_string().as_str()),
        Some(value.record_count.to_string().as_str()),
        Some(process.relation_hash.as_str()),
        Some(impact.relation_hash.as_str()),
        Some(value.relation_hash.as_str()),
        Some(relation_hash),
    ])
}

fn process_record_hash(record: &PortalProcessRecord) -> anyhow::Result<String> {
    let localized = localized_text_frame_hex(&record.functional_unit_description)?;
    sha256_fields(&[
        Some(PORTAL_LCIA_PROCESS_SCHEMA_VERSION),
        Some(PORTAL_LCIA_HASH_CONTRACT_VERSION),
        Some(record.process_index.to_string().as_str()),
        Some(record.process_id.to_string().as_str()),
        Some(record.process_version.as_str()),
        Some(record.reference_flow_id.to_string().as_str()),
        Some(record.reference_flow_version.as_str()),
        Some(record.reference_exchange_internal_id.as_str()),
        Some(record.reference_flow_amount.as_str()),
        Some(record.reference_flow_direction.as_str()),
        Some(record.functional_unit_amount.as_str()),
        Some(record.functional_unit_unit.as_str()),
        Some(localized.as_str()),
        Some(record.geography_code.as_str()),
        Some(record.geography_precision.as_str()),
        Some(record.reference_year.to_string().as_str()),
        Some(record.process_document_sha256.as_str()),
    ])
}

fn impact_record_hash(record: &PortalImpactRecord) -> anyhow::Result<String> {
    let localized = localized_text_frame_hex(&record.impact_name)?;
    sha256_fields(&[
        Some(PORTAL_LCIA_IMPACT_SCHEMA_VERSION),
        Some(PORTAL_LCIA_HASH_CONTRACT_VERSION),
        Some(record.impact_index.to_string().as_str()),
        Some(record.method_id.to_string().as_str()),
        Some(record.method_version.as_str()),
        Some(record.impact_category_id.as_str()),
        Some(localized.as_str()),
        Some(record.result_unit.as_str()),
        Some(record.method_document_sha256.as_str()),
    ])
}

fn value_record_hash(record: &PortalValueRecord) -> anyhow::Result<String> {
    sha256_fields(&[
        Some(PORTAL_LCIA_VALUE_SCHEMA_VERSION),
        Some(PORTAL_LCIA_HASH_CONTRACT_VERSION),
        Some(record.ordinal.to_string().as_str()),
        Some(record.process_index.to_string().as_str()),
        Some(record.impact_index.to_string().as_str()),
        Some(record.value_text.as_str()),
    ])
}

fn localized_text_frame_hex(values: &[PortalLocalizedText]) -> anyhow::Result<String> {
    let mut bytes = Vec::new();
    append_frame(&mut bytes, Some(values.len().to_string().as_str()))?;
    for value in values {
        append_frame(&mut bytes, Some(value.language.as_str()))?;
        append_frame(&mut bytes, Some(value.value.as_str()))?;
    }
    Ok(hex::encode(bytes))
}

fn sha256_fields(fields: &[Option<&str>]) -> anyhow::Result<String> {
    let mut hasher = Sha256::new();
    update_framed_fields(&mut hasher, fields)?;
    Ok(hex::encode(hasher.finalize()))
}

fn update_framed_fields(hasher: &mut Sha256, fields: &[Option<&str>]) -> anyhow::Result<()> {
    for field in fields {
        let mut frame = Vec::new();
        append_frame(&mut frame, *field)?;
        hasher.update(frame);
    }
    Ok(())
}

fn append_frame(buffer: &mut Vec<u8>, field: Option<&str>) -> anyhow::Result<()> {
    match field {
        Some(field) => {
            let length = i32::try_from(field.len())?;
            buffer.extend_from_slice(&length.to_be_bytes());
            buffer.extend_from_slice(field.as_bytes());
        }
        None => buffer.extend_from_slice(&(-1_i32).to_be_bytes()),
    }
    Ok(())
}

fn canonical_json_bytes<T: Serialize>(value: &T) -> anyhow::Result<Vec<u8>> {
    Ok(serde_json::to_vec(value)?)
}

fn require_nonempty(value: &str, field: &str) -> anyhow::Result<()> {
    if value.trim().is_empty() {
        Err(anyhow::anyhow!(
            "Portal LCIA projection {field} must not be empty"
        ))
    } else {
        Ok(())
    }
}

fn validate_sha256(value: &str, field: &str) -> anyhow::Result<()> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "Portal LCIA projection {field} is not a lowercase SHA-256"
        ))
    }
}

fn validate_internal_id(value: &str, field: &str) -> anyhow::Result<()> {
    let parsed = value.parse::<u64>()?;
    if parsed <= 999_999 && parsed.to_string() == value {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "Portal LCIA projection {field} is not a canonical Int6"
        ))
    }
}

fn normalize_localized_text(
    values: &[PortalLocalizedText],
    field: &str,
) -> anyhow::Result<Vec<PortalLocalizedText>> {
    if values.is_empty() {
        return Err(anyhow::anyhow!(
            "Portal LCIA projection {field} must not be empty"
        ));
    }
    if values.len() > 64 {
        return Err(anyhow::anyhow!(
            "Portal LCIA projection {field} contains too many localized values"
        ));
    }
    let mut normalized = values
        .iter()
        .map(|value| {
            let language = value.language.trim().to_ascii_lowercase();
            let text = value.value.trim();
            if value.value.chars().any(char::is_control)
                || !portal_language_tag_valid(&language)
                || !portal_public_text_valid(text, 4_096)
            {
                return Err(anyhow::anyhow!(
                    "Portal LCIA projection {field} contains invalid localized text"
                ));
            }
            Ok(PortalLocalizedText {
                language,
                value: text.to_owned(),
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    normalized.sort_by(|left, right| {
        left.language
            .cmp(&right.language)
            .then_with(|| left.value.cmp(&right.value))
    });
    if normalized
        .windows(2)
        .any(|pair| pair[0].language == pair[1].language)
    {
        return Err(anyhow::anyhow!(
            "Portal LCIA projection {field} contains duplicate language tags"
        ));
    }
    Ok(normalized)
}

fn portal_language_tag_valid(language: &str) -> bool {
    if language.len() > 35 || !language.is_ascii() {
        return false;
    }
    let mut segments = language.split('-');
    let Some(primary) = segments.next() else {
        return false;
    };
    if !(2..=3).contains(&primary.len()) || !primary.bytes().all(|byte| byte.is_ascii_lowercase()) {
        return false;
    }
    segments.all(|segment| {
        (2..=8).contains(&segment.len())
            && segment
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
    })
}

fn portal_public_text_valid(text: &str, max_length: usize) -> bool {
    if text.is_empty() || text.chars().count() > max_length || text.chars().any(char::is_control) {
        return false;
    }
    let lower = text.to_ascii_lowercase();
    if ["http://", "https://", "s3://", "gs://", "file://"]
        .iter()
        .any(|scheme| lower.contains(scheme))
    {
        return false;
    }
    !text.as_bytes().windows(2).enumerate().any(|(index, pair)| {
        pair == b".."
            && (index == 0 || matches!(text.as_bytes()[index - 1], b'/' | b'\\'))
            && (index + 2 == text.len() || matches!(text.as_bytes()[index + 2], b'/' | b'\\'))
    })
}

fn validate_source_binding(source: &PortalProjectionSourceBinding) -> anyhow::Result<()> {
    for (field, value) in [
        ("inputManifestHash", source.input_manifest_hash.as_str()),
        (
            "closureCertificateHash",
            source.closure_certificate_hash.as_str(),
        ),
        ("snapshotHash", source.snapshot_hash.as_str()),
        ("closureBundleHash", source.closure_bundle_hash.as_str()),
        ("snapshotIndexSha256", source.snapshot_index_sha256.as_str()),
        (
            "snapshotBuildContractHash",
            source.snapshot_build_contract_hash.as_str(),
        ),
        ("bundleContentHash", source.bundle_content_hash.as_str()),
        (
            "bundleManifestSha256",
            source.bundle_manifest_sha256.as_str(),
        ),
        (
            "resultArtifactSha256",
            source.result_artifact_sha256.as_str(),
        ),
        ("queryArtifactSha256", source.query_artifact_sha256.as_str()),
    ] {
        validate_sha256(value, field)?;
    }
    Ok(())
}

impl PreparedPortalLciaProjection {
    #[must_use]
    pub fn relations(&self) -> [&PreparedPortalRelation; 3] {
        [
            &self.process_relation,
            &self.impact_relation,
            &self.value_relation,
        ]
    }

    pub fn bind_source(
        self,
        source: PortalProjectionSourceBinding,
    ) -> anyhow::Result<BoundPortalLciaProjection> {
        validate_source_binding(&source)?;
        let content_hash = content_hash(
            self.chunk_descriptor_set_hash.as_str(),
            self.relation_hash.as_str(),
            &source,
            &self.process_relation,
            &self.impact_relation,
            &self.value_relation,
        )?;
        Ok(BoundPortalLciaProjection {
            contract_version: self.contract_version,
            decimal_contract_version: self.decimal_contract_version,
            hash_contract_version: self.hash_contract_version,
            content_hash,
            chunk_descriptor_set_hash: self.chunk_descriptor_set_hash,
            relation_hash: self.relation_hash,
            source,
            process_relation: self.process_relation,
            impact_relation: self.impact_relation,
            value_relation: self.value_relation,
            directory_guard: self.directory_guard,
        })
    }
}

impl BoundPortalLciaProjection {
    #[must_use]
    pub fn relations(&self) -> [&PreparedPortalRelation; 3] {
        [
            &self.process_relation,
            &self.impact_relation,
            &self.value_relation,
        ]
    }

    #[must_use]
    pub fn stage_descriptor(&self) -> Value {
        json!({
            "contractVersion": self.contract_version,
            "decimalContractVersion": self.decimal_contract_version,
            "hashContractVersion": self.hash_contract_version,
            "contentHash": self.content_hash,
            "chunkDescriptorSetHash": self.chunk_descriptor_set_hash,
            "relationHash": self.relation_hash,
            "source": &self.source,
            "relations": self.relations(),
        })
    }
}

impl PreparedPortalRelation {
    pub fn batches(&self) -> anyhow::Result<PortalProjectionBatchReader> {
        PortalProjectionBatchReader::new(self.clone())
    }
}

impl PortalProjectionBatchReader {
    fn new(relation: PreparedPortalRelation) -> anyhow::Result<Self> {
        Ok(Self {
            reader: BufReader::new(File::open(&relation.local_path)?),
            relation,
            batch_ordinal: 0,
            next_line: None,
            next_expected_ordinal: 1,
            exhausted: false,
        })
    }

    #[allow(clippy::too_many_lines)]
    pub fn next_batch(&mut self) -> anyhow::Result<Option<PortalProjectionBatch>> {
        if self.exhausted {
            return Ok(None);
        }
        let mut records = Vec::<Value>::new();
        let mut records_encoded_bytes = 2_usize;
        let mut first_ordinal = None;
        let mut last_ordinal = None;
        while records.len() < PORTAL_LCIA_MAX_BATCH_RECORDS {
            let line = if let Some(line) = self.next_line.take() {
                line
            } else {
                let mut line = Vec::new();
                if self.reader.read_until(b'\n', &mut line)? == 0 {
                    self.exhausted = true;
                    break;
                }
                line
            };
            if line.last() != Some(&b'\n') {
                return Err(anyhow::anyhow!(
                    "Portal projection spool contains an unterminated record"
                ));
            }
            let prospective = records_encoded_bytes
                .checked_add(line.len())
                .ok_or_else(|| anyhow::anyhow!("Portal projection batch byte size overflow"))?;
            if prospective > PORTAL_LCIA_MAX_BATCH_ENCODED_BYTES - BATCH_ENVELOPE_RESERVE_BYTES {
                if records.is_empty() {
                    return Err(anyhow::anyhow!(
                        "one Portal projection record exceeds the encoded batch byte cap"
                    ));
                }
                self.next_line = Some(line);
                break;
            }
            let spool: SpoolRecord<Value> = serde_json::from_slice(&line[..line.len() - 1])?;
            if spool.ordinal != self.next_expected_ordinal {
                return Err(anyhow::anyhow!(
                    "Portal projection spool ordinal gap: expected={} got={}",
                    self.next_expected_ordinal,
                    spool.ordinal
                ));
            }
            first_ordinal.get_or_insert(spool.ordinal);
            last_ordinal = Some(spool.ordinal);
            self.next_expected_ordinal += 1;
            records_encoded_bytes = prospective;
            records.push(spool.record);
        }
        if records.is_empty() {
            if self.next_expected_ordinal
                != self
                    .relation
                    .record_count
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("Portal projection relation count overflow"))?
            {
                return Err(anyhow::anyhow!(
                    "Portal projection spool count mismatch: expected={} observed={}",
                    self.relation.record_count,
                    self.next_expected_ordinal
                ));
            }
            return Ok(None);
        }
        let first_ordinal = first_ordinal
            .ok_or_else(|| anyhow::anyhow!("Portal projection batch omitted first ordinal"))?;
        let last_ordinal = last_ordinal
            .ok_or_else(|| anyhow::anyhow!("Portal projection batch omitted last ordinal"))?;
        let record_count = records.len();
        let (processes, impacts, values) = match self.relation.relation {
            "process-axis" => (records, Vec::new(), Vec::new()),
            "impact-axis" => (Vec::new(), records, Vec::new()),
            "value-grid" => (Vec::new(), Vec::new(), records),
            relation => {
                return Err(anyhow::anyhow!(
                    "unsupported Portal projection relation: {relation}"
                ));
            }
        };
        let payload = json!({
            "schemaVersion": "portal.lcia-projection.batch.v1",
            "processes": processes,
            "impacts": impacts,
            "values": values,
        });
        let encoded_bytes = canonical_json_bytes(&payload)?.len();
        if encoded_bytes > PORTAL_LCIA_MAX_BATCH_ENCODED_BYTES {
            return Err(anyhow::anyhow!(
                "Portal projection batch exceeds the encoded byte cap"
            ));
        }
        let batch = PortalProjectionBatch {
            relation: self.relation.relation,
            batch_ordinal: self.batch_ordinal,
            first_ordinal,
            last_ordinal,
            record_count,
            payload,
            encoded_bytes,
        };
        self.batch_ordinal += 1;
        Ok(Some(batch))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use flate2::{Compression, GzBuilder, write::GzEncoder};
    use tempfile::TempDir;

    use super::*;

    fn fixture_axes() -> (Vec<PortalProcessSource>, Vec<PortalImpactSource>) {
        let processes = (0..2)
            .map(|ordinal| PortalProcessSource {
                process_index: ordinal,
                process_id: Uuid::from_u128(10 + u128::from(ordinal)),
                process_version: "01.00.000".to_owned(),
                process_document_sha256: "a".repeat(64),
                reference_flow_id: Uuid::from_u128(20 + u128::from(ordinal)),
                reference_flow_version: "01.00.000".to_owned(),
                reference_exchange_internal_id: ordinal.to_string(),
                reference_flow_amount: 2.0,
                reference_flow_direction: "output".to_owned(),
                functional_unit_amount: 1.0,
                functional_unit_unit: "kg".to_owned(),
                functional_unit_description: vec![PortalLocalizedText {
                    language: "en".to_owned(),
                    value: format!("process-{ordinal}"),
                }],
                geography_code: "CN".to_owned(),
                geography_precision: "country".to_owned(),
                reference_year: 2025,
            })
            .collect();
        let impacts = (0..2)
            .map(|ordinal| PortalImpactSource {
                impact_index: ordinal,
                method_id: Uuid::from_u128(30 + u128::from(ordinal)),
                method_version: "01.00.000".to_owned(),
                method_document_sha256: "b".repeat(64),
                impact_category_id: format!("impact-{ordinal}"),
                impact_name: vec![PortalLocalizedText {
                    language: "en".to_owned(),
                    value: format!("Impact {ordinal}"),
                }],
                result_unit: "kg CO2-eq".to_owned(),
            })
            .collect();
        (processes, impacts)
    }

    fn fixture_shard(
        directory: &TempDir,
        impacts: &[PortalImpactSource],
        values: &[(u64, usize, f64)],
    ) -> PortalLciaShard {
        let path = directory.path().join("lcia.ndjson.gz");
        let file = File::create(&path).unwrap();
        let mut encoder: GzEncoder<File> =
            GzBuilder::new().mtime(0).write(file, Compression::new(6));
        let mut plain = Vec::new();
        for (process_index, impact_index, value) in values {
            let row = json!({
                "processIndex": process_index,
                "method": {
                    "id": impacts[*impact_index].method_id,
                    "version": impacts[*impact_index].method_version,
                },
                "meanAmount": value,
            });
            let mut bytes = serde_json::to_vec(&row).unwrap();
            bytes.push(b'\n');
            encoder.write_all(&bytes).unwrap();
            plain.extend_from_slice(&bytes);
        }
        let file = encoder.finish().unwrap();
        file.sync_all().unwrap();
        PortalLciaShard {
            chunk_ordinal: 0,
            first_process_ordinal: values.first().unwrap().0,
            last_process_ordinal: values.last().unwrap().0,
            sha256: file_sha256(&path),
            uncompressed_sha256: hex::encode(Sha256::digest(&plain)),
            byte_size: std::fs::metadata(&path).unwrap().len(),
            uncompressed_byte_size: u64::try_from(plain.len()).unwrap(),
            record_count: u64::try_from(values.len()).unwrap(),
            local_path: path,
        }
    }

    fn file_sha256(path: &Path) -> String {
        let bytes = std::fs::read(path).unwrap();
        hex::encode(Sha256::digest(bytes))
    }

    fn source_binding() -> PortalProjectionSourceBinding {
        PortalProjectionSourceBinding {
            input_manifest_hash: "0".repeat(64),
            closure_certificate_hash: "1".repeat(64),
            snapshot_hash: "2".repeat(64),
            closure_bundle_hash: "3".repeat(64),
            snapshot_index_sha256: "4".repeat(64),
            snapshot_build_contract_hash: "5".repeat(64),
            bundle_content_hash: "6".repeat(64),
            bundle_manifest_sha256: "7".repeat(64),
            result_artifact_sha256: "8".repeat(64),
            query_artifact_sha256: "9".repeat(64),
        }
    }

    #[test]
    fn decimal_is_shortest_fixed_and_round_trips() {
        for value in [0.0, -0.0, 1.0, -1.25, 1.0e-12, 1.0e20] {
            let decimal = canonical_portal_decimal(value).unwrap();
            assert!(!decimal.contains(['e', 'E']));
            let expected_bits = if value == 0.0 {
                0.0_f64.to_bits()
            } else {
                value.to_bits()
            };
            assert_eq!(decimal.parse::<f64>().unwrap().to_bits(), expected_bits);
        }
        assert_eq!(canonical_portal_decimal(-0.0).unwrap(), "0");
        assert!(canonical_portal_decimal(f64::NAN).is_err());
        assert!(canonical_portal_decimal(f64::INFINITY).is_err());
        assert!(canonical_portal_decimal(f64::NEG_INFINITY).is_err());
        assert!(canonical_portal_decimal(f64::MIN_POSITIVE).is_err());
        assert!(canonical_portal_decimal(f64::from_bits(1)).is_err());
        assert!(canonical_portal_decimal(f64::MAX).is_err());
    }

    #[test]
    fn decimal_enforces_38_digit_precision_boundary() {
        assert!(validate_canonical_decimal("12345678901234567890123456789012345678").is_ok());
        assert!(validate_canonical_decimal("123456789012345678901234567890123456789").is_err());
        assert!(validate_canonical_decimal("1.230").is_err());
        assert!(validate_canonical_decimal("1e3").is_err());
        assert!(validate_canonical_decimal("-0").is_err());
    }

    #[test]
    fn localized_text_matches_database_language_and_public_text_guards() {
        for language in ["en", "zh-hans", "pt-br", "und", "EN-us"] {
            let normalized = normalize_localized_text(
                &[PortalLocalizedText {
                    language: language.to_owned(),
                    value: "Public label".to_owned(),
                }],
                "test",
            )
            .expect("valid language tag");
            assert_eq!(normalized[0].language, language.to_ascii_lowercase());
        }
        for language in ["e", "1n", "en-", "en-abcdefghi", "zh_zh", "éé"] {
            assert!(
                normalize_localized_text(
                    &[PortalLocalizedText {
                        language: language.to_owned(),
                        value: "Public label".to_owned(),
                    }],
                    "test",
                )
                .is_err(),
                "language tag should fail: {language}"
            );
        }
        for text in [
            "line\nbreak",
            "https://example.test/private",
            "FILE://private/path",
            "../private",
            "safe/../private",
        ] {
            assert!(
                normalize_localized_text(
                    &[PortalLocalizedText {
                        language: "en".to_owned(),
                        value: text.to_owned(),
                    }],
                    "test",
                )
                .is_err(),
                "public text should fail: {text:?}"
            );
        }
    }

    #[test]
    fn int32be_framing_matches_the_database_cross_language_vector() {
        assert_eq!(
            sha256_fields(&[Some("A"), Some("é"), None, Some("")]).unwrap(),
            "5a01047a86055adc7954e7411667d0ef91c64f0c9ff4550dce738aa4d2f4a6ea"
        );
    }

    #[test]
    fn record_hashes_match_the_database_domain_vectors() {
        let (processes, impacts) = fixture_axes();
        let process = &processes[0];
        let process_record = PortalProcessRecord {
            process_index: process.process_index,
            process_id: process.process_id,
            process_version: process.process_version.clone(),
            process_document_sha256: process.process_document_sha256.clone(),
            reference_flow_id: process.reference_flow_id,
            reference_flow_version: process.reference_flow_version.clone(),
            reference_exchange_internal_id: process.reference_exchange_internal_id.clone(),
            reference_flow_amount: canonical_portal_decimal(process.reference_flow_amount).unwrap(),
            reference_flow_direction: process.reference_flow_direction.clone(),
            functional_unit_amount: canonical_portal_decimal(process.functional_unit_amount)
                .unwrap(),
            functional_unit_unit: process.functional_unit_unit.clone(),
            functional_unit_description: process.functional_unit_description.clone(),
            geography_code: process.geography_code.clone(),
            geography_precision: process.geography_precision.clone(),
            reference_year: process.reference_year,
        };
        assert_eq!(
            process_record_hash(&process_record).unwrap(),
            "20eac36559a4bc196e480fdb4fd22acb565658de327327103ef23f9d0fce45a2"
        );

        let impact = &impacts[0];
        let impact_record = PortalImpactRecord {
            impact_index: impact.impact_index,
            method_id: impact.method_id,
            method_version: impact.method_version.clone(),
            method_document_sha256: impact.method_document_sha256.clone(),
            impact_category_id: impact.impact_category_id.clone(),
            impact_name: impact.impact_name.clone(),
            result_unit: impact.result_unit.clone(),
        };
        assert_eq!(
            impact_record_hash(&impact_record).unwrap(),
            "88c852ad1c3748da26420ab5b2d96fa604977847eea44862c3f09573b4551d45"
        );

        let value_record = PortalValueRecord {
            ordinal: 1,
            process_index: 0,
            impact_index: 0,
            value_text: "0".to_owned(),
        };
        assert_eq!(
            value_record_hash(&value_record).unwrap(),
            "0bcbcf38ddd7c709c3e0e1e55a68226c51c5bc18be404108794f04f5a37a7879"
        );
    }

    #[test]
    fn projection_is_deterministic_and_preserves_explicit_zero() {
        let directory = TempDir::new().unwrap();
        let (processes, impacts) = fixture_axes();
        let shard = fixture_shard(
            &directory,
            &impacts,
            &[(0, 0, 0.0), (0, 1, 1.5), (1, 0, -2.0), (1, 1, 3.0)],
        );
        let first =
            prepare_portal_lcia_projection(&processes, &impacts, std::slice::from_ref(&shard))
                .unwrap()
                .bind_source(source_binding())
                .unwrap();
        let second =
            prepare_portal_lcia_projection(&processes, &impacts, std::slice::from_ref(&shard))
                .unwrap()
                .bind_source(source_binding())
                .unwrap();
        assert_eq!(first.content_hash, second.content_hash);
        assert_eq!(
            first.process_relation.relation_hash,
            "246ad58816105072e2e2965a3eef142d53d9f3cab7a18a3860897eb0df158834"
        );
        assert_eq!(
            first.impact_relation.relation_hash,
            "d62569f24d158806b3041dfdd49fdaba0c5458e287e669b667fa811020a62164"
        );
        assert_eq!(
            first.value_relation.relation_hash,
            "5b4f9d81f34d8cf7b000bc04eaa44e78394f364e1153d508fbdc0af648550b3e"
        );
        assert_eq!(
            first.relation_hash,
            "2e955ee8542f9dc9cbf2bfbdc815cf18b0bde4b76317a1776ac73b336ee0ad9c"
        );
        assert_eq!(
            first.value_relation.relation_hash,
            second.value_relation.relation_hash
        );
        let text = std::fs::read_to_string(&first.value_relation.local_path).unwrap();
        assert!(text.contains("\"value\":\"0\""));
        assert!(text.contains("\"ordinal\":1"));
        assert_eq!(first.value_relation.record_count, 4);
    }

    #[test]
    fn projection_rejects_grid_holes_reorder_and_tamper() {
        let directory = TempDir::new().unwrap();
        let (processes, impacts) = fixture_axes();
        let hole = fixture_shard(
            &directory,
            &impacts,
            &[(0, 0, 1.0), (0, 1, 2.0), (1, 1, 4.0)],
        );
        assert!(prepare_portal_lcia_projection(&processes, &impacts, &[hole],).is_err());

        let other = TempDir::new().unwrap();
        let mut reordered = fixture_shard(
            &other,
            &impacts,
            &[(0, 1, 1.0), (0, 0, 2.0), (1, 0, 3.0), (1, 1, 4.0)],
        );
        assert!(
            prepare_portal_lcia_projection(&processes, &impacts, &[reordered.clone()],).is_err()
        );
        reordered.sha256 = "0".repeat(64);
        assert!(prepare_portal_lcia_projection(&processes, &impacts, &[reordered],).is_err());
    }

    #[test]
    fn projection_rejects_missing_context_and_axis_gaps() {
        let directory = TempDir::new().unwrap();
        let (mut processes, impacts) = fixture_axes();
        let shard = fixture_shard(
            &directory,
            &impacts,
            &[(0, 0, 1.0), (0, 1, 2.0), (1, 0, 3.0), (1, 1, 4.0)],
        );
        processes[0].geography_code.clear();
        assert!(
            prepare_portal_lcia_projection(&processes, &impacts, std::slice::from_ref(&shard),)
                .is_err()
        );
        processes[0].geography_code = "CN".to_owned();
        processes[1].process_index = 2;
        assert!(prepare_portal_lcia_projection(&processes, &impacts, &[shard],).is_err());
    }

    #[test]
    fn batches_are_bounded_and_locator_free() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("relation.ndjson");
        let mut writer = RelationWriter::create(path, "value-grid", 501).unwrap();
        for ordinal in 0..501 {
            writer
                .write(
                    ordinal + 1,
                    sha256_fields(&[
                        Some("test-record.v1"),
                        Some((ordinal + 1).to_string().as_str()),
                    ])
                    .unwrap(),
                    &json!({"schemaVersion": PORTAL_LCIA_VALUE_SCHEMA_VERSION, "ordinal": ordinal + 1}),
                )
                .unwrap();
        }
        let relation = writer
            .finish("value-grid", PORTAL_LCIA_VALUE_SCHEMA_VERSION)
            .unwrap();
        let mut reader = relation.batches().unwrap();
        let first = reader.next_batch().unwrap().unwrap();
        let second = reader.next_batch().unwrap().unwrap();
        assert_eq!((first.record_count, second.record_count), (500, 1));
        assert_eq!((first.first_ordinal, first.last_ordinal), (1, 500));
        assert_eq!((second.first_ordinal, second.last_ordinal), (501, 501));
        assert!(first.encoded_bytes <= PORTAL_LCIA_MAX_BATCH_ENCODED_BYTES);
        assert!(reader.next_batch().unwrap().is_none());
        let serialized = serde_json::to_string(&first.payload).unwrap();
        for forbidden in ["url", "bucket", "objectPath", "locator", "localPath"] {
            assert!(!serialized.contains(forbidden));
        }
    }
}
