BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout = '10min';

WITH documents AS MATERIALIZED (
    SELECT 'contacts'::text AS source_category, id, btrim(version::text) AS version,
           COALESCE(json, json_ordered::jsonb) AS document FROM public.contacts
    UNION ALL
    SELECT 'flowproperties', id, btrim(version::text), COALESCE(json, json_ordered::jsonb)
    FROM public.flowproperties
    UNION ALL
    SELECT 'flows', id, btrim(version::text), COALESCE(json, json_ordered::jsonb)
    FROM public.flows
    UNION ALL
    SELECT 'lciamethods', id, btrim(version::text), COALESCE(json, json_ordered::jsonb)
    FROM public.lciamethods
    UNION ALL
    SELECT 'lifecyclemodels', id, btrim(version::text), COALESCE(json, json_ordered::jsonb)
    FROM public.lifecyclemodels
    UNION ALL
    SELECT 'processes', id, btrim(version::text), COALESCE(json, json_ordered::jsonb)
    FROM public.processes
    UNION ALL
    SELECT 'sources', id, btrim(version::text), COALESCE(json, json_ordered::jsonb)
    FROM public.sources
    UNION ALL
    SELECT 'unitgroups', id, btrim(version::text), COALESCE(json, json_ordered::jsonb)
    FROM public.unitgroups
),
targets AS MATERIALIZED (
    SELECT source_category AS target_category, id, version FROM documents
),
raw_references AS MATERIALIZED (
    SELECT
        document.source_category,
        document.id AS source_id,
        document.version AS source_version,
        reference.value->>'key' AS reference_key,
        reference.value->'value' AS reference
    FROM documents AS document
    CROSS JOIN LATERAL jsonb_path_query(
        document.document,
        'strict $.** ? (@.type() == "object").keyvalue() ? (@.key like_regex "referenceTo" flag "i" && @.value.type() == "object")'
    ) AS reference(value)
),
parsed_references AS MATERIALIZED (
    SELECT
        source_category,
        source_id,
        source_version,
        reference_key,
        reference,
        NULLIF(btrim(reference->>'@refObjectId'), '') AS raw_target_id,
        NULLIF(btrim(reference->>'@version'), '') AS requested_version,
        CASE regexp_replace(lower(COALESCE(reference->>'@type', '')), '\s+', ' ', 'g')
            WHEN 'contact' THEN 'contacts'
            WHEN 'contact data set' THEN 'contacts'
            WHEN 'flow' THEN 'flows'
            WHEN 'flow data set' THEN 'flows'
            WHEN 'flow property' THEN 'flowproperties'
            WHEN 'flow property data set' THEN 'flowproperties'
            WHEN 'lcia method' THEN 'lciamethods'
            WHEN 'lcia method data set' THEN 'lciamethods'
            WHEN 'life cycle model' THEN 'lifecyclemodels'
            WHEN 'life cycle model data set' THEN 'lifecyclemodels'
            WHEN 'lifecycle model' THEN 'lifecyclemodels'
            WHEN 'lifecycle model data set' THEN 'lifecyclemodels'
            WHEN 'process' THEN 'processes'
            WHEN 'process data set' THEN 'processes'
            WHEN 'source' THEN 'sources'
            WHEN 'source data set' THEN 'sources'
            WHEN 'unit group' THEN 'unitgroups'
            WHEN 'unit group data set' THEN 'unitgroups'
        END AS target_category
    FROM raw_references
),
normalized_references AS MATERIALIZED (
    SELECT
        parsed.*,
        CASE
            WHEN raw_target_id ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
            THEN raw_target_id::uuid
        END AS target_id,
        requested_version IS NULL
            OR requested_version ~ '^[0-9]{2}\.[0-9]{2}(\.[0-9]{3})?$' AS version_valid
    FROM parsed_references AS parsed
),
classified_references AS MATERIALIZED (
    SELECT
        reference.*,
        EXISTS (
            SELECT 1 FROM targets AS target
            WHERE target.target_category = reference.target_category
              AND target.id = reference.target_id
        ) AS target_id_exists,
        EXISTS (
            SELECT 1 FROM targets AS target
            WHERE target.target_category = reference.target_category
              AND target.id = reference.target_id
              AND target.version = reference.requested_version
        ) AS exact_target_exists
    FROM normalized_references AS reference
),
reference_groups AS (
    SELECT
        source_category,
        reference_key,
        target_category,
        count(*) AS occurrence_count,
        count(*) FILTER (WHERE reference = '{}'::jsonb) AS empty_object_count,
        count(*) FILTER (WHERE raw_target_id IS NULL) AS missing_object_id_count,
        count(*) FILTER (WHERE raw_target_id IS NOT NULL AND target_id IS NULL) AS invalid_object_id_count,
        count(*) FILTER (WHERE target_category IS NULL) AS unresolved_target_type_count,
        count(*) FILTER (
            WHERE target_id IS NOT NULL AND target_category IS NOT NULL AND NOT target_id_exists
        ) AS missing_target_id_count,
        count(*) FILTER (
            WHERE target_id IS NOT NULL AND target_id_exists AND requested_version IS NOT NULL
              AND version_valid AND NOT exact_target_exists
        ) AS missing_target_version_count,
        count(*) FILTER (WHERE NOT version_valid) AS invalid_version_count,
        count(*) FILTER (
            WHERE target_id IS NOT NULL AND target_id_exists
              AND (requested_version IS NULL OR exact_target_exists)
        ) AS resolved_count
    FROM classified_references
    GROUP BY source_category, reference_key, target_category
),
document_sizes AS (
    SELECT
        source_category,
        count(*) AS document_count,
        max(octet_length(document::text)) AS max_document_bytes,
        count(*) FILTER (WHERE octet_length(document::text) > 64 * 1024 * 1024)
            AS documents_over_support_read_limit
    FROM documents
    GROUP BY source_category
),
summary AS (
    SELECT
        count(*) AS reference_occurrence_count,
        count(*) FILTER (WHERE reference = '{}'::jsonb) AS empty_object_count,
        count(*) FILTER (WHERE raw_target_id IS NULL) AS missing_object_id_count,
        count(*) FILTER (WHERE raw_target_id IS NOT NULL AND target_id IS NULL)
            AS invalid_object_id_count,
        count(*) FILTER (WHERE target_category IS NULL) AS unresolved_target_type_count,
        count(*) FILTER (
            WHERE target_id IS NOT NULL AND target_category IS NOT NULL AND NOT target_id_exists
        ) AS missing_target_id_count,
        count(*) FILTER (
            WHERE target_id IS NOT NULL AND target_id_exists AND requested_version IS NOT NULL
              AND version_valid AND NOT exact_target_exists
        ) AS missing_target_version_count,
        count(*) FILTER (WHERE NOT version_valid) AS invalid_version_count
    FROM classified_references
)
SELECT jsonb_pretty(jsonb_build_object(
    'schemaVersion', 'worker.source-reference-audit.v1',
    'scope', 'all_dataset_revisions',
    'readOnly', true,
    'summary', to_jsonb(summary),
    'referenceGroups', COALESCE((
        SELECT jsonb_agg(to_jsonb(reference_group) ORDER BY
            reference_group.source_category,
            reference_group.reference_key,
            reference_group.target_category NULLS FIRST)
        FROM reference_groups AS reference_group
    ), '[]'::jsonb),
    'documentSizes', COALESCE((
        SELECT jsonb_agg(to_jsonb(document_size) ORDER BY document_size.source_category)
        FROM document_sizes AS document_size
    ), '[]'::jsonb)
))
FROM summary;

ROLLBACK;
