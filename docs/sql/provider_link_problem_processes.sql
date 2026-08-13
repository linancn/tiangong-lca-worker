-- Provider-link related process diagnostics
-- Purpose:
-- 1) Locate suspicious service-loop / provider-loop processes
-- 2) Locate suspicious PN / PM0.2 / particle semantic mismatch processes

-- ============================================================
-- Query 1: suspicious service-loop / provider-loop processes
-- Logic:
-- - latest version per process
-- - same process + same flow_id
-- - one Input exchange and one Output exchange
-- - input/output amount text exactly the same
-- ============================================================

with latest_processes as (
  select distinct on (p.id)
    p.id,
    p.state_code,
    p.version,
    p.created_at,
    p.json::jsonb as j
  from public.processes p
  order by p.id, p.created_at desc
),
process_meta as (
  select
    lp.id as process_id,
    lp.state_code,
    lp.version as process_version,
    coalesce(
      lp.j #>> '{processDataSet,processInformation,dataSetInformation,name,baseName,1,#text}',
      lp.j #>> '{processDataSet,processInformation,dataSetInformation,name,baseName,0,#text}'
    ) as process_name,
    coalesce(
      lp.j #>> '{processDataSet,processInformation,geography,locationOfOperationSupplyOrProduction,@location}',
      lp.j #>> '{processDataSet,processInformation,geography,subLocationOfOperationSupplyOrProduction,@subLocation}',
      lp.j #>> '{processDataSet,processInformation,dataSetInformation,locationOfOperationSupplyOrProduction}'
    ) as location,
    lp.j #>> '{processDataSet,quantitativeReference,referenceToReferenceFlow}' as reference_exchange_internal_id,
    case
      when jsonb_typeof(lp.j #> '{processDataSet,exchanges,exchange}') = 'array' then lp.j #> '{processDataSet,exchanges,exchange}'
      when lp.j #> '{processDataSet,exchanges,exchange}' is null then '[]'::jsonb
      else jsonb_build_array(lp.j #> '{processDataSet,exchanges,exchange}')
    end as exchanges
  from latest_processes lp
),
exchanges as (
  select
    pm.process_id,
    pm.process_name,
    pm.location,
    pm.state_code,
    pm.process_version,
    pm.reference_exchange_internal_id,
    ex.value #>> '{@dataSetInternalID}' as exchange_internal_id,
    ex.value #>> '{exchangeDirection}' as direction,
    ex.value #>> '{referenceToFlowDataSet,@refObjectId}' as flow_id,
    coalesce(
      ex.value #>> '{referenceToFlowDataSet,common:shortDescription,#text}',
      ex.value #>> '{referenceToFlowDataSet,shortDescription,#text}'
    ) as flow_name,
    trim(replace(replace(coalesce(ex.value #>> '{resultingAmount}', ex.value #>> '{meanAmount}', ''), chr(160), ''), ',', '')) as amount_text,
    coalesce(
      ex.value #>> '{generalComment,1,#text}',
      ex.value #>> '{generalComment,0,#text}'
    ) as comment_text
  from process_meta pm
  cross join lateral jsonb_array_elements(pm.exchanges) ex(value)
)
select
  i.process_id,
  i.process_name,
  i.location,
  i.state_code,
  i.process_version,
  i.flow_id,
  i.flow_name,
  i.reference_exchange_internal_id,
  i.exchange_internal_id as input_exchange_internal_id,
  o.exchange_internal_id as output_exchange_internal_id,
  (o.exchange_internal_id = i.reference_exchange_internal_id) as output_is_reference,
  i.amount_text as input_amount_text,
  o.amount_text as output_amount_text,
  i.comment_text as input_comment,
  o.comment_text as output_comment,
  'exact_same_flow_input_output_text' as suspicion_tag
from exchanges i
join exchanges o
  on i.process_id = o.process_id
 and i.flow_id = o.flow_id
 and i.direction = 'Input'
 and o.direction = 'Output'
where i.amount_text <> ''
  and i.amount_text = o.amount_text
order by
  (o.exchange_internal_id = i.reference_exchange_internal_id) desc,
  i.amount_text desc,
  i.process_name nulls last;

-- ============================================================
-- Query 2: suspicious PN / PM0.2 / particle semantic mismatch
-- Logic:
-- - latest version per process and flow
-- - process Output exchanges only
-- - priority hit: comment contains PN, but flow reference property is Mass
-- - also keep PM0.2 / particle-related flow hits as expansion set
-- ============================================================

with latest_processes as (
  select distinct on (p.id)
    p.id,
    p.state_code,
    p.version,
    p.created_at,
    p.json::jsonb as j
  from public.processes p
  order by p.id, p.created_at desc
),
latest_flows as (
  select distinct on (f.id)
    f.id,
    f.version,
    f.created_at,
    f.json::jsonb as j
  from public.flows f
  order by f.id, f.created_at desc
),
process_meta as (
  select
    lp.id as process_id,
    lp.state_code,
    lp.version as process_version,
    coalesce(
      lp.j #>> '{processDataSet,processInformation,dataSetInformation,name,baseName,1,#text}',
      lp.j #>> '{processDataSet,processInformation,dataSetInformation,name,baseName,0,#text}'
    ) as process_name,
    coalesce(
      lp.j #>> '{processDataSet,processInformation,geography,locationOfOperationSupplyOrProduction,@location}',
      lp.j #>> '{processDataSet,processInformation,geography,subLocationOfOperationSupplyOrProduction,@subLocation}',
      lp.j #>> '{processDataSet,processInformation,dataSetInformation,locationOfOperationSupplyOrProduction}'
    ) as location,
    case
      when jsonb_typeof(lp.j #> '{processDataSet,exchanges,exchange}') = 'array' then lp.j #> '{processDataSet,exchanges,exchange}'
      when lp.j #> '{processDataSet,exchanges,exchange}' is null then '[]'::jsonb
      else jsonb_build_array(lp.j #> '{processDataSet,exchanges,exchange}')
    end as exchanges
  from latest_processes lp
),
process_exchanges as (
  select
    pm.process_id,
    pm.process_name,
    pm.location,
    pm.state_code,
    pm.process_version,
    ex.value #>> '{@dataSetInternalID}' as exchange_internal_id,
    ex.value #>> '{exchangeDirection}' as direction,
    ex.value #>> '{referenceToFlowDataSet,@refObjectId}' as flow_id,
    trim(replace(replace(coalesce(ex.value #>> '{resultingAmount}', ex.value #>> '{meanAmount}', ''), chr(160), ''), ',', '')) as amount_text,
    coalesce(
      ex.value #>> '{generalComment,1,#text}',
      ex.value #>> '{generalComment,0,#text}'
    ) as comment_text
  from process_meta pm
  cross join lateral jsonb_array_elements(pm.exchanges) ex(value)
),
flow_meta as (
  select
    lf.id::text as flow_id,
    coalesce(
      lf.j #>> '{flowDataSet,flowInformation,dataSetInformation,name,baseName,1,#text}',
      lf.j #>> '{flowDataSet,flowInformation,dataSetInformation,name,baseName,0,#text}'
    ) as flow_name,
    coalesce(
      lf.j #>> '{flowDataSet,flowProperties,flowProperty,referenceToFlowPropertyDataSet,common:shortDescription,#text}',
      lf.j #>> '{flowDataSet,flowProperties,flowProperty,referenceToFlowPropertyDataSet,shortDescription,#text}'
    ) as reference_property_name
  from latest_flows lf
)
select
  pe.process_id,
  pe.process_name,
  pe.location,
  pe.state_code,
  pe.process_version,
  pe.exchange_internal_id,
  pe.amount_text,
  pe.flow_id,
  fm.flow_name,
  fm.reference_property_name,
  pe.comment_text,
  case
    when pe.comment_text ilike '%PN%' and coalesce(fm.reference_property_name, '') ilike '%mass%' then 'pn_comment_but_mass_flow'
    when coalesce(fm.flow_name, '') ilike '%PM0.2%' then 'pm02_flow_hit'
    when coalesce(fm.flow_name, '') ilike '%particle%' then 'particle_flow_hit'
    else 'other_particle_suspicion'
  end as suspicion_tag
from process_exchanges pe
left join flow_meta fm on fm.flow_id = pe.flow_id
where pe.direction = 'Output'
  and (
    (pe.comment_text ilike '%PN%' and coalesce(fm.reference_property_name, '') ilike '%mass%')
    or coalesce(fm.flow_name, '') ilike '%PM0.2%'
    or coalesce(fm.flow_name, '') ilike '%particle%'
  )
order by
  case when pe.comment_text ilike '%PN%' and coalesce(fm.reference_property_name, '') ilike '%mass%' then 0 else 1 end,
  length(pe.amount_text) desc,
  pe.process_name nulls last;
