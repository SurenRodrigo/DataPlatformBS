with employee_group_list as (
select distinct
    field->'1046'->'data'->>'guid' AS external_id,
    field->'1046'->'data'->>'value' AS "description"
from {{ source('raw_nrc_source', 'source_catalyst_employee') }}
)
select
    {{ dbt_utils.generate_surrogate_key(['external_id']) }} AS id,
	external_id,
    "description"
from employee_group_list as egl
where external_id!='' or "description"!=''