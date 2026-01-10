{{ config(
    materialized='incremental',
    unique_key='case_external_id',
    tags=['nrc', 'tqm_case_tables']
) }}

WITH case_data_list AS (
    SELECT *
    FROM {{ ref('int__nrc_tqm_case_deduplicated')}} 
),

division_mapping AS (
    SELECT *
    FROM {{ref('int__division_external_id_mapping')}}
    WHERE "data_source_name" = 'TQM'
)

SELECT
    ROW_NUMBER() OVER () AS id, -- Should be auto increment number
    e.legal_entity_id AS employee_legal_entity_id,
    cdl.project_id,
    cdl.case_external_id,
    cdl.case_url,
    cdl.position,
    cdl.project_level_full_path,
    cdl.project_level_name,
    cdl.project_level_id,
    EXTRACT(YEAR FROM TO_DATE(cdl.date_occurred, 'DD/MM/YYYY')) AS year,
    EXTRACT(MONTH FROM TO_DATE(cdl.date_occurred, 'DD/MM/YYYY')) AS month,
	UPPER(TO_CHAR(TO_DATE(cdl.date_occurred, 'DD/MM/YYYY'), 'MON')) AS month_name,
    cdl.date_occurred,
    cdl.date_published,
    cdl.deadline,
    cdl.date_closed,
    cdl.case_type_name,
    cdl.case_type_id,
    cdl.description,
    cdl.organization_level_3_name,
    cdl.organization_level_3_id,
    dm.company_name,
    dm.company_id,
    dm.division_name AS division_name,
    dm.division_id AS division_id,
    e.employee_id AS employee_id,
    cdl.originator_email,
    cdl.originator_name,
    cdl.originator_id,
    cdl.status_key,
    cdl.status_name,
    NULL AS severity_degree,
    cdl.comments,
    cdl.immediate_actions,
    cdl.actions,
    NULL AS longitude,
    NULL AS latitude,
    MD5(CONCAT(case_external_id,case_status, case_url,case_type, deadline, position, date_closed, originator, description, date_occurred, project_level, date_published, kpi_parameters, organization_level_3)) as hash_string,
    NOW() as last_modified,
    '{{ invocation_id }}' AS invocation_id
FROM case_data_list AS cdl
LEFT JOIN {{ ref('employee') }} e 
    ON LOWER(e.username) = LOWER(cdl.originator_email)
LEFT JOIN division_mapping AS dm
    ON dm.external_id::TEXT = cdl.organization_level_3_id