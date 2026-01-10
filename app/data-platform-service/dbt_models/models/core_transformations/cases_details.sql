WITH project AS (
    SELECT
        id AS project_id,
        department_id,
        department_name,
        division_id,
        project_number, 
        project_identifier
    FROM {{ ref('project') }}
),
division AS (
    SELECT
        tenant_id,
        company_id,
        company_name,
        division_id,
        division_name,
        external_id,
        data_source_name
    FROM {{ ref('int__division_external_id_mapping') }}
),
case_base AS (
    SELECT *
    FROM {{ ref('int__nrc_tqm_case_deduplicated') }}
    WHERE row_num = 1
),
kpi_data AS (
    SELECT
        case_base.case_external_id,
        kpi_elem ->> 'Name' AS kpi_name,
        kpi_elem ->> 'ID' AS kpi_id
    FROM case_base
    LEFT JOIN LATERAL JSONB_ARRAY_ELEMENTS(case_base.kpi_parameters::jsonb) AS kpi_elem ON TRUE
),
case_data as (
	SELECT
		case_base.project_id AS ext_project_id,
	    case_base.case_external_id,
	    case_base.case_url,
	    case_base.position,
	    case_base.project_level_full_path,
	    case_base.project_level_name,
	    case_base.project_level_id,
	    case_base.ext_project_id AS project_number,
	    kpi_data.kpi_name,
	    kpi_data.kpi_id,
		EXTRACT(YEAR FROM TO_DATE(case_base.date_occurred, 'DD/MM/YYYY')) AS year_occurred,
        EXTRACT(MONTH FROM TO_DATE(case_base.date_occurred, 'DD/MM/YYYY')) AS month_occurred,
		UPPER(TO_CHAR(TO_DATE(case_base.date_occurred, 'DD/MM/YYYY'), 'MON')) AS month,
        case_base.date_occurred,
	    case_base.case_occurred_date,
	    case_base.date_published,
	    case_base.deadline,
	    case_base.date_closed,
	    case_base.case_type_name,
	    case_base.case_type_id,
	    case_base.description,
	    case_base.organization_level_3_name,
	    case_base.organization_level_3_id,
	    case_base.originator_email,
	    case_base.originator_name,
	    case_base.originator_id,
	    case_base.status_key,
	    case_base.status_name,
	    NULL AS severity_degree,
	    NULL AS comments,
	    case_base.immediate_actions,
		NULL AS longitude,
		NULL AS latitude,
		MD5(CONCAT(case_base.case_external_id, case_base.case_type_id, case_base.date_occurred, case_base.project_level_id, case_base.immediate_actions, kpi_data.kpi_id)) AS hash_string,
		NOW() AS last_modified,
	    '{{ invocation_id }}' AS invocation_id
    from kpi_data
    left join case_base on case_base.case_external_id = kpi_data.case_external_id
),
case_details AS (
    SELECT
        division.tenant_id,
        division.company_id,
        division.company_name,
        division.division_id,
        division.division_name,
        project.department_id,
        project.department_name,
        project.project_id,
        project.project_identifier,
        case_data.*,
        division.data_source_name
    FROM case_data
    LEFT JOIN division ON division.external_id = case_data.organization_level_3_id
    LEFT JOIN project ON project.project_number = case_data.project_number AND project.division_id = division.division_id
)
SELECT * FROM case_details