WITH cases_details AS (
    SELECT * FROM {{ ref('cases_details') }}
),

cases_with_metrics AS (
    SELECT 
        tenant_id,
        company_id,
        company_name,
        division_id,
        division_name,
        department_id,
        department_name,
        project_id::TEXT,
        project_identifier,
        ext_project_id::TEXT,
        case_external_id,
        case_url,
        position,
        project_level_full_path,
        project_level_name,
        project_level_id::TEXT,
        project_number::TEXT,
        kpi_name,
        kpi_id,
        year_occurred::TEXT AS year_occurred,
        month_occurred,
        month,
        date_occurred,
        case_occurred_date,
        date_published,
        deadline,
        date_closed,
        case_type_name,
        case_type_id,
        description,
        organization_level_3_name,
        organization_level_3_id,
        originator_email,
        originator_name,
        originator_id,
        status_key,
        status_name,
        severity_degree,
        comments,
        immediate_actions,
        longitude,
        latitude,
        hash_string,
        last_modified,
        invocation_id,
        data_source_name,
        CASE 
            WHEN kpi_id = '9025' THEN 'H1'
            WHEN kpi_id = '9024' THEN 'H2'
            WHEN kpi_id = '9178' THEN 'M1'
            WHEN kpi_id = '9177' THEN 'M2'
            WHEN kpi_id = '9176' THEN 'M3'
            ELSE 'Other'
        END AS metric,
        1 AS value  -- Actual records have value = 1
    FROM cases_details
),

-- Get all unique years from actual data
unique_years AS (
    SELECT DISTINCT year_occurred AS year_val 
    FROM cases_with_metrics
),

-- Get all unique months from actual data
unique_months AS (
    SELECT DISTINCT month_occurred AS month_val 
    FROM cases_with_metrics
),

-- Get all unique metrics from actual data
unique_metrics AS (
    SELECT DISTINCT metric AS metric_val 
    FROM cases_with_metrics
),

-- Get all unique company/division/project combinations from actual data
unique_company_division_projects AS (
    SELECT DISTINCT 
        company_id,
        company_name,
        division_id,
        division_name,
        project_identifier
    FROM cases_with_metrics
    WHERE company_id IS NOT NULL 
    AND division_id IS NOT NULL 
    AND project_identifier IS NOT NULL
),

-- Generate all possible combinations of year, month, metric, company, division, and project
all_combinations AS (
    SELECT 
        unique_years.year_val,
        unique_months.month_val,
        unique_metrics.metric_val,
        company_division_project.company_id,
        company_division_project.company_name,
        company_division_project.division_id,
        company_division_project.division_name,
        company_division_project.project_identifier
    FROM unique_years
    CROSS JOIN unique_months
    CROSS JOIN unique_metrics
    CROSS JOIN unique_company_division_projects company_division_project
),

-- Get existing combinations to identify missing ones
existing_combinations AS (
    SELECT DISTINCT
        year_occurred,
        month_occurred,
        metric,
        company_id,
        division_id,
        project_identifier
    FROM cases_with_metrics
),

-- Identify missing combinations that need dummy records
missing_combinations AS (
    SELECT 
        all_combinations.year_val,
        all_combinations.month_val,
        all_combinations.metric_val,
        all_combinations.company_id,
        all_combinations.company_name,
        all_combinations.division_id,
        all_combinations.division_name,
        all_combinations.project_identifier
    FROM all_combinations
    LEFT JOIN existing_combinations 
        ON existing_combinations.year_occurred = all_combinations.year_val 
        AND existing_combinations.month_occurred = all_combinations.month_val 
        AND existing_combinations.metric = all_combinations.metric_val
        AND existing_combinations.company_id = all_combinations.company_id
        AND existing_combinations.division_id = all_combinations.division_id
        AND existing_combinations.project_identifier = all_combinations.project_identifier
    WHERE existing_combinations.year_occurred IS NULL  -- This combination doesn't exist
),

-- Create dummy records for missing combinations
dummy_records AS (
    -- Dummy records for first day of month
    SELECT 
        NULL::INTEGER AS tenant_id,
        company_id,
        company_name,
        division_id,
        division_name,
        NULL::INTEGER AS department_id,
        NULL::TEXT AS department_name,
        NULL::TEXT AS project_id,
        project_identifier,
        NULL::TEXT AS ext_project_id,
        NULL::NUMERIC AS case_external_id,
        NULL::TEXT AS case_url,
        NULL::TEXT AS position,
        NULL::TEXT AS project_level_full_path,
        NULL::TEXT AS project_level_name,
        NULL::TEXT AS project_level_id,
        NULL::TEXT AS project_number,
        NULL::TEXT AS kpi_name,
        NULL::TEXT AS kpi_id,
        year_val AS year_occurred,
        month_val AS month_occurred,
        UPPER(TO_CHAR(DATE(year_val || '-' || LPAD(month_val::TEXT, 2, '0') || '-01'), 'MON')) AS month,
        NULL::TEXT AS date_occurred,
        DATE(year_val || '-' || LPAD(month_val::TEXT, 2, '0') || '-01')::TIMESTAMP AS case_occurred_date,
        NULL::TEXT AS date_published,
        NULL::TEXT AS deadline,
        NULL::TEXT AS date_closed,
        NULL::TEXT AS case_type_name,
        NULL::TEXT AS case_type_id,
        NULL::TEXT AS description,
        NULL::TEXT AS organization_level_3_name,
        NULL::TEXT AS organization_level_3_id,
        NULL::TEXT AS originator_email,
        NULL::TEXT AS originator_name,
        NULL::TEXT AS originator_id,
        NULL::TEXT AS status_key,
        NULL::TEXT AS status_name,
        NULL::TEXT AS severity_degree,
        NULL::TEXT AS comments,
        NULL::TEXT AS immediate_actions,
        NULL::TEXT AS longitude,
        NULL::TEXT AS latitude,
        NULL::TEXT AS hash_string,
        NOW() AS last_modified,
        '{{ invocation_id }}' AS invocation_id,
        NULL::TEXT AS data_source_name,
        metric_val AS metric,
        0 AS value  -- Dummy records have value = 0
    FROM missing_combinations
    
    UNION ALL
    
    -- Dummy records for last day of month
    SELECT 
        NULL::INTEGER AS tenant_id,
        company_id,
        company_name,
        division_id,
        division_name,
        NULL::INTEGER AS department_id,
        NULL::TEXT AS department_name,
        NULL::TEXT AS project_id,
        project_identifier,
        NULL::TEXT AS ext_project_id,
        NULL::NUMERIC AS case_external_id,
        NULL::TEXT AS case_url,
        NULL::TEXT AS position,
        NULL::TEXT AS project_level_full_path,
        NULL::TEXT AS project_level_name,
        NULL::TEXT AS project_level_id,
        NULL::TEXT AS project_number,
        NULL::TEXT AS kpi_name,
        NULL::TEXT AS kpi_id,
        year_val AS year_occurred,
        month_val AS month_occurred,
        UPPER(TO_CHAR(DATE(year_val || '-' || LPAD(month_val::TEXT, 2, '0') || '-01'), 'MON')) AS month,
        NULL::TEXT AS date_occurred,
        (DATE(year_val || '-' || LPAD(month_val::TEXT, 2, '0') || '-01') + INTERVAL '1 month' - INTERVAL '1 day')::TIMESTAMP AS case_occurred_date,
        NULL::TEXT AS date_published,
        NULL::TEXT AS deadline,
        NULL::TEXT AS date_closed,
        NULL::TEXT AS case_type_name,
        NULL::TEXT AS case_type_id,
        NULL::TEXT AS description,
        NULL::TEXT AS organization_level_3_name,
        NULL::TEXT AS organization_level_3_id,
        NULL::TEXT AS originator_email,
        NULL::TEXT AS originator_name,
        NULL::TEXT AS originator_id,
        NULL::TEXT AS status_key,
        NULL::TEXT AS status_name,
        NULL::TEXT AS severity_degree,
        NULL::TEXT AS comments,
        NULL::TEXT AS immediate_actions,
        NULL::TEXT AS longitude,
        NULL::TEXT AS latitude,
        NULL::TEXT AS hash_string,
        NOW() AS last_modified,
        '{{ invocation_id }}' AS invocation_id,
        NULL::TEXT AS data_source_name,
        metric_val AS metric,
        0 AS value  -- Dummy records have value = 0
    FROM missing_combinations
),

-- Combine actual records with dummy records
final_data AS (
    SELECT * FROM cases_with_metrics
    UNION ALL
    SELECT * FROM dummy_records
)

SELECT * FROM final_data
ORDER BY year_occurred, month_occurred, metric, company_name, division_name, project_identifier 