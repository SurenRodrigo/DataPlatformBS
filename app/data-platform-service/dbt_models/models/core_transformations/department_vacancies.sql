{{
    config(
        tags=['nrc', 'jobylon_sync_tables']
    )
}}

WITH source_data AS (
    SELECT
        *
    FROM {{ ref('stg__jobylon_deparment_vacancies') }}
),

-- Check for active records per department_vacancy_id and rank records
record_analysis AS (
    SELECT 
        s.*,
        COUNT(CASE WHEN dbt_valid_to IS NULL THEN 1 END) 
            OVER (PARTITION BY department_vacancy_id) as active_count,
        -- Rank records: active records first (dbt_valid_to IS NULL), then by most recent dbt_valid_from
        ROW_NUMBER() OVER (
            PARTITION BY department_vacancy_id 
            ORDER BY 
                CASE WHEN dbt_valid_to IS NULL THEN 0 ELSE 1 END,
                dbt_valid_from DESC
        ) as record_rank
    FROM source_data s
),

source_with_removal_flag AS (
    SELECT 
        *,
        -- Mark as removed if record has dbt_valid_to (expired) AND no active records exist for this ID
        CASE 
            WHEN dbt_valid_to IS NOT NULL AND active_count = 0 THEN TRUE
            ELSE FALSE
        END AS is_removed
    FROM record_analysis
    -- Only select the "current" record for each ID:
    -- - If active records exist (active_count > 0): select the active one (record_rank = 1)
    -- - If no active records (active_count = 0): select the most recent expired one (record_rank = 1)
    WHERE record_rank = 1
),

employee_data AS (
    SELECT
        email,
        name,
        organizational_unit_id,
        legal_entity_id
    FROM {{ ref('employee') }}
    WHERE email IS NOT NULL
)

SELECT
    source.department_vacancy_id,
    source.title,
    source.description,
    array_to_string(
        array(
            select jsonb_array_elements_text(
                jsonb_path_query_array(source.locations, '$.location[*].text')
            )
        ),
        ';'
    ) as location,
    source.employment_type,
    source.last_application_date::DATE AS last_application_date,
    source.created_date::DATE AS published_at,
    source.url,
    source.company_name,
    source.contact_email AS contact_email,
    emp.legal_entity_id AS division_id,
    emp.organizational_unit_id AS organizational_unit_id,
    source.is_removed,
    -- Set date_withdrawn to dbt_valid_to date when record is marked as removed
    CASE 
        WHEN source.is_removed = TRUE THEN source.dbt_valid_to::DATE
        ELSE NULL
    END AS date_withdrawn
FROM source_with_removal_flag source
LEFT JOIN employee_data emp ON source.contact_email = emp.email
WHERE emp.organizational_unit_id IS NOT NULL AND emp.legal_entity_id IS NOT NULL