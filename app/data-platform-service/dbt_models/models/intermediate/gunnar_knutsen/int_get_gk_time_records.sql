WITH time_records AS (
    SELECT *
    FROM {{ ref('stg__admmit_hours') }}
),

time_types AS (
    SELECT
        time_type_id,
        time_types
    FROM {{ ref('int_gk_time_types') }}
),

employees AS (
    SELECT
        ext_employee_id AS employee_id,
        tenant_id,
        tenant_name,
        legal_entity_id AS division_id,
        legal_entity_name AS division_name,
        data_source_name,
        company_id,
        company_name,
        organizational_unit_id AS department_id,
        organizational_unit_name AS department_name
    FROM {{ ref('dim_gunnar_knutsen_admmit_employee') }}
),

absence_records AS (
    SELECT
        time_records.internal_id,
        time_records.hours_normal AS hours,
        time_records.date_from AS date,
        time_records.created_on AS created_date_time,
        time_records.modified_on AS modified_date_time,
        time_records.employee_id,
        time_records.project_id,
        time_records.absence_type_name,
        time_types.time_type_id,
        time_types.time_types AS time_type
    FROM time_records
    LEFT JOIN time_types
        ON time_records.absence_type_name = time_types.time_types
    WHERE time_records.absence_type_name IS NOT NULL
),

ordinary_hours_records AS (
    SELECT
        time_records.internal_id,
        time_records.hours_normal AS hours,
        time_records.date_from AS date,
        time_records.created_on AS created_date_time,
        time_records.modified_on AS modified_date_time,
        time_records.employee_id,
        time_records.project_id,
        1 AS time_type_id,
        'Ordinary Hours' AS time_type
    FROM time_records
    WHERE time_records.absence_type_name IS NULL
),

overtime_hours_records AS (
    SELECT
        time_records.internal_id,
        (COALESCE(time_records.hours_overtime, 0) + 
         COALESCE(time_records.hours_overtime2, 0) + 
         COALESCE(time_records.hours_overtime3, 0) + 
         COALESCE(time_records.hours_overtime4, 0) + 
         COALESCE(time_records.hours_overtime5, 0) + 
         COALESCE(time_records.hours_overtime6, 0) + 
         COALESCE(time_records.hours_overtime7, 0) + 
         COALESCE(time_records.hours_overtime8, 0)) AS hours,
        time_records.date_from AS date,
        time_records.created_on AS created_date_time,
        time_records.modified_on AS modified_date_time,
        time_records.employee_id,
        time_records.project_id,
        2 AS time_type_id,
        'Overtime Hours' AS time_type
    FROM time_records
    WHERE time_records.absence_type_name IS NULL
),

all_records AS (
    SELECT
        internal_id,
        hours,
        date,
        created_date_time,
        modified_date_time,
        employee_id,
        project_id,
        time_type_id,
        time_type
    FROM absence_records
    
    UNION ALL
    
    SELECT
        internal_id,
        hours,
        date,
        created_date_time,
        modified_date_time,
        employee_id,
        project_id,
        time_type_id,
        time_type
    FROM ordinary_hours_records
    
    UNION ALL
    
    SELECT
        internal_id,
        hours,
        date,
        created_date_time,
        modified_date_time,
        employee_id,
        project_id,
        time_type_id,
        time_type
    FROM overtime_hours_records
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['all_records.internal_id', 'all_records.time_type']) }} AS time_record_sk,
    all_records.internal_id::VARCHAR AS record_id,
    all_records.time_type_id,
    all_records.time_type,
    all_records.hours,
    all_records.date,
    EXTRACT(YEAR FROM all_records.date) AS year,
    EXTRACT(MONTH FROM all_records.date) AS month,
    TO_CHAR(all_records.date, 'Month') AS month_name,
    all_records.employee_id::VARCHAR,
    all_records.project_id,
    employees.company_id,
    employees.company_name,
    employees.division_id,
    employees.division_name,
    employees.department_id,
    employees.department_name,
    employees.data_source_name,
    all_records.created_date_time,
    all_records.modified_date_time
FROM all_records
LEFT JOIN employees
    ON all_records.employee_id = employees.employee_id