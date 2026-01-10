WITH
icore_time_records AS (
    SELECT *,
    CASE
        WHEN "_costunitkey" LIKE '%¤%' 
            THEN NULLIF(LEFT(SPLIT_PART("_costunitkey", '¤', 2), 5), '')::INT
        ELSE NULLIF("_costunitkey", '')::INT
    END AS project_id
    FROM {{ source('raw_nrc_source', 'source_icore_hours') }}
),

ditio_payroll_records AS (
    SELECT
        payroll_value_id AS record_id,
        EXTRACT(YEAR FROM TO_DATE(trans_datetime, 'YYYY-MM-DD'))::INTEGER::TEXT AS work_year,
        *
    FROM {{ ref('int__nrc_ditio_unnest_payroll_lines') }}
),

ditio_absense_records AS (
    SELECT 
        id AS record_id,
        EXTRACT(YEAR FROM TO_DATE(date, 'YYYY-MM-DD'))::INTEGER::TEXT AS work_year,
        *
    FROM {{ ref('stg__ditio_absence_registration') }}
    WHERE absence_type_code IS NOT NULL
),

icore_employee_history AS (
    SELECT *
    FROM {{ref('int__nrc_icore_employee_org_history_denorm')}}
),

employee AS (
    SELECT *
    FROM {{ ref('employee') }}
),

agg_project_task_details AS (
    SELECT 
        ext_task_number,
        ext_project_number,
        MIN(project_task_sk)        AS project_task_sk, -- parent linkage
        STRING_AGG(name, ', ')      AS task_names  -- combine detail names
    FROM {{ ref('project_task_details') }}
    GROUP BY ext_task_number, ext_project_number
),

project_lookup AS (
    SELECT 
        project.project_identifier,
        project.project_number,        
        project.project_name,
        project.company_id,
        project.company_name,
        ditio_projects.ext_company_id
    FROM {{ ref('project') }} project
    INNER JOIN {{ ref('int__nrc_ditio_project') }} AS ditio_projects 
        ON project.project_number = ditio_projects.project_number
        AND project.company_id = ditio_projects.company_id
),

unioned AS (
    -- First part: icore records
    SELECT
        {{ dbt_utils.generate_surrogate_key(['tr.recordid', 'tr.workyear', "'icore'"]) }} AS id,
        tt.time_type_sk,
        ts.id                        AS time_status_id,
        tr._hourstypekey           AS time_type_key,
        tt.time_type_code            AS time_type,
        tr.quantity                  AS hours,
        (TRUNC(tr.recordid))::TEXT AS record_id,
        tr.workdate::TIMESTAMP     AS date,
        tr.workyear::INTEGER::TEXT AS work_year,
        EXTRACT(MONTH FROM TO_DATE(tr.workdate, 'YYYY-MM-DD'))    AS work_month,
        TO_CHAR(TO_DATE(tr.workdate, 'YYYY-MM-DD'), 'Month')      AS work_month_name,
        SUBSTRING(tr._employeekey FROM 6)                         AS ext_employee_id,
        emp.id                       AS employee_id,
        tr._employeeperiodickey    AS accouting_period_id,
        project_id,
        CASE
            WHEN project.project_name IS NOT NULL THEN
                CONCAT(tr.project_id::TEXT, ' - ', project.project_name)
            ELSE
                tr.project_id::TEXT
        END                          AS project_name,
        project.project_identifier,
        ts.time_status_code,
        -- instead of tr."companyId"
        emp_history.company_id       AS company_id,
        emp_history.company_name     AS company_name,
        emp_history.division_id      AS division_id,
        emp_history.division_name    AS division_name,
        emp_history.department_id    AS organizational_unit_id,
        emp_history.department_name  AS organizational_unit_name,
        'iCore'                      AS data_source_name,
        NULL                         AS data_source_subname,
        NULL                         AS project_task_sk,
        NULL                         AS task_name,
        NULL                         AS task_name_detail,
        NULL                         AS description,
        NULL                         AS description_internal,
        NULL                         AS approved_date_time,
        NULL                         AS started_date_time,
        NULL                         AS stop_date_time,
        NULL                         AS created_date_time,
        -- Cast iCore datetime to timestamp (already in Norway time)
        CASE 
            WHEN tr.alteredts IS NOT NULL THEN 
                tr.alteredts::TIMESTAMP
            ELSE NULL 
        END                          AS modified_date_time,
        NULL                         AS payroll_approved_date_time,
        NULL                         AS is_deleted,
        FALSE                        AS payroll_approved,
        NULL                         AS approved,
        NULL                         AS approved_by_name,
        NULL                         AS invoiced,
        NULL                         AS resource_id,
        NULL                         AS user_company_id
    FROM icore_time_records AS tr
    LEFT JOIN icore_employee_history AS emp_history
        ON emp_history.employee_id = SUBSTRING(tr._employeekey FROM 6) 
            AND TO_DATE(tr.workdate, 'YYYY-MM-DD') BETWEEN emp_history.valid_from_date 
            AND emp_history.valid_to_date
    LEFT JOIN employee AS emp
        ON emp.employee_id = SUBSTRING(tr._employeekey FROM 6)
    LEFT JOIN {{ ref('time_type') }} AS tt
        ON tr._hourstypekey = tt.time_type_key
    LEFT JOIN {{ ref('time_status') }} AS ts
        ON tr._statuskey = ts.time_status_key
    LEFT JOIN {{ ref('project') }} project
        ON project.ext_project_id = tr.project_id
        AND project.company_id = emp_history.company_id

    UNION ALL

    -- Second part: ditio payroll lines
    -- set time_type_code as time_type_key & time_type since time type data source mapping is used 
    SELECT
        {{ dbt_utils.generate_surrogate_key(['dpr.record_id', 'dpr.work_year', "'payroll'"]) }} AS id,
        NULL                                                 AS time_type_sk,
        NULL                                                 AS time_status_id,
        tt.time_type_code                                    AS time_type_key,
        tt.time_type_code                                    AS time_type,
        (payroll_value ->> 'qty')::numeric                   AS hours,
        record_id,
        trans_datetime::TIMESTAMP                            AS date,
        dpr.work_year,
        EXTRACT(MONTH FROM TO_DATE(trans_datetime, 'YYYY-MM-DD')) AS work_month,
        TO_CHAR(TO_DATE(trans_datetime, 'YYYY-MM-DD'), 'Month') AS work_month_name,
        -- Employee id set from Ditio
        employee_number                                      AS ext_employee_id,
        -- id from catalyst_employee
        emp.id                                               AS employee_id,
        NULL                                                 AS accouting_period_id,
        CASE
            WHEN (payroll_value ->> 'projectNumber') ~ '^[0-9]+$'
            THEN (payroll_value ->> 'projectNumber')::INT
            ELSE NULL
        END                                                  AS project_id,
        COALESCE(
            CONCAT(
                CASE
                    WHEN (payroll_value ->> 'projectNumber') ~ '^[0-9]+$'
                    THEN (payroll_value ->> 'projectNumber')
                    ELSE NULL
                END,
                ' - ',
                project.project_name
            ),
            CONCAT(
                CASE
                    WHEN (payroll_value ->> 'projectNumber') ~ '^[0-9]+$'
                    THEN (payroll_value ->> 'projectNumber')
                    ELSE NULL
                END,
                ' - ',
                payroll_value ->> 'projectName'
            )
        )                                                    AS project_name,
        project.project_identifier                           AS project_identifier,
        NULL                                                 AS time_status_code,
        project.company_id                                   AS company_id,
        project.company_name                                 AS company_name,
        emp_history.division_id                              AS division_id,
        emp_history.division_name                            AS division_name,
        emp_history.department_id                            AS organizational_unit_id,
        emp_history.department_name                          AS organizational_unit_name,
        'Ditio'                                              AS data_source_name,
        'time'                                               AS data_source_subname,
        pt.project_task_sk                                   AS project_task_sk,
        pt.task_name                                         AS task_name,
        agg_project_task_details.task_names                  AS task_name_detail,
        NULL                                                 AS description,
        NULL                                                 AS description_internal,
        approved_datetime                                    AS approved_date_time,
        start_datetime                                       AS started_date_time,
        stop_datetime                                        AS stop_date_time,
        NULL                                                 AS created_date_time,
        -- Convert Ditio UTC datetime to Norway time (UTC+1/+2)
        CASE 
            WHEN modified_datetime IS NOT NULL THEN 
                (modified_datetime::TIMESTAMPTZ AT TIME ZONE 'Europe/Oslo')::TIMESTAMP
            ELSE NULL 
        END                                                  AS modified_date_time,
        NULL                                                 AS payroll_approved_date_time,
        is_deleted                                           AS is_deleted,
        FALSE                                                AS payroll_approved,
        approved,
        user_name                                            AS approved_by_name,
        NULL,
        NULL                                                 AS resource_id,
        NULL                                                 AS user_company_id
    FROM ditio_payroll_records AS dpr
    LEFT JOIN icore_employee_history AS emp_history
        ON dpr.employee_number = emp_history.employee_id
            AND TO_DATE(trans_datetime, 'YYYY-MM-DD') BETWEEN emp_history.valid_from_date
            AND emp_history.valid_to_date
    LEFT JOIN employee AS emp
        ON dpr.employee_number = emp.employee_id
    LEFT JOIN {{ ref('time_type_data_source_seed') }} AS tt_ds
        ON tt_ds.ext_timetype_id = NULLIF((payroll_value ->> 'typeId'),'')::INT
        and tt_ds.data_source_id = 4 --Ditio
    LEFT JOIN {{ ref('time_type') }} AS tt
        ON tt.time_type_sk = tt_ds.time_type_sk
    LEFT JOIN project_lookup AS project
        ON project.project_number::TEXT = dpr.payroll_value ->> 'projectNumber' 
        AND project.ext_company_id = dpr.company_id::TEXT
    LEFT JOIN agg_project_task_details
        ON agg_project_task_details.ext_task_number = payroll_value ->> 'taskNumber' 
        AND agg_project_task_details.ext_project_number::TEXT = payroll_value ->> 'projectNumber'
    LEFT JOIN {{ ref('project_task') }} pt
        ON pt.project_task_sk = agg_project_task_details.project_task_sk
    WHERE NULLIF((payroll_value ->> 'typeId'),'')::INT IN (102, 133, 134)--Regular, Overtid 50%, Overtid 100%
      

    UNION ALL

    -- Third part: ditio absense records
    -- set time_type_code as time_type_key & time_type since time type data source mapping is used
    SELECT
        {{ dbt_utils.generate_surrogate_key(['dar.record_id', 'dar.work_year', "'absence'"]) }} AS id,
        NULL                                           AS time_type_sk,
        NULL                                           AS time_status_id,
        tt.time_type_code                              AS time_type_key,
        tt.time_type_code                              AS time_type,
        qty                                            AS hours,
        record_id,
        date::TIMESTAMP                                AS date,
        dar.work_year,
        EXTRACT(MONTH FROM TO_DATE(date, 'YYYY-MM-DD'))AS work_month,
        TO_CHAR(TO_DATE(date, 'YYYY-MM-DD'), 'Month')  AS work_month_name,
        -- Employee id set from Ditio
        employee_number                                AS ext_employee_id,
        -- id from catalyst_employee
        emp.id                                         AS employee_id,
        NULL                                           AS accouting_period_id,
        pds.ext_project_id::INT                        AS project_id,
        CASE
            WHEN project.project_name IS NOT NULL THEN
                CONCAT(pds.ext_project_id::TEXT, ' - ', project.project_name)
            ELSE
                pds.ext_project_id::TEXT
        END                                           AS project_name,
        project.project_identifier,
        NULL                                           AS time_status_code,
        emp_history.company_id                         AS company_id,
        emp_history.company_name                       AS company_name,
        emp_history.division_id                        AS division_id,
        emp_history.division_name                      AS division_name,
        emp_history.department_id                      AS organizational_unit_id,
        emp_history.department_name                    AS organizational_unit_name,
        -- update foreign key data_source_id
        'Ditio'                                        AS data_source_name,
        'absence'                                      AS data_source_subname,
        NULL                                           AS project_task_sk,
        NULL                                           AS task_name,
        NULL                                           AS task_name_detail,
        NULL                                           AS description,
        NULL                                           AS description_internal,
        approved_date_time                             AS approved_date_time,
        start_time                                     AS started_date_time,
        NULL                                           AS stop_date_time,
        dar.created_at                                 AS created_date_time,
        -- Convert Ditio absence UTC datetime to Norway time (UTC+1/+2)
        CASE 
            WHEN modified_at IS NOT NULL THEN 
                (modified_at::TIMESTAMPTZ AT TIME ZONE 'Europe/Oslo')::TIMESTAMP
            ELSE NULL 
        END                                            AS modified_date_time,
        payroll_approved_date_time,
        is_deleted,
        payroll_approved,
        approved,
        approved_by_name,
        NULL                                           AS invoiced,
        NULL                                           AS resource_id,
        dar.company_id                                 AS user_company_id
    FROM ditio_absense_records AS dar
    LEFT JOIN icore_employee_history AS emp_history
        ON dar.employee_number = emp_history.employee_id 
            AND TO_DATE(date, 'YYYY-MM-DD') BETWEEN emp_history.valid_from_date 
            AND emp_history.valid_to_date
    LEFT JOIN employee AS emp
        ON dar.employee_number = emp.employee_id
    LEFT JOIN {{ ref('time_type_data_source_seed') }} AS tt_ds
        ON dar.absence_type_code::INT = tt_ds.ext_timetype_id and tt_ds.data_source_id = 4 --Ditio
    LEFT JOIN {{ ref('time_type') }} AS tt
        ON tt.time_type_sk = tt_ds.time_type_sk
    LEFT JOIN {{ ref('project_data_source') }} AS pds
        ON dar.project_id = pds.project_id
    LEFT JOIN {{ ref('project') }} project
        ON project.ext_project_id = pds.ext_project_id
        AND project.company_id = emp_history.company_id      
)

SELECT *,
        -- Ordinary hours flag
        CASE 
            WHEN time_type = 'ORD'
            THEN TRUE
            ELSE FALSE
        END AS is_ordinary_hours,
        
        -- Sick leave flag
        CASE 
            WHEN time_type IN ('SYK', 'SYKA', 'SYKE', 'SYKL', 'SYKM')
            THEN TRUE
            ELSE FALSE
        END AS is_sick_leave,
        
        -- Overtime flag
        CASE
            WHEN time_type IN ('OVT100', 'OVT050')
            THEN TRUE
            ELSE FALSE
        END AS is_overtime
FROM unioned
