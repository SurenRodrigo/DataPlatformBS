WITH

-- Extract data from Catalyst history for internal employees
catalyst_internal AS (
    SELECT
        employeeid AS employee_id,
        field,
        NULL         AS employee_periodic_key,
        NULL         AS status_code
    FROM {{ source('raw_nrc_source', 'source_catalyst_v2_employee_history') }}
    WHERE
        employeeid != ''
        AND employeeid ~ '^\d+$'
),

employees AS (
    SELECT * from catalyst_internal
    WHERE field -> '1046' -> 'data' ->> 'value' != 'Ansatt SE el. FI'
),

-- Intermediate working periods
employee_working_periods AS (
    SELECT * FROM {{ ref('int__employment_periods') }}
),

-- Extract external employee data from ICore
icore_external AS (
    SELECT
        employeeid         AS employee_id,
        companyid          AS company_id,
        _key                 AS employee_periodic_key,
        NULLIF(validfromdate, '')     AS start_date,
        NULLIF(validtodate, '')       AS end_date,
        organisationunitid AS organizational_unit_id,
        statuscode         AS status_code,
        NULL                 AS last_working_date_in_legal_entity,
        NULL                 AS reason_for_leaving,
        NULL                 AS changed_at,
        NULL                 AS legal_entity_id,
        NULL                 AS employee_group_id,
        NULL                 AS employee_group_name,
        NULL                 AS employment_relationship_id,
        NULL                 AS employment_relationship_name,
        NULL                 AS job_role_id,
        NULL                 AS employment_end_date
    FROM {{ source('raw_nrc_source', 'source_icore_employee_periodic_current') }}
    WHERE LEFT(employeeid, 3) = '800'
),

-- Catalyst organization history
catalyst_organization_history AS (
    SELECT
        employee_id,
        field,
        employee_periodic_key,
        status_code,
        JSONB_ARRAY_ELEMENTS((field -> '8' ->> 'auditChange')::jsonb) AS hist_audit
    FROM employees
),

-- Catalyst employee group history
catalyst_employee_group_history AS (
    SELECT
        employee_id,
        field,
        employee_periodic_key,
        status_code,
        JSONB_ARRAY_ELEMENTS((field -> '1046' ->> 'auditChange')::jsonb) AS hist_audit
    FROM employees
),

-- Catalyst employment relationship history
catalyst_employment_relationship_history AS (
    SELECT
        employee_id,
        field,
        employee_periodic_key,
        status_code,
        JSONB_ARRAY_ELEMENTS((field -> '1051' ->> 'auditChange')::jsonb) AS hist_audit
    FROM employees
),

-- Combine internal and external organization history
employee_organization_history_combined AS (
    SELECT
        employee_id,
        employee_periodic_key,
        company.id                            AS company_id,
        company.company_name,
        division.id                           AS legal_entity_id,
        division.legal_entity_name,
        department.id                         AS organizational_unit_id,
        department.organizational_unit_name,
        nullif(hist_audit ->> 'dataValidFrom', '')        AS start_date,
        nullif(hist_audit ->> 'dataValidTo', '')          AS end_date,
        employee_group.id                     AS employee_group_id,
        employee_group.description            AS employee_group_name,
        employee_relationship.id              AS employment_relationship_id,
        employee_relationship.description     AS employment_relationship_name,
        field -> '15' -> 'data' ->> 'guid'    AS job_role_id,
        field -> '1094' -> 'data' ->> 'value' AS last_working_date_in_legal_entity,
        field -> '1028' -> 'data' ->> 'value' AS reason_for_leaving,
        hist_audit ->> 'changedAt'            AS changed_at,
        status_code,
        NULLIF(field -> '38' -> 'data' ->> 'value', '')   AS employment_end_date
    FROM catalyst_organization_history
    LEFT JOIN {{ ref('department_seed') }} AS department
        ON hist_audit -> 'data' ->> 'guid' = department.organizational_unit_guid
    LEFT JOIN {{ ref('division_seed') }} AS division
        ON department.legal_entity_id = division.id
    LEFT JOIN {{ ref('company_seed') }} AS company
        ON division.company_id = company.id
    LEFT JOIN {{ ref('employee_group') }} AS employee_group
        ON field -> '1046' -> 'data' ->> 'guid' = employee_group.external_id
    LEFT JOIN {{ ref('employment_relationship') }} AS employee_relationship
        ON field -> '1051' -> 'data' ->> 'guid' = employee_relationship.external_id     
    WHERE
        (
            hist_audit ->> 'timeline' = '1'
        )

    UNION ALL

    SELECT
        employee_id,
        employee_periodic_key,
        company.id    AS company_id,
        company.company_name,
        division.id   AS legal_entity_id,
        division.legal_entity_name,
        department.id AS organizational_unit_id,
        department.organizational_unit_name,
        start_date,
        end_date,
        employee_group_id,
        employee_group_name,
        employment_relationship_id,
        employment_relationship_name,
        job_role_id,
        last_working_date_in_legal_entity,
        reason_for_leaving,
        changed_at,
        status_code,
        employment_end_date
    FROM icore_external AS int_his
    LEFT JOIN {{ ref('department_seed') }} AS department
        ON int_his.organizational_unit_id = department.organizational_unit_guid
    LEFT JOIN {{ ref('division_seed') }} AS division
        ON department.legal_entity_id = division.id
    LEFT JOIN {{ ref('company_seed') }} AS company
        ON division.company_id = company.id
),

-- Employee group history details
employee_group_history AS (
    SELECT
        employee_id,
        employee_periodic_key,
        company.id                            AS company_id,
        company.company_name,
        division.id                           AS legal_entity_id,
        division.legal_entity_name,
        department.id                         AS organizational_unit_id,
        department.organizational_unit_name,
        NULLIF(hist_audit ->> 'dataValidFrom', '')        AS start_date,
        nullif(hist_audit ->> 'dataValidTo', '')         AS end_date,
        employee_group.id                     AS employee_group_id,
        employee_group.description            AS employee_group_name,
        employee_relationship.id              AS employment_relationship_id,
        employee_relationship.description     AS employment_relationship_name,
        field -> '15' -> 'data' ->> 'guid'    AS job_role_id,
        field -> '1094' -> 'data' ->> 'value' AS last_working_date_in_legal_entity,
        field -> '1028' -> 'data' ->> 'value' AS reason_for_leaving,
        hist_audit ->> 'changedAt'            AS changed_at,
        status_code,
        nullif(field -> '38' -> 'data' ->> 'value', '')   AS employment_end_date,
        FALSE                                 AS external
    FROM catalyst_employee_group_history
    LEFT JOIN {{ ref('department_seed') }} AS department
        ON field -> '8' -> 'data' ->> 'guid' = department.organizational_unit_guid
    LEFT JOIN {{ ref('division_seed') }} AS division
        ON department.legal_entity_id = division.id
    LEFT JOIN {{ ref('company_seed') }} AS company
        ON division.company_id = company.id
    LEFT JOIN {{ ref('employee_group') }} AS employee_group
        ON hist_audit -> 'data' ->> 'guid' = employee_group.external_id
    LEFT JOIN {{ ref('employment_relationship') }} AS employee_relationship
        ON field -> '1051' -> 'data' ->> 'guid' = employee_relationship.external_id
    WHERE
        (
            hist_audit ->> 'timeline' = '1'
        )
),

-- Employment relationship history
employment_relationship_history AS (
    SELECT
        employee_id,
        employee_periodic_key,
        company.id                            AS company_id,
        company.company_name,
        division.id                           AS legal_entity_id,
        division.legal_entity_name,
        department.id                         AS organizational_unit_id,
        department.organizational_unit_name,
        nullif(hist_audit ->> 'dataValidFrom', '' )       AS start_date,
        nullif(hist_audit ->> 'dataValidTo', '')          AS end_date,
        employee_group.id                     AS employee_group_id,
        employee_group.description            AS employee_group_name,
        employee_relationship.id              AS employment_relationship_id,
        employee_relationship.description     AS employment_relationship_name,
        field -> '15' -> 'data' ->> 'guid'    AS job_role_id,
        field -> '1094' -> 'data' ->> 'value' AS last_working_date_in_legal_entity,
        field -> '1028' -> 'data' ->> 'value' AS reason_for_leaving,
        hist_audit ->> 'changedAt'            AS changed_at,
        status_code,
        nullif(field -> '38' -> 'data' ->> 'value', '')   AS employment_end_date,
        FALSE                                 AS external
    FROM catalyst_employment_relationship_history
    LEFT JOIN {{ ref('department_seed') }} AS department
        ON field -> '8' -> 'data' ->> 'guid' = department.organizational_unit_guid
    LEFT JOIN {{ ref('division_seed') }} AS division
        ON department.legal_entity_id = division.id
    LEFT JOIN {{ ref('company_seed') }} AS company
        ON division.company_id = company.id
    LEFT JOIN {{ ref('employee_group') }} AS employee_group
        ON field -> '1046' -> 'data' ->> 'guid' = employee_group.external_id
    LEFT JOIN {{ ref('employment_relationship') }} AS employee_relationship
        ON hist_audit -> 'data' ->> 'guid' = employee_relationship.external_id
    WHERE
        (
            hist_audit ->> 'timeline' = '1'
        )
),

-- Combine all three histories
combined_history AS (
    SELECT
        employee_id,
        employee_group_id,
        employee_group_name,
        employment_relationship_id,
        employment_relationship_name,
        job_role_id,
        employee_periodic_key,
        company_id,
        company_name,
        legal_entity_id,
        legal_entity_name,
        organizational_unit_id,
        organizational_unit_name,
        start_date,
        end_date,
        last_working_date_in_legal_entity,
        reason_for_leaving,
        changed_at,
        employment_end_date,
        status_code,
        COALESCE(LEFT(employee_id, 3) = '800', FALSE) AS external,
        CASE
            WHEN employment_end_date = '' THEN TRUE
            WHEN employment_end_date::date < CURRENT_DATE THEN FALSE
            WHEN status_code = 'TERMINATED' THEN FALSE
            ELSE TRUE
        END                                           AS active,
        'ORGANIZATION'                                AS update_type
    FROM employee_organization_history_combined

    UNION ALL

    SELECT
        employee_id,
        employee_group_id,
        employee_group_name,
        employment_relationship_id,
        employment_relationship_name,
        job_role_id,
        employee_periodic_key,
        company_id,
        company_name,
        legal_entity_id,
        legal_entity_name,
        organizational_unit_id,
        organizational_unit_name,
        start_date,
        end_date,
        last_working_date_in_legal_entity,
        reason_for_leaving,
        changed_at,
        employment_end_date,
        status_code,
        external,
        CASE
            WHEN employment_end_date = '' THEN TRUE
            WHEN employment_end_date::date < CURRENT_DATE THEN FALSE
            WHEN status_code = 'TERMINATED' THEN FALSE
            ELSE TRUE
        END,
        'EMPLOYEE_GROUP'
    FROM employee_group_history

    UNION ALL

    SELECT
        employee_id,
        employee_group_id,
        employee_group_name,
        employment_relationship_id,
        employment_relationship_name,
        job_role_id,
        employee_periodic_key,
        company_id,
        company_name,
        legal_entity_id,
        legal_entity_name,
        organizational_unit_id,
        organizational_unit_name,
        start_date,
        end_date,
        last_working_date_in_legal_entity,
        reason_for_leaving,
        changed_at,
        employment_end_date,
        status_code,
        external,
        CASE
            WHEN employment_end_date = '' THEN TRUE
            WHEN employment_end_date::date < CURRENT_DATE THEN FALSE
            WHEN status_code = 'TERMINATED' THEN FALSE
            ELSE TRUE
        END,
        'EMPLOYMENT_RELATIONSHIP'
    FROM employment_relationship_history
    ORDER BY employee_id ASC, start_date DESC
),

-- Final output with working periods
period_history AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['periods.id', 'history.employee_id','history.update_type']) }} AS id,
        history.*,
        periods.period_start,
        periods.period_end
    FROM combined_history history
    LEFT JOIN employee_working_periods periods
        ON history.employee_id = periods.employee_id
        AND (history.start_date::date, COALESCE(history.end_date::date, CURRENT_DATE))
        OVERLAPS (periods.period_start::date, COALESCE(periods.period_end::date, CURRENT_DATE))
)

-- Final SELECT
SELECT * FROM period_history