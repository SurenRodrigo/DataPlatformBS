-- depends_on: {{ ref('department_seed') }}
WITH
catalyst_internal AS (
    -- Extract data from the catalyst history table for internal employees
    SELECT
        employeeid AS employee_id,
        field,
        NULL         AS employee_periodic_key,
        NULL         AS status_code
    FROM {{ source('raw_nrc_source', 'source_catalyst_employee_history') }}
    WHERE
        employeeid != ''
        AND employeeid ~ '^\d+$'
),

icore_external AS (
    SELECT
        employeeid         AS employee_id,
        companyid          AS company_id,
        _key               AS employee_periodic_key,
        validfromdate      AS start_date,
        validtodate        AS end_date,
        organisationunitid AS organizational_unit_id,
        statuscode         AS status_code,
        NULL               AS last_working_date_in_legal_entity,
        NULL               AS reason_for_leaving,
        NULL               AS changed_at,
        NULL               AS legal_entity_id,
        NULL               AS employee_group_id,
        NULL               AS employee_group_name,
        NULL               AS employment_relationship_id,
        NULL               AS employment_relationship_name,
        NULL               AS job_role_id,
        NULL               AS employment_end_date
    FROM {{ source('raw_nrc_source', 'source_icore_employee_periodic_current') }}
    WHERE LEFT(employeeid, 3) = '800'
),

catalyst_organization_history AS (
    SELECT
        employee_id,
        field,
        employee_periodic_key,
        status_code,
        JSONB_ARRAY_ELEMENTS((field -> '8' ->> 'auditChange')::jsonb) AS hist_audit
    FROM catalyst_internal
),

catalyst_employee_group_history AS (
    SELECT
        employee_id,
        field,
        employee_periodic_key,
        status_code,
        JSONB_ARRAY_ELEMENTS((field -> '1046' ->> 'auditChange')::jsonb) AS hist_audit
    FROM catalyst_internal
),

catalyst_employment_relationship_history AS (
    SELECT
        employee_id,
        field,
        employee_periodic_key,
        status_code,
        JSONB_ARRAY_ELEMENTS((field -> '1051' ->> 'auditChange')::jsonb) AS hist_audit
    FROM catalyst_internal
),

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
        NULLIF(hist_audit ->> 'dataValidFrom', '') AS start_date,
        NULLIF(hist_audit ->> 'dataValidTo',   '') AS end_date,
        employee_group.id                     AS employee_group_id,
        employee_group.description            AS employee_group_name,
        employee_relationship.id              AS employment_relationship_id,
        employee_relationship.description     AS employment_relationship_name,
        field -> '15' -> 'data' ->> 'guid'    AS job_role_id,
        field -> '1094' -> 'data' ->> 'value' AS last_working_date_in_legal_entity,
        field -> '1028' -> 'data' ->> 'value' AS reason_for_leaving,
        hist_audit ->> 'changedAt'            AS changed_at,
        status_code,
        field -> '38' -> 'data' ->> 'value'   AS employment_end_date
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
            hist_audit ->> 'timeline' = '1' AND
            employee_group.description != 'Ansatt SE el. FI' --Removing Employees in Finland and Sweden
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
        NULLIF(hist_audit ->> 'dataValidFrom', '') AS start_date,
        NULLIF(hist_audit ->> 'dataValidTo',   '') AS end_date,
        employee_group.id                     AS employee_group_id,
        employee_group.description            AS employee_group_name,
        employee_relationship.id              AS employment_relationship_id,
        employee_relationship.description     AS employment_relationship_name,
        field -> '15' -> 'data' ->> 'guid'    AS job_role_id,
        field -> '1094' -> 'data' ->> 'value' AS last_working_date_in_legal_entity,
        field -> '1028' -> 'data' ->> 'value' AS reason_for_leaving,
        hist_audit ->> 'changedAt'            AS changed_at,
        status_code,
        field -> '38' -> 'data' ->> 'value'   AS employment_end_date,
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
            hist_audit ->> 'timeline' = '1' AND
            employee_group.description != 'Ansatt SE el. FI' --Removing Employees in Finland and Sweden
        )
),

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
        NULLIF(hist_audit ->> 'dataValidFrom', '') AS start_date,
        NULLIF(hist_audit ->> 'dataValidTo',   '') AS end_date,
        employee_group.id                     AS employee_group_id,
        employee_group.description            AS employee_group_name,
        employee_relationship.id              AS employment_relationship_id,
        employee_relationship.description     AS employment_relationship_name,
        field -> '15' -> 'data' ->> 'guid'    AS job_role_id,
        field -> '1094' -> 'data' ->> 'value' AS last_working_date_in_legal_entity,
        field -> '1028' -> 'data' ->> 'value' AS reason_for_leaving,
        hist_audit ->> 'changedAt'            AS changed_at,
        status_code,
        field -> '38' -> 'data' ->> 'value'   AS employment_end_date,
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
            hist_audit ->> 'timeline' = '1' AND
            employee_group.description != 'Ansatt SE el. FI' --Removing Employees in Finland and Sweden
        )
)

-- Combine all updates first
, combined_updates AS (
    -- Organization History
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
        'ORGANIZATION'                                AS update_type
    FROM employee_organization_history_combined

    UNION ALL

    -- Employee Group History
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
        'EMPLOYEE_GROUP' AS update_type
    FROM employee_group_history

    UNION ALL

    -- Employment Relationship History
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
        'EMPLOYMENT_RELATIONSHIP' AS update_type
    FROM employment_relationship_history
),

--Handling duplication and overlapping periods
normalized_updates AS (
    SELECT
        updates_combined.employee_id,
        updates_combined.employee_group_id,
        updates_combined.employee_group_name,
        updates_combined.employment_relationship_id,
        updates_combined.employment_relationship_name,
        updates_combined.job_role_id,
        updates_combined.employee_periodic_key,
        updates_combined.company_id,
        updates_combined.company_name,
        updates_combined.legal_entity_id,
        updates_combined.legal_entity_name,
        updates_combined.organizational_unit_id,
        updates_combined.organizational_unit_name,
        NULLIF(updates_combined.start_date, '')::date              AS start_date,
        NULLIF(updates_combined.end_date,   '')::date              AS end_date,
        updates_combined.last_working_date_in_legal_entity,
        updates_combined.reason_for_leaving,
        updates_combined.changed_at,
        NULLIF(updates_combined.changed_at, '')::timestamp         AS changed_at_ts,
        updates_combined.employment_end_date,
        updates_combined.status_code,
        updates_combined.external,
        updates_combined.update_type,
        CASE
            WHEN updates_combined.update_type = 'EMPLOYMENT_RELATIONSHIP' THEN
                COALESCE(updates_combined.employment_relationship_id::text, 'NULL')
            WHEN updates_combined.update_type = 'EMPLOYEE_GROUP' THEN
                COALESCE(updates_combined.employee_group_id::text, 'NULL')
            ELSE
                CONCAT_WS('|',
                    COALESCE(updates_combined.company_id::text, 'NULL'),
                    COALESCE(updates_combined.legal_entity_id::text, 'NULL'),
                    COALESCE(updates_combined.organizational_unit_id::text, 'NULL')
                )
        END                                            AS subtype_key,
        COALESCE(NULLIF(updates_combined.end_date, '')::date,
                 DATE '9999-12-31')                    AS end_date_for_merge
    FROM combined_updates AS updates_combined
),

updates_with_prev_end AS (
    SELECT
        normalized.*,
        LAG(normalized.end_date_for_merge) OVER (
            PARTITION BY normalized.employee_id, normalized.update_type, normalized.subtype_key
            ORDER BY normalized.start_date, normalized.end_date_for_merge
        ) AS previous_end_date_for_group
    FROM normalized_updates AS normalized
),

updates_grouped AS (
    SELECT
        prev_end.*,
        SUM(
            CASE
                WHEN prev_end.previous_end_date_for_group IS NULL
                    OR prev_end.start_date > prev_end.previous_end_date_for_group THEN 1
                ELSE 0
            END
        ) OVER (
            PARTITION BY prev_end.employee_id, prev_end.update_type, prev_end.subtype_key
            ORDER BY prev_end.start_date, prev_end.end_date_for_merge
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS group_number
    FROM updates_with_prev_end AS prev_end
),

updates_ranked AS (
    SELECT
        grouped_updates.*,
        MIN(grouped_updates.start_date) OVER (
            PARTITION BY grouped_updates.employee_id, grouped_updates.update_type, grouped_updates.subtype_key, grouped_updates.group_number
        ) AS merged_start_date,
        MAX(grouped_updates.end_date_for_merge) OVER (
            PARTITION BY grouped_updates.employee_id, grouped_updates.update_type, grouped_updates.subtype_key, grouped_updates.group_number
        ) AS merged_end_date_for_merge,
        ROW_NUMBER() OVER (
            PARTITION BY grouped_updates.employee_id, grouped_updates.update_type, grouped_updates.subtype_key, grouped_updates.group_number
            ORDER BY grouped_updates.changed_at_ts DESC NULLS LAST,
                     grouped_updates.start_date DESC,
                     grouped_updates.end_date_for_merge DESC
        ) AS row_rank
    FROM updates_grouped AS grouped_updates
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['updates_final.employee_id', 'updates_final.merged_start_date', 'updates_final.update_type']) }} AS id,
    updates_final.employee_id,
    updates_final.employee_group_id,
    updates_final.employee_group_name,
    updates_final.employment_relationship_id,
    updates_final.employment_relationship_name,
    updates_final.job_role_id,
    updates_final.employee_periodic_key,
    updates_final.company_id,
    updates_final.company_name,
    updates_final.legal_entity_id,
    updates_final.legal_entity_name,
    updates_final.organizational_unit_id,
    updates_final.organizational_unit_name,
    updates_final.merged_start_date                    AS start_date,
    NULLIF(updates_final.merged_end_date_for_merge, DATE '9999-12-31') AS end_date,
    updates_final.last_working_date_in_legal_entity,
    updates_final.reason_for_leaving,
    updates_final.changed_at,
    updates_final.employment_end_date,
    updates_final.status_code,
    updates_final.external,
    CASE
        WHEN updates_final.employment_end_date = '' THEN TRUE
        WHEN updates_final.employment_end_date::date < CURRENT_DATE THEN FALSE
        WHEN updates_final.status_code = 'TERMINATED' THEN FALSE
        ELSE TRUE
    END                                               AS active,
    updates_final.update_type
FROM updates_ranked AS updates_final
WHERE updates_final.row_rank = 1
ORDER BY
    updates_final.employee_id,
    updates_final.merged_start_date DESC