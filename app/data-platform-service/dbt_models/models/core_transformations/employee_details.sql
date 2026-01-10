WITH employee_source_data AS (
    SELECT * 
    FROM {{ source('raw_nrc_source', 'source_catalyst_employee') }}
    WHERE COALESCE(field->'0'->'data'->>'value', '') != ''
),
icore_time_records AS (
    SELECT *
    FROM {{ source('raw_nrc_source', 'source_icore_hours') }}
),
data_source AS (
    SELECT id, name
    FROM {{ ref('data_source_seed') }}
),
icore_employee_periodic_current AS (
    SELECT *
    FROM {{ source('raw_nrc_source', 'source_icore_employee_periodic_current') }}
),
department_mapping AS(
    SELECT *
    FROM {{ ref('int__department_external_id_mapping')}}
    WHERE data_source_name = 'iCore'
),
employee_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(["edl.guid"]) }} AS id,
        edl.guid AS employee_guid,
        edl.field->'0'->'data'->>'value' AS employee_id,
        edl.field->'37'->'data'->>'value' AS employment_date,
        edl.name AS "name",
        edl.field->'2'->'data'->>'value' AS first_name,
        edl.field->'26'->'data'->>'value' AS middle_name,
        edl.field->'3'->'data'->>'value' AS last_name,
        edl.field->'7'->'data'->>'value' AS email,
        edl.field->'101'->'data'->>'value' AS username,
        edl.field->'22'->'data'->>'value' AS ssn,
        edl.field->'19'->'data'->>'value' AS gender,
        edl.field->'1029'->'data'->>'value' AS corporate_seniority_date,
        edl.field->'38'->'data'->>'value' AS employment_end_date,
        edl.field->'1003'->'data'->>'value' AS company_mobile_phone,
        edl.field->'1028'->'data'->>'value' AS reason_for_leaving,
        cer.id AS employment_relationship_id,
        cdip.id AS organizational_unit_id,
        cdiv.id AS legal_entity_id,
        cc.id AS company_id,
        ceg.id AS group_id,
        cejr.id AS job_role_id,
        FALSE::BOOLEAN AS is_external,
        ds.id AS data_source_id,
        '{{ invocation_id }}' AS invocation_id
    FROM employee_source_data AS edl
    LEFT JOIN {{ ref('department_seed') }} cdip ON cdip.organizational_unit_guid = field->'8'->'data'->>'guid'
    LEFT JOIN {{ ref('division_seed') }} cdiv ON cdiv.legal_entity_guid = field->'8'->'data'->>'guid'
    LEFT JOIN {{ ref('company_seed') }} cc ON cc.company_guid = field->'8'->'data'->>'guid'
    LEFT JOIN {{ ref('employee_group') }} ceg ON ceg.external_id = field->'1046'->'data'->>'guid'
    LEFT JOIN {{ ref('employment_relationship') }} cer ON cer.external_id = field->'1051'->'data'->>'guid'
    LEFT JOIN {{ ref('employee_job_role') }} cejr ON cejr.ext_job_role_guid = field->'15'->'data'->>'guid'
    LEFT JOIN data_source ds ON ds.name = 'CatalystOne'
    WHERE ceg.description != 'Ansatt SE el. FI'
),
icore_rented_employee AS (
    SELECT DISTINCT ON (icore_employee.employeeid)
        {{ dbt_utils.generate_surrogate_key(["icore_employee.\"employeeid\""]) }} AS id,
        {{ dbt_utils.generate_surrogate_key(["icore_employee.\"employeeid\""]) }} AS employee_guid,
        icore_employee.employeeid AS employee_id,
        NULL AS employment_date,
        CONCAT(icore_employee.firstname, ' ', surname) AS name,
        icore_employee.firstName AS first_name,
        NULL AS middle_name,
        icore_employee.surname AS last_name,
        icore_employee.emailwork AS email,
        NULL AS username,
        NULL AS ssn,
        icore_employee.genderDescr AS gender,
        icore_employee.workSeniorityDate AS corporate_seniority_date,
        NULL AS employment_end_date,
        NULL AS company_mobile_phone,
        NULL AS reason_for_leaving,
        NULL AS employment_relationship_id,
        department_mapping.department_id AS organizational_unit_id,
        department_mapping.division_id AS legal_entity_id,
        department_mapping.company_id AS company_id,
        NULL AS group_id,
        NULL AS job_role_id,
        TRUE::BOOLEAN AS is_external,
        department_mapping.data_source_id AS data_source_id
    FROM {{ source('raw_nrc_source', 'source_icore_employee') }} icore_employee
    INNER JOIN icore_time_records ON icore_employee.employeeid = SUBSTRING(icore_time_records._employeekey FROM 6)
    LEFT JOIN employee_data ON icore_employee.employeeid = employee_data.employee_id
    LEFT JOIN icore_employee_periodic_current ON icore_employee.employeeid = icore_employee_periodic_current.employeeid
    LEFT JOIN department_mapping ON icore_employee_periodic_current.organisationunitid = department_mapping.external_id
    WHERE employee_data.employee_id IS NULL
    ORDER BY icore_employee.employeeid, icore_employee.alteredTs DESC
),
added_division AS (
    SELECT
    id,
    employee_guid,
    employee_id,
    employment_date,
    name,
    first_name,
    middle_name,
    last_name,
    email,
    username,
    ssn,
    gender,
    corporate_seniority_date,
    employment_end_date,
    company_mobile_phone,
    reason_for_leaving,
    employment_relationship_id,
    organizational_unit_id,
    CASE
        WHEN organizational_unit_id IS NOT NULL THEN (
            SELECT legal_entity_id FROM {{ ref('department_seed') }} dep
            WHERE dep.id = organizational_unit_id LIMIT 1
        )
        ELSE legal_entity_id
    END AS legal_entity_id,
    company_id,
    group_id,
    job_role_id,
    is_external,
    data_source_id
FROM employee_data
),
added_company AS (
    SELECT
        id,
        employee_guid,
        employee_id,
        employment_date,
        name,
        first_name,
        middle_name,
        last_name,
        email,
        username,
        ssn,
        gender,
        corporate_seniority_date,
        employment_end_date,
        company_mobile_phone,
        reason_for_leaving,
        employment_relationship_id,
        organizational_unit_id,
        legal_entity_id,
        CASE
            WHEN legal_entity_id IS NOT NULL THEN (
                SELECT company_id FROM {{ ref('division_seed') }} div
                WHERE div.id = legal_entity_id LIMIT 1
            )
            ELSE company_id
        END AS company_id,
        group_id,
        job_role_id,
        is_external,
        data_source_id
    FROM added_division
),
added_gk_div AS (
    SELECT
        id,
        employee_guid,
        employee_id,
        employment_date,
        name,
        first_name,
        middle_name,
        last_name,
        email,
        username,
        ssn,
        gender,
        corporate_seniority_date,
        employment_end_date,
        company_mobile_phone,
        reason_for_leaving,
        employment_relationship_id,
        organizational_unit_id,
        CASE
            WHEN legal_entity_id IS NULL 
            AND company_id = (SELECT id FROM {{ ref('company_seed') }}
                WHERE ext_company_id = '20') 
                THEN (
                SELECT id FROM {{ ref('division_seed') }} div
                WHERE div.legal_entity_name = 'Gunnar Knutsen'
            )
            ELSE legal_entity_id
        END AS legal_entity_id,
        company_id,
        group_id,
        job_role_id,
        is_external,
        data_source_id
    FROM added_company
    UNION ALL
    SELECT
        id,
        employee_guid,
        employee_id,
        employment_date,
        name,
        first_name,
        middle_name,
        last_name,
        email,
        username,
        ssn,
        gender,
        corporate_seniority_date,
        employment_end_date,
        company_mobile_phone,
        reason_for_leaving,
        employment_relationship_id,
        organizational_unit_id,
        legal_entity_id,
        company_id,
        group_id,
        job_role_id,
        is_external,
        data_source_id
    FROM icore_rented_employee
)
SELECT
        ac.id,
        ac.employee_guid,
        ac.employee_id,
        ac.name,
        ac.first_name,
        ac.middle_name,
        ac.last_name,
        ac.email,
        ac.username,
        ac.ssn,
        ac.gender,
        ac.corporate_seniority_date,
        ac.company_mobile_phone,
        ac.employment_relationship_id,
        ac.company_id,
        ac.data_source_id
FROM added_gk_div ac
