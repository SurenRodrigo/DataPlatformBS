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
icore_department_mapping AS(
    SELECT *
    FROM {{ ref('int__department_external_id_mapping')}}
    WHERE data_source_name = 'iCore'
),
catalyst_department_mapping AS(
    SELECT *
    FROM {{ ref('int__department_external_id_mapping')}}
    WHERE data_source_name = 'CatalystOne'
),
ditio_division_mapping AS (
    SELECT division_source.division_id, division_source.ext_division_id
    FROM {{ ref('division_data_source_seed') }} AS division_source
    INNER JOIN data_source AS ditio_data_source ON ditio_data_source.id = division_source.data_source_id
    WHERE ditio_data_source.name = 'Ditio'
),
ditio_company_mapping AS (
    SELECT company_source.company_guid, company_source.ext_company_id
    FROM {{ ref('company_data_source_seed') }} AS company_source
    INNER JOIN data_source AS ditio_data_source ON ditio_data_source.id = company_source.data_source_id
    WHERE ditio_data_source.name = 'Ditio'
),
employee_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(["edl.guid"]) }} AS id,
        edl.guid AS employee_guid,
        edl.field->'0'->'data'->>'value' AS employee_id,
        edl.field->'37'->'data'->>'value' AS employment_date,
        edl.name AS name,
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
        cer.description AS employment_relationship_name,
        cdip.id AS organizational_unit_id,
        cdm.department_name AS organizational_unit_name,
        cdiv.id AS legal_entity_id,
        cc.id AS company_id,
        ceg.id AS group_id,
        ceg.description AS group_name,
        cejr.id AS job_role_id,
        cejr.description AS job_role_name,
        FALSE::BOOLEAN AS is_external,
        ds.id AS data_source_id,
        edl.createdon AS created_at,
        edl.lastmodified AS last_modified_at
    FROM employee_source_data AS edl
    LEFT JOIN {{ ref('department_seed') }} cdip ON cdip.organizational_unit_guid = field->'8'->'data'->>'guid'
    LEFT JOIN catalyst_department_mapping cdm ON cdm.department_id = cdip.id
    LEFT JOIN {{ ref('division_seed') }} cdiv ON cdiv.legal_entity_guid = field->'8'->'data'->>'guid'
    LEFT JOIN {{ ref('company_seed') }} cc ON cc.company_guid = field->'8'->'data'->>'guid'
    LEFT JOIN {{ ref('employee_group') }} ceg ON ceg.external_id = field->'1046'->'data'->>'guid'
    LEFT JOIN {{ ref('employment_relationship') }} cer ON cer.external_id = field->'1051'->'data'->>'guid'
    LEFT JOIN {{ ref('employee_job_role') }} cejr ON cejr.ext_job_role_guid = field->'15'->'data'->>'guid'
    LEFT JOIN data_source ds ON ds.name = 'CatalystOne'
    WHERE ceg.description != 'Ansatt SE el. FI' --Removing Employees in Finland and Sweden
),
icore_rented_employee AS (
    SELECT DISTINCT ON (icore_employee.employeeid)
        {{ dbt_utils.generate_surrogate_key(["icore_employee.\"employeeid\""]) }} AS id,
        {{ dbt_utils.generate_surrogate_key(["icore_employee.\"employeeid\""]) }} AS employee_guid,
        icore_employee.employeeid AS employee_id,
        NULL AS employment_date,
        CONCAT(icore_employee.firstname, ' ', surname) AS name,
        icore_employee.firstname AS first_name,
        NULL AS middle_name,
        icore_employee.surname AS last_name,
        icore_employee.emailwork AS email,
        NULL AS username,
        NULL AS ssn,
        icore_employee.genderdescr AS gender,
        icore_employee.worksenioritydate AS corporate_seniority_date,
        NULL AS employment_end_date,
        NULL AS company_mobile_phone,
        NULL AS reason_for_leaving,
        NULL AS employment_relationship_id,
        NULL AS employment_relationship_name,
        icore_department_mapping.department_id AS organizational_unit_id,
        icore_department_mapping.department_name AS organizational_unit_name,
        icore_department_mapping.division_id AS legal_entity_id,
        icore_department_mapping.company_id AS company_id,
        NULL AS group_id,
        NULL AS group_name,
        NULL AS job_role_id,
        NULL AS job_role_name,
        TRUE::BOOLEAN AS is_external,
        icore_department_mapping.data_source_id AS data_source_id,
        NULL AS created_at,
        NULL AS last_modified_at
    FROM {{ source('raw_nrc_source', 'source_icore_employee') }} icore_employee
    INNER JOIN icore_time_records ON icore_employee.employeeid = SUBSTRING(icore_time_records._employeekey FROM 6)
    LEFT JOIN employee_data ON icore_employee.employeeid = employee_data.employee_id
    LEFT JOIN icore_employee_periodic_current ON icore_employee.employeeid = icore_employee_periodic_current.employeeid
    LEFT JOIN icore_department_mapping ON icore_employee_periodic_current.organisationunitid = icore_department_mapping.external_id
    WHERE employee_data.employee_id IS NULL
    ORDER BY icore_employee.employeeid, icore_employee.alteredts DESC
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
    employment_relationship_name,
    organizational_unit_id,
    organizational_unit_name,
    CASE
        WHEN organizational_unit_id IS NOT NULL THEN (
            SELECT legal_entity_id FROM {{ ref('department_seed') }} dep
            WHERE dep.id = organizational_unit_id LIMIT 1
        )
        ELSE legal_entity_id
    END AS legal_entity_id,
    company_id,
    group_id,
    group_name,
    job_role_id,
    job_role_name,
    is_external,
    data_source_id,
    created_at,
    last_modified_at
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
        employment_relationship_name,
        organizational_unit_id,
        organizational_unit_name,
        legal_entity_id,
        CASE
            WHEN legal_entity_id IS NOT NULL THEN (
                SELECT company_id FROM {{ ref('division_seed') }} div
                WHERE div.id = legal_entity_id LIMIT 1
            )
            ELSE company_id
        END AS company_id,
        group_id,
        group_name,
        job_role_id,
        job_role_name,
        is_external,
        data_source_id,
        created_at,
        last_modified_at
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
        employment_relationship_name,
        organizational_unit_id,
        organizational_unit_name,
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
        group_name,
        job_role_id,
        job_role_name,
        is_external,
        data_source_id,
        created_at,
        last_modified_at
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
        employment_relationship_name,
        organizational_unit_id,
        organizational_unit_name,
        legal_entity_id,
        company_id,
        group_id,
        group_name,
        job_role_id,
        job_role_name,
        is_external,
        data_source_id,
        created_at,
        last_modified_at
    FROM icore_rented_employee
)
SELECT
        ac.id,
        ac.employee_guid,
        ac.employee_id,
        ac.employment_date,
        ac.name,
        ac.first_name,
        ac.middle_name,
        ac.last_name,
        ac.email,
        ac.username,
        ac.ssn,
        ac.gender,
        ac.corporate_seniority_date,
        ac.employment_end_date,
        ac.company_mobile_phone,
        ac.reason_for_leaving,
        ac.employment_relationship_id,
        ac.employment_relationship_name,
        ac.organizational_unit_id,
        ac.organizational_unit_name,
        ac.legal_entity_id,
        ditio_division_mapping.ext_division_id AS ext_ditio_legal_entity_id,
        ac.company_id,
        ditio_company_mapping.ext_company_id   AS ext_ditio_company_id,
        ac.group_id,
        ac.group_name,
        ac.job_role_id,
        ac.job_role_name,
        ac.is_external,
        ac.data_source_id,
        ac.created_at,
        ac.last_modified_at
FROM added_gk_div AS ac
LEFT JOIN ditio_division_mapping ON ditio_division_mapping.division_id = ac.legal_entity_id
LEFT JOIN ditio_company_mapping ON ditio_company_mapping.company_guid = ac.company_id
