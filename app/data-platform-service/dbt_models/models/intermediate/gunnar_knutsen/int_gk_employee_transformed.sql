WITH employee AS (
    SELECT
        internal_id,
        client_id,
        employed_date::DATE,
        name,
        CASE
            WHEN ARRAY_LENGTH(STRING_TO_ARRAY(name, ' '), 1) = 1
                THEN
                    name
            ELSE
                (STRING_TO_ARRAY(name, ' '))[1]
        END                  AS first_name,
        CASE
            WHEN ARRAY_LENGTH(STRING_TO_ARRAY(name, ' '), 1) = 1
                THEN
                    NULL
            WHEN ARRAY_LENGTH(STRING_TO_ARRAY(name, ' '), 1) = 2
                THEN
                    NULL
            ELSE
                ARRAY_TO_STRING(
                    (STRING_TO_ARRAY(name, ' '))[
                        2:ARRAY_LENGTH(STRING_TO_ARRAY(name, ' '), 1) - 1
                    ],
                    ' '
                )
        END                  AS middle_name,
        CASE
            WHEN ARRAY_LENGTH(STRING_TO_ARRAY(name, ' '), 1) = 1
                THEN
                    NULL
            WHEN ARRAY_LENGTH(STRING_TO_ARRAY(name, ' '), 1) = 2
                THEN
                    (STRING_TO_ARRAY(name, ' '))[2]
            ELSE
                (STRING_TO_ARRAY(name, ' '))[ARRAY_LENGTH(STRING_TO_ARRAY(name, ' '), 1)]
        END                  AS last_name,
        CASE
            WHEN gender = 'M' THEN 'Male'
            WHEN gender = 'F' THEN 'Female'
            ELSE gender
        END                  AS gender,
        end_of_employment::DATE,
        private_phone_number AS company_mobile_phone,
        department_id        AS organizational_unit_id,
        department_name      AS organizational_unit_name
    FROM {{ ref('stg__admmit_employee') }}
)

SELECT
    employee.internal_id,
    employee.client_id,
    employee.employed_date,
    employee.name,
    employee.first_name,
    employee.middle_name,
    employee.last_name,
    employee.gender,
    employee.end_of_employment,
    employee.company_mobile_phone,
    employee.organizational_unit_id,
    employee.organizational_unit_name,
    department.legal_entity_id,
    division.legal_entity_name,
    division.company_id,
    company.company_name
FROM employee
LEFT JOIN {{ ref('department_seed') }} AS department
    ON employee.organizational_unit_id = department.external_id
LEFT JOIN {{ ref('division_seed') }} AS division
    ON department.legal_entity_id = division.id
LEFT JOIN {{ ref('company_seed') }} AS company
    ON division.company_id = company.id
