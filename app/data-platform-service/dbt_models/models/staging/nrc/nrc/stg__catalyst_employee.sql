SELECT    
    guid                                                                AS employee_guid,
    NULLIF(TRIM(field->'0'->'data'->>'value'),'')                       AS employee_id,
    NULLIF(TRIM(field->'37'->'data'->>'value'), '')                     AS employment_date,
    NULLIF(TRIM(name), '')                                              AS name,
    NULLIF(TRIM(field->'2'->'data'->>'value'), '')                      AS first_name,
    NULLIF(TRIM(field->'26'->'data'->>'value'), '')                     AS middle_name,
    NULLIF(TRIM(field->'3'->'data'->>'value'), '')                      AS last_name,
    NULLIF(TRIM(field->'7'->'data'->>'value'), '')                      AS email,
    NULLIF(TRIM(field->'101'->'data'->>'value'), '')                    AS username,
    NULLIF(TRIM(field->'22'->'data'->>'value'), '')                     AS ssn,
    NULLIF(TRIM(field->'19'->'data'->>'value'), '')                     AS gender,
    NULLIF(TRIM(field->'1029'->'data'->>'value'), '')                   AS corporate_seniority_date,
    NULLIF(TRIM(field->'38'->'data'->>'value'), '')                     AS employment_end_date,
    NULLIF(TRIM(field->'1003'->'data'->>'value'), '')                   AS company_mobile_phone,
    NULLIF(TRIM(field->'1028'->'data'->>'value'), '')                   AS reason_for_leaving,
    NULLIF(TRIM(field->'1046'->'data'->>'guid'), '')                    AS employee_group_guid,
    NULLIF(TRIM(field->'1046'->'data'->>'value'), '')                   AS employee_group_description,
    NULLIF(TRIM(field->'15'->'data'->>'guid'), '')                      AS job_role_guid,
    NULLIF(TRIM(field->'15'->'data'->>'value'), '')                     AS job_role_description,
    NULLIF(TRIM(field->'1051'->'data'->>'guid'), '')                    AS employment_relationship_guid,
    NULLIF(TRIM(field->'1051'->'data'->>'value'), '')                   AS employment_relationship_description,
    CASE 
        WHEN TRIM(field->'19'->'data'->>'value') = 'Kvinne' THEN TRUE
        ELSE FALSE
    END AS is_woman,
    CASE
        WHEN TRIM(field->'1051'->'data'->>'value') IN (
            'Midlertidig ansatt', 'Midlertidig ansatt som tilkallingsvikar'
        ) THEN TRUE
        ELSE FALSE
    END AS is_temp,
    CASE
        WHEN TRIM(field->'1046'->'data'->>'value') ='Lærling' THEN TRUE
        ELSE FALSE
    END AS is_apprentice,
    CASE
        WHEN TRIM(field->'1046'->'data'->>'value') ='Innleid personell' THEN TRUE
        ELSE FALSE
    END AS is_rented_personnel,
    -- DBT metadata
    dbt_scd_id,
    dbt_updated_at,
    dbt_valid_from,
    dbt_valid_to
FROM {{ ref('catalyst_employee_snapshot') }}
WHERE dbt_valid_to IS NULL
    AND employeeid != ''
    AND employeeid ~ '^\d+$'
