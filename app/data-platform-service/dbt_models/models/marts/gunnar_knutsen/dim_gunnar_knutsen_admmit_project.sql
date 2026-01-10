SELECT
    {{ dbt_utils.generate_surrogate_key(['internal_id','client_id'])}} 
                     AS project_id,
    2                AS tenant_id,
    'Gunnar Knutsen' AS tenant_name,
    6                AS data_source_id,
    'Admmit'         AS data_source_name,
    department_id    AS organizational_unit_id,
    customer_id,
    internal_id      AS ext_project_id,
    manager_id       AS project_leader_id,
    internal_id      AS project_number,
    project_number   AS project_name,
    description      AS project_description,
    planned_start_date,
    actual_start_date,
    planned_end_date,
    actual_end_date,
    NULL             AS project_address,
    NULL             AS project_location_latitude,
    NULL             AS project_location_longitude,
    is_active
FROM {{ ref('stg__admmit_project') }}
