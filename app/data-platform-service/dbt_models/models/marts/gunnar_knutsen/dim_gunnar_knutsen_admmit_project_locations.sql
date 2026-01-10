SELECT
    {{ dbt_utils.generate_surrogate_key(['internal_id'])}} AS project_location_id,
    2                          AS tenant_id,
    'Gunnar Knutsen'           AS tenant_name,
    project_id,
    6                          AS data_source_id,
    'Admmit'                   AS data_source_name,
    hanger_name                AS location_name,
    hanger_id                  AS ext_location_id,
    hanger_registration_number AS location_registration_number,
    CASE
        WHEN hanger_id IS NOT NULL THEN 'Hanger'
    END                        AS location_type,
    NULL::BOOLEAN              AS is_active
FROM {{ ref('int_get_gk_project_locations') }}
