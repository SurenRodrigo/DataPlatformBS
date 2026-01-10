SELECT
    {{ dbt_utils.generate_surrogate_key(['project_no']) }} AS id,
    project_no,
    project_type_1_hfm,
    production_area,
    project_2,
    project_type_4
FROM {{ ref('stg__excel_project_mapping') }}

