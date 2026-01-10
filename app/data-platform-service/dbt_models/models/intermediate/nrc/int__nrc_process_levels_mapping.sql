WITH process_level_seed AS (
    SELECT 
        division_id,
        division_code,
        process_level_id
    FROM {{ ref('process_levels_seed') }}
),

organization_structure AS (
    SELECT 
        pr.id AS project_id,
        pr.project_number,
        pr.project_name,
        deim.division_id,
        deim.division_name
    FROM {{ ref('project') }} pr
    LEFT JOIN {{ ref('int__division_external_id_mapping') }} deim
        ON pr.division_id = deim.division_id
    WHERE deim.data_source_name = 'TQM'
)

SELECT 
    pl.division_id,
    pl.division_code,
    pl.process_level_id,
    os.project_number,
    os.project_id,
    os.project_name,
    os.division_name
FROM process_level_seed AS pl
JOIN organization_structure AS os
    ON pl.division_id = os.division_id
