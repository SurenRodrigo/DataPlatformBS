{{ config(
    tags=['nrc', 'tqm_case_tables']
) }}

WITH all_tqm_projects AS (
    SELECT
        ext_project_id,
        project_level_id
    FROM {{ ref('int__nrc_tqm_case_deduplicated') }}
)

SELECT 
    ext_project_id     AS project_id,
    project_level_id   AS ext_project_level_id
FROM all_tqm_projects
WHERE 
    ext_project_id IS NOT NULL 
    AND project_level_id IS NOT NULL
GROUP BY project_id, ext_project_level_id