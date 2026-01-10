WITH ditio_wbs AS (

    SELECT * FROM {{ ref('stg__ditio_work_breakdown_structure')}}
    WHERE external_number IS NOT NULL

),


ditio_projects AS (

    SELECT * FROM {{ ref('int__nrc_ditio_project')}}

),


unit4_project_tasks AS (

    SELECT * FROM {{ ref(('project_task'))}}

),


final AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['ditio_wbs.id']) }} AS project_task_detail_sk,
        unit4_project_tasks.project_task_sk,
        ditio_wbs.id                                             AS ext_project_task_detail_id,
        ditio_wbs.name,
        ditio_wbs.full_path_name                                 AS task_full_path_name,
        ditio_wbs.folder_path_name                               AS task_folder_path_name,
        ditio_wbs.parent_name                                    AS task_parent_name,
        ditio_wbs.name_with_number                               AS task_description,
        ditio_wbs.company_id                                     AS wbs_company_id,
        ditio_projects.project_number                            AS ext_project_number,
        ditio_wbs.external_number                                AS ext_task_number,
        ditio_wbs.chapter_id,
        unit4_project_tasks.ext_project_task_id,
        7                         AS data_source_id
    FROM ditio_wbs
    LEFT JOIN ditio_projects
        ON ditio_wbs.project_id = ditio_projects.ext_project_guid
    LEFT JOIN unit4_project_tasks
        ON ditio_projects.project_number = unit4_project_tasks.project_id
        AND ditio_wbs.external_number = unit4_project_tasks.ext_project_task_id

)


SELECT *
FROM final
WHERE project_task_sk IS NOT NULL
