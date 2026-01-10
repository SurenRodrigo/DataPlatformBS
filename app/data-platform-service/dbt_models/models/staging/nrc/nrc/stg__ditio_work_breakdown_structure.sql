SELECT
    id,
    name,
    chapterid          AS chapter_id,
    companyid          AS company_id,
    createdby          AS created_by,
    deletedby          AS deleted_by,
    isdeleted          AS is_deleted,
    projectid          AS project_id,
    isexternal         AS is_external,
    modifiedby         AS modified_by,
    parentname         AS parent_name,
    companyname        AS company_name,
    fullpathname       AS full_path_name,
    NULLIF(externalnumber, '')     AS external_number,
    folderpathname     AS folder_path_name,
    namewithnumber     AS name_with_number,
    createddatetime    AS created_date_time,
    deleteddatetime    AS deleted_date_time,
    modifieddatetime   AS modified_date_time,
    -- DBT metadata
    dbt_scd_id,
    dbt_updated_at,
    dbt_valid_from,
    dbt_valid_to
FROM {{ ref('ditio_work_breakdown_structure_snapshot') }}
WHERE dbt_valid_to IS NULL
