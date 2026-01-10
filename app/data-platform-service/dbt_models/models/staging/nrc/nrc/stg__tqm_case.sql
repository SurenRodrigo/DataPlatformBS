{{ config(
    tags=['nrc', 'tqm_case_tables']
) }}

SELECT
    (projectlevel ->> 'ID')::INT  AS project_id,
    caseid                        AS case_external_id,
    caseurl                       AS case_url,
    position                      AS position,
    projectlevel                  AS project_level,
    projectlevel ->> 'FullPath'   AS project_level_full_path,
    projectlevel ->> 'Name'       AS project_level_name,
    CASE 
        WHEN SPLIT_PART(projectlevel ->> 'Name', '-', 1) ~ '^\d+$' 
        THEN SPLIT_PART(projectlevel ->> 'Name', '-', 1)::INT
        WHEN SPLIT_PART(projectlevel ->> 'Name', ' ', 1) ~ '^\d+$'
        THEN SPLIT_PART(projectlevel ->> 'Name', ' ', 1)::INT
        ELSE NULL 
    END AS ext_project_id,
    (projectlevel ->> 'ID')::INT  AS project_level_id,
    dateoccurred                  AS date_occurred,
    TO_TIMESTAMP(dateoccurred, 'DD/MM/YYYY') AT TIME ZONE 'UTC' AS case_occurred_date,
    datepublished                 AS date_published,
    deadline                      AS deadline,
    dateclosed                    AS date_closed,
    casetype                      AS case_type,
    casetype ->> 'Name'           AS case_type_name,
    casetype ->> 'ID'             AS case_type_id,
    description                   AS description,
    organizationlevel3            AS organization_level_3,
    organizationlevel3 ->> 'Name' AS organization_level_3_name,
    organizationlevel3 ->> 'ID'   AS organization_level_3_id,
    kpiparameters                 AS kpi_parameters,
    originator                    AS originator,
    originator ->> 'Login'        AS originator_email,
    originator ->> 'Name'         AS originator_name,
    originator ->> 'ID'           AS originator_id,
    status                        AS case_status,
    status ->> 'Key'              AS status_key,
    status ->> 'Name'             AS status_name,
    immediateactions              AS immediate_actions,    
    actions                       AS actions,
    NULLIF(TRIM(comments),'')     AS comments,
    MD5(
        caseid
        || (casetype ->> 'ID')
        || description
        || dateoccurred
        || COALESCE((projectlevel ->> 'ID'), '')
        || COALESCE(immediateactions, '')
    )                               AS unique_case_hash
FROM {{ source('raw_nrc_source', 'source_tqm_case') }}
