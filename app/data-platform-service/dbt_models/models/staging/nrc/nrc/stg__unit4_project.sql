-- This sql will filter out projects where department id is null except for companyId = 70
SELECT
    projectid::INT              AS project_id,
    companyid                   AS company_id,
    costcentre                  AS cost_centre,
    mainproject                 AS main_project,
    projectname                 AS project_name,
    projecttype                 AS project_type,
    wbs,
    status,
    message,
    CASE
        WHEN
            companyid = '70'
            AND (category1 IS NULL OR TRIM(category1) = '' OR category1 = '%')
            THEN '70'
        WHEN TRIM(category1) = '' THEN NULL
        ELSE category1
    END                         AS department_id,
    NULLIF(category2, '')       AS category2,
    NULLIF(category3, '')       AS category3,
    NULLIF(category4, '')       AS category4,
    probability,
    authorisation,
    contactpoints               AS contact_points,
    hasactivities               AS has_activities,
    posttimecosts               AS post_time_costs,
    relatedvalues               AS related_values,
    workflowstate               AS workflow_state,
    isglobalproject             AS is_global_project,
    projectduration             AS project_duration,
    projectmanagerid            AS project_manager_id,
    authoriseovertime           AS authorise_overtime,
    customfieldgroups           AS custom_field_groups,
    containsworkorders          AS contains_work_orders,
    customerinformation         AS customer_information,
    issupportingproject         AS is_supporting_project,
    authorisenormalhours        AS authorise_normal_hours,
    hastimesheetlimitcontrol    AS has_time_sheet_limit_control,
    lastupdated ->> 'updatedAt' AS last_updated_at,
    lastupdated ->> 'updatedBy' AS last_updated_by,
    -- DBT metadata
    dbt_scd_id,
    dbt_updated_at,
    dbt_valid_from,
    dbt_valid_to
FROM {{ ref('unit4_project_snapshot') }}
WHERE
    dbt_valid_to IS NULL
    AND (
        companyid = '70'
        OR (companyid != '70' AND category1 IS NOT NULL AND TRIM(category1) != '')
    )
