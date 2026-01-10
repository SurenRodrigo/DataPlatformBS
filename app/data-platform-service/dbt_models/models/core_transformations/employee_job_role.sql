WITH uniqueJobRoles AS (
    SELECT distinct
        field->'15'->'data'->>'guid' AS ext_job_role_guid,
        field->'15'->'data'->>'value' AS ext_job_role_description
    from {{source('raw_nrc_source', 'source_catalyst_employee')}}
)
SELECT 
    {{ dbt_utils.generate_surrogate_key(['jr.ext_job_role_guid']) }} AS id,
    jr.ext_job_role_guid,
    jr.ext_job_role_description AS "description"
FROM uniqueJobRoles AS jr