{{
    config(
        tags=['nrc', 'push_candidate_tables', 'push_ditio_candidate_tables']
      )
}}

-- This table now simply defines the configuration for the push targets.
-- It will be overwritten on each dbt run.
-- Removed incremental materialization
SELECT 
    1 AS id,
    '{{ env_var("TQM_CREATE_CASE_URL") }}' AS post_url,
    jsonb_build_object(
        'OL1Key', '{{ env_var("TQM_OL1_KEY") }}',
        'Token', '{{ env_var("TQM_TOKEN") }}'
    ) AS headers

UNION ALL

SELECT 
    2 AS id,
    '{{ env_var("DITIO_PATCH_CASE_URL") }}' AS post_url,
    jsonb_build_object(
        'Content-Type', 'application/json',
        'Accept', 'text/plain',
        'Authorization', 'auth_key'
    ) AS headers