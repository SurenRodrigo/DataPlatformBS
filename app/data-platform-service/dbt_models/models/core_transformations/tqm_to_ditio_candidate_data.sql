{{
    config(
        materialized='incremental',
        unique_key='composite_key',
        alias='push_ditio_candidate_data',
        tags=['nrc', 'push_ditio_candidate_tables']
    )
}}

-- Create the push_candidate_ditio_data table with mapped values from case 
WITH precomputed_case AS (
    SELECT
        tqm_case.case_external_id,
        tqm_case.case_type_id,
        tqm_case.comments,
        tqm_case.description, -- full description for the content hash
        regexp_replace(
            description,
            ',?\s*{{ var("ditio_env_url") }}/core/api/file/\S+',
            '',
            'g'
        ) AS clean_description, -- this is to passed in the PATCH URL body
        tqm_case.originator_email,
        tqm_case.project_id,

        -- Precomputed 'measures' string (reused in body + content_hash)
        CONCAT_WS(
            E'\n',
            CASE 
                WHEN NULLIF(TRIM(tqm_case.immediate_actions), '') IS NOT NULL 
                THEN 'ImmediateActions: ' || TRIM(tqm_case.immediate_actions)
                ELSE NULL 
            END,
            CASE 
                WHEN (
                    SELECT string_agg(replace(replace(action ->> 'PlannedActions', '&quot;', '"'), '&#10;', ' '), ', ')
                    FROM jsonb_array_elements(tqm_case.actions::jsonb) AS action
                    WHERE action ->> 'PlannedActions' IS NOT NULL
                ) IS NOT NULL 
                THEN 'PlannedActions: ' || (
                    SELECT string_agg(replace(replace(action ->> 'PlannedActions', '&quot;', '"'), '&#10;', ' '), ', ')
                    FROM jsonb_array_elements(tqm_case.actions::jsonb) AS action
                    WHERE action ->> 'PlannedActions' IS NOT NULL
                )
                ELSE NULL
            END,
            CASE 
                WHEN (
                    SELECT string_agg(replace(replace(action ->> 'ExecutedActions', '&quot;', '"'), '&#10;', ' '), ', ')
                    FROM jsonb_array_elements(tqm_case.actions::jsonb) AS action
                    WHERE action ->> 'ExecutedActions' IS NOT NULL
                ) IS NOT NULL 
                THEN 'ExecutedActions: ' || (
                    SELECT string_agg(replace(replace(action ->> 'ExecutedActions', '&quot;', '"'), '&#10;', ' '), ', ')
                    FROM jsonb_array_elements(tqm_case.actions::jsonb) AS action
                    WHERE action ->> 'ExecutedActions' IS NOT NULL
                )
                ELSE NULL
            END
        ) AS measures_full,

         -- Status mapping to Numeric representation of status
        CASE
            WHEN tqm_case.status_key = 'Pending' THEN 0
            WHEN tqm_case.status_key = 'Closed' THEN 2
            WHEN tqm_case.status_key IN ('InProgress', 'Done', 'Approved') THEN 1
            ELSE NULL
        END AS status_id

    FROM {{ ref('case') }} tqm_case
    WHERE originator_email= 'tqmapi'
),

source_data AS (    
    -- TQM to Ditio
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'precomputed_case.case_external_id', 
            'precomputed_case.case_type_id'
        ]) }} AS composite_key,
        candidate.id AS push_candidate_id, -- This references the config table (push_candidates)
        CAST(precomputed_case.case_external_id::BIGINT AS TEXT) AS case_id, -- id of TQM system
        precomputed_case.comments AS ditio_case_id,
        jsonb_strip_nulls(
            jsonb_build_object(
                'ditio_case_id', precomputed_case.comments,
                'externalId', CAST(precomputed_case.case_external_id::BIGINT AS TEXT),-- TQM Case ID
                'description', precomputed_case.clean_description,
                'measures', precomputed_case.measures_full,
                'status', precomputed_case.status_id,
                'risk', NULL, --TODO: Numeric risk level [NO FIELD TO MAP]
                'damageType', NULL, --TODO: Numeric damage type [NO FIELD TO MAP]
                'absenceDays', NULL --TODO: Numeric absence Days [NO FIELD TO MAP]
            )
        ) AS body,
        -- Generate a content hash of ALL fields that determine if a re-post is needed
        {{ dbt_utils.generate_surrogate_key([
            'precomputed_case.comments', 
            'precomputed_case.description',
            'precomputed_case.measures_full',
            'precomputed_case.status_id'
        ]) }} AS content_hash,
        precomputed_case.project_id, -- project id of tqm
        precomputed_case.case_type_id
    FROM precomputed_case
    JOIN {{ ref('push_candidates') }} candidate
        ON candidate.id = 2 -- Ditio PATCH candidate
)

SELECT
    source_data.composite_key,
    source_data.push_candidate_id,
    source_data.project_id,
    source_data.case_id,    
    source_data.case_type_id,
    source_data.ditio_case_id,
    source_data.body,
    source_data.content_hash,
    {% if is_incremental() %}
        -- Inserted_at: Set when the record first appears OR when content changes significantly enough to warrant re-processing
        CASE 
            WHEN {{ this }}.composite_key IS NULL 
            OR source_data.content_hash != {{ this }}.content_hash 
            THEN current_timestamp 
            ELSE {{ this }}.inserted_at -- Keep original inserted_at if content hasn't changed
        END AS inserted_at,
        -- Reset post status fields if content has changed
        CASE
            WHEN {{ this }}.composite_key IS NOT NULL AND source_data.content_hash != {{ this }}.content_hash THEN NULL
            ELSE {{ this }}.last_posted_at
        END AS last_posted_at,
        CASE
            WHEN {{ this }}.composite_key IS NOT NULL AND source_data.content_hash != {{ this }}.content_hash THEN NULL
            ELSE {{ this }}.last_post_attempted_at
        END AS last_post_attempted_at,
        CASE
            WHEN {{ this }}.composite_key IS NOT NULL AND source_data.content_hash != {{ this }}.content_hash THEN NULL
            ELSE {{ this }}.last_post_status
        END AS last_post_status,
        CASE
            WHEN {{ this }}.composite_key IS NOT NULL AND source_data.content_hash != {{ this }}.content_hash THEN NULL
            ELSE {{ this }}.last_post_error
        END AS last_post_error
    {% else %}
        -- Initial load
        current_timestamp AS inserted_at,
        NULL::TIMESTAMP WITH TIME ZONE AS last_posted_at,
        NULL::TIMESTAMP WITH TIME ZONE AS last_post_attempted_at,
        NULL::VARCHAR AS last_post_status,
        NULL::TEXT AS last_post_error
    {% endif %}
FROM source_data
{% if is_incremental() %}
LEFT JOIN {{ this }} 
    ON source_data.composite_key = {{ this }}.composite_key
WHERE 
    -- Always include new records
    {{ this }}.composite_key IS NULL 
    OR 
    -- Include records where content has changed
    source_data.content_hash != {{ this }}.content_hash
{% endif %}