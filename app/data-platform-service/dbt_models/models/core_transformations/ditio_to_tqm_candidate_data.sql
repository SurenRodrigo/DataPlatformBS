{{
    config(
        materialized='incremental',
        unique_key='composite_key',
        alias='push_candidate_data',
        tags=['nrc', 'push_candidate_tables']
    )
}}

-- Create the push_candidate_data table with mapped values from ditio_cases
WITH 
process_level_mapping AS (
    SELECT 
        pl.division_id,
        pl.process_level_id,
        pl.project_number
    FROM {{ ref('int__nrc_process_levels_mapping') }} pl
),
case_type_mapping AS (
    SELECT 
        ds.case_type_id, -- Assuming this is a static ID for the external system
        ds.ext_case_id AS ext_case_id,
        ct.case_type AS case_type,
        ct.case_id AS case_id
    FROM {{ ref('case_type_data_source_seed') }} ds
    LEFT JOIN {{ ref('case_types_seed') }} ct
        ON ds.case_type_id = ct.case_type_id
    WHERE ds.tqm_source_id = 6
),
project_level_mapping AS (
    SELECT
        project_id,
        ext_project_level_id
    FROM {{ ref('int__nrc_project_level_mapping') }}
),

formatted_attachments AS (
    SELECT
        dc.case_id,
        CASE
            WHEN dc.attachments IS NULL OR dc.attachments = '[]' OR dc.attachments::text = '[]' THEN NULL
            ELSE (
                SELECT STRING_AGG(', {{ var("ditio_env_url") }}/core/api/file/' || jsonb_extract_path_text(file_obj, 'File'), '')
                FROM jsonb_array_elements(
                    CASE 
                        WHEN jsonb_typeof(dc.attachments::jsonb) = 'array' THEN dc.attachments::jsonb
                        ELSE '[]'::jsonb
                    END
                ) AS file_obj
            )
        END AS formatted_attachment_urls
    FROM {{ ref('ditio_case') }} dc
),

source_data AS (
    -- Ditio to TQM
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'dc.project_number', 
            'dc.longitude', 
            'dc.latitude', 
            'dc.created_at', 
            'dc.base_type', 
            'dc.risk_text'
        ]) }} AS composite_key,
        pc.id AS push_candidate_id, -- This references the config table (push_candidates)
        dc.case_id AS case_id,
        jsonb_build_object(
            'CaseTypeID', ct.case_id, 
            'SeverityDegree', dc.risk_text,
            'Anonymous', false,-- Always keep this as FALSE, so that the originator will be integration user 
            'DateOccurred', dc.created_at,
            'Comments', dc.case_id,
            'Description',
                    CASE
                        WHEN fa.formatted_attachment_urls IS NULL THEN 
                            dc.description || CONCAT(' | Created By: ', dc.created_by_name, ' (', dc.created_by_emp_number, ')')
                        ELSE 
                            dc.description || fa.formatted_attachment_urls || CONCAT(' | Created By: ', dc.created_by_name, ' (', dc.created_by_emp_number, ')')
                    END,
            'ImmediateActions', dc.measures,
            'ProcessLevelID', pl.process_level_id, 
            'Latitude', dc.latitude,
            'Longitude', dc.longitude,
            'ProjectLevelID', plm.ext_project_level_id
        ) AS body,
        -- Generate a content hash of ALL fields that determine if a re-post is needed
        {{ dbt_utils.generate_surrogate_key([
            'dc.description', 
            'dc.title', 
            'dc.measures',
            'dc.attachments',
            'dc.risk_text',
            'dc.created_at'
        ]) }} AS content_hash,
        -- Store original fields for reference and future filtering if needed
        dc.created_at AS original_created_at,
        dc.project_number,
        dc.base_type,
        dc.external_type_id,
        dc.risk_text
    FROM {{ ref('ditio_case') }} dc
    LEFT JOIN process_level_mapping pl
        ON dc.project_number = pl.project_number
    LEFT JOIN case_type_mapping ct
        ON dc.external_type_id::integer = ct.ext_case_id::integer
    LEFT JOIN project_level_mapping plm
        ON dc.project_number = plm.project_id
    LEFT JOIN formatted_attachments fa
        ON dc.case_id = fa.case_id
    JOIN {{ ref('push_candidates') }} pc
        ON pc.id = 1 -- TQM post candidate
    WHERE dc.external_type_id is not null
)

SELECT
    source_data.composite_key,
    source_data.push_candidate_id,
    source_data.case_id,
    source_data.original_created_at,
    source_data.project_number,
    source_data.base_type,
    source_data.risk_text,
    source_data.content_hash,
    
    {% if is_incremental() %}
        -- Always preserve the original inserted_at if the record already exists
        CASE 
            WHEN {{ this }}.composite_key IS NULL 
            THEN current_timestamp 
            ELSE {{ this }}.inserted_at 
        END AS inserted_at,

        -- body: update if new or content changed, else preserve old
        CASE
            WHEN {{ this }}.composite_key IS NULL 
                 OR source_data.content_hash != {{ this }}.content_hash
            THEN source_data.body
            ELSE {{ this }}.body
        END AS body,
        
        -- Preserve post status fields unconditionally
        {{ this }}.last_posted_at AS last_posted_at,
        {{ this }}.last_post_attempted_at AS last_post_attempted_at,
        {{ this }}.last_post_status AS last_post_status,
        {{ this }}.last_post_error AS last_post_error
    {% else %}
        -- Initial load
        source_data.body AS body,
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