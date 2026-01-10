{{
    config(
        tags=['nrc', 'push_candidate_tables']
      )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['id']) }} AS id,
    id                   AS case_id,
    basetype             AS base_type,
    typename             AS type_name,
    risktext             AS risk_text, 
    createdat            AS created_at, 
    description,  
    title, 
    measures,
    projectid            AS project_id,    
    projectnumber::INT   AS project_number,
    longitude            AS longitude,
    latitude             AS latitude,
    typeexternalnumber::NUMERIC   AS external_type_id,
    createdby   ->> 'name' AS created_by_name,
    createdby   ->> 'employeeNumber' AS created_by_emp_number,
    CASE
        WHEN jsonb_typeof(imagefilereferences::jsonb) = 'array' AND jsonb_array_length(imagefilereferences::jsonb) > 0 THEN
            jsonb_agg(
                jsonb_build_object(
                    'File', ir ->> 'id',
                    'MIMEType', ir ->> 'fileType',
                    'Name', ir ->> 'fileName'
                )
            )
        ELSE NULL
    END AS attachments,
    CASE
        WHEN jsonb_typeof(imagefilereferences::jsonb) = 'array' AND jsonb_array_length(imagefilereferences::jsonb) > 0 THEN
            jsonb_agg(ir ->> 'id')
        ELSE NULL
    END AS image_ids,
    CASE
        WHEN MAX(fileids::text) IS NOT NULL AND jsonb_typeof(MAX(fileids::text)::jsonb) = 'array' THEN
            MAX(fileids::text)::jsonb
        ELSE NULL
    END AS file_ids,
    requiresfurtheraction AS requires_further_action,
    resolvedonlocation    AS resolved_on_location,
    accuracy              AS accuracy
FROM {{ source('raw_nrc_source', 'source_ditio_incidents_registration') }}
LEFT JOIN LATERAL jsonb_array_elements(imagefilereferences::jsonb) AS ir ON true
WHERE projectnumber ~ '^[0-9]+$'
GROUP BY 
    id, 
    baseType,
    typename,
    riskText, 
    createdAt, 
    description, 
    title, 
    measures, 
    projectid, 
    projectnumber, 
    longitude, 
    latitude, 
    imagefilereferences::jsonb,
    fileids::text,
    typeexternalnumber,
    createdby ->> 'name',
    createdby ->> 'employeeNumber',
    requiresfurtheraction,
    resolvedonlocation,
    accuracy
