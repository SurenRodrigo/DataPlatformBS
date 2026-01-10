WITH source_data AS (
    SELECT
        tqm.caseid        AS case_external_id,
        tqm.kpiparameters AS kpi_array
    FROM {{ source('raw_nrc_source', 'source_tqm_case') }} AS tqm
),

unnested_kpi AS (
    SELECT
        case_external_id,
        kpi ->> 'ID'   AS kpi_id,
        kpi ->> 'Name' AS kpi_name
    FROM source_data,
        LATERAL JSONB_ARRAY_ELEMENTS(kpi_array::jsonb) AS kpi
)

SELECT
    ROW_NUMBER() OVER () AS id,
    case_external_id,
    kpi_id,
    kpi_name
FROM unnested_kpi
