WITH 
time_type_data AS (
    SELECT icore.hourstypecode, icore.hourstypedescr, icore."_key" 
    FROM {{ source('raw_nrc_source', 'source_icore_hours_type') }} icore
),

unique_ditio_time_types AS (
    SELECT DISTINCT 
        val->>'typeId' AS time_type_code,
        val->>'typeName' AS description,
        val->>'lineTypeName' AS time_type_key
    FROM {{ ref('stg__ditio_payroll_lines') }} pl,
    LATERAL jsonb_array_elements(pl.payroll_values::jsonb) AS val
),

icore_time_types AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['_key','source.name']) }} AS time_type_sk,
        hourstypecode as time_type_code,
        hourstypedescr as description,
        _key as time_type_key,
        source.name as data_source_name,                                                                    
        NULL as source_endpoint
    FROM time_type_data
    JOIN {{ ref('data_source_seed') }} AS source ON source.id = 2 --iCore
),

ditio_absence_types_nrcg AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['absence.id','absence.typeid','source.name',"'absence-nrcg'"]) }} AS time_type_sk,
        typeid as time_type_code,
        absence.name as description,
        typename as time_type_key,
        source.name as data_source_name,
        'absence-nrcg' as source_endpoint
    FROM {{ source('raw_nrc_source', 'source_ditio_timestyr_nrcg_absence_types') }} AS absence
    JOIN {{ ref('data_source_seed') }} AS source ON source.id = 4 --Ditio
),

ditio_absence_types_kept AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['absence.id','absence.typeid','source.name',"'absence-kept'"]) }} AS time_type_sk,
        typeid as time_type_code,
        absence.name as description,
        typename as time_type_key,
        source.name as data_source_name,
        'absence-kept' as source_endpoint
    FROM {{ source('raw_nrc_source', 'source_ditio_timestyr_kept_absence_types') }} AS absence
    JOIN {{ ref('data_source_seed') }} AS source ON source.id = 4 --Ditio
),

ditio_time_types AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['time_type_key','types.description','source.name',"'payroll'"]) }} AS time_type_sk,
        types.time_type_code,
        types.description,
        types.time_type_key,
        source.name as data_source_name,
        'payroll' as source_endpoint
    FROM unique_ditio_time_types types
    JOIN {{ ref('data_source_seed') }} AS source ON source.id = 4 --Ditio
)

SELECT * FROM icore_time_types

UNION ALL

SELECT * FROM ditio_absence_types_nrcg

UNION ALL

SELECT * FROM ditio_absence_types_kept

UNION ALL

SELECT * FROM ditio_time_types

