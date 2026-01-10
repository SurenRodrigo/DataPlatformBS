-- Staging model for pyairbyte_cache_sweden.maskiner_table_1
-- Maps Swedish price list mapping columns to English names with proper types

SELECT
    NULLIF("Prislista", '')                 AS price_list,
    NULLIF(
        "Hemmaprojekt"::TEXT, ''
    )::INTEGER                              AS home_project
FROM {{ source('raw_se_source', 'maskiner_table_1') }}

