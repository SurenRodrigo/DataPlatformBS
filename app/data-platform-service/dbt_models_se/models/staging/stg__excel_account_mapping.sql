-- Staging model for pyairbyte_cache_sweden.ktomap_table_1
-- Maps Swedish account mapping columns to English names with proper types

SELECT
    "Konto"::INTEGER                   AS account_no,
    NULLIF("Namn", '')                 AS account_name,
    NULLIF("Kontogrupp TB1", '')       AS account_group_tb1
FROM {{ source('raw_se_source', 'ktomap_table_1') }}