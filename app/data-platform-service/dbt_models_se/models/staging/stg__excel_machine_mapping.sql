-- Staging model for pyairbyte_cache_sweden.maskiner_table_2
-- Maps Swedish machine mapping columns to English names with proper types

SELECT
    NULLIF("Prislista", '')                 AS price_list,
    NULLIF("Artikel", '')                   AS article,
    NULLIF("Artikelnamn", '')               AS article_name,
    "Hemmaprojekt"::INTEGER                 AS home_project
FROM {{ source('raw_se_source', 'maskiner_table_2') }}

