-- Maps Swedish project mapping columns to English names with proper types

SELECT
    NULLIF(TRIM("Projektnr"::TEXT), '')::INTEGER         AS project_no,
    NULLIF(TRIM("Projekttyp 1 (HFM)"::TEXT), '')         AS project_type_1_hfm,
    NULLIF(TRIM("Produktionsomr"::TEXT), '')             AS production_area,
    NULLIF(TRIM("Projekt2"::TEXT), '')                   AS project_2,
    NULLIF(TRIM("Projekttyp 4"::TEXT), '')               AS project_type_4
FROM {{ source('raw_se_source', 'projektmappning ny_table_1') }}

