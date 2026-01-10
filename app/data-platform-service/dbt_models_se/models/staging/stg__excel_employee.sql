-- Staging model for pyairbyte_cache_sweden.anv_table_1
-- Maps Swedish column names to English

SELECT
    NULLIF(
        "Anställningsnr"::TEXT, ''
    )::INTEGER                                         AS employee_no,
    NULLIF("För- och efternamn", '')                   AS full_name,
    NULLIF("Inloggning", '')                           AS login,
    NULLIF("Primär grupp", '')                         AS primary_group,
    NULLIF("Yrkesroll", '')                            AS job_role,
    NULLIF(
        "Anställningsform", ''
    )                                                  AS employment_type,
    NULLIF("Namn", '')                                 AS name,
    NULLIF("Startdatum", '')::TIMESTAMP                AS start_date,
    NULLIF("Slutdatum", '')::TIMESTAMP                 AS end_date,
    COALESCE("Aktiv"::NUMERIC = 1, FALSE)              AS is_active,
    NULLIF("Språk", '')                                AS language,
    "Organisatoriskt projekt"::INTEGER                 AS project_no
FROM {{ source('raw_se_source', 'anv_table_1') }}
