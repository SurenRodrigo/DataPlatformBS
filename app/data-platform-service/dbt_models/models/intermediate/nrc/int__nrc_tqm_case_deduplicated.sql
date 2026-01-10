{{ config(
    tags=['nrc', 'tqm_case_tables']
) }}

WITH tqm_cases AS (

    SELECT *
    FROM {{ ref('stg__tqm_case') }}

),


deduplicated AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY unique_case_hash
            ORDER BY
                date_published,
                case_external_id
        ) AS row_num
    FROM tqm_cases

)


SELECT * FROM deduplicated
WHERE row_num = 1
