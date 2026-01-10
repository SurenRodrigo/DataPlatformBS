WITH visma_manual_data AS (
    SELECT *
    FROM {{ ref('stg__gk_manual_work_order_item') }}
),

article_data_raw AS (
    SELECT *
    FROM {{ ref('stg__gk_article_data') }}
),

article_data AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY REGEXP_REPLACE(
                    LOWER(TRIM(ext_article_name_loading)),
                    '\s+',
                    '',
                    'g'
                )
                ORDER BY admmit_article_name
            ) AS rn
        FROM article_data_raw
    ) sub
    WHERE rn = 1
),

joined_data AS (
    SELECT
        visma_manual_data.department,
        visma_manual_data.road_no,
        visma_manual_data.date,
        visma_manual_data.time,
        visma_manual_data.vehicle,
        visma_manual_data.customer,
        visma_manual_data.project_id,
        visma_manual_data.project_name,
        visma_manual_data.item,
        article_data.ext_article_name_loading AS item_name_load,
        article_data.ext_article_name_unloading AS item_name_unload,
        article_data.admmit_article_name,
        visma_manual_data.item_id,
        visma_manual_data."pcs/m³",
        visma_manual_data.weight,
        visma_manual_data.sum_total,
        ROW_NUMBER() OVER (
            PARTITION BY
                visma_manual_data.date,
                visma_manual_data.item,
                visma_manual_data.project_id,
                visma_manual_data.vehicle,
                visma_manual_data.time,
                visma_manual_data.road_no
            ORDER BY visma_manual_data.item
        ) AS dedup_rn
    FROM visma_manual_data
    LEFT JOIN article_data
        ON REGEXP_REPLACE(
            LOWER(TRIM(visma_manual_data.item)),
            '\s+',
            '',
            'g'
        ) = REGEXP_REPLACE(
            LOWER(TRIM(article_data.ext_article_name_loading)),
            '\s+',
            '',
            'g'
        )
)

SELECT *
FROM joined_data
WHERE dedup_rn = 1
