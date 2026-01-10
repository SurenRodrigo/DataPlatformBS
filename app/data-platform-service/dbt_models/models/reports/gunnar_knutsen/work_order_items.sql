WITH work_order_items AS (
    SELECT *
    FROM {{ ref('stg__admmit_work_order_item') }}
)

SELECT *
FROM work_order_items
