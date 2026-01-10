WITH admmit_data AS (
    SELECT
        date,
        item_name,
        internal_project_id,
        project_task_id,
        project_name,
        vehicle_registration_number,
        item_id,
        number,
        order_no,
        quantity,
        CONCAT(order_no, '-', number) AS ticket_id
    FROM {{ ref('dim_gunnar_knutsen_admmit_work_order_item') }}
),

visma_data AS (
    SELECT
        date,
        department,
        road_no,
        time,
        customer,
        item,
        admmit_article_name,
        item_id,
        project_id,
        project_name,
        vehicle,
        weight,
        "pcs/m³",
        sum_total
    FROM {{ ref('int_gk_manual_work_order_item') }}
),

project_data AS (
    SELECT
		customer_id,
        ext_project_id
    FROM {{ ref('dim_gunnar_knutsen_admmit_project') }}
),

customer_data AS (
    SELECT
        ext_customer_id,
        name
    FROM {{ ref('dim_gunnar_knutsen_admmit_customer') }}
),

comparison_data AS (
    SELECT
        visma_data.department,
        visma_data.road_no,
        visma_data.date,
        visma_data.time,
        visma_data.vehicle,
        visma_data.customer,
        customer_data.name AS customer_name,
        visma_data.item AS item_name,
        visma_data.project_name,
        visma_data.project_id,
        visma_data."pcs/m³",
        visma_data.weight,
        admmit_data.quantity,
        visma_data.sum_total,
        admmit_data.ticket_id,
        CASE
            WHEN admmit_data.date IS NULL THEN 'Not found'
            WHEN abs(coalesce(visma_data.weight::numeric, 0) - coalesce(admmit_data.quantity::numeric, 0)) <= 0.01 THEN 'Matched'
            ELSE 'Mismatched'
        END AS mismatch
    FROM visma_data
    LEFT JOIN admmit_data
        ON
            admmit_data.internal_project_id = visma_data.project_id
            -- AND REPLACE(LOWER(visma_data.item), ' ', '') LIKE '%' || REPLACE(LOWER(admmit_data.item_name), ' ', '') || '%'
            AND regexp_replace(lower(trim(visma_data.admmit_article_name)), '[^a-z0-9]+', '', 'g') = regexp_replace(lower(trim(admmit_data.item_name)), '[^a-z0-9]+', '', 'g')
            AND upper(regexp_replace(trim(visma_data.vehicle), '[^a-z0-9]+', '', 'g')) = upper(regexp_replace(trim(admmit_data.vehicle_registration_number), '[^a-z0-9]+', '', 'g'))
            AND visma_data.date = admmit_data.date
    LEFT JOIN project_data
		ON project_data.ext_project_id = admmit_data.project_task_id
    LEFT JOIN customer_data
		ON customer_data.ext_customer_id = project_data.customer_id
),

ranked_data AS (
    SELECT
        department,
        road_no,
        date,
        time,
        vehicle,
        customer,
        customer_name,
        project_id,
        project_name,
        item_name,
        weight,
        sum_total,
        mismatch,
        ticket_id,
        ROW_NUMBER() OVER (
            PARTITION BY
                department,
                road_no,
                date,
                time,
                vehicle,
                customer,
                customer_name,
                project_id,
                project_name,
                item_name,
                weight,
                sum_total
            ORDER BY
                CASE mismatch
                    WHEN 'Matched' THEN 1
                    WHEN 'Mismatched' THEN 2
                    ELSE 3
                END,
                abs(coalesce(weight::numeric, 0) - coalesce(quantity::numeric, 0)) ASC
        ) AS row_num
    FROM comparison_data
)

SELECT
    ticket_id,
    department,
    road_no,
    date,
    time,
    vehicle,
    customer,
    customer_name,
    project_id,
    project_name,
    item_name,
    weight,
    sum_total,
    mismatch
FROM ranked_data
WHERE row_num = 1
