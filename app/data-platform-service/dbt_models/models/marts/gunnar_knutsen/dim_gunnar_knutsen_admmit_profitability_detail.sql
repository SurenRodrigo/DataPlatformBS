SELECT
    {{ dbt_utils.generate_surrogate_key(['internal_id']) }}
                            AS profitability_detailed_id,
    2                       AS tenant_id,
    'Gunnar Knutsen'        AS tenant_name,
    6                       AS data_source_id,
    'Admmit'                AS data_source_name,
    work_order_id,
    item_name               AS article,
    est_price               AS price_out,
    est_settle              AS settlement,
    COALESCE(est_cost, 0)
    * 0.1                   AS surcharge_disposal,
    COALESCE(est_cost, 0)
    * 1.1                   AS price_out_based_on_disposal,
    COALESCE(
        est_cost, 0
    )                       AS cost_disposal,
    COALESCE(est_price, 0)
    - (
        COALESCE(est_settle, 0) + COALESCE(est_cost, 0) * 1.1
    )                       AS project_settlement_surcharge,
    CASE
        WHEN COALESCE(est_settle, 0) = 0 THEN NULL
        ELSE
            (
                COALESCE(est_price, 0)
                - (COALESCE(est_settle, 0) + COALESCE(est_cost, 0) * 1.1)
            ) / COALESCE(est_settle, 0)
    END                     AS percentage_surcharge_settlement,
    NOT COALESCE(CASE
        WHEN COALESCE(est_settle, 0) = 0 THEN NULL
        ELSE
            (
                COALESCE(est_price, 0)
                - (COALESCE(est_settle, 0) + COALESCE(est_cost, 0) * 1.1)
            ) / COALESCE(est_settle, 0)
    END < 0.1, FALSE)       AS sufficient_profitability_flag,
    COALESCE(est_settle, 0)
    + (
        COALESCE(est_price, 0)
        - (COALESCE(est_settle, 0) + COALESCE(est_cost, 0) * 1.1)
    )                       AS price_out_based_on_transport,
    date,
    time_start,
    time_stop,
    project_id,
    project_name,
    order_id,
    load_place_id,
    unload_place_id,
    comment,
    vehicle_registration_number,
    customer_name,
    item_id,
    item_name,
    quantity,
    unit_name,
    non_settleable,
    non_invoicable,
    price,
    est_cost,
    is_expense,
    weighing_no,
    COALESCE(price, 0)
    * COALESCE(quantity, 0) AS total_revenue,
    COALESCE(est_cost, 0)
    * COALESCE(quantity, 0) AS total_cost,
    COALESCE(est_expense, 0)
    * COALESCE(quantity, 0) AS total_expense


FROM {{ ref('stg__admmit_work_order_item') }}
