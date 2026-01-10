WITH fact_invoice AS (
    SELECT *
    FROM {{ ref('fct_nrc_invoice_transaction') }}
    WHERE is_paid = FALSE
)

SELECT
    company_name,
    division_name,
    department_name,
    project_number,
    project_identifier,
    period,
    year,
    month,
    month_name,
    period_date,
    due_date,
    amount,
    remaining_amount,
    (CURRENT_DATE - due_date) AS overdue_days,
    CASE
        WHEN (CURRENT_DATE - due_date) BETWEEN 1 AND 14 THEN '0–14 days'
        WHEN (CURRENT_DATE - due_date) BETWEEN 15 AND 30 THEN '15–30 days'
        WHEN (CURRENT_DATE - due_date) > 30 THEN 'Over 30 days'
        ELSE 'Not Overdue'
    END AS overdue_bucket,
    transaction_number,
    transaction_date,
    transaction_type,
    invoice_number,
    invoice_description,
    invoice_order_number,
    customer_id,
    customer_name,
    sequence_number,
    debit_credit_indicator,
    currency_code,
    last_updated_at
FROM fact_invoice