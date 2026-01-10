WITH organization_mapping AS (
    SELECT
        company_id,
        company_name,
        tenant_id,
        tenant_name
    FROM {{ ref('int__department_denormalized') }}
    GROUP BY company_id, company_name, tenant_id, tenant_name
),

company_mapping AS (
    SELECT
        company_data_source.company_guid,
        company_data_source.ext_company_id,
        organization_mapping.company_name,
        organization_mapping.tenant_id,
        organization_mapping.tenant_name
    FROM {{ ref('data_source_seed') }} AS data_source
    LEFT JOIN {{ ref('company_data_source_seed') }} AS company_data_source
        ON data_source.id = company_data_source.data_source_id
    LEFT JOIN organization_mapping
        ON company_data_source.company_guid = organization_mapping.company_id
    WHERE data_source.name = 'Unit4'
),

project AS (
    SELECT
        ext_project_id,
        project_identifier,
        department_id,
        department_name,
        division_id,
        division_name,
        company_id
    FROM {{ ref('project') }}
),

customer AS (
    SELECT
        ext_customer_id,
        name AS customer_name
    FROM {{ ref('customer') }}
),

invoice_transactions AS (
    SELECT
        transaction_number,
        transaction_type,
        company_id,
        account_column2_dim_value AS project_number,
        account_column4_dim_value AS project_task_id,
        invoice_customer_id       AS customer_id,
        period,
        year,
        month,
        month_name,
        period_date,
        transaction_date,
        invoice_number,
        invoice_description,
        invoice_order_number,
        CASE
            WHEN invoice_due_date = '1900-01-01 00:00:00.000'
                THEN NULL
            ELSE invoice_due_date
        END                       AS due_date,
        CASE
            WHEN payment_followup_pay_date = '1900-01-01 00:00:00.000'
                THEN NULL
            ELSE payment_followup_pay_date
        END                       AS pay_date,
        CASE
            WHEN payment_followup_payment_period != 0
                 AND payment_followup_pay_date <> '1900-01-01 00:00:00.000'
                THEN TRUE
            ELSE FALSE
        END                       AS is_paid,
        sequence_number,
        bacs_id,
        amount,
        remaining_amount,
        debit_credit_indicator,
        currency_code,
        last_updated_at
    FROM {{ ref('stg__unit4_invoice_paid_transaction') }}

    UNION ALL

    SELECT
        transaction_number,
        transaction_type,
        company_id,
        account_column2_dim_value AS project_number,
        account_column4_dim_value AS project_task_id,
        invoice_customer_id       AS customer_id,
        period,
        year,
        month,
        month_name,
        period_date,
        transaction_date,
        invoice_number,
        invoice_description,
        invoice_order_number,
        CASE
            WHEN invoice_due_date = '1900-01-01 00:00:00.000'
                THEN NULL
            ELSE invoice_due_date
        END                       AS due_date,
        CASE
            WHEN payment_followup_pay_date = '1900-01-01 00:00:00.000'
                THEN NULL
            ELSE payment_followup_pay_date
        END                       AS pay_date,
        CASE
            WHEN payment_followup_payment_period != 0
                 AND payment_followup_pay_date <> '1900-01-01 00:00:00.000'
                THEN TRUE
            ELSE FALSE
        END                       AS is_paid,
        sequence_number,
        bacs_id,
        amount,
        remaining_amount,
        debit_credit_indicator,
        currency_code,
        last_updated_at
    FROM {{ ref('stg__unit4_invoice_unpaid_transaction') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(
        ['transaction_number', 'sequence_number', 'bacs_id', 'is_paid', 'customer_id', 'invoice_number']
    ) }}                         AS transaction_sk,
    transaction.transaction_number,
    transaction.transaction_type,
    company_mapping.tenant_id,
    company_mapping.tenant_name,
    company_mapping.company_guid AS company_id,
    company_mapping.company_name,
    project.division_id,
    project.division_name,
    project.department_id,
    project.department_name,
    transaction.project_number,
    project.project_identifier,
    transaction.project_task_id,
    transaction.customer_id,
    customer.customer_name,
    transaction.period,
    transaction.year,
    transaction.month,
    transaction.month_name,
    transaction.period_date,
    transaction.transaction_date,
    transaction.invoice_number,
    transaction.invoice_description,
    transaction.invoice_order_number,
    transaction.sequence_number,
    transaction.due_date,
    transaction.pay_date,
    transaction.is_paid,
    transaction.amount,
    transaction.remaining_amount,
    transaction.debit_credit_indicator,
    transaction.currency_code,
    transaction.last_updated_at
FROM invoice_transactions AS transaction
LEFT JOIN company_mapping
    ON transaction.company_id::TEXT = company_mapping.ext_company_id
LEFT JOIN project
    ON transaction.project_number = project.ext_project_id
    AND company_mapping.company_guid = project.company_id
LEFT JOIN customer
    ON transaction.customer_id = customer.ext_customer_id
