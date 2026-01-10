SELECT
    period::INT                                           AS period,
    LEFT(
        (period::INT)::TEXT, 4
    )::INT               AS year,
    RIGHT(
        (period::INT)::TEXT, 2
    )::INT               AS month,
    TO_CHAR(
        TO_DATE(RIGHT((period::INT)::TEXT, 2), 'MM'), 'MON'
    )                    AS month_name,
    (
        DATE_TRUNC(
            'month',
            TO_DATE(
                LEFT((period::INT)::TEXT, 4) || RIGHT((period::INT)::TEXT, 2), 'YYYYMM'
            )
        )
        + INTERVAL '1 month'
        - INTERVAL '1 day'
    )::DATE              AS period_date,
    NULLIF(companyid, '')::INT                          AS company_id,
    (sequencenumber::INT)::TEXT                         AS sequence_number,
    (
        transactionnumber::INT
    )::TEXT                                               AS transaction_number,
    NULLIF(taxcode, '')                                 AS tax_code,
    transactiondate::TIMESTAMP                          AS transaction_date,
    NULLIF(
        transactiontype, ''
    )                                                     AS transaction_type,
    revaluationdate::TIMESTAMP                          AS revaluation_date,
    (amounts ->> 'amount')::NUMERIC                       AS amount,
    (amounts ->> 'amount3')::NUMERIC                      AS amount3,
    (amounts ->> 'amount4')::NUMERIC                      AS amount4,
    (amounts ->> 'amountPaid')::NUMERIC                   AS amount_paid,
    (amounts ->> 'restAmount3')::NUMERIC                  AS rest_amount3,
    (amounts ->> 'restAmount4')::NUMERIC                  AS rest_amount4,
    NULLIF((amounts ->> 'currencyCode'), '')              AS currency_code,
    (amounts ->> 'exchangeRate')::NUMERIC                 AS exchange_rate,
    (amounts ->> 'remCurrAmount')::NUMERIC                AS rem_curr_amount,
    (amounts ->> 'currencyAmount')::NUMERIC               AS currency_amount,
    CASE
        WHEN (amounts ->> 'debitCreditFlag')::INT = 1 THEN 'DEBIT'
        WHEN (amounts ->> 'debitCreditFlag')::INT = -1 THEN 'CREDIT'
    END                                                   AS debit_credit_indicator,
    (
        amounts ->> 'remainingAmount'
    )::NUMERIC                                            AS remaining_amount,
    (
        amounts ->> 'revaluationAmount'
    )::NUMERIC                                            AS revaluation_amount,
    (
        amounts ->> 'restCurrencyAmount'
    )::NUMERIC                                            AS rest_currency_amount,
    (
        amounts ->> 'revaluationAmount3'
    )::NUMERIC                                            AS revaluation_amount3,
    (
        amounts ->> 'revaluationAmount4'
    )::NUMERIC                                            AS revaluation_amount4,
    NULLIF((invoice ->> 'bacsId'), '')                    AS bacs_id,
    (
        invoice ->> 'dueDate'
    )::DATE                                          AS invoice_due_date,
    (
        invoice ->> 'valueDate'
    )::TIMESTAMP                                          AS invoice_value_date,
    NULLIF(
        (invoice ->> 'contractId'), ''
    )                                                     AS invoice_contract_id,
    NULLIF(
        (invoice ->> 'customerId'), ''
    )                                                     AS invoice_customer_id,
    NULLIF(
        (invoice ->> 'orderNumber'), ''
    )                                                     AS invoice_order_number,
    NULLIF(
        (invoice ->> 'currDocDescr'), ''
    )                                                     AS invoice_curr_doc_descr,
    NULLIF(
        (invoice ->> 'payRecipient'), ''
    )                                                     AS invoice_pay_recipient,
    NULLIF((invoice ->> 'invoiceNumber'), '')             AS invoice_number,
    (
        invoice ->> 'paymentOnAccount'
    )::BOOLEAN                                            AS invoice_payment_on_account,
    NULLIF(
        (invoice ->> 'legalActionStatus'), ''
    )                                                     AS invoice_legal_action_status,
    NULLIF(
        (invoice ->> 'invoiceDescription'), ''
    )                                                     AS invoice_description,
    NULLIF((
        invoice ->> 'currencyDocumentation'
    ), ''
    )                                                     AS invoice_currency_documentation,
    NULLIF((
        invoice ->> 'externalArchiveReference'
    ), ''
    )                                                     AS invoice_external_archive_reference,
    (lastupdated ->> 'updatedAt')::TIMESTAMP            AS last_updated_at,
    NULLIF((lastupdated ->> 'updatedBy'), '')           AS last_updated_by,
    NULLIF(
        (debtCollection ->> 'debtCollectionAgency'), ''
    )                                                     AS debt_collection_agency,
    NULLIF(
        (debtCollection ->> 'debtCollectionStatus'), ''
    )                                                     AS debt_collection_status,
    NULLIF(
        (debtCollection ->> 'debtCollectionReference'), ''
    )                                                     AS debt_collection_reference,
    NULLIF(
        (paymentfollowup ->> 'status'), ''
    )                                                     AS payment_followup_status,
    (
        paymentfollowup ->> 'payDate'
    )::TIMESTAMP                                          AS payment_followup_pay_date,
    (
        paymentfollowup ->> 'lastDueDate'
    )::TIMESTAMP                                          AS payment_followup_last_due_date,
    NULLIF((
        paymentfollowup ->> 'commitmentId'
    ), ''
    )                                                     AS payment_followup_commitment_id,
    (
        paymentfollowup ->> 'lastReminded'
    )::TIMESTAMP                                          AS payment_followup_last_reminded,
    NULLIF((
        paymentfollowup ->> 'remittanceId'
    ), ''
    )                                                     AS payment_followup_remittance_id,
    NULLIF((
        paymentfollowup ->> 'complaintCode'
    ), ''
    )                                                     AS payment_followup_complaint_code,
    (
        paymentfollowup ->> 'complaintDate'
    )::TIMESTAMP                                          AS payment_followup_complaint_date,
    NULLIF((
        paymentfollowup ->> 'paymentMethod'
    ), ''
    )                                                     AS payment_followup_payment_method,
    NULLIF((
        paymentfollowup ->> 'paymentPeriod'
    ), ''
    )::INT                                                AS payment_followup_payment_period,
    NULLIF((
        paymentfollowup ->> 'paymentPlanId'
    ), ''
    )                                                     AS payment_followup_payment_plan_id,
    NULLIF((
        paymentfollowup ->> 'reminderLevel'
    ), ''
    )                                                     AS payment_followup_reminder_level,
    NULLIF((
        paymentfollowup ->> 'interestStatus'
    ), ''
    )                                                     AS payment_followup_interest_status,
    NULLIF((
        paymentfollowup ->> 'debtCollectionCode'
    ), ''
    )                                                     AS payment_followup_debt_collection_code,
    NULLIF((sundryinformation ->> 'place'), '')         AS sundry_place,
    NULLIF((sundryInformation ->> 'swift'), '')         AS sundry_swift,
    NULLIF((sundryInformation ->> 'address'), '')       AS sundry_address,
    NULLIF((sundryinformation ->> 'postcode'), '')      AS sundry_postcode,
    NULLIF((sundryinformation ->> 'province'), '')      AS sundry_province,
    NULLIF(
        (sundryinformation ->> 'bankAccount'), ''
    )                                                     AS sundry_bank_account,
    NULLIF(
        (sundryinformation ->> 'bankAddress'), ''
    )                                                     AS sundry_bank_address,
    NULLIF(
        (sundryinformation ->> 'bankCountry'), ''
    )                                                     AS sundry_bank_country,
    NULLIF(
        (sundryinformation ->> 'countryCode'), ''
    )                                                     AS sundry_country_code,
    NULLIF(
        (sundryinformation ->> 'bankAccountType'), ''
    )                                                     AS sundry_bank_account_type,
    NULLIF(
        (sundryinformation ->> 'bankClearingCode'), ''
    )                                                     AS sundry_bank_clearing_code,
    NULLIF(
        (sundryinformation ->> 'sundryCustomerName'), ''
    )                                                     AS sundry_customer_name,
    NULLIF((
        sundryinformation ->> 'vatRegistrationNumber'
    ), ''
    )                                                     AS sundry_vat_registration_number,
    (discountinformation ->> 'discount')::NUMERIC       AS discount,
    (discountinformation ->> 'discountDate')::TIMESTAMP AS discount_date,
    (
        discountinformation ->> 'discountPercent'
    )::NUMERIC                                            AS discount_percent,
    NULLIF((accountinginformation ->> 'account'), '')   AS account_number,
    NULLIF(
        accountinginformation -> 'column1' ->> 'dimValue', ''
    )                                                     AS account_column1_dim_value,
    NULLIF(
        accountinginformation -> 'column1' ->> 'attributeId', ''
    )                                                     AS account_column1_attribute_id,
    NULLIF(
        accountinginformation -> 'column2' ->> 'dimValue', ''
    )::INT                                                AS account_column2_dim_value,
    NULLIF(
        accountinginformation -> 'column2' ->> 'attributeId', ''
    )                                                     AS account_column2_attribute_id,
    NULLIF(
        accountinginformation -> 'column3' ->> 'dimValue', ''
    )                                                     AS account_column3_dim_value,
    NULLIF(
        accountinginformation -> 'column3' ->> 'attributeId', ''
    )                                                     AS account_column3_attribute_id,
    NULLIF(
        accountinginformation -> 'column4' ->> 'dimValue', ''
    )                                                     AS account_column4_dim_value,
    NULLIF(
        accountinginformation -> 'column4' ->> 'attributeId', ''
    )                                                     AS account_column4_attribute_id,
    NULLIF(
        accountinginformation -> 'column5' ->> 'dimValue', ''
    )                                                     AS account_column5_dim_value,
    NULLIF(
        accountinginformation -> 'column5' ->> 'attributeId', ''
    )                                                     AS account_column5_attribute_id,
    NULLIF(
        accountinginformation -> 'column6' ->> 'dimValue', ''
    )                                                     AS account_column6_dim_value,
    NULLIF(
        accountinginformation -> 'column6' ->> 'attributeId', ''
    )                                                     AS account_column6_attribute_id,
    NULLIF(
        accountinginformation -> 'column7' ->> 'dimValue', ''
    )                                                     AS account_column7_dim_value,
    NULLIF(
        accountinginformation -> 'column7' ->> 'attributeId', ''
    )                                                     AS account_column7_attribute_id,
    dbt_scd_id,
    dbt_updated_at,
    dbt_valid_from,
    dbt_valid_to
FROM {{ ref('unit4_invoice_unpaid_transaction_snapshot') }}
WHERE
    dbt_valid_to IS NULL
    AND dbt_is_deleted = 'False'
