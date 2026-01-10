SELECT
    id::INTEGER                            AS id,
    NULLIF(TRIM(description), '')          AS description,
    NULLIF(created, '')::TIMESTAMP         AS created_at,
    createduserid::INTEGER                 AS created_user_id,
    NULLIF(changed, '')::TIMESTAMP         AS last_modified_at,
    changeduserid::INTEGER                 AS changed_user_id,
    NULLIF(TRIM(accountno), '')::INTEGER   AS account_no,
    NULLIF(TRIM(vatcode), '')              AS vat_code,
    NULLIF(TRIM(externalcode), '')         AS external_code,
    vatid::INTEGER                         AS vat_id,
    cost::BOOL                             AS is_cost,
    work::BOOL                             AS is_work,
    material::BOOL                         AS is_material,
    contractorvat::BOOL                    AS is_contractor_vat,
    NULLIF(TRIM(complementaccountno), '')  AS complement_account_no,
    invoiceableonimport::BOOL              AS is_invoiceable_on_import,
    NULLIF(TRIM(invoicearticleno), '')     AS invoice_article_no,
    disabled::BOOL                         AS is_disabled,
    dbt_scd_id,
    dbt_updated_at,
    dbt_valid_from,
    dbt_valid_to
FROM {{ ref('next_erp_account_snapshot') }}
WHERE dbt_valid_to IS NULL

