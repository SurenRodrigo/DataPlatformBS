SELECT
    {{ dbt_utils.generate_surrogate_key(['id']) }} AS account_sk,
    id,
    account_no,
    description,
    vat_code,
    external_code,
    vat_id,
    is_cost,
    is_work,
    is_material,
    is_contractor_vat,
    complement_account_no,
    is_invoiceable_on_import,
    invoice_article_no,
    is_disabled,
    created_at,
    last_modified_at
FROM {{ ref('stg__next_erp_account') }}

