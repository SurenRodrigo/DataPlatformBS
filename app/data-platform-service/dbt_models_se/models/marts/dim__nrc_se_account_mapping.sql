SELECT
    {{ dbt_utils.generate_surrogate_key(['account_no']) }} as id,
    account_no,
    account_name,
    account_group_tb1
FROM {{ ref('stg__excel_account_mapping') }}
