SELECT
    {{ dbt_utils.generate_surrogate_key(['price_list', 'home_project']) }} AS id,
    price_list,
    home_project
FROM {{ ref('stg__excel_price_list_mapping') }}

