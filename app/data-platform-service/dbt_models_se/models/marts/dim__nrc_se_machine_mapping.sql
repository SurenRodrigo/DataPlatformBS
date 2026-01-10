SELECT
    {{ dbt_utils.generate_surrogate_key(['price_list', 'article', 'home_project']) }} AS id,
    price_list,
    article,
    article_name,
    home_project
FROM {{ ref('stg__excel_machine_mapping') }}

