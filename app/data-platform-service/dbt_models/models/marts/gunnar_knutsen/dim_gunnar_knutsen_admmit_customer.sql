SELECT
    {{ dbt_utils.generate_surrogate_key(['customer_id','customer_name'])}} 
                     AS customer_id,
    2                AS tenant_id,
    'Gunnar Knutsen' AS tenant_name,
    6                AS data_source_id,
    'Admmit'         AS data_source_name,
    3                AS company_id,
    NULL             AS company_registration_number,
    customer_id      AS ext_customer_id,
    customer_name    AS name,
    NULL             AS description,
    NULL::BOOLEAN    AS is_active
FROM {{ ref('int_get_gk_customers') }}
