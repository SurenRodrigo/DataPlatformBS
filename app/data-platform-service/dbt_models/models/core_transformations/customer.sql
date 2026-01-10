WITH
comapny_data_source AS (
    SELECT 
        company_data_source_seed.company_guid,
        company_data_source_seed.data_source_id,
        company_data_source_seed.ext_company_id,
        company_seed.id AS company_id,
        company_seed.company_name AS company_name
    FROM {{ ref('company_data_source_seed') }} company_data_source_seed
    LEFT JOIN {{ ref('company_seed') }} company_seed ON
        company_data_source_seed.company_guid = company_seed.id
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['stg_customer."customer_id"']) }} AS customer_id,
    tenant.id AS tenant_id,
    data_sources.id AS data_source_id,
    company_data_source.company_id AS company_id,
    customer_id AS ext_customer_id,
    stg_customer.name,
    COALESCE(company_reg_no, '') AS company_registration_no,
    stg_customer.description,
    CONCAT(address_street, ', ', address_place, ', ', address_post_code, ', ', address_province, ', ', address_country_code) AS customer_address,
    COALESCE(phone_number1, phone_number2, phone_number3, phone_number4, phone_number5, phone_number6, phone_number7, '') AS phone_number,
    NULL AS is_active,
    last_modified_date,
    last_modified_by
FROM {{ ref('stg__unit4_customer') }} stg_customer
LEFT JOIN {{ ref('tenant_seed') }} tenant ON
    tenant.id = 1
LEFT JOIN {{ ref('data_source_seed') }} data_sources ON
    data_sources.id = 7
LEFT JOIN comapny_data_source company_data_source ON
    company_data_source.ext_company_id = '45'
