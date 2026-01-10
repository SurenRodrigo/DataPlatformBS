SELECT
    {{ dbt_utils.generate_surrogate_key(['internal_id','client_id'])}}
                      AS employee_id,
    internal_id       AS ext_employee_id,                
    2                 AS tenant_id,
    'Gunnar Knutsen'  AS tenant_name,
    6                 AS data_source_id,
    'Admmit'          AS data_source_name,
    company_id,
    company_name,
    legal_entity_id,
    legal_entity_name,
    organizational_unit_id,
    organizational_unit_name,
    employed_date     AS employment_date,
    end_of_employment AS employment_end_date,
    name,
    first_name,
    middle_name,
    last_name,
    gender,
    NULL              AS email,
    NULL              AS username,
    NULL              AS ssn,
    NULL              AS corporate_seniority_date,
    company_mobile_phone
   
FROM {{ ref('int_gk_employee_transformed') }}