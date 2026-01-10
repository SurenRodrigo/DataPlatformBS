SELECT
    customer_id,
    customer_name
FROM {{ ref('stg__admmit_project') }}    
GROUP BY customer_id, customer_name