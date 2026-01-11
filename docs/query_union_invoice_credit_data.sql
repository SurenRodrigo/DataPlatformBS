-- Union query to combine invoice_data and credit_data
-- Credit data line_total and gross_profit are negated to represent credits as negative values
-- This allows proper aggregation where credits offset invoice amounts

SELECT 
    id,
    invoice_number,
    customer_name,
    item_code,
    seller_id,
    seller_name,
    quantity,
    line_total,
    gross_profit,
    customer_code,
    items_group_name,
    posted_date,
    end_of_month_bucket,
    item_category,
    year,
    customer_group,
    cohort,
    ipc,
    dim,
    created_at,
    updated_at,
    'invoice' AS record_type
FROM invoice_data

UNION ALL

SELECT 
    id,
    invoice_number,
    customer_name,
    item_code,
    seller_id,
    NULL AS seller_name,  -- credit_data doesn't have seller_name
    quantity,
    -line_total AS line_total,  -- Negate line_total for credits
    -gross_profit AS gross_profit,  -- Negate gross_profit for credits
    customer_code,
    items_group_name,
    posted_date,
    end_of_month_bucket,
    item_category,
    year,
    customer_group,
    cohort,
    NULL AS ipc,  -- credit_data doesn't have ipc
    NULL AS dim,  -- credit_data doesn't have dim
    created_at,
    updated_at,
    'credit' AS record_type
FROM credit_data

ORDER BY posted_date DESC, invoice_number ASC, record_type ASC;

-- Example aggregation queries using the union dataset:

-- Total line_total and gross_profit by year (credits will offset invoices)
/*
SELECT 
    year,
    SUM(line_total) AS net_line_total,
    SUM(gross_profit) AS net_gross_profit,
    COUNT(*) AS total_records,
    COUNT(CASE WHEN record_type = 'invoice' THEN 1 END) AS invoice_count,
    COUNT(CASE WHEN record_type = 'credit' THEN 1 END) AS credit_count
FROM (
    SELECT 
        year,
        line_total,
        gross_profit,
        'invoice' AS record_type
    FROM invoice_data
    
    UNION ALL
    
    SELECT 
        year,
        -line_total AS line_total,
        -gross_profit AS gross_profit,
        'credit' AS record_type
    FROM credit_data
) AS union_data
GROUP BY year
ORDER BY year DESC;
*/

-- Net totals by customer for a specific year
/*
SELECT 
    customer_code,
    customer_name,
    SUM(line_total) AS net_line_total,
    SUM(gross_profit) AS net_gross_profit,
    COUNT(*) AS total_records
FROM (
    SELECT 
        customer_code,
        customer_name,
        line_total,
        gross_profit
    FROM invoice_data
    WHERE year = 2024
    
    UNION ALL
    
    SELECT 
        customer_code,
        customer_name,
        -line_total AS line_total,
        -gross_profit AS gross_profit
    FROM credit_data
    WHERE year = 2024
) AS union_data
GROUP BY customer_code, customer_name
HAVING SUM(line_total) != 0  -- Filter out customers with zero net balance
ORDER BY net_line_total DESC;
*/
