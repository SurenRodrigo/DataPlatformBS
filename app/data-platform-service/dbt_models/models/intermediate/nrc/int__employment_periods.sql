WITH catalyst_internal AS (
    SELECT
        employeeid AS employee_id,
        field
    FROM {{ source('raw_nrc_source', 'source_catalyst_v2_employee_history') }}
    WHERE employeeid ~ '^\d+$'
),

-- Raw hire dates (Field 37)
raw_hire_dates AS (
    SELECT
        employee_id,
        NULLIF(history.value -> 'data' ->> 'value', '')::date AS hire_date
    FROM catalyst_internal,
        LATERAL JSONB_ARRAY_ELEMENTS((field -> '37' ->> 'auditChange')::jsonb) AS history (value)
    WHERE history.value ->> 'timeline' = '1' -- Filter for active timeline entries
),

-- Raw termination dates (Field 38)
raw_termination_dates AS (
    WITH audit_changes_f38 AS (
        SELECT
            catalyst_int.employee_id,
            history.value,
            NULLIF(history.value ->> 'dataValidFrom', '')::date AS data_valid_from,
            NULLIF(history.value -> 'data' ->> 'value', '')::date AS termination_date_value
        FROM catalyst_internal catalyst_int,
            LATERAL JSONB_ARRAY_ELEMENTS((field -> '38' ->> 'auditChange')::jsonb) AS history (value)
        WHERE history.value ->> 'timeline' = '1' -- Filter for active timeline entries
    ),
    latest_f38_audit AS (
        SELECT
            employee_id,
            MAX(data_valid_from) AS latest_data_valid_from
        FROM audit_changes_f38
        GROUP BY employee_id
    )
    SELECT
        audit_change.employee_id,
        audit_change.termination_date_value AS termination_date,
        (audit_change.termination_date_value IS NOT NULL) AS is_raw_termination
    FROM audit_changes_f38 audit_change
    LEFT JOIN latest_f38_audit latest_audit
        ON audit_change.employee_id = latest_audit.employee_id
    WHERE
        audit_change.termination_date_value IS NOT NULL 
        OR (
            audit_change.termination_date_value IS NULL 
            AND audit_change.data_valid_from = latest_audit.latest_data_valid_from 
        )
),

adjusted_hire_dates AS (
    SELECT 
        employee_id,
        hire_date
    FROM raw_hire_dates
    
    UNION ALL
    
    SELECT 
        employee_id,
        date_value AS hire_date
    FROM {{ ref('int__nrc_employee_edge_case') }}
    WHERE date_type = 'HIRING' 
      AND action = 'ADD'
      
    EXCEPT
    
    SELECT 
        employee_id,
        date_value AS hire_date
    FROM {{ ref('int__nrc_employee_edge_case') }}
    WHERE date_type = 'HIRING' 
      AND action = 'REMOVE'
),

adjusted_termination_dates AS (
    SELECT 
        employee_id,
        termination_date,
        is_raw_termination
    FROM raw_termination_dates
    
    UNION ALL
    
    SELECT 
        employee_id,
        date_value AS termination_date,
        TRUE AS is_raw_termination
    FROM {{ ref('int__nrc_employee_edge_case') }}
    WHERE date_type = 'TERMINATION' 
      AND action = 'ADD'
      
    EXCEPT
    
    SELECT 
        employee_id,
        date_value AS termination_date,
        TRUE AS is_raw_termination
    FROM {{ ref('int__nrc_employee_edge_case') }}
    WHERE date_type = 'TERMINATION' 
      AND action = 'REMOVE'
),

all_period_starts_ranked AS (
    SELECT
        employee_id,
        period_start_date,
        is_joiner_flag,
        ROW_NUMBER() OVER (PARTITION BY employee_id, period_start_date ORDER BY is_joiner_flag DESC) as row_num
    FROM (
        SELECT
            employee_id,
            hire_date AS period_start_date,
            TRUE AS is_joiner_flag
        FROM adjusted_hire_dates
        UNION ALL 
        SELECT
            employee_id,
            (termination_date + INTERVAL '1 day')::date AS period_start_date,
            FALSE AS is_joiner_flag
        FROM adjusted_termination_dates
        WHERE termination_date IS NOT NULL
    ) AS combined_starts
),
all_period_starts_deduplicated AS (
    SELECT
        employee_id,
        period_start_date,
        is_joiner_flag
    FROM all_period_starts_ranked
    WHERE row_num = 1 
),

final_periods_staging AS (
    SELECT
        period_starts.employee_id,
        period_starts.period_start_date AS period_start,
        period_starts.is_joiner_flag AS is_joiner,
        (SELECT MIN(hire_dates.hire_date) FROM adjusted_hire_dates hire_dates WHERE hire_dates.employee_id = period_starts.employee_id AND hire_dates.hire_date > period_starts.period_start_date) AS next_actual_hire_after_start,
        (SELECT MIN(term_dates.termination_date) FROM adjusted_termination_dates term_dates WHERE term_dates.employee_id = period_starts.employee_id AND term_dates.termination_date > period_starts.period_start_date AND term_dates.is_raw_termination = TRUE) AS next_actual_termination_after_start
    FROM all_period_starts_deduplicated AS period_starts
),

calculated_periods AS (
    SELECT
        final_period.employee_id,
        final_period.period_start,
        CASE
            WHEN final_period.next_actual_termination_after_start IS NOT NULL
                 AND (final_period.next_actual_hire_after_start IS NULL OR final_period.next_actual_termination_after_start <= (final_period.next_actual_hire_after_start - INTERVAL '1 day')::date)
                THEN final_period.next_actual_termination_after_start
            WHEN final_period.next_actual_hire_after_start IS NOT NULL THEN (final_period.next_actual_hire_after_start - INTERVAL '1 day')::date
            WHEN final_period.next_actual_hire_after_start IS NULL AND final_period.next_actual_termination_after_start IS NULL THEN
                (SELECT termination_date FROM adjusted_termination_dates term_date WHERE term_date.employee_id = final_period.employee_id AND term_date.termination_date > final_period.period_start ORDER BY termination_date DESC NULLS FIRST LIMIT 1)
            ELSE NULL
        END AS period_end,
        final_period.is_joiner,
        CASE
            WHEN final_period.next_actual_termination_after_start IS NOT NULL
                 AND (final_period.next_actual_hire_after_start IS NULL OR final_period.next_actual_termination_after_start <= (final_period.next_actual_hire_after_start - INTERVAL '1 day')::date)
                THEN TRUE
            WHEN final_period.next_actual_hire_after_start IS NULL
                 AND final_period.next_actual_termination_after_start IS NULL
                 AND final_period.is_joiner = FALSE
                 AND EXISTS (
                    SELECT 1
                    FROM adjusted_termination_dates t
                    WHERE t.employee_id = final_period.employee_id
                      AND t.termination_date IS NULL
                 )
                THEN TRUE
            ELSE FALSE
        END AS is_leaver
    FROM final_periods_staging final_period
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['calculated_period.employee_id', 'calculated_period.period_start', 'calculated_period.period_end']) }} AS id,
    calculated_period.employee_id,
    calculated_period.period_start,
    calculated_period.period_end,
    calculated_period.is_joiner,
    calculated_period.is_leaver
FROM calculated_periods calculated_period
WHERE (calculated_period.is_joiner = TRUE OR calculated_period.is_leaver = TRUE OR calculated_period.period_end IS NULL)
AND NOT (calculated_period.is_joiner = FALSE AND calculated_period.is_leaver = FALSE AND calculated_period.period_end IS NULL)
ORDER BY calculated_period.employee_id, calculated_period.period_start