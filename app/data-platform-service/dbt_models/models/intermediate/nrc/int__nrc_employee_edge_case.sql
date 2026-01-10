SELECT
    {{ dbt_utils.generate_surrogate_key([
        'employee_id',
        'date_type',
        'date_value',
        'action'
    ]) }}                 AS edge_case_sk,
    employee_id::TEXT     AS employee_id,
    date_type::TEXT       AS date_type,
    date_value::DATE      AS date_value,
    action::TEXT          AS action,
    created_at::TIMESTAMP AS created_at,
    updated_at::TIMESTAMP AS updated_at
FROM {{ ref('employee_edge_case_seed') }}
