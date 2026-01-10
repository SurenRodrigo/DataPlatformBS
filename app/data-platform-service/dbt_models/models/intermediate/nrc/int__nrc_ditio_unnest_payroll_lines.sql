-- this will contain separate records per each object in 'payrollValuesAsExportLines'
SELECT
    {{ dbt_utils.generate_surrogate_key(["id", "pl.value->>'id'"]) }} AS payroll_value_id,
    id as payroll_line_parent_id,
    approved,
    user_name,
    verified,
    company_id,
    is_deleted,
    modified_by,
    stop_datetime,
    start_datetime,
    trans_datetime,
    employee_number,
    deleted_datetime,
    approved_datetime,
    modified_datetime,
    verified_datetime,
    pl.value                     AS payroll_value
FROM {{ ref('stg__ditio_payroll_lines') }} AS dpl
CROSS JOIN LATERAL jsonb_array_elements(dpl.payroll_values::jsonb) AS pl(value)