SELECT
    id,
    approved,
    username                     AS user_name,
    verified,
    companyid                    AS company_id,
    isdeleted                    AS is_deleted,
    modifiedby                   AS modified_by,
    stopdatetime                 AS stop_datetime,
    startdatetime                AS start_datetime,
    transdatetime                AS trans_datetime,
    employeenumber               AS employee_number,
    deleteddatetime              AS deleted_datetime,
    approveddatetime             AS approved_datetime,
    modifieddatetime             AS modified_datetime,
    verifieddatetime             AS verified_datetime,
    payrollvaluesasexportlines   AS payroll_values,
    -- DBT metadata
    dbt_scd_id,
    dbt_updated_at,
    dbt_valid_from,
    dbt_valid_to
FROM {{ ref('ditio_payroll_lines_snapshot') }}
WHERE dbt_valid_to IS NULL
