SELECT
    company_name                            AS "Company",
    division_name                           AS "Division",
    department_name                         AS "Department",
    project_number                          AS "Project No",
    project_name                            AS "Project Name",
    project_identifier                      AS "Project",
    period_value                            AS "Period",
    acc_estimate                            AS "Acc. Est. Revenue",
    --   ?? AS "Acc. Revenue", -- Check if the right value -> yet to be clarified,
    acc_estimate_margin                     AS "Acc. Est. Margin",
    acc_contribution                        AS "Acc. Margin",
    period_date                             AS "Period Date"
FROM
    {{ ref('project_portfolio') }}
