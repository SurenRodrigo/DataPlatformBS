SELECT
    company_name                            AS "Company",
    division_name                           AS "Division",
    department_name                         AS "Department",
    project_number                          AS "Project No",
    project_name                            AS "Project Name",
    project_identifier                      AS "Project",
    period_value                            AS "Period",
    status                                  AS "Status",
    acc_income                              AS "Acc. Income",
    acc_cost                                AS "Acc. Cost",
    acc_contribution                        AS "Acc. Margin",
    ytd_income                              AS "YTD Income",
    ytd_contribution                        AS "YTD Margin",
    order_stock                             AS "Order Stock",
    period_date                             AS "Period Date"
FROM
    {{ ref('project_portfolio') }}
