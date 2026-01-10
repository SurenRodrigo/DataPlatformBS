SELECT
    company_name                            AS "Company",
    division_name                           AS "Division",
    department_name                         AS "Department",
    project_number                          AS "Project No",
    project_name                            AS "Project Name",
    project_identifier                      AS "Project",
    period_value                            AS "Period",
    status                                  AS "Status",
    year                                    AS "Year",
    acc_income                              AS "Acc. Income",
    acc_cost                                AS "Acc. Cost",
    acc_ebit                                AS "Acc. EBIT",
    order_stock                             AS "Order Stock",
    ytd_ebit                                AS "YTD EBIT",
    period_ebit                             AS "Period EBIT",
    period_date                             AS "Period Date"
FROM
    {{ ref('project_portfolio') }}
