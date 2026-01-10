SELECT
    company_name                            AS "Company",
    division_name                           AS "Division",
    department_name                         AS "Department",
    project_number                          AS "Project No",
    project_name                            AS "Project Name",
    project_identifier                      AS "Project",
    period_value                            AS "Period",
    acc_cost                                AS "Acc. Investment",
    period_investment                       AS "Period Investment",
    year_investment                         AS "Year Investment",
    invested_capital                        AS "Invested Capital",
    period_date                             AS "Period Date",
    year                                    AS "Year" ,
    status                                  AS "Status" 
FROM
    {{ ref('project_portfolio') }}
