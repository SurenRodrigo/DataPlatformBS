SELECT
    company_name                            AS "Company",
    division_name                           AS "Division",
    department_name                         AS "Department",
    project_number                          AS "Project No",
    project_name                            AS "Project Name",
    project_identifier                      AS "Project",
    period_value                            AS "Period",
    period_date                             AS "Period Date",
    period_income                           AS "Period Income",
    (period_income - period_contribution)   AS "Period Cost",
    period_contribution                     AS "Period Margin",
    ytd_income                              AS "YTD Income",
    (ytd_income - ytd_contribution)         AS "YTD Cost",
    ytd_contribution                        AS "YTD Margin",
    year                                    AS "Year" ,
    status                                  AS "Status" 
FROM {{ ref('project_portfolio') }}
ORDER BY
    company_name ASC,
    project_number DESC,
    period_value ASC
