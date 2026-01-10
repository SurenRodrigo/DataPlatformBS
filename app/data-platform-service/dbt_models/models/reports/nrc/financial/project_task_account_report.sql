SELECT
    task_account.company_name        AS "Company",
    task_account.division_name       AS "Division",
    task_account.department_name     AS "Department",
    task_account.project_id          AS "Project No",
    task_account.project_name        AS "Project Name",
    task_account.project_identifier  AS "Project",
    task_account.ext_project_task_id AS "Production Code",
    task_account.project_task_name   AS "Name",
    task_account.period_value        AS "Period",
    task_account.year                AS "Year",
    task_account.period_date         AS "Period Date",
    TO_CHAR(task_account.period_date, 'Month') AS month_name,


    MAX(
        CASE WHEN task_account.amount_type = 'Cost' THEN task_account.amount_period END
    )                                AS "This Period Cost",
    MAX(
        CASE WHEN task_account.amount_type = 'Income' THEN task_account.amount_period END
    )                                AS "This Period Income",
    MAX(CASE WHEN task_account.amount_type = 'Income' THEN task_account.amount_period END)
    - MAX(
        CASE WHEN task_account.amount_type = 'Cost' THEN task_account.amount_period END
    )                                AS "This Period Net",

    MAX(
        CASE WHEN task_account.amount_type = 'Cost' THEN task_account.amount_acc END
    )                                AS "Accumulated Cost",
    MAX(
        CASE WHEN task_account.amount_type = 'Income' THEN task_account.amount_acc END
    )                                AS "Accumulated Income",
    MAX(CASE WHEN task_account.amount_type = 'Income' THEN task_account.amount_acc END)
    - MAX(
        CASE WHEN task_account.amount_type = 'Cost' THEN task_account.amount_acc END
    )                                AS "Accumulated Net",

    MAX(
        CASE WHEN task_account.amount_type = 'Cost' THEN task_account.amount_ytd END
    )                                AS "YTD Cost",
    MAX(
        CASE WHEN task_account.amount_type = 'Income' THEN task_account.amount_ytd END
    )                                AS "YTD Income",
    MAX(CASE WHEN task_account.amount_type = 'Income' THEN task_account.amount_ytd END)
    - MAX(
        CASE WHEN task_account.amount_type = 'Cost' THEN task_account.amount_ytd END
    )                                AS "YTD Net"

FROM {{ ref('project_task_account') }} AS task_account

GROUP BY
    task_account.company_name,
    task_account.division_name,
    task_account.department_name,
    task_account.project_id,
    task_account.project_name,
    task_account.project_identifier,
    task_account.ext_project_task_id,
    task_account.project_task_name,
    task_account.period_value,
    task_account.period_date,
    task_account.year

ORDER BY
    task_account.company_name ASC,
    task_account.project_id DESC,
    task_account.ext_project_task_id ASC,
    task_account.period_value ASC,
    task_account.year ASC
