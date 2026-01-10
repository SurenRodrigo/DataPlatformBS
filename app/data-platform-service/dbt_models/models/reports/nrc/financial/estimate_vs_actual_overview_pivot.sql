WITH base AS (
    SELECT
        company_name                            AS "Company",
        division_name                           AS "Division",
        department_name                         AS "Department",
        project_number                          AS "Project No",
        project_name                            AS "Project Name",
        project_identifier                      AS "Project",
        period_value                            AS "Period",
        period_date                             AS "Period Date",
        EXTRACT(YEAR FROM period_date)          AS "Year",
        status                                  AS "Status",
        acc_estimate                            AS "Acc. Est. Revenue",
        acc_estimate_margin                     AS "Acc. Est. Margin",
        acc_contribution                        AS "Acc. Margin"
    FROM {{ ref('project_portfolio') }}
),
unpivoted AS (
    SELECT
        "Company",
        "Division",
        "Department",
        "Project No",
        "Project Name",
        "Project",
        "Year",
        "Status",
        metric,
        TO_CHAR("Period Date", 'Mon') AS month,
        CASE metric
            WHEN 'Acc. Est. Revenue' THEN "Acc. Est. Revenue"
            WHEN 'Acc. Est. Margin' THEN "Acc. Est. Margin"
            WHEN 'Acc. Margin' THEN "Acc. Margin"
        END AS value
    FROM base
    CROSS JOIN (VALUES
        ('Acc. Est. Revenue'),
        ('Acc. Est. Margin'),
        ('Acc. Margin')
    ) AS m(metric)
)
SELECT
    "Company",
    "Division",
    "Department",
    "Project No",
    "Project Name",
    "Project",
    "Year",
    "Status",
    metric,
    MAX(CASE WHEN month = 'Jan' THEN value END) AS "Jan",
    MAX(CASE WHEN month = 'Feb' THEN value END) AS "Feb",
    MAX(CASE WHEN month = 'Mar' THEN value END) AS "Mar",
    MAX(CASE WHEN month = 'Apr' THEN value END) AS "Apr",
    MAX(CASE WHEN month = 'May' THEN value END) AS "May",
    MAX(CASE WHEN month = 'Jun' THEN value END) AS "Jun",
    MAX(CASE WHEN month = 'Jul' THEN value END) AS "Jul",
    MAX(CASE WHEN month = 'Aug' THEN value END) AS "Aug",
    MAX(CASE WHEN month = 'Sep' THEN value END) AS "Sep",
    MAX(CASE WHEN month = 'Oct' THEN value END) AS "Oct",
    MAX(CASE WHEN month = 'Nov' THEN value END) AS "Nov",
    MAX(CASE WHEN month = 'Dec' THEN value END) AS "Dec"
FROM unpivoted
GROUP BY
    "Company",
    "Division",
    "Department",
    "Project No",
    "Project Name",
    "Project",
    "Year",
    "Status",
    metric
ORDER BY
    "Company",
    "Project No",
    "Year",
    metric