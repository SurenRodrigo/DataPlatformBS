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
        year                                    AS "Year",
        status                                  AS "Status",
        period_income                           AS "Period Income",
        (period_income - period_contribution)   AS "Period Cost",
        period_contribution                     AS "Period Margin",
        ytd_income                              AS "YTD Income",
        (ytd_income - ytd_contribution)         AS "YTD Cost",
        ytd_contribution                        AS "YTD Margin"
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
            WHEN 'Period Income' THEN "Period Income"
            WHEN 'Period Cost' THEN "Period Cost"
            WHEN 'Period Margin' THEN "Period Margin"
            WHEN 'YTD Income' THEN "YTD Income"
            WHEN 'YTD Cost' THEN "YTD Cost"
            WHEN 'YTD Margin' THEN "YTD Margin"
        END AS value
    FROM base
    CROSS JOIN (VALUES
        ('Period Income'),
        ('Period Cost'),
        ('Period Margin'),
        ('YTD Income'),
        ('YTD Cost'),
        ('YTD Margin')
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