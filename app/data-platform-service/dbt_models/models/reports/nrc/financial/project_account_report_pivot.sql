WITH base AS (
    SELECT
        "Year",
        "Company",
        "Account",
        "Account Name",
        "Project ID",
        "Project Name",
        "Project",
        "Division",
        "Department",
        "Accumulated",
        "Year to Date (YTD)",
        "Last Quarter",
        "Period Date"
    FROM {{ ref('project_account_report')}}
),
unpivoted AS (
    SELECT
        "Year",
        "Company",
        "Account",
        "Account Name",
        "Project ID",
        "Project Name",
        "Project",
        "Division",
        "Department",
        metric,
        TO_CHAR("Period Date", 'Mon') AS month,
        CASE metric
            WHEN 'Accumulated' THEN "Accumulated"
            WHEN 'Year to Date (YTD)' THEN "Year to Date (YTD)"
            WHEN 'Last Quarter' THEN "Last Quarter"
        END AS value
    FROM base
    CROSS JOIN (VALUES
        ('Accumulated'),
        ('Year to Date (YTD)'),
        ('Last Quarter')
    ) AS m(metric)
)
SELECT
    "Year",
    "Company",
    "Account",
    "Account Name",
    "Project ID",
    "Project Name",
    "Project",
    "Division",
    "Department",
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
    "Year",
    "Company",
    "Account",
    "Account Name",
    "Project ID",
    "Project Name",
    "Project",
    "Division",
    "Department",
    metric
ORDER BY
    "Year",
    "Company",
    "Account",
    "Project",
    metric