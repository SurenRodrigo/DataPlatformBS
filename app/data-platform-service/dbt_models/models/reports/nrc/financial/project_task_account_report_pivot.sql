WITH base AS (
    SELECT
        "Company",
        "Division",
        "Department",
        "Project No",
        "Project Name",
        "Project",
        "Production Code",
        "Name",
        "Period",
        "Period Date",
        "Year",
        "This Period Cost",
        "This Period Income",
        "This Period Net",
        "Accumulated Cost",
        "Accumulated Income",
        "Accumulated Net",
        "YTD Cost",
        "YTD Income",
        "YTD Net"
    FROM {{ ref('project_task_account_report') }}
),
unpivoted AS (
    SELECT
        "Company",
        "Division",
        "Department",
        "Project No",
        "Project Name",
        "Project",
        "Production Code",
        "Name",
        "Year",
        metric,
        TO_CHAR("Period Date", 'Mon') AS month,
        CASE metric
            WHEN 'This Period Cost' THEN "This Period Cost"
            WHEN 'This Period Income' THEN "This Period Income"
            WHEN 'This Period Net' THEN "This Period Net"
            WHEN 'Accumulated Cost' THEN "Accumulated Cost"
            WHEN 'Accumulated Income' THEN "Accumulated Income"
            WHEN 'Accumulated Net' THEN "Accumulated Net"
            WHEN 'YTD Cost' THEN "YTD Cost"
            WHEN 'YTD Income' THEN "YTD Income"
            WHEN 'YTD Net' THEN "YTD Net"
        END AS value
    FROM base
    CROSS JOIN (VALUES
        ('This Period Cost'),
        ('This Period Income'),
        ('This Period Net'),
        ('Accumulated Cost'),
        ('Accumulated Income'),
        ('Accumulated Net'),
        ('YTD Cost'),
        ('YTD Income'),
        ('YTD Net')
    ) AS m(metric)
)
SELECT
    "Company",
    "Division",
    "Department",
    "Project No",
    "Project Name",
    "Project",
    "Production Code",
    "Name",
    "Year",
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
    "Production Code",
    "Name",
    "Year",
    metric
ORDER BY
    "Company",
    "Project No",
    "Production Code",
    "Year",
    metric