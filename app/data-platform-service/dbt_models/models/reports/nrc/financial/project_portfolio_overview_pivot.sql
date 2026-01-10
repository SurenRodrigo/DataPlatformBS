WITH base AS (
    SELECT
        company_name                            AS "Company",
        division_name                           AS "Division",
        department_name                         AS "Department",
        project_number                          AS "Project No",
        project_name                            AS "Project Name",
        project_identifier                      AS "Project",
        year                                    AS "Year",
        period_value                            AS "Period",
        status                                  AS "Status",
        acc_income                              AS "Acc. Income",
        acc_cost                                AS "Acc. Cost",
        acc_ebit                                AS "Acc. EBIT",
        order_stock                             AS "Order Stock",
        ytd_ebit                                AS "YTD EBIT",
        period_ebit                             AS "Period EBIT",
        period_date                             AS "Period Date"
    FROM
        {{ ref('project_portfolio') }}
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
            WHEN 'Acc. Income' THEN "Acc. Income"
            WHEN 'Acc. Cost' THEN "Acc. Cost"
            WHEN 'Acc. EBIT' THEN "Acc. EBIT"
            WHEN 'Order Stock' THEN "Order Stock"
            WHEN 'YTD EBIT' THEN "YTD EBIT"
            WHEN 'Period EBIT' THEN "Period EBIT"
        END AS value
    FROM base
    CROSS JOIN (VALUES
        ('Acc. Income'),
        ('Acc. Cost'),
        ('Acc. EBIT'),
        ('Order Stock'),
        ('YTD EBIT'),
        ('Period EBIT')
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
    metric AS "Metric",
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
    "Status",
    "Metric"