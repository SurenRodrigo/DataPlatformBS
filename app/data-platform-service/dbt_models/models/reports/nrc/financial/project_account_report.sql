WITH base_data AS (
    SELECT
        *
    FROM {{ ref('project_account') }}
),

-- Filtered valid months only for joining
valid_months AS (
    SELECT *
    FROM base_data
    WHERE period BETWEEN 1 AND 12
),

-- Self-join to find value 3 months before
joined_data AS (
    SELECT
        curr.project_account_sk,
        curr.data_source_id,
        curr.tenant_id,
        curr.client                                       AS "Client",
        curr.company_id,
        curr.company_name                                 AS "Company",
        curr.division_id,
        curr.division_name                                AS "Division",
        curr.department_id,
        curr.department_name                              AS "Department",
        curr.project_id                                   AS "Project ID",
        curr.ext_project_id                               AS "Project No",
        curr.project_name                                 AS "Project Name",
        curr.project_identifier                           AS "Project",
        curr.year                                         AS "Year",
        curr.period                                       AS "Period",
        curr.period_value                                 AS "Period Value",
        curr.period_date                                  AS "Period Date",
        curr.account                                      AS "Account",
        curr.account_name                                 AS "Account Name",
        curr.amount_acc                                   AS "Accumulated",
        curr.amount_ytd                                   AS "Year to Date (YTD)",
        COALESCE(prev.amount_acc, 0)                      AS previous_quarter_amount,
        COALESCE(curr.amount_acc - prev.amount_acc, 0)    AS "Last Quarter"
    FROM base_data AS curr
    LEFT JOIN valid_months AS prev
        ON
            curr.client = prev.client
            AND curr.ext_project_id = prev.ext_project_id
            AND curr.account = prev.account
            AND prev.period_date = curr.period_date - interval '3 months'
)

SELECT * FROM joined_data
