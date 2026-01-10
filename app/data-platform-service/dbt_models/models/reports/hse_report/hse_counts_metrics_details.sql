with cases_data as (
	select * from {{ ref('cases_details')}}
),
years AS (
  SELECT DISTINCT
    company_id,
    company_name,
    division_id,
    division_name,
    department_id,
    department_name,
    project_number,
    project_level_name,
    project_level_full_path,
    project_id,
    year_occurred::TEXT
  FROM cases_data
),
months_years AS (
  SELECT 'No Records' AS id,
    years.*, months.month, months.month_name, metrics.metric
  FROM years
  CROSS JOIN (
    SELECT 1 AS month, 'JAN' AS month_name
    UNION ALL SELECT 2, 'FEB'
    UNION ALL SELECT 3, 'MAR'
    UNION ALL SELECT 4, 'APR'
    UNION ALL SELECT 5, 'MAY'
    UNION ALL SELECT 6, 'JUN'
    UNION ALL SELECT 7, 'JUL'
    UNION ALL SELECT 8, 'AUG'
    UNION ALL SELECT 9, 'SEP'
    UNION ALL SELECT 10, 'OCT'
    UNION ALL SELECT 11, 'NOV'
    UNION ALL SELECT 12, 'DEC'
  ) months
  CROSS JOIN (
    SELECT 'H1' AS metric
    UNION ALL SELECT 'H2'
    UNION ALL SELECT 'M1'
    UNION ALL SELECT 'M2'
    UNION ALL SELECT 'M3'
    UNION ALL SELECT 'Health and Safety Cases' 
    UNION ALL SELECT 'External Environment Related Cases' 
    UNION ALL SELECT 'Improvement Suggestions' 
    UNION ALL SELECT 'Positive Feedback' 
    UNION ALL SELECT 'Quality Related Cases'
  ) metrics
  WHERE MAKE_DATE(years.year_occurred::INT, months.month, 1) <= DATE_TRUNC('month', CURRENT_DATE)
),
case_data AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(["case_external_id", "case_type_id", "kpi_id"]) }} AS id,
    company_id,
    company_name,
    division_id,
    division_name,
    department_id,
    department_name,
    project_number,
    project_level_name,
    project_level_full_path,
    project_id,
    year_occurred::TEXT,
    month_occurred,
    month,
    case_external_id,
    case_type_name,
    case_type_id,
    kpi_id,
    kpi_name,
    case_occurred_date AS date_occurred,
    date_published,
    deadline,
    date_closed,
    description,
    originator_email,
    originator_name,
    originator_id,
    status_key,
    status_name,
    CASE 
      WHEN kpi_id = '9025' THEN 'H1'
      WHEN kpi_id = '9024' THEN 'H2'
      WHEN kpi_id = '9178' THEN 'M1'
      WHEN kpi_id = '9177' THEN 'M2'
      WHEN kpi_id = '9176' THEN 'M3'
      ELSE NULL
    END AS metric
  FROM cases_data cases
  WHERE kpi_id IN ('9025', '9024', '9178', '9177', '9176')
),
case_type_kpis AS (
  SELECT 
    case_type_id,
    case_external_id,
    STRING_AGG(DISTINCT kpi_id, ', ') AS all_kpi_ids
  FROM cases_data
  WHERE kpi_id IS NOT NULL
  GROUP BY case_type_id, case_external_id
),
case_data_unique AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['cases."case_external_id"', 'cases."case_type_id"', 'cases."project_level_name"', 'cases."division_id"']) }} AS id,
    cases.company_id,
    cases.company_name,
    cases.division_id,
    cases.division_name,
    cases.department_id,
    cases.department_name,
    cases.project_number,
    cases.project_level_name,
    cases.project_level_full_path,
    cases.project_id,
    cases.year_occurred::TEXT,
    cases.month_occurred,
    cases.month,
    cases.case_external_id,
    cases.case_type_name,
    cases.case_type_id,
    null as kpi_id,
    null as kpi_name,
    case_occurred_date AS date_occurred,
    cases.date_published,
    cases.deadline,
    cases.date_closed,
    cases.description,
    cases.originator_email,
    cases.originator_name,
    cases.originator_id,
    cases.status_key,
    cases.status_name,
    CASE 
      WHEN cases.case_type_id = '329' THEN 'Health and Safety Cases' 
      WHEN cases.case_type_id = '331' THEN 'External Environment Related Cases' 
      WHEN cases.case_type_id = '333' THEN 'Improvement Suggestions' 
      WHEN cases.case_type_id = '334' THEN 'Positive Feedback' 
      WHEN cases.case_type_id = '332' THEN 'Quality Related Cases' 
      ELSE NULL
    END AS metric,
    ROW_NUMBER() OVER (
            PARTITION BY 
                cases.case_type_id, cases.case_external_id, cases.project_level_name, cases.year_occurred, cases.month_occurred,
                cases.company_id, cases.division_id, cases.department_id
            ORDER BY 
                cases.date_occurred DESC, cases.case_external_id
        ) as rn
  FROM cases_data as cases
  WHERE cases.case_type_id IN ('329', '331', '333', '332', '334')
),
full_grid AS (
  SELECT 
    COALESCE(cases.id, dates.id) AS id,
    COALESCE(cases.year_occurred, dates.year_occurred) as year_occurred,
    COALESCE(cases.month_occurred, dates.month) as month,
    COALESCE(cases.month, dates.month_name) as month_name,
    COALESCE(cases.company_id, dates.company_id) AS company_id,
    COALESCE(cases.company_name, dates.company_name) AS company_name,
    COALESCE(cases.division_id, dates.division_id) AS division_id,
    COALESCE(cases.division_name, dates.division_name) AS division_name,
    COALESCE(cases.department_id, dates.department_id) AS department_id,
    COALESCE(cases.department_name, dates.department_name) AS department_name,
    COALESCE(cases.project_number, dates.project_number) AS project_number,
    COALESCE(cases.project_level_name, dates.project_level_name) AS project_level_name,
    COALESCE(cases.project_level_full_path, dates.project_level_full_path) AS project_level_full_path,
    COALESCE(cases.project_id, dates.project_id) AS project_id,
    project.project_identifier, 
    cases.case_external_id,
    cases.case_type_name,
    cases.case_type_id,
    cases.kpi_id,
    cases.kpi_name,
    COALESCE(
      cases.date_occurred, 
      CASE 
        WHEN dates.id = 'No Records' THEN 
          DATE(dates.year_occurred || '-' || LPAD(dates.month::TEXT, 2, '0') || '-01') + INTERVAL '1 month' - INTERVAL '1 day'
        ELSE NULL 
      END
    ) AS date_occurred,
    cases.date_published,
    cases.deadline,
    cases.date_closed,
    cases.description,
    cases.originator_email,
    cases.originator_name,
    cases.originator_id,
    cases.status_key,
    cases.status_name,
    COALESCE(cases.metric, dates.metric) AS metric
  FROM months_years dates
  LEFT JOIN case_data cases
    ON cases.year_occurred = dates.year_occurred AND cases.month_occurred = dates.month AND cases.metric = dates.metric
    and cases.company_id = dates.company_id AND cases.division_id = dates.division_id AND cases.project_level_name = dates.project_level_name
  LEFT JOIN {{ ref('project') }} project
    ON project.company_id = COALESCE(cases.company_id, dates.company_id)
    AND project.project_number = COALESCE(cases.project_number, dates.project_number)

  union all
  
  SELECT 
    COALESCE(cases.id, dates.id) AS id,
    COALESCE(cases.year_occurred, dates.year_occurred) AS year_occurred,
    COALESCE(cases.month_occurred, dates.month) as month,
    COALESCE(cases.month, dates.month_name) as month_name,
    COALESCE(cases.company_id, dates.company_id) AS company_id,
    COALESCE(cases.company_name, dates.company_name) AS company_name,
    COALESCE(cases.division_id, dates.division_id) AS division_id,
    COALESCE(cases.division_name, dates.division_name) AS division_name,
    COALESCE(cases.department_id, dates.department_id) AS department_id,
    COALESCE(cases.department_name, dates.department_name) AS department_name,
    COALESCE(cases.project_number, dates.project_number) AS project_number,
    COALESCE(cases.project_level_name, dates.project_level_name) AS project_level_name,
    COALESCE(cases.project_level_full_path, dates.project_level_full_path) AS project_level_full_path,
    COALESCE(cases.project_id, dates.project_id) AS project_id,
    project.project_identifier, 
    cases.case_external_id,
    cases.case_type_name,
    cases.case_type_id,
    cases.kpi_id,
    cases.kpi_name,
    COALESCE(
      cases.date_occurred, 
      CASE 
        WHEN dates.id = 'No Records' THEN 
          DATE(dates.year_occurred || '-' || LPAD(dates.month::TEXT, 2, '0') || '-01') + INTERVAL '1 month' - INTERVAL '1 day'
        ELSE NULL 
      END
    ) AS date_occurred,
    cases.date_published,
    cases.deadline,
    cases.date_closed,
    cases.description,
    cases.originator_email,
    cases.originator_name,
    cases.originator_id,
    cases.status_key,
    cases.status_name,
    COALESCE(cases.metric, dates.metric) AS metric
  FROM months_years dates
  LEFT JOIN case_data_unique cases
    ON cases.year_occurred = dates.year_occurred AND cases.month_occurred = dates.month AND cases.metric = dates.metric
    and cases.company_id = dates.company_id AND cases.division_id = dates.division_id AND cases.project_level_name = dates.project_level_name
  LEFT JOIN {{ ref('project') }} project
    ON project.company_id = COALESCE(cases.company_id, dates.company_id)
    AND project.project_number = COALESCE(cases.project_number, dates.project_number)
  WHERE cases.rn = 1
)
SELECT *
FROM full_grid
ORDER BY year_occurred, month