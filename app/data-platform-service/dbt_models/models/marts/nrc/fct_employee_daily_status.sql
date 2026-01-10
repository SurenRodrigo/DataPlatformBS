WITH dimension_calendar AS (
    SELECT 
        date_key,
        year,
        month,
        month_name
    FROM {{ ref('int__gen_date_spine') }}
),

employee AS (
    SELECT
        employee_id,
        gender,
        reason_for_leaving
    FROM {{ ref('employee') }} AS employee_base
),

employment_periods AS (
    SELECT
        employee_id,
        period_start,
        period_end,
        is_joiner,
        is_leaver
    FROM {{ ref('int__employment_periods') }}
),

employee_group_history AS (
    SELECT
        employee_id,
        employee_group_name,
        start_date,
        end_date
    FROM {{ ref('employee_history') }}
    WHERE update_type = 'EMPLOYEE_GROUP'
),

employment_relationship_history AS (
    SELECT
        employee_id,
        employment_relationship_name,
        start_date,
        end_date
    FROM {{ ref('employee_history') }}
    WHERE update_type = 'EMPLOYMENT_RELATIONSHIP'
),

employee_organization_history AS (
    SELECT
        employee_id,
        company_name,
        company_id,
        legal_entity_name,
        legal_entity_id,
        organizational_unit_name,
        organizational_unit_id,
        start_date,
        end_date
    FROM {{ ref('employee_history') }}
    WHERE update_type = 'ORGANIZATION'
),

employee_dates AS (
    SELECT
        dimension_calendar.date_key,
        dimension_calendar.year,
        dimension_calendar.month,
        dimension_calendar.month_name,
        employee.employee_id,
        employee.gender,
        employee.reason_for_leaving
    FROM dimension_calendar
    CROSS JOIN employee
)

SELECT
  employee_dates.date_key,
  employee_dates.year,
  employee_dates.month,
  employee_dates.month_name,
  employee_dates.employee_id,
  employee_dates.gender,
  CASE
    WHEN employment_periods.employee_id IS NOT NULL THEN TRUE
    ELSE FALSE
  END AS is_active,
  employee_group_history.employee_group_name AS active_group,
  employment_relationship_history.employment_relationship_name AS active_relationship,
  employee_organization_history.company_name AS active_company,
  employee_organization_history.company_id AS active_company_id,
  employee_organization_history.legal_entity_name AS active_division,
  employee_organization_history.legal_entity_id AS active_division_id,
  employee_organization_history.organizational_unit_name AS active_department,
  employee_organization_history.organizational_unit_id AS active_department_id,
  CASE 
    WHEN employee_dates.gender = 'Mann' THEN TRUE
    ELSE FALSE
  END AS is_male,

  CASE 
    WHEN employee_dates.gender = 'Kvinne' THEN TRUE
    ELSE FALSE
  END AS is_female,

  CASE 
    WHEN employee_group_history.employee_group_name = 'Lærling' THEN TRUE
    ELSE FALSE
  END AS is_apprentice,

  CASE 
    WHEN employment_relationship_history.employment_relationship_name IN (
      'Midlertidig ansatt', 
      'Midlertidig ansatt som tilkallingsvikar',
      'Ekstra/engasjement'
    ) THEN TRUE
    ELSE FALSE
  END AS is_temp_employee,

  CASE 
    WHEN employee_group_history.employee_group_name IN (
      'Funksjonær', 
      'Innleid personell'
    ) THEN TRUE
    ELSE FALSE
  END AS is_white_collar,

  CASE 
    WHEN employee_group_history.employee_group_name IN (
      'Yrkesarbeider', 
      'Lærling',
      'Fagarbeider'
    ) THEN TRUE
    ELSE FALSE
  END AS is_blue_collar,

  CASE 
    WHEN employment_periods.period_start = employee_dates.date_key AND employment_periods.is_joiner THEN TRUE
    ELSE FALSE
  END AS is_joiner,

  CASE 
    WHEN employment_periods.period_end = employee_dates.date_key AND employment_periods.is_leaver THEN TRUE
    ELSE FALSE
  END AS is_leaver,

  CASE 
    WHEN employment_periods.period_end = employee_dates.date_key AND employment_periods.is_leaver AND employee_dates.reason_for_leaving IN ('Eget ønske', 'Pensjonert') THEN TRUE
    ELSE FALSE
  END AS is_voluntary_leaver,

  CASE 
    WHEN employment_periods.period_end = employee_dates.date_key AND employment_periods.is_leaver AND employee_dates.reason_for_leaving = 'Pensjonert' THEN TRUE
    ELSE FALSE
  END AS is_retired

FROM employee_dates
LEFT JOIN employment_periods
  ON employee_dates.employee_id = employment_periods.employee_id
    AND employee_dates.date_key >= employment_periods.period_start
    AND (
        employment_periods.period_end IS NULL
        OR employee_dates.date_key <= employment_periods.period_end
    )
LEFT JOIN employee_group_history
  ON employee_dates.employee_id = employee_group_history.employee_id
    AND employee_dates.date_key >= employee_group_history.start_date::DATE
    AND (
      employee_group_history.end_date IS NULL
      OR employee_dates.date_key <= employee_group_history.end_date::DATE
    )    
LEFT JOIN employment_relationship_history
  ON employee_dates.employee_id = employment_relationship_history.employee_id
    AND employee_dates.date_key >= employment_relationship_history.start_date::DATE
    AND (
      employment_relationship_history.end_date IS NULL
      OR employee_dates.date_key <= employment_relationship_history.end_date::DATE
    )
LEFT JOIN employee_organization_history
  ON employee_dates.employee_id = employee_organization_history.employee_id
    AND employee_dates.date_key >= employee_organization_history.start_date::DATE
    AND (
      employee_organization_history.end_date IS NULL
      OR employee_dates.date_key <= employee_organization_history.end_date::DATE
    )              

ORDER BY date_key ASC, employee_id ASC
