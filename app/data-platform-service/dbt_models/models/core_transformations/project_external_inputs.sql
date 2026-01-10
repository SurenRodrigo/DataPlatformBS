-- depends_on: {{ ref('int_external_input_types') }}
-- depends_on: {{ ref('project') }}
-- depends_on: {{ ref('department_seed') }}
{% set hr_relation = source('raw_nrc_source', 'source_sharepoint_hr_external_inputs') %}
{% set hr_columns = adapter.get_columns_in_relation(hr_relation) %}
{% set available_hr_columns = [] %}
{% for col in hr_columns %}
    {% do available_hr_columns.append(col.name | upper) %}
{% endfor %}

{% set hse_relation = source('raw_nrc_source', 'source_sharepoint_hse_external_inputs') %}
{% set hse_columns = adapter.get_columns_in_relation(hse_relation) %}
{% set available_hse_columns = [] %}
{% for col in hse_columns %}
    {% do available_hse_columns.append(col.name | upper) %}
{% endfor %}

{# Retrieve the list of dynamic inputs from external_input_types #}
{% set inputs = loop_through_column_values('public_intermediate.int_external_input_types', "input_name") %}

{# Filter out inputs that are not present in each source #}
{% set filtered_hr_inputs = [] %}
{% set filtered_hse_inputs = [] %}
{% for input in inputs %}
    {% if input | upper in available_hr_columns %}
         {% do filtered_hr_inputs.append(input) %}
    {% endif %}
    {% if input | upper in available_hse_columns %}
         {% do filtered_hse_inputs.append(input) %}
    {% endif %}
{% endfor %}

WITH 
hr_external_inputs AS (
    SELECT *
    FROM {{ hr_relation }}
),
hse_external_inputs AS (
    SELECT *
    FROM {{ hse_relation }}
),
pivoted_hr_inputs AS (
    {% for input in filtered_hr_inputs %}
        {{ log("Including HR input: " ~ input, info=True) }}
        {# Get the actual column name from HR source (preserving case) #}
        {% set actual_hr_name = input %}
        {% for col in hr_columns %}
            {% if col.name | upper == input | upper %}
                {% set actual_hr_name = col.name %}
            {% endif %}
        {% endfor %}
    SELECT
         "Year" AS "year",
         "Month" AS "month",
         "TenantID" AS tenant_id,
         "CompanyID"::TEXT AS company_id,
         "DivisionCode" AS division_code,
         "DepartmentCode" AS department_code,
         "ProjectNumber"::TEXT AS project_number,
         '{{ input }}' AS field,
         {{ adapter.quote(actual_hr_name) }} AS "value",
         "LastModified" AS last_modified
    FROM hr_external_inputs
    {% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}
),
pivoted_hse_inputs AS (
    {% for input in filtered_hse_inputs %}
        {{ log("Including HSE input: " ~ input, info=True) }}
        {# Get the actual column name from HSE source (preserving case) #}
        {% set actual_hse_name = input %}
        {% for col in hse_columns %}
            {% if col.name | upper == input | upper %}
                {% set actual_hse_name = col.name %}
            {% endif %}
        {% endfor %}
    SELECT
         "Year" AS "year",
         "Month" AS "month",
         "TenantID" AS tenant_id,
         "CompanyID"::TEXT AS company_id,
         "DivisionCode" AS division_code,
         "DepartmentCode" AS department_code,
         "ProjectNumber"::TEXT AS project_number,
         '{{ input }}' AS field,
         {{ adapter.quote(actual_hse_name) }} AS "value",
         "LastModified" AS last_modified
    FROM hse_external_inputs
    {% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}
),


department_mapping AS (
    SELECT *
    FROM {{ ref('int__department_denormalized') }}
),

project_hse_inputs AS (
    SELECT *
    FROM {{ ref('int__nrc_project_external_inputs') }}
),

combined AS (
    SELECT * FROM pivoted_hr_inputs
    UNION ALL
    SELECT * FROM pivoted_hse_inputs
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['external_inputs.year', 'external_inputs.month', 'external_inputs.tenant_id', 'external_inputs.company_id', 'external_inputs.division_code', 'external_inputs.department_code', 'external_inputs.project_number']) }} AS project_external_input_id,
    external_inputs.tenant_id AS tenant_id,
    ext_project_id AS project_id,
    project.project_number AS project_number,
    (project.project_number || ' - ' || project.project_name) AS project_name,
    department.company_id AS company_id,
    department.company_name AS company_name,
    department.division_id AS division_id,
    department.division_name AS division_name,
    department.department_id AS organizational_unit_id,
    department.department_name AS organizational_unit_name,
    external_input_types.external_input_type_id AS external_input_type_id,
    CURRENT_TIMESTAMP AS input_date_time,
    date_part('month', to_date(external_inputs.month, 'MON'))::INT AS "month",
    external_inputs.month As month_name,
    external_inputs.year AS "year",
    external_inputs.value AS "value",
    external_input_types.is_active AS is_active,
    last_modified
FROM combined external_inputs
LEFT JOIN {{ ref('project') }} project ON external_inputs.project_number::INT = project.project_number
LEFT JOIN department_mapping department ON external_inputs.department_code = department.department_id
LEFT JOIN {{ ref('int_external_input_types') }} external_input_types ON external_inputs.field = external_input_types.input_name
WHERE external_inputs.value IS NOT NULL

UNION ALL

SELECT
    {{ dbt_utils.generate_surrogate_key(['project_external_inputs.year', 'project_external_inputs.month', 'project_external_inputs.project_number']) }} AS project_external_input_id,
    project_external_inputs.tenant_id AS tenant_id,
    ext_project_id AS project_id,
    project.project_number AS project_number,
    (project.project_number || ' - ' || project.project_name) AS project_name,
    NULL AS company_id,
    NULL AS company_name,
    NULL AS division_id,
    NULL AS division_name,
    NULL AS organizational_unit_id,
    NULL AS organizational_unit_name,
    external_input_types.external_input_type_id AS external_input_type_id,
    CURRENT_TIMESTAMP AS input_date_time,
    date_part('month', to_date(project_external_inputs.month, 'MON'))::INT AS "month",
    project_external_inputs.month As month_name,
    project_external_inputs.year AS "year",
    project_external_inputs.value AS "value",
    external_input_types.is_active AS is_active,
    last_modified
FROM project_hse_inputs project_external_inputs
LEFT JOIN {{ ref('project') }} project ON project_external_inputs.project_number::INT = project.project_number
LEFT JOIN department_mapping department ON project_external_inputs.department_id = department.department_id
LEFT JOIN {{ ref('int_external_input_types') }} external_input_types ON project_external_inputs.field = external_input_types.input_name
WHERE project_external_inputs.value IS NOT NULL
