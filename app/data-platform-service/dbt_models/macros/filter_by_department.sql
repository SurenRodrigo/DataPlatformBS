{% macro loop_through_column_values(table_name, column_name) %}
  {% set query %}
    SELECT DISTINCT {{ column_name }} FROM {{ table_name }}
    WHERE {{ column_name }} IS NOT NULL
  {% endset %}
 
  {% set results = run_query(query) %}
 
  {% if results %}
    {% set values = results.columns[0].values() %}
    {{ return(values) }}
  {% endif %}
{% endmacro %}