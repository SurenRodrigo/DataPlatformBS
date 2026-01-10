{% macro column_exists_in_relation(relation, column_name) %}
    {% set columns = adapter.get_columns_in_relation(relation) %}
    {% for col in columns %}
        {% if col.name | upper == column_name | upper %}
            {{ return(true) }}
        {% endif %}
    {% endfor %}
    {{ return(false) }}
{% endmacro %}
