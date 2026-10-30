{% macro age_from_birth_year(col) -%}
    (year(current_date) - {{ col }})
{%- endmacro %}
