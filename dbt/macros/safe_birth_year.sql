{#- HIPAA Safe Harbor: ages over 89, and any birth year that reveals one, must be
    aggregated into a single 90+ group. Members aged 90 or over get a null
    birth_year; their age_band ('65+') already covers them. -#}
{% macro safe_birth_year(col) -%}
    case when {{ age_from_birth_year(col) }} >= 90 then null else {{ col }} end
{%- endmacro %}
