{% macro parse_italian_number(column) %}
    -- Converts Italian formatted numbers (1.234,56) to doubles
    try_cast(replace(replace({{ column }}, '.', ''), ',', '.') as double)
{% endmacro %}

{% macro parse_italian_pct(column) %}
    -- Converts Italian formatted percentages (62,61%) to doubles
    try_cast(replace(replace(replace({{ column }}, '%', ''), '.', ''), ',', '.') as double)
{% endmacro %}
