{% macro fiscal_year(date_column) %}
extract(year from {{ date_column }})
{% endmacro %}
