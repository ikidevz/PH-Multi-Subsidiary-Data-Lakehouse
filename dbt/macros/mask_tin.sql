{% macro mask_tin(value) %}
sha256({{ value }})
{% endmacro %}
