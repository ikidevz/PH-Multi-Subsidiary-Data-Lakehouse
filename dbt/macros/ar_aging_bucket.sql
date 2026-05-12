{% macro ar_aging_bucket(days_outstanding) %}
case
    when {{ days_outstanding }} between 0 and 30 then '0-30'
    when {{ days_outstanding }} between 31 and 60 then '31-60'
    when {{ days_outstanding }} between 61 and 90 then '61-90'
    else '90+'
end
{% endmacro %}
