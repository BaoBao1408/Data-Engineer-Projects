{% test minimum_row_count(model, min_rows) %}
{{
  config(
    severity = 'warn'
    )
}}
SELECT 
    COUNT(*) AS row_count 
FROM 
    {{ model }}
HAVING 
    COUNT(*) < {{ min_rows }}
{% endtest %}