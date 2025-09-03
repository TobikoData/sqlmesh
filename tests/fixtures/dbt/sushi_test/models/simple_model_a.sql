{{ check_model_is_table(model) }}

{% if 'SELECT' in model.raw_code %}
  {{ check_model_is_table_alt(model) }}
{% endif %}

SELECT 1 AS a
