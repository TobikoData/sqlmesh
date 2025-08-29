{{ check_model_is_table(model) }}

{% if 'DISTINCT' in model.raw_code %}
  {{ check_model_is_table_alt(model) }}
{% endif %}

SELECT DISTINCT
  customer_id::INT AS customer_id
FROM {{ ref('orders') }} as o
