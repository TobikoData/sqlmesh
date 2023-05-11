{% test revenue_less_than(model, column_name, amount) %}
	select *
	from {{ model }}
	where {{ column_name }} >= {{ amount }}
{% endtest %}
