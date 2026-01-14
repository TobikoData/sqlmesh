{%- test greater_than_amount(model, column_name, amount) -%}
	select *
	from {{ model }}
	where {{ column_name }} < {{ amount }}
{%- endtest -%}
