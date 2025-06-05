{% macro func_with_params(amount, category) %}
    case
        {% for row in [
            { 'category': '1', 'range': [0, 10], 'consider': True },
            { 'category': '2', 'range': [11, 20], 'consider': None }
        ] %}
        when {{ category }} = '{{ row.category }}'
            and {{ amount }} >= {{ row.range[0] }}
            {% if row.consider is not none %}
            and {{ amount }} < {{ row.range[1] }}
            {% endif %}
        then
            ({{ amount }} * {{ row.range[0] }} + {{ row.range[1] }}) * 4
        {% endfor %}
        else null
    end
{% endmacro %}