
{% if execute %}
    {% for dataset, nodes in graph.nodes.values() | selectattr("resource_type", "equalto", "model") | groupby('schema')  %}
        {% if loop.first %}
            SELECT 1 AS c
        {% endif %}
    {% endfor %}
{% endif %}
