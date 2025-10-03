{% macro insert_into_shared_hook_table(hook_name) %}
INSERT INTO shared_hook_table (
    id,
    hook_name,
    execution_order,
    created_at
)
VALUES (
    (SELECT COALESCE(MAX(id), 0) + 1 FROM shared_hook_table),
    '{{ hook_name }}',
    (SELECT COALESCE(MAX(id), 0) + 1 FROM shared_hook_table),
    NOW()
)
{% endmacro %}
