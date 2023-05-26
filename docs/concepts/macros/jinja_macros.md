# Jinja macros

SQLMesh supports macros from the [Jinja](https://jinja.palletsprojects.com/en/3.1.x/) templating system. 

Jinja's macro approach is pure string substitution. Unlike SQLMesh macros, they assemble SQL query text without building a semantic representation. 

**NOTE:** SQLMesh supports the standard Jinja function library only - it does **not** support dbt-specific jinja functions like `{{ ref() }}`. 

## Basics

Jinja uses curly braces `{}` to differentiate macro from non-macro text. It uses the second character after the left brace to determine what the text inside the braces will do.

The three curly brace symbols are:

- `{{...}}` creates Jinja expressions. Expressions are replaced by text that is incorporated into the rendered SQL query; they can contain macro variables and functions.
- `{%...%}` creates Jinja statements. Statements give instructions to Jinja, such as setting variable values, control flow with `if`, `for` loops, and defining macro functions.
- `{#...#}` creates Jinja comments. These comments will not be included in the rendered SQL query.

## User-defined variables

Define your own variables with the Jinja statement `{% set ... %}`. For example, we could specify the name of the `num_orders` column in the `sqlmesh_example.full_model` like this:

```sql linenums="1"
MODEL (
  name sqlmesh_example.full_model,
  kind FULL,
  cron '@daily',
  audits [assert_positive_order_ids],
);

{% set my_col = 'num_orders' %} -- Jinja definition of variable `my_col`

SELECT
  item_id,
  count(distinct id) AS {{ my_col }}, -- Reference to Jinja variable {{ my_col }}
FROM
    sqlmesh_example.incremental_model
GROUP BY item_id
```

Note that the Jinja set statement is written after the `MODEL` statement and before the SQL query.

Jinja variables can be string, integer, or float data types. They can also be an iterable data structure, such as a list, tuple, or dictionary. Each of these data types and structures supports multiple [Python methods](https://jinja.palletsprojects.com/en/3.1.x/templates/#python-methods), such as the `upper()` method for strings.

## Macro operators

### Control flow operators

#### for loops

For loops let you iterate over a collection of items to condense repetitive code and easily change the values used by the code. 

Jinja for loops begin with `{% for ... %}` and end with `{% endfor %}`. This example demonstrates creating indicator variables with `CASE WHEN` using a Jinja for loop:

```sql linenums="1"
SELECT
    {% for vehicle_type in ['car', 'truck', 'bus']}
        CASE WHEN user_vehicle = '{{ vehicle_type }}' THEN 1 ELSE 0 END as vehicle_{{ vehicle_type }},
    {% endfor %}
FROM table
```

Note that the `vehicle_type` values are quoted in the list `['car', 'truck', 'bus']`. Jinja removes those quotes during processing, so the reference `'{{ vehicle_type }}` in the `CASE WHEN` statement must be in quotes. The reference `vehicle_{{ vehicle_type }}` does not require quotes.

Also note that a comma is present at the end of the `CASE WHEN` line. Trailing commas are not valid SQL and would normally require special handling, but SQLMesh's semantic understanding of the query allows it to identify and remove the offending comma.

The example renders to this after SQLMesh processing:

```sql linenums="1"
SELECT
    CASE WHEN user_vehicle = 'car' THEN 1 ELSE 0 END as vehicle_car,
    CASE WHEN user_vehicle = 'truck' THEN 1 ELSE 0 END as vehicle_truck,
    CASE WHEN user_vehicle = 'bus' THEN 1 ELSE 0 END as vehicle_bus
FROM table
```

In general, it is a best practice to define lists of values separately from their use. We could do that like this:

```sql linenums="1"
{% set vehicle_types = ['car', 'truck', 'bus'] %}

SELECT
    {% for vehicle_type in vehicle_types }
        CASE WHEN user_vehicle = '{{ vehicle_type }}' THEN 1 ELSE 0 END as vehicle_{{ vehicle_type }},
    {% endfor %}
FROM table
```

The rendered query would be the same as before.

#### if 

if statements allow you to take an action (or not) based on some condition. 

Jinja if statements begin with `{% if ... %}` and ends with `{% endif %}`. The starting `if` statement must contain code that evaluates to `True` or `False`. For example, all of `True`, `1 + 1 == 2`, and `'a' in ['a', 'b']` evaluate to `True`.

As an example, you might want a model to only include a column if the model was being run for testing purposes. We can do that by setting a variable indicating whether it's a testing run that determines whether the query includes `testing_column`:

```sql linenums="1"
{% set testing = True %}

SELECT
    normal_column,
    {% if testing %}
        testing_column
    {% endif %}
FROM table
```

Because `testing` is `True`, the rendered query would be:

```sql linenums="1"
SELECT
    normal_column,
    testing_column
FROM table
```

## User-defined macro functions

More information to come.
