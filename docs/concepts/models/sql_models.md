# SQL models

SQL Models are the main types of models of SQLMesh. SQL models consist of a `Model` DDL, optional sql statements to run before the main query, and the main SQL query. SQL models are designed to look and feel like you're simply using SQL, but can be customized for advanced use cases as well.

## Definition
To create a SQL model, just add a file named `my_model.sql` into the `models/` directory (or subdirectory of `models/`) in your SQLMesh project. Although the name of the file doesn't matter, it is recommended to name it the model. Each file can only have one model defined within it.

### Example
```sql
-- The Model DDL where you specify options regarding the model.
MODEL (
  name db.customers
);

/*
  Optional SQL statements to run before the main query.
  You should NOT do things with side effects that could
  error out when multiple queries run like creating physical tables.
*/
CACHE TABLE countries AS SELECT * FROM raw.countries;

/*
  This is the main SQL statement that makes up your model.
  Although it is not required, it is best practice to specify explicit types
  for all columns in the final SELECT statement.
*/
SELECT
  r.id::INT,
  r.name::TEXT,
  c.country::TEXT
FROM raw.restaurants AS r
JOIN countries AS c
  ON r.id = c.restaurant_id;
```

### Model DDL
The Model DDL is used to specify metadata about the model like name, [kind](../model_kinds), owner, and cron. The Model statement should be the first statement in your model SQL file.

Check out the full list of `model` [properties](../overview/#properties).

### Statements
Statements are optional SQL statements that can help you prepare the main query. You can do things like create temp tables or set permissions. However, you should be careful not to run any command that could conflict with a parallel running model like creating a physical table.

### Main Query
The main query must contain a standalone SQL Statement that explicitly lists out its columns. The result of this query will be used to populate the model table.

## Automatic Dependencies
SQLMesh parses your SQL so it has a first class understanding of what you're trying to do. There is no need for you to manually specify dependencies to other models with special tags or commands. For example, given a model with the query

```sql
SELECT employees.id
FROM employees
JOIN countries
  ON employees.id = countries.employee_id
```

SQLMesh will detect that the model depends on both employees and countries. When evaluating this model, it will ensure that employees and countries will be evaluated first. External dependencies that are not defined in SQLMesh are also supported. SQLMesh can either depend on them via a schedule or through signals if you are using [Airflow](../../../integrations/airflow).

Although automatic dependency detection works most of the time, there may be specific cases where you want to define them manually. You can do so in the Model DDL with the [dependencies property](../overview/#properties).

## Conventions
SQLMesh encourages explicitly typing and selecting each column of a model. This allows for SQLMesh to understand the types of your models as well as prevent unexpected type inference from occurring.

### Typing
SQLMesh encourages explicit type casting to ensure that the data in your model is exactly as you want it. SQL's type coercion can be tricky to deal with and so it is best if you don't leave that up to chance.

### Explicit Selects
Although `SELECT *` can be convenient, it's dangerous because a model's result can change due to external factors (the upstream source adding or removing a column). In general, you should always list out every column you need. When selecting from external sources, `SELECT *` is prohibited and will raise an exception.


## Transpilation
SQLMesh leverages [SQLGlot](https://github.com/tobymao/sqlglot) in order to parse and transpile SQL. Thus you can write your SQL in any supported dialect and transpile it into another supported dialect. You can also use advanced syntax that may not be available in your engine of choice. For example, `x::int` is equivalent to `CAST(x as INT)` but is only supported in some dialects. SQLGlot brings you this feature regardless of the engine you are running in.

Additionally you don't have to worry about things like trailing commas as SQLGlot will remove them at parse time.

## Macros
Although SQL is very powerful, it is often required to run SQL queries with dynamic components like date filters. Additionally, large queries can be difficult to read and maintain, so having macros is powerful tool to address these advanced use cases.

SQLMesh supports a powerful [macro syntax](../../macros) and [Jinja](https://jinja.palletsprojects.com/en/3.1.x/).
