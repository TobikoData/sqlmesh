# SQL models

SQL models are the main types of models used by SQLMesh. SQL models consist of:

* a `Model` DDL
* any optional sql statements, and 
* the model's `SELECT` statement

SQL models are designed to look and feel like you're simply using SQL, but can be customized for advanced use cases.

## Definition
To create a SQL model, add a file named `my_model.sql` into the `models/` directory (or subdirectory of `models/`) within your SQLMesh project. Although the name of the file doesn't matter, it is recommended to name it something that includes "model." Each file can only have one model defined within it.

### Example
```sql linenums="1"
-- The Model DDL where you specify options regarding the model.
MODEL (
  name db.customers,
  kind FULL,
);

/*
  Optional SQL statements to run before the model's `SELECT` statement.
  You should NOT do things that cause side effects that could
  error out when multiple queries run, such as creating physical tables.
*/
CACHE TABLE countries AS SELECT * FROM raw.countries;

/*
  This is the main SQL statement that comprises your model.
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
The Model DDL is used to specify metadata about the model such as name, [kind](../model_kinds), owner, cron, and others. The Model statement should be the first statement in your model SQL file.

Refer to `model` [properties](../overview/#properties) for the full list of properties.

### Optional statements
Optional SQL statements can help you prepare the model's `SELECT` statement. You can do things like create temporary tables or set permissions. However, you should be careful not to run any command that could conflict with the evaluation of the model's query, such as creating a physical table.

### Model `SELECT` statement
The model's `SELECT` statement must be a standalone SQL Statement that explicitly lists out its columns. The result of this query will be used to populate the model table.

## Automatic dependencies
SQLMesh parses your SQL, so it has a first-class understanding of what you're trying to do. There is no need for you to manually specify dependencies to other models with special tags or commands. For example, given a model with the query as follows:

```sql linenums="1"
SELECT employees.id
FROM employees
JOIN countries
  ON employees.id = countries.employee_id
```

SQLMesh will detect that the model depends on both employees and countries. When evaluating this model, it will ensure that employees and countries are evaluated first. External dependencies that are not defined in SQLMesh are also supported. SQLMesh can either depend on them implicitly through a candence, or through signals if you are using [Airflow](../../../integrations/airflow).

Although automatic dependency detection works most of the time, there may be specific cases in which you want to define them manually. You can do so in the Model DDL with the [dependencies property](../overview/#properties).

## Conventions
SQLMesh encourages explicitly data typing and assigning a data type for each model's column individually. This allows SQLMesh to understand the types of your models, as well as to prevent unexpected type inference from occurring.

### Column types
SQLMesh encourages explicit type casting. SQL's type coercion can be tricky to deal with, so it is best to ensure that the data in your model is exactly as you want it.

### Explicit SELECTs
Although `SELECT *` can be convenient, it can be dangerous because a model's result can change due to external factors (for example, the upstream source adding or removing a column). In general, you should always list out every column you need. When selecting from external sources, `SELECT *` is prohibited and will raise an exception.

## Transpilation
SQLMesh leverages [SQLGlot](https://github.com/tobymao/sqlglot) in order to parse and transpile SQL. Therefore, you can write your SQL in any supported dialect and transpile it into another supported dialect. You can also use advanced syntax that may not be available in your engine of choice. For example, `x::int` is equivalent to `CAST(x as INT)`, but is only supported in some dialects. SQLGlot brings you this feature regardless of what engine you're using. Additionally, you won't have to worry about things such as trailing commas, as SQLGlot will remove them at parse time.

## Macros
Although SQL is very powerful, it is often required to run SQL queries with dynamic components such as date filters. For example, you may want to change the date ranges in a `between` statement so that you can get the latest batch of data. SQLMesh provides these dates automatically through macro variables.

Additionally, large queries can be difficult to read and maintain. In order to make queries less bloated and repetitive, SQLMesh supports a powerful [macro syntax](../../macros) as well as [Jinja](https://jinja.palletsprojects.com/en/3.1.x/), allowing you to write macros that can make your SQL queries easier to manage.
