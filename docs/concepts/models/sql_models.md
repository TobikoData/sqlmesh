# SQL models

SQL models are the main types of models used by SQLMesh. SQL models consist of three sections:

* `MODEL` DDL
* Optional sql statements
* The model's `SELECT` statement

SQL models are designed to look and feel like you're simply using SQL, but they can be customized for advanced use cases.

## Definition
To create an example SQL model, add a file named `my_model.sql` into the `models/` directory (or a subdirectory of `models/`) within your SQLMesh project. 

Although the name of the file doesn't matter, it is recommended to name it something that includes the word "model". Each file can only have one model defined within it.

### Example
```sql linenums="1"
-- The Model DDL where you specify model metadata and configuration information.
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
The `MODEL` DDL is used to specify metadata about the model such as name, [kind](./model_kinds.md), owner, cron, and others. The Model statement should be the first statement in your model SQL file.

Refer to `model` [properties](./overview.md#properties) for the full list of allowed properties.

### Optional statements
Optional SQL statements can help you prepare the model's `SELECT` statement. For example, you might create temporary tables or set permissions. 

However, be careful not to run any command that could conflict with the execution of the model's query, such as creating a physical table.

### Model `SELECT` query
The model must contain a standalone `SELECT` query. The result of this query will be used to populate the model table or view.

## Automatic dependencies
SQLMesh parses your SQL, so it understands what the code does and how it relates to other models. There is no need for you to manually specify dependencies to other models with special tags or commands. 

For example, consider a model with this query:

```sql linenums="1"
SELECT employees.id
FROM employees
JOIN countries
  ON employees.id = countries.employee_id
```

SQLMesh will detect that the model depends on both `employees` and `countries`. When executing this model, it will ensure that `employees` and `countries` are executed first. 

External dependencies not defined in SQLMesh are also supported. SQLMesh can either depend on them implicitly through the order in which they are executed, or through signals if you are using [Airflow](../../integrations/airflow.md).

Although automatic dependency detection works most of the time, there may be specific cases for which you want to define dependencies manually. You can do so in the Model DDL with the [dependencies property](./overview.md#properties).

## Conventions
SQLMesh encourages explicitly assigning a data type for each model column. This allows SQLMesh to understand the data types in your models, and it prevents incorrect type inference.

### Column types
SQLMesh encourages explicit type casting. SQL's type coercion can be tricky to deal with, so it is best to ensure that the data in your model is exactly as you want it.

### Explicit SELECTs
Although `SELECT *` is convenient, it is dangerous because a model's results can change due to external factors (e.g., an upstream source adding or removing a column). In general, we encourage listing out every column you need or using [`create_external_models`](../../reference/cli.md#create_external_models) to capture the schema of an external data source. 

If you select from an external source, `SELECT *` will prevent SQLMesh from performing some optimization steps and from determining upstream column-level lineage. Use an [`external` model kind](./model_kinds.md#external) to enable optimizations and upstream column-level lineage for external sources.

## Transpilation
SQLMesh leverages [SQLGlot](https://github.com/tobymao/sqlglot) to parse and transpile SQL. Therefore, you can write your SQL in any supported dialect and transpile it into another supported dialect. 

You can also use advanced syntax that may not be available in your engine of choice. For example, `x::int` is equivalent to `CAST(x as INT)`, but is only supported in some dialects. SQLGlot allows you to use this feature regardless of what engine you're using. 

Additionally, you won't have to worry about minor formatting differences such as trailing commas, as SQLGlot will remove them at parse time.

## Macros
Although standard SQL is very powerful, complex data systems often require running SQL queries with dynamic components such as date filters. For example, you may want to change the date ranges in a `between` statement so that you can get the latest batch of data. SQLMesh provides these dates automatically through [macro variables](../macros/macro_variables.md).

Additionally, large queries can be difficult to read and maintain. In order to make queries more compact, SQLMesh supports a powerful [macro syntax](../macros/overview.md) as well as [Jinja](https://jinja.palletsprojects.com/en/3.1.x/), allowing you to write macros that make your SQL queries easier to manage.
