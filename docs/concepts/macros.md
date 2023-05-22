# Macros

SQL is a static language. It does not have features like variables or control flow logic (if-then) that allow SQL commands to behave differently in different situations.

However, data pipelines are dynamic and need different behavior depending on context. SQL is made dynamic with *macros*. 

SQLMesh supports two macro systems: SQLMesh macros and the [Jinja](https://jinja.palletsprojects.com/en/3.1.x/) templating system. 

This page describes how to use SQLMesh macros to build dynamic data pipelines.

## Macro systems: two approaches

SQLMesh macros behave differently than those of templating systems like [Jinja](https://jinja.palletsprojects.com/en/3.1.x/) - this section describes how and why. 

Macro systems are based on string substitution. The macro system scans code files, identifies special characters that signify macro content, and replaces the macro elements with other text. 

In a general sense, that is the entire functionality templating systems. They have tools that provide control flow logic (if-then) and other functionality, but *that functionality is solely to support substituting in the correct strings*. Templating systems are intentionally agnostic to the programming language being templated, and most of them work for everything from blog posts to HTML to SQL.

In contrast, SQLMesh macros are designed specifically for generating SQL code. They have *semantic understanding* of the SQL code being built by analyzing it with the Python [sqlglot](https://github.com/tobymao/sqlglot) library, and they allow use of Python code so users can tidily implement sophisticated macro logic.

### SQLMesh macro approach

This section describes how SQLMesh macros work under the hood. Feel free to skip over this section and return if and when it is useful. This information is **not** required to use SQLMesh macros, but it will be useful for debugging any macros exhibiting puzzling behavior. 

The critical distinction between the SQLMesh macro approach and templating systems is the role string substitution plays. In templating systems, string substitution is the entire and only point. In SQLMesh, string substitution is just one step toward modifying the semantic representation of the SQL query. *SQLMesh macros work by building and modifying the semantic representation of the SQL query.*

The SQLMesh macro processor is implemented based on two facts: the input text may contain Python code, and the code returned by the macro processor must be semantically valid SQL code. The processor must differentiate Python code from other types of text so it can *evaluate* the code with Python - we refer to this as "evaluation mode." In contrast, for non-Python code it uses "literal mode" to modify the semantical representation with substituted strings.

After processing all the non-SQL text, it uses the values to modify the semantic representation of the query to its final state.

It uses the following five step approach to accomplish this:

1. Parse the text with the appropriate sqlglot SQL dialect (e.g., Postgres, BigQuery, etc.). During the parsing, it detects the special macro symbol `@` to differentiate non-SQL from SQL text. The parser builds a semantic representation of the SQL code's structure, capturing non-SQL text as "placeholder" values to use in subsequent steps.

2. Examine the placeholder values to classify them as one of the following types:
- Definitions of macro variables with the `@DEF` operator (see more about [user-defined macro variables](#User-defined-variables) below)
- Macro variables, both [SQLMesh pre-defined](#predefined-variables) and [user-defined](#User-defined-variables) with the `@DEF` operator
- Macro functions, both [SQLMesh's](#sqlmesh-macro-operators) and [user-defined](#user-defined-macro-functions)

3. Substitute macro variable values where the macro symbol `@` is detected. In most cases, this is direct string substitution as with a templating system.

4. Identify Python code in the non-SQL text, and evaluate that code with Python.

5. Modify the semantic representation of the SQL query with both the substituted macro variables from (3) and the results returned from the Python code in (4)


## Variables

The most common use case for macros is variable substitution. For example, you might have a SQL query that filters by date in the `WHERE` clause. 

Instead of manually changing the date each time the model is run, you can use a macro variable to make the date dynamic. With the dynamic approach the date changes automatically based on when the query is run. 

Consider this query that filters for rows where column `my_date` is after '2023-01-01':

```sql linenums="1"
SELECT *
FROM table
WHERE my_date > '2023-01-01'
```

To make this query's date dynamic you could use the predefined SQLMesh macro variable `@latest_ds`: 

```sql linenums="1"
SELECT *
FROM table
WHERE my_date > @latest_ds
```

The `@` symbol tells SQLMesh that `@latest_ds` is a macro variable that require substitution before the SQL is executed. 

The macro variable `@latest_ds` is predefined, so its value will be automatically set by SQLMesh based on when the model was last executed. If the model was last run on January 1, 2023 the resulting query would be:

```sql linenums="1"
SELECT *
FROM table
WHERE my_date > '2023-01-01'
```

This example used one of SQLMesh's predefined variables, but you can also create your own custom macro variables.

We describe SQLMesh's predefined variables next and custom macro variables in the subsequent [User-defined Variables](#user-defined-variables) section.

### Predefined Variables
SQLMesh comes with predefined variables that can be used in your queries. They are automatically set by the SQLMesh runtime. 

These variables are related to time and are comprised of a combination of prefixes (start, end, latest) and postfixes (date, ds, ts, epoch, millis).

Prefixes:

* start - The inclusive starting interval of a model run.
* end - The inclusive end interval of a model run.
* latest - The most recent date SQLMesh ran the model, determined from its [snapshot](./architecture/snapshots.md).

Postfixes:

* date - A python date object that converts into a native SQL Date.
* ds - A date string with the format: '%Y-%m-%d'
* ts - An ISO 8601 datetime formatted string: '%Y-%m-%d %H:%M:%S'.
* epoch - An integer representing seconds since epoch.
* millis - An integer representing milliseconds since epoch.

All predefined macro variables:

* date
    * @start_date
    * @end_date
    * @latest_date

* datetime
    * @start_dt
    * @end_dt
    * @latest_dt

* ds
    * @start_ds
    * @end_ds
    * @latest_ds

* ts
    * @start_ts
    * @end_ts
    * @latest_ts

* epoch
    * @start_epoch
    * @end_epoch
    * @latest_epoch

* millis
    * @start_millis
    * @end_millis
    * @latest_millis

More information to come on python date objects and epoch values.

### User-defined variables

Define your own macro variables with the `@DEF` macro operator. For example, you could set the macro variable `macro_var` to the value `1` with:

```sql linenums="1"
@DEF(macro_var, 1);
```

SQLMesh has three basic requirements for using the `@DEF` operator:
1. The `MODEL` statement must end with a semi-colon `;`
2. All `@DEF` uses must come after the `MODEL` DDL statement and before the SQL code
3. Each `@DEF` use must end with a semi-colon `;`

For example, consider the following model `sqlmesh_example.full_model` from the SQLMesh quickstart guide:

```sql linenums="1"
MODEL (
  name sqlmesh_example.full_model,
  kind FULL,
  cron '@daily',
  audits [assert_positive_order_ids],
);

SELECT
  item_id,
  count(distinct id) AS num_orders,
FROM
    sqlmesh_example.incremental_model
GROUP BY item_id
```

This model could be extended to use a user-defined macro variable to filter the query results based on `item_size` like this:

```sql linenums="1"
MODEL (
  name sqlmesh_example.full_model,
  kind FULL,
  cron '@daily',
  audits [assert_positive_order_ids],
); -- NOTE: semi-colon at end of MODEL statement

@DEF(size, 1); -- NOTE: semi-colon at end of @DEF operator

SELECT
  item_id,
  count(distinct id) AS num_orders,
FROM
    sqlmesh_example.incremental_model
WHERE
    item_size > @size -- Reference to macro variable `@size` defined above with `@DEF()`
GROUP BY item_id
```

This example defines the macro variable `size` with `@DEF(size, 1)`. When the model is run, SQLMesh will substitute in the number `1` where `@size` appears in the `WHERE` clause.

More information to come on quoting behavior when quotes are included/excluded in the value passed to `@DEF()`.

## SQLMesh macro operators

SQLMesh's macro system has multiple operators that allow different forms of dynamism in models.

### Functional operators

Macro systems use control flow operators such as `for` loops and `if` statements to enable powerful dynamic SQL code. SQLMesh macros use approaches from functional programming to implement these operators, which allows them to be both powerful and concise.

#### `for` loops
Before diving in, let's dissect a `for` loop to understand its components. `for` loops have two primary parts: a collection of items and an action that should be taken for each item. For example, here is a `for` loop in Python:

```python linenums="1"
for number in [4, 5, 6]:
    print(number)
```

This loop prints the name of each number present in the brackets:

```python linenums="1"
4
5
6
```

The first line of the example sets up the loop, doing two things:
1. Telling Python that code inside the loop will refer to each item as `number`
2. Telling Python to step through the list of items in brackets

The second line tells Python what action should be taken for each item. In this case, it prints the item.

The loop executes one time for each item in the list, substituting in the item for the word `number` in the code. For example, the first time through the loop the code would execute as `print(1)` and the second time as `print(2)`.

#### `@EACH` basics
The SQLMesh `@EACH` operator is used to implement the equivalent of a `for` loop in SQLMesh macros. `@EACH` gets its name from the fact that a loop performs the action "for each" item in the collection. It is fundamentally equivalent to the Python loop above, but you specify the two loop components differently. 

This example accomplishes a similar task to the Python example above:

```sql linenums="1"
SELECT
    @EACH([4, 5, 6], number -> number)
FROM table
```
The loop is set up by the first argument: `@EACH([4, 5, 6]` tells SQLMesh to step through the list of items in brackets.

The second argument `number -> number` tells SQLMesh what action should be taken for each item using an "anonymous" function (aka "lambda" function). The left side of the arrow states what name the code on the right side will refer to each item as, just like `for number` in the Python example.

The right side of the arrow specifies what should be done to each item in the list. `number -> number` tells `@EACH` that for each item `number` it should return that item (e.g., `1`).

SQLMesh macros use their semantic understanding of SQL code to take automatic actions based on where in a SQL query macro variables are used. If `@EACH` is used in the `SELECT` clause of a SQL statement:
1. It prints the item
2. It knows fields are separated by commas in `SELECT`, so it automatically separates the printed items with commas

Given the automatic print and comma-separation, the anonymous function `number -> number` tells `@EACH` that for each item `number` it should print the item and separate the items with commas. Therefore, the complete output from the query is:

```sql linenums="1"
SELECT
    4, 
    5, 
    6
FROM table
```

#### `@EACH` string substitution

The basic example above is too simple to be useful. Most uses of `@EACH` will likely involve using the values as one or both of a literal value and an identifier. 

For example, a column `favorite_number` in our data might contain values `4`, `5`, and `6`, and we want to unpack that column into three indicator (i.e., binary, dummy, one-hot encoded) columns. We could write that by hand as:

```sql linenums="1"
SELECT
    CASE WHEN favorite_number = 4 THEN 1 ELSE 0 END as favorite_4, 
    CASE WHEN favorite_number = 5 THEN 1 ELSE 0 END as favorite_5,
    CASE WHEN favorite_number = 6 THEN 1 ELSE 0 END as favorite_6
FROM table
```

In this code each data value is being used in two distinct ways. For example, `4` is being used:
1. As a literal value in `favorite_number = 4`
2. As part of a column name in `favorite_4`

We first describe each of these uses separately.

For the literal value, `@EACH` substitutes in the exact value that is passed in the brackets, *including quotes*. For example, consider this query similar to the `CASE WHEN` example above:

```sql linenums="1"
SELECT
  @EACH([4,5,6], x -> CASE WHEN favorite_number = x THEN 1 ELSE 0 END as column)
FROM table
```

It renders to this SQL:

```sql linenums="1"
SELECT 
  CASE WHEN favorite_number = 4 THEN 1 ELSE 0 END AS column, 
  CASE WHEN favorite_number = 5 THEN 1 ELSE 0 END AS column,
  CASE WHEN favorite_number = 6 THEN 1 ELSE 0 END AS column
FROM table
```

Note that the number `4`, `5`, and `6` are unquoted in *both* the input `@EACH` array in brackets and the resulting SQL query. 

We can instead quote them in the input `@EACH` array:

```sql linenums="1"
SELECT
  @EACH(['4','5','6'], x -> CASE WHEN favorite_number = x THEN 1 ELSE 0 END as column)
FROM table
```

And they will be quoted in the resulting SQL query:

```sql linenums="1"
SELECT 
  CASE WHEN favorite_number = '4' THEN 1 ELSE 0 END AS column, 
  CASE WHEN favorite_number = '5' THEN 1 ELSE 0 END AS column,
  CASE WHEN favorite_number = '6' THEN 1 ELSE 0 END AS column
FROM table
```

#### `@IF`

More information to come.

#### `@REDUCE`

More information to come.

#### `@FILTER`

More information to come.

### Meta Operators
@SQL
@''

More information to come.

## User-defined macro functions

More information to come.

## SQL clause operators

SQLMesh's macro system has six operators that correspond to different clauses in SQL syntax. They are:

- `@WITH`: common table expression `WITH` clause
- `@JOIN`: table `JOIN` clause(s)
- `@WHERE`: filtering `WHERE` clause
- `@GROUP_BY`: grouping `GROUP BY` clause
- `@HAVING`: group by filtering `HAVING` clause
- `@ORDER_BY`: ordering `ORDER BY` clause

Each of these operators is used to dynamically add the code for its corresponding clause to a model's SQL query.

### How SQL clause operators work
The SQL clause operators take a single argument that determines whether the clause is generated. 

If the argument is True the clause code is generated, if False the code is not. The argument's truth is determined by executing its contents as Python code. 

As an example, let's revisit the example model from the [User-defined Variables](#user-defined-variables) section above. 

As written, the model will always include the `WHERE` clause. We could make its presence dynamic by using the `@WHERE` macro operator:

```sql linenums="1"
MODEL (
  name sqlmesh_example.full_model,
  kind FULL,
  cron '@daily',
  audits [assert_positive_order_ids],
);

@DEF(size, 1);

SELECT
  item_id,
  count(distinct id) AS num_orders,
FROM
    sqlmesh_example.incremental_model
@WHERE(True) item_id > @size
GROUP BY item_id
```

The `@WHERE` argument is set to `True`, so the WHERE code is included in the rendered model:

```sql linenums="1"
SELECT
  item_id,
  count(distinct id) AS num_orders,
FROM
    sqlmesh_example.incremental_model
WHERE item_id > 1
GROUP BY item_id
```

If the `@WHERE` argument were instead set to `False` the `WHERE` clause would be omitted from the query.

These operators aren't too useful if the argument's value is hard-coded. Instead, the argument can consist of code executable by Python. 

For example, the `WHERE` clause will be included in this query because 1 less than 2:

```sql linenums="1"
MODEL (
  name sqlmesh_example.full_model,
  kind FULL,
  cron '@daily',
  audits [assert_positive_order_ids],
);

@DEF(size, 1);

SELECT
  item_id,
  count(distinct id) AS num_orders,
FROM
    sqlmesh_example.incremental_model
@WHERE(1 < 2) item_id > @size
GROUP BY item_id
```

The operator's argument code can include macro variables. 

In this example, the two numbers being compared are defined as macro variables instead of being hard-coded:

```sql linenums="1"
MODEL (
  name sqlmesh_example.full_model,
  kind FULL,
  cron '@daily',
  audits [assert_positive_order_ids],
);

@DEF(left_number, 1);
@DEF(right_number, 2);
@DEF(size, 1);

SELECT
  item_id,
  count(distinct id) AS num_orders,
FROM
    sqlmesh_example.incremental_model
@WHERE(@left_number < @right_number) item_id > @size
GROUP BY item_id
```

The argument to `@WHERE` will be "1 < 2" as in the previous example after the macro variables `left_number` and `right_number` are substituted in.

### SQL clause operator examples

This section provides brief examples of each SQL clause operator's usage.

The examples use variants of this simple select statement:

```sql linenums="1"
SELECT *
FROM all_cities
```

#### `@WITH` operator

```sql linenums="1"
@WITH(True) all_cities as (select * from city) 
select *
FROM all_cities
```

renders to

```sql linenums="1"
WITH all_cities as (select * from city) 
select *
FROM all_cities
```

#### `@JOIN` operator

```sql linenums="1"
select *
FROM all_cities
LEFT OUTER @JOIN(True) country 
    ON city.country = country.name
```

renders to

```sql linenums="1"
select *
FROM all_cities
LEFT OUTER JOIN country 
    ON city.country = country.name
```

The `@JOIN` operator recognizes that `LEFT OUTER` is a component of the `JOIN` specification and will omit it if the argument evaluates to False.

#### `@WHERE` operator

```sql linenums="1"
SELECT *
FROM all_cities
@WHERE(True) city_name = 'Toronto'
```

renders to

```sql linenums="1"
SELECT *
FROM all_cities
WHERE city_name = 'Toronto'
```

#### `@GROUP_BY` operator

```sql linenums="1"
SELECT *
FROM all_cities
@GROUP_BY(True) city_id
```

renders to

```sql linenums="1"
SELECT *
FROM all_cities
GROUP BY city_id
```

#### `@HAVING` operator

```sql linenums="1"
SELECT 
count(city_pop) as population
FROM all_cities
GROUP BY city_id
@HAVING(True) population > 1000
```

renders to

```sql linenums="1"
SELECT 
count(city_pop) as population
FROM all_cities
GROUP BY city_id
HAVING population > 1000
```

#### `@ORDER_BY` operator

```sql linenums="1"
SELECT *
FROM all_cities
@ORDER_BY(True) city_pop
```

renders to

```sql linenums="1"
SELECT *
FROM all_cities
ORDER BY city_pop
```



## Jinja
[Jinja](https://jinja.palletsprojects.com/en/3.1.x/) is a popular templating tool for creating dynamic SQL and is supported by SQLMesh, but there are some drawbacks which lead for us to create our own macro system.

* Jinja is not valid SQL and not parseable.
```sql linenums="1"
-- templating allows for arbitrary string replacements which is not feasible to parse
SE{{ 'lect' }} x {{ 'AS ' + var }}
FROM {{ 'table CROSS JOIN z' }}
```

* Jinja is verbose and difficult to debug.
```sql linenums="1"
TBD example with multiple for loops with trailing or leading comma
```
* No concepts of types. Easy to miss quotes.

```sql linenums="1"
SELECT *
FROM table
WHERE ds BETWEEN '{{ start_ds }}' and '{{ end_ds }}'  -- quotes are needed
WHERE ds BETWEEN {{ start_ds }} and {{ end_ds }}  -- error because ds is a string
```
