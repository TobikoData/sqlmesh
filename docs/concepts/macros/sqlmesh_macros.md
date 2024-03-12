# SQLMesh macros

## Macro systems: two approaches

SQLMesh macros behave differently than those of templating systems like [Jinja](https://jinja.palletsprojects.com/en/3.1.x/).

Macro systems are based on string substitution. The macro system scans code files, identifies special characters that signify macro content, and replaces the macro elements with other text.

In a general sense, that is the entire functionality of templating systems. They have tools that provide control flow logic (if-then) and other functionality, but *that functionality is solely to support substituting in the correct strings*.

Templating systems are intentionally agnostic to the programming language being templated, and most of them work for everything from blog posts to HTML to SQL.

In contrast, SQLMesh macros are designed specifically for generating SQL code. They have *semantic understanding* of the SQL code being created by analyzing it with the Python [sqlglot](https://github.com/tobymao/sqlglot) library, and they allow use of Python code so users can tidily implement sophisticated macro logic.

### SQLMesh macro approach

This section describes how SQLMesh macros work under the hood. Feel free to skip over this section and return if and when it is useful. This information is **not** required to use SQLMesh macros, but it will be useful for debugging any macros exhibiting puzzling behavior.

The critical distinction between the SQLMesh macro approach and templating systems is the role string substitution plays. In templating systems, string substitution is the entire and only point.

In SQLMesh, string substitution is just one step toward modifying the semantic representation of the SQL query. *SQLMesh macros work by building and modifying the semantic representation of the SQL query.*

After processing all the non-SQL text, it uses the substituted values to modify the semantic representation of the query to its final state.

It uses the following five step approach to accomplish this:

1. Parse the text with the appropriate sqlglot SQL dialect (e.g., Postgres, BigQuery, etc.). During the parsing, it detects the special macro symbol `@` to differentiate non-SQL from SQL text. The parser builds a semantic representation of the SQL code's structure, capturing non-SQL text as "placeholder" values to use in subsequent steps.

2. Examine the placeholder values to classify them as one of the following types:

    - Creation of user-defined macro variables with the `@DEF` operator (see more about [user-defined macro variables](#user-defined-variables))
    - Macro variables, both [SQLMesh pre-defined](./macro_variables.md) and [user-defined](#User-defined-variables)
    - Macro functions, both [SQLMesh's](#sqlmesh-macro-operators) and [user-defined](#user-defined-macro-functions)

3. Substitute macro variable values where they are detected. In most cases, this is direct string substitution as with a templating system.

4. Execute any macro functions and substitute the returned values.

5. Modify the semantic representation of the SQL query with the substituted variable values from (3) and functions from (4).


## User-defined variables

Define your own macro variables with the `@DEF` macro operator. For example, you could set the macro variable `macro_var` to the value `1` with:

```sql linenums="1"
@DEF(macro_var, 1);
```

SQLMesh has three basic requirements for using the `@DEF` operator:

1. The `MODEL` statement must end with a semi-colon `;`
2. All `@DEF` uses must come after the `MODEL` statement and before the SQL query
3. Each `@DEF` use must end with a semi-colon `;`

For example, consider the following model `sqlmesh_example.full_model` from the [SQLMesh quickstart guide](../../quick_start.md):

```sql linenums="1"
MODEL (
  name sqlmesh_example.full_model,
  kind FULL,
  cron '@daily',
  audits (assert_positive_order_ids),
);

SELECT
  item_id,
  count(distinct id) AS num_orders,
FROM
    sqlmesh_example.incremental_model
GROUP BY item_id
```

This model could be extended with a user-defined macro variable to filter the query results based on `item_size` like this:

```sql linenums="1"
MODEL (
  name sqlmesh_example.full_model,
  kind FULL,
  cron '@daily',
  audits (assert_positive_order_ids),
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

### Macro functions

In addition to inline user-defined variables, SQLMesh also supports inline macro functions. These functions can be used to express more readable and reusable logic than is possible with variables alone. Lets look at an example:

```sql linenums="1"
MODEL(...);

@DEF(
  rank_to_int,
  x -> case when left(x, 1) = 'A' then 1 when left(x, 1) = 'B' then 2 when left(x, 1) = 'C' then 3 end
);

SELECT
  id,
  cust_rank_1,
  cust_rank_2,
  cust_rank_3
  @rank_to_int(cust_rank_1) as cust_rank_1_int,
  @rank_to_int(cust_rank_2) as cust_rank_2_int,
  @rank_to_int(cust_rank_3) as cust_rank_3_int
FROM
  some.model
```

Multiple arguments can be expressed in a macro function as well:

```sql linenums="1"
@DEF(pythag, (x,y) -> sqrt(pow(x, 2) + pow(y, 2)));

SELECT
  sideA,
  sideB,
  @pythag(sideA, sideB) AS sideC
FROM
  some.triangle
```

```sql linenums="1"
@DEF(nrr, (starting_mrr, expansion_mrr, churned_mrr) -> (starting_mrr + expansion_mrr - churned_mrr) / starting_mrr);

SELECT
  @nrr(fy21_mrr, fy21_expansions, fy21_churns) AS fy21_net_retention_rate,
  @nrr(fy22_mrr, fy22_expansions, fy22_churns) AS fy22_net_retention_rate,
  @nrr(fy23_mrr, fy23_expansions, fy23_churns) AS fy23_net_retention_rate,
FROM
  some.revenue
```

You can nest macro functions like so:

```sql linenums="1"
MODEL (
  name dummy.model,
  kind FULL
);

@DEF(area, r -> pi() * r * r);
@DEF(container_volume, (r, h) -> @area(@r) * h);

SELECT container_id, @container_volume((cont_di / 2), cont_hi) AS area
```

### User-defined variable quoting

More information to come on quoting behavior when quotes are included/excluded in the value passed to `@DEF()`.

## Macro operators

SQLMesh's macro system has multiple operators that allow different forms of dynamism in models.

### Control flow operators

Macro systems use control flow operators such as `for` loops and `if` statements to enable powerful dynamic SQL code.

This section describes how SQLMesh's control flow operators `@EACH` and `@IF` work.

#### @EACH introduction
Before diving into the `@EACH` operator, let's dissect a `for` loop to understand its components.

`for` loops have two primary parts: a collection of items and an action that should be taken for each item. For example, here is a `for` loop in Python:

```python linenums="1"
for number in [4, 5, 6]:
    print(number)
```

This for loop prints each number present in the brackets:

```python linenums="1"
4
5
6
```

The first line of the example sets up the loop, doing two things:

1. Telling Python that code inside the loop will refer to each item as `number`
2. Telling Python to step through the list of items in brackets

The second line tells Python what action should be taken for each item. In this case, it prints the item.

The loop executes one time for each item in the list, substituting in the item for the word `number` in the code. For example, the first time through the loop the code would execute as `print(4)` and the second time as `print(5)`.

#### @EACH basics
The SQLMesh `@EACH` operator is used to implement the equivalent of a `for` loop in SQLMesh macros.

`@EACH` gets its name from the fact that a loop performs the action "for each" item in the collection. It is fundamentally equivalent to the Python loop above, but you specify the two loop components differently.

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

Because of the automatic print and comma-separation, the anonymous function `number -> number` tells `@EACH` that for each item `number` it should print the item and separate the items with commas. Therefore, the complete output from the example above is:

```sql linenums="1"
SELECT
    4,
    5,
    6
FROM table
```

#### @EACH string substitution

The basic example above is too simple to be useful. Many uses of `@EACH` will involve using the values as one or both of a literal value and an identifier.

For example, a column `favorite_number` in our data might contain values `4`, `5`, and `6`, and we want to unpack that column into three indicator (i.e., binary, dummy, one-hot encoded) columns. We could write that by hand as:

```sql linenums="1"
SELECT
    CASE WHEN favorite_number = 4 THEN 1 ELSE 0 END as favorite_4,
    CASE WHEN favorite_number = 5 THEN 1 ELSE 0 END as favorite_5,
    CASE WHEN favorite_number = 6 THEN 1 ELSE 0 END as favorite_6
FROM table
```

In this code each number is being used in two distinct ways. For example, `4` is being used:

1. As a literal numeric value in `favorite_number = 4`
2. As part of a column name in `favorite_4`

We describe each of these uses separately.

For the literal numeric value, `@EACH` substitutes in the exact value that is passed in the brackets, *including quotes*. For example, consider this query similar to the `CASE WHEN` example above:

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

Now we can place the array values at the end of a column name by using the SQLMesh macro operator `@` inside the `@EACH` operator:

```sql linenums="1"
SELECT
  @EACH(['4','5','6'], x -> CASE WHEN favorite_number = x THEN 1 ELSE 0 END as column_@x)
FROM table
```

This query will render to:

```sql linenums="1"
SELECT
  CASE WHEN favorite_number = '4' THEN 1 ELSE 0 END AS column_4,
  CASE WHEN favorite_number = '5' THEN 1 ELSE 0 END AS column_5,
  CASE WHEN favorite_number = '6' THEN 1 ELSE 0 END AS column_6
FROM table
```

This syntax works regardless of whether the array values are quoted or not.


NOTE: SQLMesh macros support placing macro values at the end of a column name simply using `column_@x`. However if you wish to substitute the variable anywhere else in the identifier, you need to use the more explicit substitution syntax `@{}`. This avoids ambiguity. These are valid `@{x}_column` or `my_@{x}_column`.

#### @IF

SQLMesh's `@IF` macro allows components of a SQL query to change based on the result of a logical condition.

It includes three elements:

1. A logical condition that evaluates to `TRUE` or `FALSE`
2. A value to return if the condition is `TRUE`
3. A value to return if the condition is `FALSE` [optional]

These elements are specified as:

```sql linenums="1"
@IF([logical condition], [value if TRUE], [value if FALSE])
```

The value to return if the condition is `FALSE` is optional - if it is not provided and the condition is `FALSE`, then the macro has no effect on the resulting query.

It's also possible to use this macro in order to conditionally execute pre/post-statements:

```sql linenums="1"
@IF([logical condition], [statement to execute if TRUE]);
```

The `@IF` pre/post-statement itself must end with a semi-colon, but the inner statement argument must not.

The logical condition should be written *in SQL* and is evaluated with [SQLGlot's](https://github.com/tobymao/sqlglot) SQL engine. It supports the following operators:

- Equality: `=` for equals, `!=` or `<>` for not equals
- Comparison: `<`, `>`, `<=`, `>=`,
- Between: `[number] BETWEEN [low number] AND [high number]`
- Membership: `[item] IN ([comma-separated list of items])`

For example, the following simple conditions are all valid SQL and evaluate to `TRUE`:

- `'a' = 'a'`
- `'a' != 'b'`
- `0 < 1`
- `1 >= 1`
- `2 BETWEEN 1 AND 3`
- `'a' IN ('a', 'b')`

`@IF` can be used to modify any part of a SQL query. For example, this query conditionally includes `sensitive_col` in the query results:

```sql linenums="1"
SELECT
  col1,
  @IF(1 > 0, sensitive_col)
FROM table
```

Because `1 > 0` evaluates to `TRUE`, the query is rendered as:

```sql linenums="1"
SELECT
  col1,
  sensitive_col
FROM table
```

Note that `@IF(1 > 0, sensitive_col)` does not include the third argument specifying a value if `FALSE`. Had the condition evaluated to `FALSE`, `@IF` would return nothing and only `col1` would be selected.

Alternatively, we could specify that `nonsensitive_col` be returned if the condition evaluates to `FALSE`:

```sql linenums="1"
SELECT
  col1,
  @IF(1 > 2, sensitive_col, nonsensitive_col)
FROM table
```

Because `1 > 2` evaluates to `FALSE`, the query is rendered as:

```sql linenums="1"
SELECT
  col1,
  nonsensitive_col
FROM table
```

Another example is conditionally executing a pre/post-statement depending on the current [runtime stage](./macro_variables.md#predefined-variables). For instance, the following `@IF` post-statement will only be executed at model evaluation time:

```sql linenums="1"
MODEL (
  name sqlmesh_example.full_model,
  kind FULL,
  cron '@daily',
  grain item_id,
  audits (assert_positive_order_ids),
);

SELECT
  item_id,
  count(distinct id) AS num_orders,
FROM
    sqlmesh_example.incremental_model
GROUP BY item_id
ORDER BY item_id;

@IF(
    @runtime_stage = 'evaluating',
    ALTER TABLE sqlmesh_example.full_model ALTER item_id TYPE VARCHAR
);
```

NOTE: we can also, say, alter the type of a column if the `@runtime_stage` is `'creating'`, but that will only have meaningful effects if the corresponding model is of an incremental kind, since `FULL` models are rebuilt on each evaluation and hence any changes made at their creation stage will be overwritten.

#### @EVAL

`@EVAL` evaluates its arguments with SQLGlot's SQL executor.

It allows you to execute mathematical or other calculations in SQL code. It behaves similarly to the first argument of the [`@IF` operator](#if), but it is not limited to logical conditions.

For example, consider a query adding 5 to a macro variable:

```sql linenums="1"
MODEL (
  ...
);

@DEF(x, 1);

SELECT
  @EVAL(5 + @x) as my_six
FROM table
```

After macro variable substitution, this would render as `@EVAL(5 + 1)` and be evaluated to `6`, resulting in the final rendered query:

```sql linenums="1"
SELECT
  6 as my_six
FROM table
```

### Helper macros

SQLMesh's macro system includes two helper operators that do not directly act on the SQL query's semantic representation. Instead, they manipulate arrays of values that are used by other SQLMesh macro operators like `@EACH`.

Like [`@EACH`](#each-basics), they take two arguments: an array of items in brackets `[]` and an anonymous function specifying what actions to take with the items.

#### @FILTER

`@FILTER` is used to subset an input array of items to only those meeting the logical condition specified in the anonymous function. Its output can be consumed by other macro operators such as [`@EACH`](#each-basics) or [`@REDUCE`](#reduce).

The user-specified anonymous function must evaluate to `TRUE` or `FALSE`. `@FILTER` applies the function to each item in the array, only including the item in the output array if it meets the condition.

The anonymous function should be written *in SQL* and is evaluated with [SQLGlot's](https://github.com/tobymao/sqlglot) SQL engine. It supports standard SQL equality and comparison operators - see [`@IF`](#if) above for more information about supported operators.

For example, consider this `@FILTER` call:

```sql linenums="1"
@FILTER([1,2,3], x -> x > 1)
```

It applies the condition `x > 1` to each item in the input array `[1,2,3]` and returns `[2,3]`.

#### @REDUCE

`@REDUCE` is used to combine the items in an array.

The anonymous function specifies how the items in the input array should be combined. In contrast to `@EACH` and `@FILTER`, the anonymous function takes two arguments whose values are named in parentheses.

For example, an anonymous function for `@EACH` might be specified `x -> x + 1`. The `x` to the left of the arrow tells SQLMesh that the array items will be referred to as `x` in the code to the right of the arrow.

Because the `@REDUCE` anonymous function takes two arguments, the text to the left of the arrow must contain two comma-separated names in parentheses. For example, `(x, y) -> x + y` tells SQLMesh that items will be referred to as `x` and `y` in the code to the right of the arrow.

Note that even though the anonymous function takes only two arguments the input array can contain as many items as necessary.

Consider the anonymous function `(x, y) -> x + y`. Conceptually, only the `y` argument corresponds to items in the array; the `x` argument is a temporary value created when the function is evaluated.

For the call `@REDUCE([1,2,3,4], (x, y) -> x + y)`, the anonymous function is applied to the array in the following steps:

1. Take the first two items in the array as `x` and `y`. Apply the function to them: `1 + 2` = `3`.
2. Take the output of step (1) as `x` and the next item in the array `3` as `y`. Apply the function to them: `3 + 3` = `6`.
3. Take the output of step (2) as `x` and the next item in the array `4` as `y`. Apply the function to them: `6 + 4` = `10`.
4. No items remain. Return value from step (3): `10`.

`@REDUCE` will almost always be used with another macro operator. For example, we might want to build a `WHERE` clause from multiple column names:

```sql linenums="1"
SELECT
  my_column
FROM
  table
WHERE
  col1 = 1 and col2 = 1 and col3 = 1
```

We can use `@EACH` to build each column's predicate (`col1 = 1`) and `@REDUCE` to combine them into a single statement:

```sql linenums="1"
SELECT
  my_column
FROM
  table
WHERE
  @REDUCE(
    @EACH([col1, col2, col3], x -> x = 1), -- Builds each individual predicate `col1 = 1`
    (x, y) -> x AND y -- Combines individual predicates with `AND`
  )
```

### SQL clause operators

SQLMesh's macro system has six operators that correspond to different clauses in SQL syntax. They are:

- `@WITH`: common table expression `WITH` clause
- `@JOIN`: table `JOIN` clause(s)
- `@WHERE`: filtering `WHERE` clause
- `@GROUP_BY`: grouping `GROUP BY` clause
- `@HAVING`: group by filtering `HAVING` clause
- `@ORDER_BY`: ordering `ORDER BY` clause

Each of these operators is used to dynamically add the code for its corresponding clause to a model's SQL query.

#### How SQL clause operators work
The SQL clause operators take a single argument that determines whether the clause is generated.

If the argument is `TRUE` the clause code is generated, if `FALSE` the code is not. The argument should be written *in SQL* and its value is evaluated with [SQLGlot's](https://github.com/tobymao/sqlglot) SQL engine.

Each SQL clause operator may only be used once in a query, but any common table expressions or subqueries may contain their own single use of the operator as well.

As an example of SQL clause operators, let's revisit the example model from the [User-defined Variables](#user-defined-variables) section above.

As written, the model will always include the `WHERE` clause. We could make its presence dynamic by using the `@WHERE` macro operator:

```sql linenums="1"
MODEL (
  name sqlmesh_example.full_model,
  kind FULL,
  cron '@daily',
  audits (assert_positive_order_ids),
);

@DEF(size, 1);

SELECT
  item_id,
  count(distinct id) AS num_orders,
FROM
    sqlmesh_example.incremental_model
@WHERE(TRUE) item_id > @size
GROUP BY item_id
```

The `@WHERE` argument is set to `TRUE`, so the WHERE code is included in the rendered model:

```sql linenums="1"
SELECT
  item_id,
  count(distinct id) AS num_orders,
FROM
    sqlmesh_example.incremental_model
WHERE item_id > 1
GROUP BY item_id
```

If the `@WHERE` argument were instead set to `FALSE` the `WHERE` clause would be omitted from the query.

These operators aren't too useful if the argument's value is hard-coded. Instead, the argument can consist of code executable by the SQLGlot SQL engine.

For example, the `WHERE` clause will be included in this query because 1 less than 2:

```sql linenums="1"
MODEL (
  name sqlmesh_example.full_model,
  kind FULL,
  cron '@daily',
  audits (assert_positive_order_ids),
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
  audits (assert_positive_order_ids),
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

The argument to `@WHERE` will be "1 < 2" as in the previous hard-coded example after the macro variables `left_number` and `right_number` are substituted in.

### SQL clause operator examples

This section provides brief examples of each SQL clause operator's usage.

The examples use variants of this simple select statement:

```sql linenums="1"
SELECT *
FROM all_cities
```

#### @WITH operator

The `@WITH` operator is used to create [common table expressions](https://en.wikipedia.org/wiki/Hierarchical_and_recursive_queries_in_SQL#Common_table_expression), or "CTEs."

CTEs are typically used in place of derived tables (subqueries in the `FROM` clause) to make SQL code easier to read. Less commonly, recursive CTEs support analysis of hierarchical data with SQL.

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

#### @JOIN operator

The `@JOIN` operator specifies joins between tables or other SQL objects; it supports different join types (e.g., INNER, OUTER, CROSS, etc.).

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

The `@JOIN` operator recognizes that `LEFT OUTER` is a component of the `JOIN` specification and will omit it if the `@JOIN` argument evaluates to False.

#### @WHERE operator

The `@WHERE` operator adds a filtering `WHERE` clause(s) to the query when its argument evaluates to True.

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

#### @GROUP_BY operator

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

#### @HAVING operator

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

#### @ORDER_BY operator

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

## User-defined macro functions

User-defined macro functions allow the same macro code to be used in multiple models.

SQLMesh supports user-defined macro functions written in two languages - SQL and Python:

- SQL macro functions must use the [Jinja templating system](./jinja_macros.md#user-defined-macro-functions).
- Python macro functions use the SQLGlot library to allow more complex operations than macro variables and operators provide alone.

### Python macro functions

Python macro functions should be placed in `.py` files in the SQLMesh project's `macros` directory. Multiple functions can be defined in one `.py` file, or they can be distributed across multiple files.

An empty `__init__.py` file must be present in the SQLMesh project's `macros` directory. It will be created automatically when the project scaffold is created with `sqlmesh init`.

Each `.py` file containing a macro definition must import SQLMesh's `macro` decorator with `from sqlmesh import macro`.

Python macros are defined as regular python functions adorned with the SQLMesh `@macro()` decorator. The first argument to the function must be `evaluator`, which provides the macro evaluation context in which the macro function will run.

Python macros will parse all arguments with SQLGlot before they are used in the function body. Therefore, the function code exclusively processes SQLGlot expressions and may need to extract the expression's attributes/contents for use.

Python macro functions may return values of either `string` or SQLGlot `expression` types. SQLMesh will automatically parse returned strings into a SQLGlot expression after the function is executed so they can be incorporated into the model query's semantic representation.

Macro functions may return a list of strings or expressions that all play the same role in the query (e.g., specifying column definitions). For example, a list containing multiple `CASE WHEN` statements would be incorporated into the query properly, but a list containing both `CASE WHEN` statements and a `WHERE` clause would not.

#### Macro function basics

This example demonstrates the core requirements for defining a python macro - it takes no user-supplied arguments and returns the string `text`.

```python linenums="1"
from sqlmesh import macro

@macro() # Note parentheses at end of `@macro()` decorator
def print_text(evaluator):
  return 'text'
```

We could use this in a SQLMesh SQL model like this:

```sql linenums="1"
SELECT
  @print_text() as my_text
FROM table
```

After processing, it will render to this:

```sql linenums="1"
SELECT
  text as my_text
FROM table
```

Note that the python function returned a string `'text'`, but the rendered query uses `text` as a column name. That is due to the function's returned text being parsed as SQL code and integrated into the query's semantic representation.

The rendered query will treat `text` as a string if we double-quote the single-quoted value in the function definition as `"'text'"`:

```python linenums="1"
from sqlmesh import macro

@macro()
def print_text(evaluator):
  return "'text'"
```

When run in the same model query as before, this will render to:

```sql linenums="1"
SELECT
  'text' as my_text
FROM table
```

#### Returning more than one value

Macro functions are a convenient way to tidy model code by creating multiple outputs from one function call. Python macro functions do this by returning a list of strings or SQLGlot expressions.

For example, we might want to create indicator variables from the values in a string column. We can do that by passing in the name of column and a list of values for which it should create indicators, which we then interpolate into `CASE WHEN` statements.

Because SQLMesh parses the input objects, they become SQLGLot expressions in the function body. Therefore, the function code **cannot** treat the input list as a regular Python list.

Two things will happen to the input Python list before the function code is executed:

1. Each of its entries will be parsed by SQLGlot. Different inputs are parsed into different SQLGlot expressions:
    - Numbers are parsed into [`Literal` expressions](https://sqlglot.com/sqlglot/expressions.html#Literal)
    - Quoted strings are parsed into [`Literal` expressions](https://sqlglot.com/sqlglot/expressions.html#Literal)
    - Unquoted strings are parsed into [`Column` expressions](https://sqlglot.com/sqlglot/expressions.html#Column)

2. The parsed entries will be contained in a SQLGlot [`Array` expression](https://sqlglot.com/sqlglot/expressions.html#Array), the SQL entity analogous to a Python list

Because the input  `Array` expression named `values` is not a Python list, we cannot iterate over it directly - instead, we iterate over its `expressions` attribute with `values.expressions`:

```python linenums="1"
from sqlmesh import macro

@macro()
def make_indicators(evaluator, string_column, values):
  cases = []

  for value in values.expressions: # Iterate over `values.expressions`
    cases.append(f"CASE WHEN {string_column} = '{value}' THEN '{value}' ELSE NULL END AS {string_column}_{value}")

  return cases
```

We call this function in a model query to create `CASE WHEN` statements for the `vehicle` column values `truck` and `bus` like this:

```sql linenums="1"
SELECT
  @make_indicators(vehicle, [truck, bus])
FROM table
```

Which renders to:

```sql linenums="1"
SELECT
  CASE WHEN vehicle = 'truck' THEN 'truck' ELSE NULL END AS vehicle_truck,
  CASE WHEN vehicle = 'bus' THEN 'bus' ELSE NULL END AS vehicle_bus,
FROM table
```

Note that in the call `@make_indicators(vehicle, [truck, bus])` none of the three values is quoted.

Because they are unquoted, SQLGlot will parse them all as `Column` expressions. In the places we used single quotes when building the string (`'{value}'`), they will be single-quoted in the output. In the places we did not quote them (`{string_column} = ` and `{string_column}_{value}`), they will not.

#### Accessing macro variable values

Both predefined and user-defined macro variables can be accessed within a Python macro function.

The first argument to every macro function, the macro evaluation context `evaluator`, contains macro variable values in its `locals` attribute. `evaluator.locals` is a dictionary whose key:value pairs are macro variables names and the associated values.

For example, a function could access the predefined `execution_epoch` variable containing the epoch timestamp of when the execution started.

```python linenums="1"
from sqlmesh import macro

@macro()
def get_execution_epoch(evaluator):
    return evaluator.locals['execution_epoch']
```

The function would return the `execution_epoch` value when called in a model query:

```sql linenums="1"
SELECT
  @get_execution_epoch() as execution_epoch
FROM table
```

The same approach works for user-defined macro variables, where the key `"execution_epoch"` would be replaced with the name of the user-defined variable to be accessed.

One downside of that approach to accessing user-defined variables is that the name of the variable is hard-coded into the function. A more flexible approach is to pass the name of the macro variable as a function argument:

```python linenums="1"
from sqlmesh import macro

@macro()
def get_macro_var(evaluator, macro_var):
    return evaluator.locals[macro_var]
```

We could define a macro variable `my_macro_var` with a value of 1 and pass it to the `get_macro_var` function like this:

```sql linenums="1"
MODEL (...);

@DEF(my_macro_var, 1); -- Define macro variable 'my_macro_var'

SELECT
  @get_macro_var('my_macro_var') as macro_var_value -- Access my_macro_var value from Python macro function
FROM table
```

The model query would render to:

```sql linenums="1"
SELECT
  1 as macro_var_value
FROM table
```

#### Using SQLGlot expressions

SQLMesh automatically parses strings returned by Python macro functions into [SQLGlot](https://github.com/tobymao/sqlglot) expressions so they can be incorporated into the model query's semantic representation. Functions can also return SQLGlot expressions directly.

For example, consider a macro function that uses the `BETWEEN` operator in the predicate of a `WHERE` clause. A function returning the predicate as a string might look like this, where the function arguments are substituted into a Python f-string:

```python linenums="1"
from sqlmesh import macro

@macro()
def between_where(evaluator, column_name, low_val, high_val):
    return f"{column_name} BETWEEN {low_val} AND {high_val}"
```

The function could then be called in a query:

```sql linenums="1"
SELECT
  a
FROM table
WHERE @between_where(a, 1, 3)
```

And it would render to:

```sql linenums="1"
SELECT
  a
FROM table
WHERE a BETWEEN 1 and 3
```

Alternatively, the function could return a [SQLGLot expression](https://github.com/tobymao/sqlglot/blob/main/sqlglot/expressions.py) equivalent to that string by using SQLGlot's expression methods for building semantic representations:

```python linenums="1"
from sqlmesh import macro

@macro()
def between_where(evaluator, column, low_val, high_val):
    return column.between(low_val, high_val)
```

The methods are available because the `column` argument is parsed as a SQLGlot [Column expression](https://sqlglot.com/sqlglot/expressions.html#Column) when the macro function is executed.

Column expressions are sub-classes of the [Condition class](https://sqlglot.com/sqlglot/expressions.html#Condition), so they have builder methods like [`between`](https://sqlglot.com/sqlglot/expressions.html#Condition.between) and [`like`](https://sqlglot.com/sqlglot/expressions.html#Condition.like).

#### Accessing model schemas

Model schemas can be accessed within a Python macro function through its evaluation context's `column_to_types` method, when they can be statically determined. For instance, a schema of an [external model](../models/external_models.md) can be accessed only after the `sqlmesh create_external_models` command has been executed.

As an example, consider the following macro function which aims to rename the columns of a target model by adding a prefix to them:

```python linenums="1"
from sqlglot import exp
from sqlmesh.core.macros import macro

@macro()
def prefix_columns(evaluator, model_name, prefix):
    prefix = prefix.name
    renamed_projections = []

    # The following converts `model_name`, which is a SQLGlot expression, into a lookup key,
    # assuming that it does not contain quotes. If it did, we would have to generate SQL for
    # each part of `model_name` separately and then concatenate these parts, because in that
    # case `model_name.sql()` would produce an invalid lookup key.
    model_name_sql = model_name.sql()

    for name in evaluator.columns_to_types(model_name_sql):
        new_name = prefix + name
        renamed_projections.append(exp.column(name).as_(new_name))

    return renamed_projections
```

This can then be used in a SQL model as shown below:

```sql linenums="1"
MODEL (
  name schema.child,
  kind FULL
);

SELECT
  @prefix_columns(schema.parent, 'stg_')
FROM
  schema.parent
```

Note that `columns_to_types` expects an _unquoted model name_, such as `schema.parent`. Since macro arguments are SQLGlot expressions, they need to be processed accordingly in order to extract meaningful information from them. For instance, the lookup key in the above macro definition is extracted by generating the SQL code for `model_name` using the `sql` method.

Having access to the schema of an upstream model can be useful for various reasons:

- Renaming columns so that downstream consumers are not tightly coupled to external or source tables
- Selecting only a subset of columns that satisfy some criteria (e.g. columns whose names start with a specific prefix)
- Applying transformations to columns, such as masking PII or computing various statistics based on the column types

Thus, leveraging `columns_to_types` can also enable one to write code according to the [DRY](https://en.wikipedia.org/wiki/Don%27t_repeat_yourself) principle, as they can implement these transformations in a single function instead of duplicating them in each model of interest.

### Accessing snapshots

After a SQLMesh project has been successfully loaded, its snapshots can be accessed in Python macro functions and Python models that generate SQL through the `get_snapshot` method of `MacroEvaluator`.

This enables the inspection of physical table names or the processed intervals for certain snapshots at runtime, as shown in the example below:

```python linenums="1"
from sqlmesh.core.macros import macro

@macro()
def some_macro(evaluator):
    if evaluator.runtime_stage == "evaluating":
        # Check the intervals a snapshot has data for and alter the behavior of the macro accordingly
        intervals = evaluator.get_snapshot("some_model_name").intervals
        ...
    ...
```

## Typed Macros

Typed macros in SQLMesh bring the power of type hints from Python, enhancing readability, maintainability, and usability of your SQL macros. These macros enable developers to specify expected types for arguments, making the macros more intuitive and less error-prone.

### Benefits of Typed Macros

1. **Improved Readability**: By specifying types, the intent of the macro is clearer to other developers or future you.
2. **Reduced Boilerplate**: No need for manual type conversion within the macro function, allowing you to focus on the core logic.
3. **Enhanced Autocompletion**: IDEs can provide better autocompletion and documentation based on the specified types.

### Defining a Typed Macro

Typed macros in SQLMesh use Python's type hints. Here's a simple example of a typed macro that repeats a string a given number of times:

```python linenums="1"
from sqlmesh import macro

@macro()
def repeat_string(evaluator, text: str, count: int) -> str:
    return text * count
```

Usage in SQLMesh:

```sql linenums="1"
SELECT
  @repeat_string('SQLMesh ', 3) as repeated_string
FROM some_table;
```

This macro takes two arguments: `text` of type `str` and `count` of type `int`, and it returns a string. Without type hints, the inputs to the macro would have been two `exp.Literal` objects you would have had to convert to strings and integers manually.

### Supported Types

SQLMesh supports common Python types for typed macros including:

- `str`
- `int`
- `float`
- `bool`
- `List[T]` - where `T` is any supported type including sqlglot expressions
- `Tuple[T]` - where `T` is any supported type including sqlglot expressions
- `Union[T1, T2, ...]` - where `T1`, `T2`, etc. are any supported types including sqlglot expressions

We also support SQLGlot expressions as type hints, allowing you to ensure inputs are coerced to the desired SQL AST node your intending on working with. Some useful examples include:

- `exp.Table`
- `exp.Column`
- `exp.Literal`
- `exp.Identifier`

While these might be obvious examples, you can effectively coerce an input into _any_ SQLGlot expression type, which can be useful for more complex macros. When coercing to more complex types, you will almost certainly need to pass a string literal since expression to expression coercion is limited. When a string literal is passed to a macro that hints at a SQLGlot expression, the string will be parsed using SQLGlot and coerced to the correct type. Failure to coerce to the correct type will result in the original expression being passed to the macro and a warning being logged for the user to address as-needed.

```python linenums="1"
@macro()
def stamped(evaluator, query: exp.Select) -> exp.Subquery:
    return query.select(exp.Literal.string(str(datetime.now())).as_("stamp")).subquery()

# Coercing to a complex node like `exp.Select` works as expected given a string literal input
# SELECT * FROM @stamped('SELECT a, b, c')
```

When coercion fails, there will always be a warning logged but we will not crash. We believe the macro should be flexible by default, meaning the default behavior is preserved if we cannot coerce. Give that, the user can express whatever level of additional checks they want. For example, if you would like to raise an error when the coercion fails, you can use an `assert` statement. For example:

```python linenums="1"
@macro()
def my_macro(evaluator, table: exp.Table) -> exp.Column:
    assert isinstance(table, exp.Table)
    table.set("catalog", "dev")
    return table

# Works
# SELECT * FROM @my_macro('some.table')
# SELECT * FROM @my_macro(some.table)

# Raises an error thanks to the users inclusion of the assert, otherwise would pass through the string literal and log a warning
# SELECT * FROM @my_macro('SELECT 1 + 1')
```

In using assert this way, you still get the benefits of reducing/removing the boilerplate needed to coerce types; but you **also** get guarantees about the type of the input. This is a useful pattern and is user-defined, so you can use it as you see fit. It ultimately allows you to keep the macro definition clean and focused on the core business logic.

### Advanced Typed Macros

You can create more complex macros using advanced Python features like generics. For example, a macro that accepts a list of integers and returns their sum:

```python linenums="1"
from typing import List
from sqlmesh import macro

@macro()
def sum_integers(evaluator, numbers: List[int]) -> int:
    return sum(numbers)
```

Usage in SQLMesh:

```sql linenums="1"
SELECT
  @sum_integers([1, 2, 3, 4, 5]) as total
FROM some_table;
```

Generics can be nested and are resolved recursively allowing for fairly robust type hinting.

See examples of the coercion function in action in the test suite [here](../../../tests/core/test_macros.py).

### Conclusion

Typed macros in SQLMesh not only enhance the development experience by making macros more readable and easier to use but also contribute to more robust and maintainable code. By leveraging Python's type hinting system, developers can create powerful and intuitive macros for their SQL queries, further bridging the gap between SQL and Python.

## Mixing macro systems

SQLMesh supports both SQLMesh and [Jinja](./jinja_macros.md) macro systems. We strongly recommend using only one system in a model - if both are present, they may fail or behave in unintuitive ways.
