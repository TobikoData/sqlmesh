# Auditing
Audits are one of the tools SQLMesh provides to validate your models. Along with [tests](tests.md), they are a great way to ensure the quality of your data and to build trust in it across your organization. Unlike tests, audits are used to validate the output of a model after every evaluation.

A comprehensive suite of audits can identify data issues upstream, whether they are from your vendors or other teams. Audits also empower your data engineers and analysts to work with confidence by catching problems early as they work on new features or make updates to your models.

## Example audit
In SQLMesh, audits are defined in `.sql` files in an `audit` directory in your SQLMesh project. Multiple audits can be defined in a single file, so you can organize them to your liking. Audits are SQL queries that should not return any rows; in other words, they query for bad data, so returned rows indicates that something is wrong. In its simplest form, an audit is defined with the custom AUDIT expression along with a query, as in the following example:

```sql linenums="1"
AUDIT (
  name assert_item_price_is_not_null,
  dialect spark
);
SELECT * from sushi.items
WHERE ds BETWEEN @start_ds AND @end_ds AND
   price IS NULL;
```

In the example, we defined an audit named `assert_item_price_is_not_null`, ensuring that every sushi item has a price.

**Note:** If the query is in a different dialect than the rest of your project, you can specify it here as we did in the example, and SQLGlot will automatically understand how to execute the query.

In order for this audit to take effect it should first be included in the target model's definition:
```sql linenums="1"
MODEL (
  name sushi.items,
  audits (
    assert_item_price_is_not_null()
  )
);
```
Now the `assert_item_price_is_not_null` will run every time the `sushi.items` model is evaluated.

## Generic audits
Audits can also be parameterized and implemented in a model-agnostic way.

As an example consider the following audit definition which checks whether the target column exceeds a configured threshold:
```sql linenums="1"
AUDIT (
  name does_not_exceed_threshold
);
SELECT * FROM @this_model
WHERE @column >= @threshold;
```
In the example above we utilized [macros](macros.md) to parameterize the audit implementation. `@this_model` is a special macro which refers to a model that is being audited. For incremental models, this macro also ensures that only relevant data intervals are affected. `@column` and `@threshold` are generic parameters, values for which are set in the model definition.

The generic audit can now be applied to a model by being referenced in its definition:
```sql linenums="1"
MODEL (
  name sushi.items,
  audits (
    does_not_exceed_threshold(column=id, threshold=1000),
    does_not_exceed_threshold(column=price, threshold=100)
  )
);
```
Notice how `column` and `threshold` parameters have been set at this point. These values will later be propagated into the audit query and returned by the `@column` and `@threshold` macros accordingly.

Note that the same audit can be applied more than once to the same model with different sets of parameters.

## Built-in audits
SQLMesh comes with a suite of built-in generic audits which covers a broad set of common use cases.

### not_null
Ensures that specified columns are not null.

Example:
```sql linenums="1"
MODEL (
  name sushi.orders,
  audits (
    not_null(columns=[id, customer_id, waiter_id])
  )
);
```

### unique_values
Ensures that provided columns only contain unique values.

Example:
```sql linenums="1"
MODEL (
  name sushi.orders,
  audits (
    unique_values(columns=[id])
  )
);
```

### accepted_values
Ensures that the value of the target column is one of the accepted values.

Example:
```sql linenums="1"
MODEL (
  name sushi.items,
  audits (
    accepted_values(column=name, values=['Hamachi', 'Unagi', 'Sake'])
  )
);
```

### number_of_rows
Ensures that the number of rows in the model's table exceeds the configured threshold. For incremental models, this check only applies to a data interval that is being evaluated, not to the entire table.

Example:
```sql linenums="1"
MODEL (
  name sushi.orders,
  audits (
    number_of_rows(threshold=10)
  )
);
```

## Running audits
### The CLI audit command

You can execute audits with the `sqlmesh audit` command as follows:
```bash
$ sqlmesh --path project audit -start 2022-01-01 -end 2022-01-02
Found 1 audit(s).
assert_item_price_is_not_null FAIL.

Finished with 1 audit error(s).

Failure in audit assert_item_price_is_not_null for model sushi.items (audits/items.sql).
Got 3 results, expected 0.
SELECT * FROM sqlmesh.sushi__items__1836721418_83893210 WHERE ds BETWEEN '2022-01-01' AND '2022-01-02' AND price IS NULL
Done.
```

### Automated auditing
When you apply a plan, SQLMesh will automatically run each model's audits. By default, SQLMesh will halt the pipeline when an audit fails in order to prevent potentially invalid data from propagating further downstream. This behavior can be changed for individual audits. Refer to [Non-blocking audits](#non-blocking-audits).

## Advanced usage
### Skipping audits
Audits can be skipped by setting the `skip` argument to `true` as in the following example:

```sql linenums="1" hl_lines="3"
AUDIT (
  name assert_item_price_is_not_null,
  skip true
);
SELECT * from sushi.items
WHERE ds BETWEEN @start_ds AND @end_ds AND
   price IS NULL;
```

### Non-blocking audits
By default, audits that fail will stop the execution of the pipeline in order to prevent bad data from propagating further. An audit can be configured to notify you when it fails without blocking the execution of the pipeline, as in the following example:

```sql linenums="1" hl_lines="3"
AUDIT (
  name assert_item_price_is_not_null,
  blocking false
);
SELECT * from sushi.items
WHERE ds BETWEEN @start_ds AND @end_ds AND
   price IS NULL;
```
