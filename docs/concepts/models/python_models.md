# Python models

Although SQL is a powerful tool, some use cases are better handled by Python. For example, Python may be a better option in pipelines that involve machine learning, interacting with external APIs, or complex business logic that cannot be expressed in SQL.

SQLMesh has first-class support for models defined in Python; there are no restrictions on what can be done in the Python model as long as it returns a Pandas or Spark DataFrame instance.

## Definition

To create a Python model, add a new file with the `*.py` extension to the `models/` directory. Inside the file, define a function named `execute`. For example:

```python linenums="1"
import typing as t
from datetime import datetime

from sqlmesh import ExecutionContext, model

@model(
    "my_model.name",
    columns={
        "column_name": "int",
    },
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
```

The `execute` function is wrapped with the `@model` [decorator](https://wiki.python.org/moin/PythonDecorators), which is used to capture the model's metadata (similar to the `MODEL` DDL statement in [SQL models](./sql_models.md)).

Because SQLMesh creates tables before evaluating models, the schema of the output DataFrame is a required argument. The `@model` argument `columns` contains a dictionary of column names to types.

The function takes an `ExecutionContext` that is able to run queries and to retrieve the current time interval that is being processed, along with arbitrary key-value arguments passed in at runtime. The function can either return a Pandas, PySpark, or Snowpark Dataframe instance.

If the function output is too large, it can also be returned in chunks using Python generators.

## `@model` specification

The arguments provided in the `@model` specification have the same names as those provided in a SQL model's `MODEL` DDL.

Python model `kind`s are specified with a Python dictionary containing the kind's name and arguments. All model kind arguments are listed in the [models configuration reference page](../../reference/model_configuration.md#model-kind-properties).

The model `kind` dictionary must contain a `name` key whose value is a member of the [`ModelKindName` enum class](https://sqlmesh.readthedocs.io/en/stable/_readthedocs/html/sqlmesh/core/model/kind.html#ModelKindName). The `ModelKindName` class must be imported at the beginning of the model definition file before being used in the `@model` specification.

Supported `kind` dictionary `name` values are:

- `ModelKindName.VIEW`
- `ModelKindName.FULL`
- `ModelKindName.SEED`
- `ModelKindName.INCREMENTAL_BY_TIME_RANGE`
- `ModelKindName.INCREMENTAL_BY_UNIQUE_KEY`
- `ModelKindName.INCREMENTAL_BY_PARTITION`
- `ModelKindName.SCD_TYPE_2_BY_TIME`
- `ModelKindName.SCD_TYPE_2_BY_COLUMN`
- `ModelKindName.EMBEDDED`
- `ModelKindName.CUSTOM`
- `ModelKindName.MANAGED`
- `ModelKindName.EXTERNAL`

This example demonstrates how to specify an incremental by time range model kind in Python:

```python linenums="1"
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName

@model(
    "docs_example.incremental_model",
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_TIME_RANGE,
        time_column="model_time_column"
    )
)
```

## Execution context
Python models can do anything you want, but it is strongly recommended for all models to be [idempotent](../glossary.md#idempotency). Python models can fetch data from upstream models or even data outside of SQLMesh.

Given an execution `ExecutionContext` "context", you can fetch a DataFrame with the `fetchdf` method:

```python linenums="1"
df = context.fetchdf("SELECT * FROM my_table")
```

## Optional pre/post-statements

Optional pre/post-statements allow you to execute SQL commands before and after a model runs, respectively.

For example, pre/post-statements might modify settings or create indexes. However, be careful not to run any statement that could conflict with the execution of another statement if models run concurrently, such as creating a physical table.

Pre- and post-statements are issued with the SQLMesh [`fetchdf` method](../../reference/cli.md#fetchdf) [described above](#execution-context).

Pre-statements may be specified anywhere in the function body before it `return`s or `yield`s. Post-statements must execute after the function completes, so instead of `return`ing a value the function must `yield` the value. The post-statement must be specified after the `yield`.

This example function includes both pre- and post-statements:

``` python linenums="1" hl_lines="9-10 12 17-18"
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:

    # pre-statement
    context.fetchdf("SET GLOBAL parameter = 'value';")

    # post-statement requires using `yield` instead of `return`
    yield pd.DataFrame([
        {"id": 1, "name": "name"}
    ])

    # post-statement
    context.fetchdf("CREATE INDEX idx ON example.pre_post_statements (id);")
```


## Dependencies
In order to fetch data from an upstream model, you first get the table name using `context`'s `table` method. This returns the appropriate table name for the current runtime [environment](../environments.md):

```python linenums="1"
table = context.table("docs_example.upstream_model")
df = context.fetchdf(f"SELECT * FROM {table}")
```

The `table` method will automatically add the referenced model to the Python model's dependencies.

The only other way to set dependencies of models in Python models is to define them explicitly in the `@model` decorator using the keyword `depends_on`. The dependencies defined in the model decorator take precedence over any dynamic references inside the function.

In this example, only `upstream_dependency` will be captured, while `another_dependency` will be ignored:

```python linenums="1"
@model(
    "my_model.with_explicit_dependencies",
    depends_on=["docs_example.upstream_dependency"], # captured
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:

    # ignored due to @model dependency "upstream_dependency"
    context.table("docs_example.another_dependency")
```


## Global variables

[User-defined global variables](../../reference/configuration.md#variables) can be accessed from within the Python model using function arguments, where the name of the argument represents a variable key. For example:

```python linenums="1" hl_lines="9"
@model(
    "my_model.name",
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    my_var: Optional[str] = None,
    **kwargs: t.Any,
) -> pd.DataFrame:
    ...
```

Make sure to assign a default value to such arguments if you anticipate a missing variable key. Please note that arguments must be specified explicitly; in other words, variables can be accessed using `kwargs`.

Alternatively, variables can be accessed using the `context.var` method. For example:
```python linenums="1" hl_lines="11 12"
@model(
    "my_model.name",
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    var_value = context.var("<var_name>")
    another_var_value = context.var("<another_var_name>", "default_value")
    ...
```
## Examples
### Basic
The following is an example of a Python model returning a static Pandas DataFrame.

**Note:** All of the [metadata](./overview.md#properties) field names are the same as those in the SQL `MODEL` DDL.

```python linenums="1"
import typing as t
from datetime import datetime

import pandas as pd
from sqlmesh import ExecutionContext, model

@model(
    "docs_example.basic",
    owner="janet",
    cron="@daily",
    columns={
        "id": "int",
        "name": "text",
    },
    column_descriptions={
        "id": "Unique ID",
        "name": "Name corresponding to the ID",
    },
    audits=[
        ("not_null", {"columns": ["id"]}),
    ],
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:

    return pd.DataFrame([
        {"id": 1, "name": "name"}
    ])
```

### SQL Query and Pandas
The following is a more complex example that queries an upstream model and outputs a Pandas DataFrame:

```python linenums="1"
import typing as t
from datetime import datetime

import pandas as pd
from sqlmesh import ExecutionContext, model

@model(
    "docs_example.sql_pandas",
    columns={
        "id": "int",
        "name": "text",
    },
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    # get the upstream model's name and register it as a dependency
    table = context.table("upstream_model")

    # fetch data from the model as a pandas DataFrame
    # if the engine is spark, this returns a spark DataFrame
    df = context.fetchdf(f"SELECT id, name FROM {table}")

    # do some pandas stuff
    df[id] += 1
    return df
```

### PySpark
This example demonstrates using the PySpark DataFrame API. If you use Spark, the DataFrame API is preferred to Pandas since it allows you to compute in a distributed fashion.

```python linenums="1"
import typing as t
from datetime import datetime

import pandas as pd
from pyspark.sql import DataFrame, functions

from sqlmesh import ExecutionContext, model

@model(
    "docs_example.pyspark",
    columns={
        "id": "int",
        "name": "text",
        "country": "text",
    },
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> DataFrame:
    # get the upstream model's name and register it as a dependency
    table = context.table("upstream_model")

    # use the spark DataFrame api to add the country column
    df = context.spark.table(table).withColumn("country", functions.lit("USA"))

    # returns the pyspark DataFrame directly, so no data is computed locally
    return df
```


### Snowpark
This example demonstrates using the Snowpark DataFrame API. If you use Snowflake, the DataFrame API is preferred to Pandas since it allows you to compute in a distributed fashion.

```python linenums="1"
import typing as t
from datetime import datetime

import pandas as pd
from snowflake.snowpark.dataframe import DataFrame

from sqlmesh import ExecutionContext, model

@model(
    "docs_example.snowpark",
    columns={
        "id": "int",
        "name": "text",
        "country": "text",
    },
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> DataFrame:
    # returns the snowpark DataFrame directly, so no data is computed locally
    df = context.snowpark.create_dataframe([[1, "a", "usa"], [2, "b", "cad"]], schema=["id", "name", "country"])
    df = df.filter(df.id > 1)
    return df
```

### Batching
If the output of a Python model is very large and you cannot use Spark, it may be helpful to split the output into multiple batches.

With Pandas or other single machine DataFrame libraries, all data is stored in memory. Instead of returning a single DataFrame instance, you can return multiple instances using the Python generator API. This minimizes the memory footprint by reducing the size of data loaded into memory at any given time.

This examples uses the Python generator `yield` to batch the model output:

```python linenums="1" hl_lines="20"
@model(
    "docs_example.batching",
    columns={
        "id": "int",
    },
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    # get the upstream model's table name
    table = context.table("upstream_model")

    for i in range(3):
        # run 3 queries to get chunks of data and not run out of memory
        df = context.fetchdf(f"SELECT id from {table} WHERE id = {i}")
        yield df
```

## Serialization
SQLMesh executes Python code locally where SQLMesh is running by using our custom [serialization framework](../architecture/serialization.md).
