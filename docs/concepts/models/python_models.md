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

The function takes an `ExecutionContext` that is able to run queries and to retrieve the current time interval that is being processed, along with arbitrary key-value arguments passed in at runtime. The function can either return a Pandas or PySpark Dataframe instance.

If the function output is too large, it can also be returned in chunks using Python generators.

## Execution context
Python models can do anything you want, but it is strongly recommended for all models to be [idempotent](../glossary.md#idempotency). Python models can fetch data from upstream models or even data outside of SQLMesh. 

Given an execution `ExecutionContext` "context", you can fetch a DataFrame with the `fetchdf` method:

```python linenums="1"
df = context.fetchdf("SELECT * FROM my_table")
```

## Dependencies
In order to fetch data from an upstream model, you first get the table name using `context`'s `table` method. This returns the appropriate table name for the current runtime [environment](../environments.md):

```python linenums="1"
table = context.table("upstream_model")
df = context.fetchdf(f"SELECT * FROM {table}")
```

The `table` method will automatically add the referenced model to the Python model's dependencies. 

The only other way to set dependencies of models in Python models is to define them explicitly in the `@model` decorator using the keyword `dependencies`. The dependencies defined in the model decorator take precedence over any dynamic references inside the function. 

In this example, only `upstream_dependency` will be captured, while `another_dependency` will be ignored:

```python linenums="1"
@model(
    "my_model.with_explicit_dependencies",
    dependencies=["upstream_dependency"], # captured
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:

    # ignored due to @model dependency "upstream_dependency"
    context.table("another_dependency") 
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
    "basic",
    owner="janet",
    cron="@daily",
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
    "sql_pandas",
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
    "pyspark",
    columns={
        "id": "int",
        "name": "text",
        "country": "text",
    },
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime
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

### Batching
If the output of a Python model is very large and you cannot use Spark, it may be helpful to split the output into multiple batches. 

With Pandas or other single machine DataFrame libraries, all data is stored in memory. Instead of returning a single DataFrame instance, you can return multiple instances using the Python generator API. This minimizes the memory footprint by reducing the size of data loaded into memory at any given time.

This examples uses the Python generator `yield` to batch the model output:

```python linenums="1" hl_lines="20"
@model(
    "batching",
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
