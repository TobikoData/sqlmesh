# Python models

Although SQL is a powerful tool, there are some use cases that are better handled by Python. For example pipelines that involve machine learning, interacting with external APIs, and complex business logic that cannot be expressed in SQL. SQLMesh has first class support for models defined in Python. There are no restrictions on what can be done in the Python model implementation as long as it returns a Pandas or a Spark DataFrame instance.

## Definition

To create a Python model add a new file with the `*.py` extension to the `models/` directory. Inside the file, define a function named `execute` as in the following example:

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
    latest: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
```

The `execute` function is wrapped with the `model` [decorator](https://wiki.python.org/moin/PythonDecorators) which is used to capture model's metadata, similarly to the `MODEL` statement in [SQL models](#sql_models.md). The function itself takes in an Execution context which can be used to run SQL queries, retrieve the current time interval that is being processed, as well as arbitrary key-value arguments passed in at runtime. You can either return a Pandas or a PySpark Dataframe instance.

Because SQLMesh creates tables before evaluating models, the schema of the output dataframe is a required argument. The argument `columns` is a dictionary of column name to type.

If the output is too large, it can also be returned in chunks using Python generators.

## Execution Context
Python models can do anything you want, although it is strongly recommended for all models to be [idempotent](../../glossary/#idempotency). Python models can easily fetch data from upstream models or even data outside of SQLMesh. Given an execution context, you can fetch a dataframe with `fetchdf`.

```python linenums="1"
df = context.fetchdf("SELECT * FROM my_table")
```

## Dependencies
In order to fetch data from an upstream model, it's required to get the table name using the `table` method. This returns the appropriate table name given the current runtime [environment](../../environments).

```python linenums="1"
table = context.table("upstream_model")
df = context.fetchdf(f"SELECT * FROM {table}")
```

Using `table` will automatically add the referenced model to the Python model's dependencies. The only other way to set dependencies of models in Python models is to define them explicitly in the decorator using the keyword `dependencies`.

```python linenums="1"
@model(
    "my_model.with_explicit_dependencies",
    dependencies=["upstream_dependency"],
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    latest: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    context.table("another_dependency")
```

The dependencies defined in the model decorator take precedence over any dynamic references inside the function. Therefore, in the example above, only `upstream_dependency` will be captured while `another_dependency` will be ignored.


## Examples
### Basic
The following is simple example of a Python model returning a static Pandas dataframe. Note that all of the [meta-data](../overview#properties) fields are the same is in SQL.

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
    latest: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    return pd.DataFrame([
        {"id": 1, "name": "name"}
    ])
```

### SQL Query and Pandas
The following is a more complex example that queries an upstream model and then outputs a Pandas dataframe.

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
    latest: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    # get the upstream model's name and register it as a dependency
    table = context.table("upstream.model")

    # fetch data from the model as a pandas dataframe
    # if the engine is spark, this would return a spark dataframe
    df = context.fetchdf(f"SELECT id, name FROM {table}")

    # do some pandas stuff
    df[id] += 1
    return df
```

### PySpark
This example shows using the PySpark dataframe API. If you use Spark, this is preferred over Pandas because you're able to computation in a distributed fashion.

```python
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
    latest: datetime,
    **kwargs: t.Any,
) -> DataFrame:
    # get the upstream model's name and register it as a dependency
    table = context.table("upstream.model")

    # use the spark dataframe api to add the country column
    df = context.spark.table(table).withColumn("country", functions.lit("USA"))

    # returns the pyspark dataframe directly, this means no data is computed locally
    return df
```

### Batching
If the output of a Python model is very large and you cannot use Spark, it may be required to split up the upload into multiple batches. With Pandas or other single machine dataframe libraries, all data is stored in memory. Instead of returning a single DataFrame instance, you can return multiple instances using Python generator API to minimize the memory footprint by reducing the size of data that is loaded into memory at any given point.

```
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
    latest: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    # get the upstream model's table name
    table = context.table("upstream.model")

    for i in range(3):
        # run 3 queries to get chunks of data to not run out of memory
        df = context.fetchdf(f"SELECT id from {table} WHERE id = {i}")
        yield df
```

## Serialization
SQLMesh executes Python code locally to where SQLMesh is running. It leverages a custom [serialization framework](../../architecture/serialization).
