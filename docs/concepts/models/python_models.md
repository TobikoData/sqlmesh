# Python models

Although SQL is a powerful tool, there are some use cases that are better handled by Python. For example pipelines that involve machine learning, interacting with external APIs, and complex business logic that cannot be expressed in SQL. SQLMesh has first class support for models defined in Python. There are no restrictions on what can be done in the Python model implementation as long as it returns a Pandas or a Spark DataFrame instance.

## Definition

To create a Python model add a new file with the `*.py` extension to the `models/` directory. Inside the file, define a function named `execute` as in the following example:

```python
import typing as t
from datetime import datetime

from sqlmesh import ExecutionContext, model

@model(
    "my_model.name",
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

If the output is too large, it can also be returned in chunks using Python generators:

## Execution Context
Python models can do anything you want, although it is strongly recommended for all models to be [idempotent](../../glossary/#idempotency). Python models can easily fetch data from upstream models or even data outside of SQLMesh. Given an execution context, you can fetch a dataframe with `fetchdf`.

```python
df = context.fetchdf("SELECT * FROM my_table")
```

## Dependencies
In order to fetch data from an upstream model, it's required to get the table name using the `table` method. This returns the appropriate table name given the current runtime [environment](../../environments).

```python
table = context.table("upstream_model")
df = context.fetchdf(f"SELECT * FROM {table}")
```

Using `table` will automatically add the referenced model to the Python model's dependencies. The only other way to set dependencies of models in Python models is to define them explicitly in the decorator using the keyword `dependencies`.

```
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
    context.table("another dependency")
```

The dependencies defined in the model decorator take precedence over any dynamic references inside the function. Therefore, in the example above, only `upstream_dependency` will be captured while `another_dependency` will be ignored.

## Batching
If the result of a Python model is very large, it may be required to split up the upload into multiple batches. With PySpark, this won't be a problem because all computation is done in a distributed fashion, but with Pandas, all data is stored in memory. Instead of returning a single dataframe, you can yield dataframes to minimize the amount of data needed to be processed at once.

```
@model(
    "my_model.with_batching",
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    latest: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    ...

    for _ in range(3):
        yield df
```


## Example
The following is an example of a Python model. Note that all of the [meta-data](../overview#properties) fields are the same is in SQL.

```python
import random
import typing as t
from datetime import datetime

import numpy as np
import pandas as pd
from sqlglot.expressions import to_column

from examples.sushi.helper import iter_dates
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model import IncrementalByTimeRangeKind
from sqlmesh.utils.date import to_ds

ITEMS = [
    "Ahi",
    "Aji",
    "Amaebi",
    "Anago",
    "Aoyagi",
    "Bincho",
    "Katsuo",
    "Ebi",
    "Escolar",
    "Hamachi",
    "Hamachi Toro",
    "Hirame",
    "Hokigai",
    "Hotate",
    "Ika",
    "Ikura",
    "Iwashi",
    "Kani",
    "Kanpachi",
    "Maguro",
    "Saba",
    "Sake",
    "Sake Toro",
    "Tai",
    "Tako",
    "Tamago",
    "Tobiko",
    "Toro",
    "Tsubugai",
    "Umi Masu",
    "Unagi",
    "Uni",
]


@model(
    "sushi.items",
    kind=IncrementalByTimeRangeKind(time_column="ds"),
    start="Jan 1 2022",
    cron="@daily",
    batch_size=30,
    columns={
        "id": "int",
        "name": "text",
        "price": "double",
        "ds": "text",
    },
    audits=[
        ("accepted_values", {"column": to_column("name"), "values": ITEMS}),
        ("not_null", {"columns": [to_column("name"), to_column("price")]}),
        ("assert_items_price_exceeds_threshold", {"price": 0}),
    ],
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    latest: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    dfs = []
    for dt in iter_dates(start, end):
        num_items = random.randint(10, len(ITEMS))
        dfs.append(
            pd.DataFrame(
                {
                    "name": random.sample(ITEMS, num_items),
                    "price": np.random.uniform(3.0, 10.0, size=num_items).round(2),
                    "ds": to_ds(dt),
                }
            )
            .reset_index()
            .rename(columns={"index": "id"})
        )

    return pd.concat(dfs)
```

## Serialization
SQLMesh executes Python code locally to where SQLMesh is running. It leverages a custom [serialization framework](../../architecture/serialization).
