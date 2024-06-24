# Custom materializations guide

SQLMesh supports a variety of [model kinds](../concepts/models/model_kinds.md) to capture the most common semantics of how transformations can be evaluated and materialized.

There are times, however, when a specific use case doesn't align with any of the supported materialization strategies. For scenarios like this, SQLMesh allows users to create their own materialization implementation using Python.

Please note that this is an advanced feature and should only be considered if all other approaches to addressing a use case have been exhausted. If you're at this decision point, we recommend you reach out to our team in the community slack: [here](https://tobikodata.com/community.html)

## Creating a materialization

The fastest way to add a new custom materialization is to add a new `.py` file with the implementation to the `materializations/` folder of the project. SQLMesh will automatically import all Python modules in this folder at project load time and register the custom materializations accordingly.

To create a custom materialization strategy, you need to inherit the `CustomMaterialization` base class and, at a very minimum, provide an implementation for the `insert` method.

For example, a simple custom full-refresh materialization strategy might look like the following:

```python linenums="1"
from __future__ import annotations

import typing as t

from sqlmesh import CustomMaterialization, Model

if t.TYPE_CHECKING:
    from sqlmesh import QueryOrDF


class CustomFullMaterialization(CustomMaterialization):
    NAME = "my_custom_full"

    def insert(
        self,
        table_name: str,
        query_or_df: QueryOrDF,
        model: Model,
        is_first_insert: bool,
        **kwargs: t.Any,
    ) -> None:
        self.adapter.replace_query(table_name, query_or_df)

```

Let's unpack the above implementation:

* `NAME` - determines the name of the custom materialization. This name will be used in model definitions to reference a specific strategy. If not specified, the name of the class will be used instead.
* The `insert` method comes with the following arguments:
    * `table_name` - the name of a target table (or a view) into which the data should be inserted.
    * `query_or_df` - a query (a SQLGlot expression) or a DataFrame (pandas, PySpark, or Snowpark) instance which has to be inserted.
    * `model` - the associated model definition object which can be used to get any model parameters as well as custom materialization settings.
    * `is_first_insert` - whether this is the first insert for the current version of the model.
    * `kwargs` - contains additional and future arguments.
* The `self.adapter` instance is used to interact with the target engine. It comes with a set of useful high-level APIs like `replace_query`, `create_table`, and `table_exists`, but also supports execution of arbitrary SQL expressions with its `execute` method.

You can also control how the associated data objects (tables, views, etc.) are created and deleted by overriding the `create` and `delete` methods accordingly:

```python linenums="1"
from __future__ import annotations

import typing as t

from sqlmesh import CustomMaterialization, Model


class CustomFullMaterialization(CustomMaterialization):
    ...

    def create(
        self,
        table_name: str,
        model: Model,
        is_table_deployable: bool,
        **render_kwargs: t.Any,
    ) -> None:
        # Custom creation logic.

    def delete(self, name: str, **kwargs: t.Any) -> None:
        # Custom deletion logic.
```

## Using custom materializations in models

In order to use the newly created materialization, use the special model kind `CUSTOM`:

```sql linenums="1"
MODEL (
  name my_db.my_model,
  kind CUSTOM (materialization 'my_custom_full')
);
```

The name of the materialization strategy is provided in the `materialization` attribute of the `CUSTOM` kind.

Additionally, you can provide an optional list of arbitrary key-value pairs in the `materialization_properties` attribute:

```sql linenums="1"
MODEL (
  name my_db.my_model,
  kind CUSTOM (
    materialization 'my_custom_full',
    materialization_properties (
      'config_key' = 'config_value'
    )
  )
);
```

These properties can be accessed with the model reference within the materialization implementation:

```python linenums="1" hl_lines="12"
class CustomFullMaterialization(CustomMaterialization):
    NAME = "my_custom_full"

    def insert(
        self,
        table_name: str,
        query_or_df: QueryOrDF,
        model: Model,
        is_first_insert: bool,
        **kwargs: t.Any,
    ) -> None:
        config_value = model.custom_materialization_properties["config_key"]
        # Proceed with implementing the insertion logic.
        # Example for existing materialization for look and feel: https://github.com/TobikoData/sqlmesh/blob/main/sqlmesh/core/snapshot/evaluator.py
```

## Packaging custom materializations

To share custom materializations across multiple SQLMesh projects, you need to create and publish a Python package containing your implementation.

When using SQLMesh with Airflow or other external schedulers, note that the `materializations/` folder might not be available on the Airflow cluster side. Therefore, you'll need a package that can be installed there.

Custom materializations can be packaged into a Python package and exposed via [setuptools entrypoints](https://packaging.python.org/en/latest/guides/creating-and-discovering-plugins/#using-package-metadata) mechanism. Once the package is installed, SQLMesh will automatically load custom materializations from the entrypoint list.

If your custom materialization class is defined in the `my_package/my_materialization.py` module, you can expose it as an entry point in the `pyproject.toml` file as follows:

```toml
[project.entry-points."sqlmesh.materializations"]
my_materialization = "my_package.my_materialization:CustomFullMaterialization"
```

Or in `setup.py`:

```python
setup(
    ...,
    entry_points={
        "sqlmesh.materializations": [
            "my_materialization = my_package.my_materialization:CustomFullMaterialization",
        ],
    },
)
```

Refer to the [custom_materializations](https://github.com/TobikoData/sqlmesh/tree/main/examples/custom_materializations) package example for more details.
