# Configs
Configs define settings for things like engines (eg. Snowflake or Spark), schedulers (eg. Airflow or Dagster), and the SQL dialect. The config file is defined in config.py in the root directory of your SQLMesh project.

## Settings
### connections
A dictionary of supported connection and their configurations. The key represents a unique connection name.

```python
import duckdb
from sqlmesh.core.config import Config, DuckDBConnectionConfig

Config(
    connections={
        "default": DuckDBConnectionConfig(database="local.duckdb"),
    },
)
```

### scheduler
Identifies which scheduler backend to use. The scheduler backend is used for both storing metadata and executing [plans](/concepts/plans). By default, the `BuiltinSchedulerBackend` is used which uses the existing SQL engine to store metadata and has a simple scheduler. The `AirflowSchedulerBackend` should be used if you want to integrate with Airflow.


```python
from sqlmesh.core.config import AirflowSchedulerConfig, Config

Config(scheduler=AirflowSchedulerConfig())
```

### notification_targets
Notification targets are used to receive logging or updates as SQLMesh processes things. Notification targets can be used to implement things like integration with Github or Slack.

### dialect
The default sql dialect of model queries. Default: same as engine dialect. The dialect is used if a [model](/concepts/models) does not define a dialect. Note that this dialect only specifies what the model is written as. At runtime, model queries will be transpiled to the correct engine dialect.

### physical_schema
The default schema used to store materialized tables. By default this will store all physical tables managed by SQLMesh in the `sqlmesh` schema/db in your warehouse.

### snapshot_ttl
Duration before unpromoted snapshots are removed. This is defined as a string with the default be `in 1 week`. Other [relative strings](https://dateparser.readthedocs.io/en/latest/) can be used liked `in 30 days`.

### time_column_format
The default format to use for all model time columns. Defaults to %Y-%m-%d.

This time format uses python format codes. https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes.

### backfill_concurrent_tasks
The number of concurrent tasks used for model backfilling during plan application. Default: 1.

### ddl_concurrent_tasks
The number of concurrent tasks used for DDL operations (table / view creation, deletion, etc). Default: 1.

### evaluation_concurrent_tasks
The number of concurrent tasks used for model evaluation when running with the built-in scheduler. Default: 1.

### users
A list of users that can be used for approvals/notifications.

## Precedence

You can configure your project in multiple places, and SQLMesh will prioritize configurations according to
the following order. From least to greatest precedence:

- A Config object defined in a config.py file at the root of your project:

```python
# config.py
import duckdb
from sqlmesh.core.engine_adapter import EngineAdapter
local_config = Config(
    connections={
        "default": DuckDBConnectionConfig(database="local.duckdb"),
    },
)
# End config.py

>>> from sqlmesh import Context
>>> context = Context(path="example", config="local_config")

```

- A Config object used when initializing a Context:

```python
>>> from sqlmesh import Context
>>> from sqlmesh.core.config import Config
>>> my_config = Config()
>>> context = Context(path="example", config=my_config)

```

- Individual config parameters used when initializing a Context:

```python
>>> from sqlmesh import Context
>>> from sqlmesh.core.engine_adapter import create_engine_adapter
>>> adapter = create_engine_adapter(duckdb.connect, "duckdb")
>>> context = Context(
...     path="example",
...     engine_adapter=adapter,
...     dialect="duckdb",
... )
```

## Using Config

The most common way to configure your SQLMesh project is with a `config.py` module at the root of the
project. A SQLMesh Context will automatically look for Config objects there. You can have multiple
Config objects defined, and then tell Context which one to use. For example, you can have different
Configs for local and production environments, Airflow, and Model tests.

Example config.py:
```python
import duckdb

from sqlmesh.core.config import Config, AirflowSchedulerBackend

from my_project.utils import load_test_data


# An in memory DuckDB config.
config = Config()

# A stateful DuckDB config.
local_config = Config(
    connections={
        "default": DuckDBConnectionConfig(database="local.duckdb"),
    },
)

# The config to run model tests.
test_config = Config()

# A config that uses Airflow
airflow_config = Config(
    scheduler_backend=AirflowSchedulerBackend(),
)
```

To use a Config, pass in its variable name to Context.
```python
>>> from sqlmesh import Context
>>> context = Context(path="example", config="local_config")

```

For more information about the Config class and its parameters, see `sqlmesh.core.config.Config`.
