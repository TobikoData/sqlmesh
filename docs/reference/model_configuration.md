# Model configuration

This page lists SQLMesh model configuration options and their parameters.

Learn more about specifying SQLMesh model properties in the [model concepts overview page](../concepts/models/overview.md#model-properties).

## General model properties

Configuration options for SQLMesh model properties. Supported by all model kinds other than [`SEED` models](#seed-models).

| Option             | Description                                                                                                                                                                                                                                                                                                                      |       Type        | Required |
| ------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :---------------: | :------: |
| `name`             | The model name. Must include at least a qualifying schema (`<schema>.<model>`) and may include a catalog (`<catalog>.<schema>.<model>`). If any project model name includes a catalog, all model names must include a catalog or a default catalog [must be set](#default-schema--catalog).                                      |        str        |    Y     |
| `kind`             | The model kind ([Additional Details](#model-kind-properties)) (Default: `VIEW`)                                                                                                                                                                                                                                                  |    str \| dict    |    N     |
| `audits`           | SQLMesh [audits](../concepts/audits.md) that should run against the model's output                                                                                                                                                                                                                                               |    array[str]     |    N     |
| `dialect`          | The SQL dialect in which the model's query is written. All SQL dialects [supported by the SQLGlot library](https://github.com/tobymao/sqlglot/blob/main/sqlglot/dialects/dialect.py) are allowed.                                                                                                                                |        str        |    N     |
| `owner`            | The owner of a model; may be used for notification purposes                                                                                                                                                                                                                                                                      |        str        |    N     |
| `stamp`            | Arbitrary string used to indicate a model's version without changing the model name                                                                                                                                                                                                                                              |        str        |    N     |
| `tags`             | Arbitrary strings used to organize or classify a model                                                                                                                                                                                                                                                                           |    array[str]     |    N     |
| `cron`             | The cron expression specifying how often the model should be refreshed. (Default: `@daily`)                                                                                                                                                                                                                                      |        str        |    N     |
| `interval_unit`    | The temporal granularity of the model's data intervals. Supported values: `year`, `month`, `day`, `hour`, `half_hour`, `quarter_hour`, `five_minute`. (Default: inferred from `cron`)                                                                                                                                            |        str        |    N     |
| `start`            | The date/time that determines the earliest date interval that should be processed by a model. Can be a datetime string, epoch time in milliseconds, or a relative datetime such as `1 year ago`.                                                                                                                                 |    str \| int     |    N     |
| `batch_size`       | The maximum number of intervals that can be evaluated in a single backfill task. If this is `None`, all intervals will be processed as part of a single task. If this is set, a model's backfill will be chunked such that each individual task only contains jobs with the maximum of `batch_size` intervals. (Default: `None`) |        int        |    N     |
| `grains`           | The column(s) whose combination uniquely identifies each row in the model                                                                                                                                                                                                                                                        | str \| array[str] |    N     |
| `references`       | The model column(s) used to join to other models' grains                                                                                                                                                                                                                                                                         | str \| array[str] |    N     |
| `depends_on`       | Models on which this model depends. (Default: dependencies inferred from model code)                                                                                                                                                                                                                                             |    array[str]     |    N     |
| `storage_format`   | The storage format that should be used to store physical tables; only applicable to engines such as Spark                                                                                                                                                                                                                        |        str        |    N     |
| `partitioned_by`   | The column(s) used to partition the model's physical table; only applicable to engines that support partitioning                                                                                                                                                                                                                 |        str        |    N     |
| `clustered_by`     | The column(s) used to cluster the model's physical table; only applicable to engines that support clustering                                                                                                                                                                                                                     |        str        |    N     |
| `columns`          | The column names and data types returned by the model. Disables [automatic inference of column names and types](../concepts/models/overview.md#conventions) from the SQL query.                                                                                                                                                  |    array[str]     |    N     |
| `table_properties` | Arbitrary table properties specific to the target engine. Specified as key-value pairs (`key = value`)                                                                                                                                                                                                                           |       dict        |    N     |
| `allow_partials`   | Whether this model can process partial (incomplete) data intervals                                                                                                                                                                                                                                                               |       bool        |    N     |
| `description`      | Description of the model. Automatically registered in the SQL engine's table COMMENT field or equivalent (if supported by the engine).                                                                                                                                                                                           |        str        |    N     |

### Model defaults

The SQLMesh project-level configuration must contain the `model_defaults` key and must specify a value for its `dialect` key. Other values are set automatically unless explicitly overridden in the model definition. Learn more about project-level configuration in the [configuration guide](../guides/configuration.md).

The SQLMesh project-level `model_defaults` key supports the following options, described in the [general model properties](#general-model-properties) table above:

- kind
- dialect
- cron
- owner
- start
- batch_size
- storage_format

## Model kind properties

Configuration options for kind-specific SQLMesh model properties, in addition to the [general model properties](#general-model-properties) listed above.

Learn more about model kinds at the [model kind concepts page](../concepts/models/model_kinds.md). Learn more about specifying model kind in Python models at the [Python models concepts page](../concepts/models/python_models.md#model-specification).

### `VIEW` models

Configuration options for models of the [`VIEW` kind](../concepts/models/model_kinds.md#view) (in addition to [general model properties](#general-model-properties)).

| Option         | Description                                                                                          | Type | Required |
| -------------- | ---------------------------------------------------------------------------------------------------- | :--: | :------: |
| `materialized` | Whether views should be materialized (for engines supporting materialized views). (Default: `False`) | bool |    N     |

Python model configuration object: [ViewKind()](https://sqlmesh.readthedocs.io/en/stable/_readthedocs/html/sqlmesh/core/model/kind.html#ViewKind)

### `FULL` models

The [`FULL` model kind](../concepts/models/model_kinds.md#full) does not support any configuration options other than the [general model properties listed above](#general-model-properties).

Python model configuration object: [FullKind()](https://sqlmesh.readthedocs.io/en/stable/_readthedocs/html/sqlmesh/core/model/kind.html#FullKind)

### Incremental models

Configuration options for all incremental models (in addition to [general model properties](#general-model-properties)).

| Option       | Description                                                                                                                                                                                                                                                                                                                      | Type | Required |
| ------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :--: | :------: |
| `batch_size` | The maximum number of intervals that can be evaluated in a single backfill task. If this is `None`, all intervals will be processed as part of a single task. If this is set, a model's backfill will be chunked such that each individual task only contains jobs with the maximum of `batch_size` intervals. (Default: `None`) | int  |    N     |
| `lookback`   | The number of time unit intervals prior to the current interval that should be processed. (Default: `0`)                                                                                                                                                                                                                         | int  |    N     |

#### Incremental by time range

Configuration options for [`INCREMENTAL_BY_TIME_RANGE` models](../concepts/models/model_kinds.md#incremental_by_time_range) (in addition to [general model properties](#general-model-properties) and [incremental model properties](#incremental-models)).

| Option                | Description                                                                                                                             | Type | Required |
| --------------------- | --------------------------------------------------------------------------------------------------------------------------------------- | :--: | :------: |
| `time_column`         | The model column containing each row's timestamp.                                                                                       | str  |    Y     |
| `format`              | Argument to `time_column`. Format of the time column's data. (Default: `%Y-%m-%d`)                                                      | str  |    N     |
| `forward_only`        | Whether the model's changes should always be classified as [forward-only](../concepts/plans.md#forward-only-change). (Default: `False`) | bool |    N     |
| `disable_restatement` | Whether [restatements](../concepts/plans.md#restatement-plans) should be disabled for the model. (Default: `False`)                     | bool |    N     |

Python model configuration object: [IncrementalByTimeRangeKind()](https://sqlmesh.readthedocs.io/en/stable/_readthedocs/html/sqlmesh/core/model/kind.html#IncrementalByTimeRangeKind)

#### Incremental by unique key

Configuration options for [`INCREMENTAL_BY_UNIQUE_KEY` models](../concepts/models/model_kinds.md#incremental_by_unique_key) (in addition to [general model properties](#general-model-properties) and [incremental model properties](#incremental-models)).

| Option                | Description                                                                                                                             |       Type       | Required |
| --------------------- | --------------------------------------------------------------------------------------------------------------------------------------- | :--------------: | :------: |
| `unique_key`          | The model column(s) containing each row's unique key.                                                                                   | str \| list[str] |    Y     |
| `forward_only`        | Whether the model's changes should always be classified as [forward-only](../concepts/plans.md#forward-only-change). (Default: `False`) |       bool       |    N     |
| `disable_restatement` | Whether [restatements](../concepts/plans.md#restatement-plans) should be disabled for the model. (Default: `False`)                     |       bool       |    N     |

Python model configuration object: [IncrementalByUniqueKeyKind()](https://sqlmesh.readthedocs.io/en/stable/_readthedocs/html/sqlmesh/core/model/kind.html#IncrementalByUniqueKeyKind)

#### SCD Type 2 models

Configuration options for [`SCD_TYPE_2` models](../concepts/models/model_kinds.md#scd-type-2) (in addition to [general model properties](#general-model-properties) and [incremental model properties](#incremental-models)).

| Option                    | Description                                                                                                                                                                                 |   Type    | Required |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :-------: | :------: |
| `unique_key`              | The model column(s) containing each row's unique key.                                                                                                                                       | list[str] |    Y     |
| `valid_from_name`         | The model column containing each row's valid from date. (Default: `valid_from`)                                                                                                             |    str    |    N     |
| `valid_to_name`           | The model column containing each row's valid to date. (Default: `valid_to`)                                                                                                                 |    str    |    N     |
| `invalidate_hard_deletes` | If set to true, when a record is missing from the source table it will be marked as invalid - see [here](../concepts/models/model_kinds.md#deletes) for more information. (Default: `True`) |   bool    |    N     |

##### SCD Type 2 By Time

Configuration options for [`SCD_TYPE_2_BY_TIME` models](../concepts/models/model_kinds.md#scd-type-2) (in addition to [general model properties](#general-model-properties), [incremental model properties](#incremental-models), and [SCD Type 2 properties](#scd-type-2-models)).

| Option                     | Description                                                                                                                                                                      | Type | Required |
| -------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :--: | :------: |
| `updated_at_name`          | The model column containing each row's updated at date. (Default: `updated_at`)                                                                                                  | str  |    N     |
| `updated_at_as_valid_from` | By default, for new rows the `valid_from` column is set to 1970-01-01 00:00:00. This sets `valid_from` to the value of `updated_at` when the row is inserted. (Default: `False`) | bool |    N     |

##### SCD Type 2 By Column

Configuration options for [`SCD_TYPE_2_BY_COLUMN` models](../concepts/models/model_kinds.md#scd-type-2) (in addition to [general model properties](#general-model-properties), [incremental model properties](#incremental-models), and [SCD Type 2 properties](#scd-type-2-models)).

| Option                         | Description                                                                                                                                                                 |       Type        | Required |
| ------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :---------------: | :------: |
| `columns`                      | Columns whose changed data values indicate a data update (instead of an `updated_at` column). `*` to represent that all columns should be checked.                          | str \| array[str] |    Y     |
| `execution_time_as_valid_from` | By default, for new rows `valid_from` is set to 1970-01-01 00:00:00. This changes the behavior to set it to the execution_time of when the pipeline ran. (Default: `False`) |       bool        |    N     |

Python model configuration object: [SCDType2Kind()](https://sqlmesh.readthedocs.io/en/stable/_readthedocs/html/sqlmesh/core/model/kind.html#SCDType2Kind)

### `SEED` models

Configuration options for [`SEED` models](../concepts/models/model_kinds.md#seed). `SEED` models do not support all the general properties supported by other models; they only support the properties listed in this table.

Top-level options inside the MODEL DDL:

| Option        | Description                                                                                                                                                                                                                                                                                 |    Type    | Required |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :--------: | :------: |
| `name`        | The model name. Must include at least a qualifying schema (`<schema>.<model>`) and may include a catalog (`<catalog>.<schema>.<model>`). If any project model name includes a catalog, all model names must include a catalog or a default catalog [must be set](#default-schema--catalog). |    str     |    Y     |
| `kind`        | The model kind. Must be `SEED`.                                                                                                                                                                                                                                                             |            |          |
| `columns`     | The column names and data types in the CSV file. Disables automatic inference of column names and types by the pandas CSV reader. NOTE: order of columns overrides the order specified in the CSV header row (if present).                                                                  | array[str] |    N     |
| `audits`      | SQLMesh [audits](../concepts/audits.md) that should run against the model's output                                                                                                                                                                                                          | array[str] |    N     |
| `owner`       | The owner of a model; may be used for notification purposes                                                                                                                                                                                                                                 |    str     |    N     |
| `stamp`       | Arbitrary string used to indicate a model's version without changing the model name                                                                                                                                                                                                         |    str     |    N     |
| `tags`        | Arbitrary strings used to organize or classify a model                                                                                                                                                                                                                                      | array[str] |    N     |
| `description` | Description of the model. Automatically registered in the SQL engine's table COMMENT field or equivalent (if supported by the engine).                                                                                                                                                      |    str     |    N     |

Options specified within the top-level `kind` property:

| Option         | Description                                                                                                                                                                             | Type | Required |
| -------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---- | -------- |
| `path`         | Path to seed CSV file.                                                                                                                                                                  | str  | Y        |
| `batch_size`   | The maximum number of CSV rows ingested in each batch. All rows ingested in one batch if not specified.                                                                                 | int  | N        |
| `csv_settings` | Pandas CSV reader settings (overrides [default values](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html)). Specified as key-value pairs (`key = value`). | dict | N        |

Options specified within the `kind` property's `csv_settings` property (overrides [default Pandas CSV reader settings](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html)):

| Option             | Description                                                                                                                                                                                                                                                                         | Type | Required |
| ------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---- | -------- |
| `delimiter`        | Character or regex pattern to treat as the delimiter. More information at the [Pandas documentation](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html).                                                                                              | str  | N        |
| `quotechar`        | Character used to denote the start and end of a quoted item. More information at the [Pandas documentation](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html).                                                                                       | str  | N        |
| `doublequote`      | When quotechar is specified, indicate whether or not to interpret two consecutive quotechar elements INSIDE a field as a single quotechar element. More information at the [Pandas documentation](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html). | bool | N        |
| `escapechar`       | Character used to escape other characters. More information at the [Pandas documentation](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html).                                                                                                         | str  | N        |
| `skipinitialspace` | Skip spaces after delimiter. More information at the [Pandas documentation](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html).                                                                                                                       | bool | N        |
| `lineterminator`   | Character used to denote a line break. More information at the [Pandas documentation](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html).                                                                                                             | str  | N        |
| `encoding`         | Encoding to use for UTF when reading/writing (ex. 'utf-8'). More information at the [Pandas documentation](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html).                                                                                        | str  | N        |

Python model configuration object: [SeedKind()](https://sqlmesh.readthedocs.io/en/stable/_readthedocs/html/sqlmesh/core/model/kind.html#SeedKind)