# Spark

## Local/Built-in Scheduler
**Engine Adapter Type**: `spark`

NOTE: Spark may not be used for the SQLMesh [state connection](../../reference/configuration.md#connections).

### Connection options

| Option       | Description                                                                                   |  Type  | Required |
|--------------|-----------------------------------------------------------------------------------------------|:------:|:--------:|
| `type`       | Engine type name - must be `spark`                                                            | string |    Y     |
| `config_dir` | Value to set for `SPARK_CONFIG_DIR`                                                           | string |    N     |
| `catalog`    | The catalog to use when issuing commands. See [Catalog Support](#catalog-support) for details | string |    N     |
| `config`     | Key/value pairs to set for the Spark Configuration.                                           |  dict  |    N     |

## Catalog Support

SQLMesh's Spark integration is only designed/tested with a single catalog usage in mind. 
Therefore all SQLMesh models must be defined with a single catalog.

If `catalog` is not set, then the behavior changes based on spark release:

* If >=3.4, then the default catalog is determined at runtime
* If <3.4, then the default catalog is `spark_catalog` 
