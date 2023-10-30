# DuckDB

## Local/Built-in Scheduler
**Engine Adapter Type**: `duckdb`

### Connection options

| Option     | Description                                                                                                                                             |  Type  | Required |
|------------|---------------------------------------------------------------------------------------------------------------------------------------------------------|:------:|:--------:|
| `type`     | Engine type name - must be `duckdb`                                                                                                                     | string |    Y     |
| `database` | The optional database name. If not specified, the in-memory database is used. Cannot be defined if using `catalogs`.                                    | string |    N     |
| `catalogs` | Mapping to define multiple catalogs. Name is the alias and value is the path. First entry is the default catalog. Cannot be defined if using `database` |  dict  |    N     |

#### Catalogs Example

```yaml linenums="1"
type: duckdb
catalogs:
  persistent: 'local.duckdb'
  ephemeral: ':memory:'
```

Creates two catalogs: `persistent` and `ephemeral`. The `persistent` catalog will be the default catalog since it is the first entry.

## Airflow Scheduler
DuckDB only works when running locally; therefore it does not support Airflow.
