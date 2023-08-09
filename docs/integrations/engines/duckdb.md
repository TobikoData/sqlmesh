# DuckDB

## Local/Built-in Scheduler
**Engine Adapter Type**: `duckdb`

### Connection options

| Option     | Description                                                                  |  Type  | Required |
|------------|------------------------------------------------------------------------------|:------:|:--------:|
| `database` | The optional database name. If not specified, the in-memory database is used | string |    N     |

## Airflow Scheduler
DuckDB only works when running locally; therefore it does not support Airflow.
