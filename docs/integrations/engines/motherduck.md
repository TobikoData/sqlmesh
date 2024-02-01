# MotherDuck

## Local/Built-in Scheduler
**Engine Adapter Type**: `motherduck`

### Connection options

| Option             | Description                                                                                                 | Type   | Required |
|--------------------|-------------------------------------------------------------------------------------------------------------|:------:|:--------:|
| `type`             | Engine type name - must be `motherduck`                                                                     | string | Y        |
| `database`         | The database name.                                                                                          | string | Y        |
| `token`            | The optional MotherDuck token. If not specified, the user will be prompted to login with their web browser. | string | N        |
| `extensions`       | Extension to load into duckdb. Only autoloadable extensions are supported.                                  | list   | N        |
| `connector_config` | Configuration to pass into the duckdb connector.                                                            | dict   | N        |
