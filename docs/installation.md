# Installation

This page provides instructions for installing SQLMesh on your computer.

## Python virtual environment

It is recommended, but not required, that you use a python virtual environment with SQLMesh.

First, create the virtual environment:
```bash
python -m venv .env
```

Then activate it:
```bash
source .env/bin/activate
```

## Install SQLMesh core

Install the core SQLMesh library with `pip`:
```bash
pip install sqlmesh
```

## Install extras
Some SQLMesh functionality requires additional Python libraries.

`pip` will automatically install them for you if you specify the relevant name in brackets. For example, you install the SQLMesh browser UI extras with `pip install "sqlmesh[web]"`.

Some extras add features, like the SQLMesh browser UI or Github CI/CD bot:

??? info "Feature extras commands"
    | Feature             | `pip` command                   |
    | ------------------- | ------------------------------- |
    | Browser UI          | `pip install "sqlmesh[web]"`    |
    | dbt projects        | `pip install "sqlmesh[dbt]"`    |
    | dlt projects        | `pip install "sqlmesh[dlt]"`    |
    | Github CI/CD bot    | `pip install "sqlmesh[github]"` |
    | Slack notifications | `pip install "sqlmesh[slack]"`  |
    | Development setup   | `pip install "sqlmesh[dev]"`    |
    | LLM SQL prompt      | `pip install "sqlmesh[llm]"`    |

Other extras are required to use specific SQL engines, like Bigquery or Postgres:

??? info "SQL engine extras commands"
    | SQL engine    | `pip` command                        |
    | ------------- | ------------------------------------ |
    | Athena        | `pip install "sqlmesh[athena]"`      |
    | Bigquery      | `pip install "sqlmesh[bigquery]"`    |
    | ClickHouse    | `pip install "sqlmesh[clickhouse]"`  |
    | Databricks    | `pip install "sqlmesh[databricks]"`  |
    | GCP Postgres  | `pip install "sqlmesh[gcppostgres]"` |
    | MS SQL Server | `pip install "sqlmesh[mssql]"`       |
    | MySQL         | `pip install "sqlmesh[mysql]"`       |
    | Postgres      | `pip install "sqlmesh[postgres]"`    |
    | Redshift      | `pip install "sqlmesh[redshift]"`    |
    | Snowflake     | `pip install "sqlmesh[snowflake]"`   |
    | Trino         | `pip install "sqlmesh[trino]"`       |

Multiple extras can be installed at once, as in `pip install "sqlmesh[web,slack]"`.

## Next steps

Now that you've installed SQLMesh, it's time to get started with the SQLMesh example project.

SQLMesh has three user interfaces - choose one for the example project and jump right in:

- [Command line interface (CLI)](./quickstart/cli.md)
- [Notebook interface](./quickstart/notebook.md)
- [Browser UI graphical interface](./quickstart/ui.md)

Have an existing dbt project you want to run? Install the dbt extra and [check out SQLMesh's dbt adapter](./integrations/dbt.md).
