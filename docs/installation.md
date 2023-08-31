# Installation

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
Some SQLMesh functionality requires additional Python libraries. `pip` will automatically install them for you if you specify the relevant name in brackets.

Some extras add features:

| Feature             | `pip` command                   |
|---------------------|---------------------------------|
| Browser UI          | `pip install "sqlmesh[web]"`    |
| dbt projects        | `pip install "sqlmesh[dbt]"`    |
| Github CI/CD bot    | `pip install "sqlmesh[github]"` |
| Slack notifications | `pip install "sqlmesh[slack]"`  |
| Development setup   | `pip install "sqlmesh[dev]"`    |
| LLM SQL prompt      | `pip install "sqlmesh[llm]"`    |

Other extras are required to use specific SQL engines:

| SQL engine    | `pip` command                        |
|---------------|--------------------------------------|
| Bigquery      | `pip install "sqlmesh[bigquery]"`    |
| Databricks    | `pip install "sqlmesh[databricks]"`  |
| GCP Postgres  | `pip install "sqlmesh[gcppostgres]"` |
| MS SQL Server | `pip install "sqlmesh[mssql]"`       |
| MySQL         | `pip install "sqlmesh[mysql]"`       |
| Postgres      | `pip install "sqlmesh[postgres]"`    |
| Redshift      | `pip install "sqlmesh[redshift]"`    |
| Snowflake     | `pip install "sqlmesh[snowflake]"`   |

Multiple extras can be installed at once, as in `pip install "sqlmesh[web,slack]"`.

## Next steps
* Jump right in with the [quickstart example project](quick_start.md).
* Have an existing dbt project? Install the dbt extra and [check out SQLMesh's dbt adapter](./integrations/dbt.md).
