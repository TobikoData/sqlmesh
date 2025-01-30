import os
from os.path import exists

from setuptools import find_packages, setup

description = open("README.md").read() if exists("README.md") else ""

setup(
    name="sqlmesh",
    description="",
    long_description=description,
    long_description_content_type="text/markdown",
    url="https://github.com/TobikoData/sqlmesh",
    author="TobikoData Inc.",
    author_email="engineering@tobikodata.com",
    license="Apache License 2.0",
    packages=find_packages(include=["sqlmesh", "sqlmesh.*", "web*"]),
    package_data={"web": ["client/dist/**"], "": ["py.typed"]},
    entry_points={
        "console_scripts": [
            "sqlmesh = sqlmesh.cli.main:cli",
            "sqlmesh_cicd = sqlmesh.cicd.bot:bot",
        ],
        "airflow.plugins": [
            "sqlmesh_airflow = sqlmesh.schedulers.airflow.plugin:SqlmeshAirflowPlugin",
        ],
    },
    use_scm_version={
        "write_to": "sqlmesh/_version.py",
        "fallback_version": "0.0.0",
        "local_scheme": "no-local-version",
    },
    setup_requires=["setuptools_scm"],
    install_requires=[
        "astor",
        "click",
        "croniter",
        "duckdb!=0.10.3",
        "dateparser",
        "hyperscript>=0.1.0",
        "importlib-metadata; python_version<'3.12'",
        "ipywidgets",
        "jinja2",
        "pandas",
        "pydantic>=2.0.0",
        "requests",
        "rich[jupyter]",
        "ruamel.yaml",
        "setuptools; python_version>='3.12'",
        "sqlglot[rs]~=26.3.9",
        "tenacity",
        "time-machine",
    ],
    extras_require={
        "athena": ["PyAthena[Pandas]"],
        "azuresql": ["pymssql"],
        "bigquery": [
            "google-cloud-bigquery[pandas]",
            "google-cloud-bigquery-storage",
        ],
        "bigframes": ["bigframes>=1.32.0"],
        "clickhouse": ["clickhouse-connect"],
        "databricks": ["databricks-sql-connector"],
        "dev": [
            "agate==1.7.1",
            f"apache-airflow=={os.environ.get('AIRFLOW_VERSION', '2.9.1')}",
            "opentelemetry-proto==1.27.0",  # pip was having trouble resolving this transitive dependency of airflow
            "beautifulsoup4",
            "clickhouse-connect",
            "cryptography",
            "custom-materializations",
            "databricks-sql-connector",
            "dbt-bigquery",
            "dbt-core",
            "dbt-duckdb>=1.7.1",
            "dbt-snowflake",
            "Faker",
            "google-auth",
            "google-cloud-bigquery",
            "google-cloud-bigquery-storage",
            "mypy~=1.13.0",
            "pandas-stubs",
            "pre-commit",
            "psycopg2-binary",
            "pydantic",
            "PyAthena[Pandas]",
            "PyGithub",
            "pyspark~=3.5.0",
            "pytest",
            "pytest-asyncio",
            "pytest-mock",
            "pytest-retry",
            "pytest-xdist",
            "pytz",
            "redshift_connector",
            "ruff~=0.7.0",
            "snowflake-connector-python[pandas,secure-local-storage]>=3.0.2",
            "sqlalchemy-stubs",
            "trino",
            "types-croniter",
            "types-dateparser",
            "types-python-dateutil",
            "types-pytz",
            "types-requests==2.28.8",
            "typing-extensions",
        ],
        "cicdtest": [
            "dbt-athena-community",
            "dbt-clickhouse",
            "dbt-databricks",
            "dbt-redshift",
            "dbt-sqlserver>=1.7.0",
            "dbt-trino",
        ],
        "dbt": [
            "dbt-core<2",
        ],
        "dlt": [
            "dlt",
        ],
        "gcppostgres": [
            "cloud-sql-python-connector[pg8000]",
        ],
        "github": [
            "PyGithub",
        ],
        "llm": [
            "langchain",
            "openai",
        ],
        "mssql": [
            "pymssql",
        ],
        "mysql": [
            "mysql-connector-python",
        ],
        "mwaa": [
            "boto3",
        ],
        "postgres": [
            "psycopg2",
        ],
        "redshift": [
            "redshift_connector",
        ],
        "slack": [
            "slack_sdk",
        ],
        "snowflake": [
            "cryptography",
            "snowflake-connector-python[pandas,secure-local-storage]",
            # as at 2024-08-05, snowflake-snowpark-python is only available up to Python 3.11
            "snowflake-snowpark-python; python_version<'3.12'",
        ],
        "trino": [
            "trino",
        ],
        "web": [
            "fastapi==0.115.5",
            "watchfiles>=0.19.0",
            "uvicorn[standard]==0.22.0",
            "sse-starlette>=0.2.2",
            "pyarrow",
        ],
    },
    classifiers=[
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Programming Language :: SQL",
        "Programming Language :: Python :: 3 :: Only",
    ],
)
