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
    package_data={"web": ["client/dist/**"]},
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
        "duckdb",
        "dateparser",
        "fsspec",
        "hyperscript",
        "ipywidgets",
        "jinja2",
        "pandas",
        "pydantic",
        "requests",
        "rich[jupyter]",
        "ruamel.yaml",
        "sqlglot~=20.3.0",
    ],
    extras_require={
        "bigquery": [
            "google-cloud-bigquery[pandas]",
            "google-cloud-bigquery-storage",
        ],
        "databricks": [
            "databricks-sql-connector",
            "databricks-cli",
        ],
        "dev": [
            f"apache-airflow=={os.environ.get('AIRFLOW_VERSION', '2.3.3')}",
            "autoflake==1.7.7",
            "agate==1.6.3",
            "google-cloud-bigquery",
            "google-cloud-bigquery-storage",
            "black==22.6.0",
            "dbt-bigquery; python_version >= '3.8'",
            # Remove once we drop support for Python 3.7
            "dbt-bigquery==1.5.2; python_version < '3.8'",
            "dbt-core",
            "dbt-duckdb>=1.4.2",
            "dbt-snowflake",
            "Faker",
            "freezegun",
            "google-auth",
            "isort==5.10.1",
            "mkdocs-include-markdown-plugin==4.0.3",
            "mkdocs-material==9.0.5",
            "mypy~=1.4.0",
            # Pendulum 3.0.0 contains a breaking change for Airflow.
            # To test if this is fixed with future versions, check if this line works:
            # https://github.com/apache/airflow/blob/main/airflow/settings.py#L59
            # `TIMEZONE = pendulum.tz.timezone("UTC")`
            "pendulum<3.0.0",
            "pre-commit",
            "pandas-stubs",
            "pdoc",
            "psycopg2-binary",
            "pyarrow>=10.0.1,<10.1.0",
            "PyGithub",
            "pytest",
            "pytest-asyncio<0.23.0",
            "pytest-lazy-fixture",
            "pytest-mock",
            "pyspark>=3.4.0",
            "pytz",
            "snowflake-connector-python[pandas,secure-local-storage]>=3.0.2",
            "sqlalchemy-stubs",
            "tenacity==8.1.0",
            "types-croniter",
            "types-dateparser",
            "typing-extensions",
            "types-pytz",
            "types-requests==2.28.8",
        ],
        "dbt": [
            "dbt-core<1.5.0",
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
            "snowflake-connector-python[pandas,secure-local-storage]",
            "pyarrow>=10.0.1,<10.1.0",
        ],
        "trino": [
            "trino",
        ],
        "web": [
            "fastapi==0.100.0",
            "watchfiles>=0.19.0",
            "pyarrow>=10.0.1",
            "uvicorn[standard]==0.22.0",
            "sse-starlette>=0.2.2",
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
