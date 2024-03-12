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
        # Issue with Snowflake connector and cryptography 42+
        # Check here if they have added support: https://github.com/dbt-labs/dbt-snowflake/blob/main/dev-requirements.txt#L12
        "cryptography~=41.0.7",
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
        "sqlglot[rs]~=22.4.0",
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
            "agate==1.7.1",
            "beautifulsoup4",
            "black==24.1.1",
            "dbt-core",
            "dbt-duckdb>=1.7.1",
            "Faker",
            "freezegun",
            "google-auth",
            "google-cloud-bigquery",
            "google-cloud-bigquery-storage",
            "isort==5.10.1",
            "mypy~=1.8.0",
            # Pendulum 3.0.0 contains a breaking change for Airflow.
            # To test if this is fixed with future versions, check if this line works:
            # https://github.com/apache/airflow/blob/main/airflow/settings.py#L59
            # `TIMEZONE = pendulum.tz.timezone("UTC")`
            "pendulum<3.0.0",
            "pre-commit",
            "pandas-stubs",
            "psycopg2-binary",
            "pyarrow>=10.0.1,<10.1.0",
            # All Airflow releases require flast-appbuilder==4.3.10 and
            # 4.3.10 requires email-validator==1.3.1
            # https://github.com/apache/airflow/blob/main/pyproject.toml#L685
            # https://github.com/dpgaspar/Flask-AppBuilder/blob/master/requirements.txt#L25
            "pydantic<2.6.0",
            "PyGithub",
            "pytest",
            "pytest-asyncio<0.23.0",
            "pytest-mock",
            "pytest-xdist",
            "pyspark==3.4.0",
            "pytz",
            "snowflake-connector-python[pandas,secure-local-storage]>=3.0.2",
            "sqlalchemy-stubs",
            "tenacity==8.1.0",
            "types-croniter",
            "types-dateparser",
            "types-python-dateutil",
            "types-pytz",
            "types-requests==2.28.8",
            "typing-extensions",
        ],
        "cicdtest": [
            "dbt-bigquery",
            "dbt-databricks",
            "dbt-redshift",
            "dbt-snowflake",
            "dbt-sqlserver",
            "dbt-trino",
        ],
        "dbt": [
            "dbt-core<2",
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
        "motherduck": [
            "duckdb<0.10.0",
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
