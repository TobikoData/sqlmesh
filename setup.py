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
        ],
        "airflow.plugins": [
            "sqlmesh_airflow = sqlmesh.schedulers.airflow.plugin:SqlmeshAirflowPlugin",
        ],
    },
    use_scm_version={"write_to": "sqlmesh/_version.py", "fallback_version": "0.0.0"},
    setup_requires=["setuptools_scm"],
    install_requires=[
        "astor",
        "click",
        "croniter",
        "duckdb",
        "dateparser",
        "hyperscript",
        "jinja2",
        "pandas",
        "pydantic",
        "requests",
        "rich",
        "ruamel.yaml",
        "sqlglot>=11.4.4",
    ],
    extras_require={
        "dev": [
            f"apache-airflow=={os.environ.get('AIRFLOW_VERSION', '2.3.3')}",
            "autoflake==1.7.7",
            "black==22.6.0",
            "dbt-core",
            "Faker",
            "google-auth",
            "isort==5.10.1",
            "mkdocs-include-markdown-plugin==4.0.3",
            "mkdocs-material==9.0.5",
            "mypy~=1.0.0",
            "ipywidgets",
            "pre-commit",
            "pandas-stubs",
            "pdoc",
            "psycopg2-binary",
            "PyGithub",
            "pytest",
            "pytest-asyncio",
            "pytest-mock",
            "pyspark",
            "pytz",
            "sqlalchemy-stubs",
            "tenacity==8.1.0",
            "types-croniter",
            "types-dateparser",
            "types-pytz",
            "types-requests==2.28.8",
        ],
        "web": [
            "fastapi==0.89.1",
            "hyperscript==0.0.1",
            "pyarrow==11.0.0",
            "uvicorn==0.20.0",
        ],
        "snowflake": [
            "snowflake-connector-python[pandas]",
        ],
        "bigquery": [
            "google-cloud-bigquery[pandas]",
        ],
        "databricks": [
            "databricks-sql-connector",
            "databricks-cli",
        ],
        "redshift": [
            "redshift_connector",
        ],
        "dbt": [
            "dbt-core",
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
