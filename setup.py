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
    packages=find_packages(include=["sqlmesh", "sqlmesh.*"]),
    entry_points={
        "console_scripts": [
            "sqlmesh = sqlmesh.cli.main:cli",
        ],
    },
    install_requires=[
        "astor; python_version < '3.9.0'",
        "click",
        "croniter",
        "duckdb",
        "dateparser",
        "hyperscript",
        "pandas",
        "pydantic",
        "requests",
        "rich",
        "ruamel.yaml",
        "sqlglot>=10.2.0",
    ],
    extras_require={
        "dev": [
            f"apache-airflow=={os.environ.get('AIRFLOW_VERSION', '2.3.3')}",
            "autoflake==1.7.7",
            "black==22.6.0",
            "google-auth",
            "isort==5.10.1",
            "mypy==0.981",
            "ipywidgets",
            "pre-commit",
            "pandas-stubs",
            "pdoc",
            "psycopg2-binary",
            "PyGithub",
            "pytest",
            "pytest-mock",
            "pyspark",
            "sqlalchemy-stubs",
            "tenacity",
            "types-croniter",
            "types-dateparser",
            "types-requests==2.28.8",
        ],
        "web": [
            "fastapi==0.85.0",
            "hyperscript==0.0.1",
            "uvicorn==0.18.3",
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
