# Tutorial

This tutorial will run you through an example project using [DuckDB](https://duckdb.org/), an embedded database. You'll learn the basic concepts of SQLMesh with minimal setup. You can translate this project into a data warehouse like Snowflake or BigQuery with minimal effort by adding a new engine adapter.

## Install
Install SQLMesh from pip by running:

```pip install sqlmesh```

It is recommended but not required to use a virtualenv. Run these commands in the directory of the tutorial project.

```
# optionally create your directory
mkdir ~/sqlmesh-example
cd ~/sqlmesh-example

# setup the virtualenv
python -m venv .env
source .env/bin/activate

# install sqlmesh in your virtualenv
pip install sqlmesh
```

If you use venv, make sure to it is activated whenever you run SQLMesh commands. Your commandline prompt should show (.env) user@. Run `source .env/bin/activate` to reactivate your venv.

## Initialize

From within your project folder, run:

```
sqlmesh init
```

This will create directories and files to organize your SQLMesh project code.

- config.py
    - The file for database configuration.
- ./models
    - Where you put your sql and python models.  # TODO add links
- ./audits
    - Where you put shared audits.
- ./tests
    - Where you put unit tests.
- ./macros
    - Where you put macros.
