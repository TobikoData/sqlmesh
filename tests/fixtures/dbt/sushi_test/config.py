from pathlib import Path

from sqlmesh.core.config import ModelDefaultsConfig
from sqlmesh.dbt.loader import sqlmesh_config

variables = {"start": "Jan 1 2022"}


config = sqlmesh_config(
    Path(__file__).parent, variables=variables, model_defaults=ModelDefaultsConfig(dialect="duckdb")
)


test_config = config
