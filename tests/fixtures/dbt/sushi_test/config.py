from pathlib import Path

from sqlmesh.core.config import ModelDefaultsConfig
from sqlmesh.dbt.loader import sqlmesh_config


config = sqlmesh_config(
    Path(__file__).parent, model_defaults=ModelDefaultsConfig(dialect="duckdb", start="Jan 1 2022")
)


test_config = config


test_config_with_var_override = sqlmesh_config(
    Path(__file__).parent,
    model_defaults=ModelDefaultsConfig(dialect="duckdb", start="Jan 1 2022"),
    variables={
        "some_var": "overridden_from_config_py",
    },
)


test_config_with_normalization_strategy = sqlmesh_config(
    Path(__file__).parent,
    model_defaults=ModelDefaultsConfig(
        dialect="duckdb,normalization_strategy=LOWERCASE", start="Jan 1 2022"
    ),
)
