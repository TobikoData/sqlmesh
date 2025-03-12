import pytest
from pathlib import Path
from sqlmesh.cli.example_project import init_example_project
from sqlmesh.core.config import Config, ModelDefaultsConfig
from sqlmesh.core.context import Context
from sqlmesh.utils.errors import ConfigError


@pytest.fixture
def sample_models(request):
    models = {
        "sql": {
            "contents": """
MODEL (
    name test_schema.test_model,
    kind FULL,
);

SELECT 1;
""",
            "path": "models/sql_model.sql",
        },
        "python": {
            "contents": """import typing as t
import pandas as pd
from sqlmesh import ExecutionContext, model

@model(
    "test_schema.test_model",
    kind="FULL",
    columns={
        "id": "int",
    }
)
def execute(
    context: ExecutionContext,
    **kwargs: t.Any,
) -> pd.DataFrame:
    return pd.DataFrame([
        {"id": 1}
    ])
""",
            "path": "models/python_model.py",
        },
        "external": {
            "contents": """
- name: test_schema.test_model
  columns:
    id: INT
""",
            "path": "external_models/external_model.yaml",
        },
    }
    requested_models = request.param.split("_")
    return [v for k, v in models.items() if k in requested_models]


@pytest.mark.parametrize(
    "sample_models",
    ["sql_python", "python_external", "sql_external", "sql_python_external"],
    indirect=True,
)
def test_duplicate_model_names_different_kind(tmp_path: Path, sample_models):
    """Test different (SQL, Python and external) models with duplicate model names raises ValueError."""
    model_1, *models = sample_models
    if len(models) == 2:
        model_2, model_3 = models
    else:
        model_2, model_3 = models[0], None

    init_example_project(tmp_path, dialect="duckdb")
    config = Config(model_defaults=ModelDefaultsConfig(dialect="duckdb"))

    path_1: Path = tmp_path / model_1["path"]
    path_2: Path = tmp_path / model_2["path"]

    path_1.parent.mkdir(parents=True, exist_ok=True)
    path_1.write_text(model_1["contents"])
    path_2.parent.mkdir(parents=True, exist_ok=True)
    path_2.write_text(model_2["contents"])

    if model_3:
        path_3: Path = tmp_path / model_3["path"]
        path_3.parent.mkdir(parents=True, exist_ok=True)
        path_3.write_text(model_3["contents"])

    with pytest.raises(
        ValueError, match=r'Duplicate model name\(s\) found: "memory"."test_schema"."test_model".'
    ):
        Context(paths=tmp_path, config=config)


@pytest.mark.parametrize("sample_models", ["sql", "external"], indirect=True)
def test_duplicate_model_names_same_kind(tmp_path: Path, sample_models):
    """Test same (SQL and external) models with duplicate model names raises ValueError."""

    def duplicate_model_path(fpath):
        return Path(fpath).parent / ("duplicate" + Path(fpath).suffix)

    model = sample_models[0]
    init_example_project(tmp_path, dialect="duckdb")
    config = Config(model_defaults=ModelDefaultsConfig(dialect="duckdb"))

    path_1: Path = tmp_path / model["path"]
    path_1.parent.mkdir(parents=True, exist_ok=True)
    path_1.write_text(model["contents"])

    duplicate_fpath = tmp_path / duplicate_model_path(model["path"])
    duplicate_fpath.write_text(model["contents"])

    with pytest.raises(
        ValueError,
        match=r'Duplicate key \'"memory"."test_schema"."test_model"\' found in UniqueKeyDict<models>. Call dict.update\(\.\.\.\) if this is intentional.',
    ):
        Context(paths=tmp_path, config=config)


@pytest.mark.isolated
def test_duplicate_python_model_names_raise_error(tmp_path: Path) -> None:
    """Test python models with duplicate model names raises ConfigError if the functions are not identical."""
    init_example_project(tmp_path, dialect="duckdb")
    config = Config(model_defaults=ModelDefaultsConfig(dialect="duckdb"))
    model_name = "test_schema.test_model"

    path_a = tmp_path / "models/test_schema/test_model_a.py"
    path_b = tmp_path / "models/test_schema/test_model_b.py"

    model_payload_a = f"""from sqlmesh import model
@model(
    name="{model_name}",
    columns={{'"COL"': "int"}},
)
def my_model(context, **kwargs):
    pass"""

    model_payload_b = f"""import typing as t
import pandas as pd
from sqlmesh import ExecutionContext, model

@model(
    name="{model_name}",
    kind="FULL",
    columns={{
        "id": "int",
    }}
)
def execute(
    context: ExecutionContext,
    **kwargs: t.Any,
) -> pd.DataFrame:
    return pd.DataFrame([
        {{"id": 1}}
    ])
"""

    path_a.parent.mkdir(parents=True, exist_ok=True)
    path_a.write_text(model_payload_a)
    path_b.write_text(model_payload_b)

    with pytest.raises(
        ConfigError,
        match=r"Failed to load model definition at '.*'.\nDuplicate key 'test_schema.test_model' found in UniqueKeyDict<python_models>.",
    ):
        Context(paths=tmp_path, config=config)


@pytest.mark.slow
def test_duplicate_python_model_names_no_error(tmp_path: Path) -> None:
    """Test python models with duplicate model names raises no error if the functions are identical."""
    init_example_project(tmp_path, dialect="duckdb")
    config = Config(model_defaults=ModelDefaultsConfig(dialect="duckdb"))
    model_name = "test_schema.test_model"

    path_a = tmp_path / "models/test_schema1/test_model_a.py"
    path_b = tmp_path / "models/test_schema2/test_model_b.py"

    model_payload_a = f"""from sqlmesh import model
@model(
    name="{model_name}",
    columns={{'"COL"': "int"}},
    description="model_payload_a",
)
def my_model(context, **kwargs):
    pass"""

    model_payload_b = f"""from sqlmesh import model
@model(
    name="{model_name}",
    columns={{'"COL"': "int"}},
    description="model_payload_b",
)
def my_model(context, **kwargs):
    pass"""

    path_a.parent.mkdir(parents=True, exist_ok=True)
    path_b.parent.mkdir(parents=True, exist_ok=True)
    path_a.write_text(model_payload_a)
    context = Context(paths=tmp_path, config=config)
    context.load()
    model = context.get_model(f"{model_name}")
    assert model.description == "model_payload_a"
    path_b.write_text(model_payload_b)
    context.load()  # raise no error to duplicate key if the functions are identical (by registry class_method)
    model = context.get_model(f"{model_name}")
    assert (
        model.description != "model_payload_b"
    )  # model will not be overwritten by model_payload_b
    assert model.description == "model_payload_a"
