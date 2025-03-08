import pytest


@pytest.fixture
def sample_models(request):
    models = {
        "sql": {
            "contents": """
MODEL (
    name sushi.test_model,
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
    "sushi.test_model",
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
- name: sushi.test_model
  columns:
    id: INT
""",
            "path": "external_models/external_model.yaml",
        },
    }
    requested_models = request.param.split("_")
    return [v for k, v in models.items() if k in requested_models]


def write_model(contents, path):
    with open(path, mode="w", encoding="utf8") as f:
        f.write(contents)


@pytest.mark.parametrize("sample_models", ["sql", "python", "external"], indirect=True)
def test_duplicate_model_names_same_kind(sample_models, init_and_plan_context):
    def duplicate_model_path(fpath):
        from pathlib import Path

        return Path(fpath).parent / ("duplicate" + Path(fpath).suffix)

    model = sample_models[0]
    print(model)
    context, _ = init_and_plan_context("examples/sushi")

    write_model(model["contents"], context.path / model["path"])
    write_model(model["contents"], context.path / duplicate_model_path(model["path"]))

    with pytest.raises(
        ValueError,
        match=r'Duplicate key \'"memory"."sushi"."test_model"\' found in UniqueKeyDict<models>. Call dict.update\(\.\.\.\) if this is intentional.',
    ):
        context.load()


@pytest.mark.parametrize(
    "sample_models",
    ["sql_python", "python_external", "sql_external", "sql_python_external"],
    indirect=True,
)
def test_duplicate_model_names_different_kind(sample_models, init_and_plan_context):
    model_1, *models = sample_models
    if len(models) == 2:
        model_2, model_3 = models
    else:
        model_2, model_3 = models[0], None
    context, _ = init_and_plan_context("examples/sushi")

    write_model(model_1["contents"], context.path / model_1["path"])
    write_model(model_2["contents"], context.path / model_2["path"])

    if model_3:
        write_model(model_3["contents"], context.path / model_3["path"])

    with pytest.raises(
        ValueError, match=r'Duplicate model name\(s\) found: "memory"."sushi"."test_model".'
    ):
        context.load()
