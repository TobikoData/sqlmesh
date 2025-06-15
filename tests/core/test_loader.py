import pytest
from pathlib import Path
from sqlmesh.cli.example_project import init_example_project
from sqlmesh.core.config import Config, ModelDefaultsConfig
from sqlmesh.core.context import Context
from sqlmesh.utils.errors import ConfigError
import sqlmesh.core.constants as c
from sqlmesh.core.config import load_config_from_yaml
from sqlmesh.utils.yaml import dump


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
import pandas as pd  # noqa: TID253
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
        ConfigError, match=r'Duplicate model name\(s\) found: "memory"."test_schema"."test_model".'
    ):
        Context(paths=tmp_path, config=config)


@pytest.mark.parametrize("sample_models", ["sql", "external"], indirect=True)
def test_duplicate_model_names_same_kind(tmp_path: Path, sample_models):
    """Test same (SQL and external) models with duplicate model names raises ConfigError."""

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
        ConfigError,
        match=r".*Duplicate .* model name: 'test_schema.test_model'",
    ):
        Context(paths=tmp_path, config=config)


@pytest.mark.registry_isolation
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
import pandas as pd  # noqa: TID253
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
        match=r"Failed to load model from file '.*'.\n\n  Duplicate name: 'test_schema.test_model'.",
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


def test_load_migrated_dbt_adapter_dispatch_macros(tmp_path: Path):
    init_example_project(tmp_path, dialect="duckdb")

    migrated_package_path = tmp_path / "macros" / c.MIGRATED_DBT_PACKAGES / "dbt_utils"
    migrated_package_path.mkdir(parents=True)

    (migrated_package_path / "deduplicate.sql").write_text("""
    {%- macro deduplicate(relation) -%}
        {{ return(adapter.dispatch('deduplicate', 'dbt_utils')(relation)) }}
    {% endmacro %}
    """)

    (migrated_package_path / "default__deduplicate.sql").write_text("""
    {%- macro default__deduplicate(relation) -%}
         select 'default impl' from {{ relation }}
    {% endmacro %}
    """)

    (migrated_package_path / "duckdb__deduplicate.sql").write_text("""
    {%- macro duckdb__deduplicate(relation) -%}
        select 'duckdb impl' from {{ relation }}
    {% endmacro %}
    """)

    # this should be pruned from the JinjaMacroRegistry because the target is duckdb, not bigquery
    (migrated_package_path / "bigquery__deduplicate.sql").write_text("""
    {%- macro bigquery__deduplicate(relation) -%}
        select 'bigquery impl' from {{ relation }}
    {% endmacro %}
    """)

    (tmp_path / "models" / "test_model.sql").write_text("""
    MODEL (
        name sqlmesh_example.test,
        kind FULL,
    );
JINJA_QUERY_BEGIN;
{{ dbt_utils.deduplicate(__migrated_ref(schema='sqlmesh_example', identifier='full_model')) }}
JINJA_END;
    """)

    config_path = tmp_path / "config.yaml"
    assert config_path.exists()
    config = load_config_from_yaml(config_path)
    config["variables"] = {}
    config["variables"][c.MIGRATED_DBT_PROJECT_NAME] = "test"

    config_path.write_text(dump(config))

    ctx = Context(paths=tmp_path)

    model = ctx.models['"db"."sqlmesh_example"."test"']
    assert model.dialect == "duckdb"
    assert {(package, name) for package, name, _ in model.jinja_macros.all_macros} == {
        ("dbt_utils", "deduplicate"),
        ("dbt_utils", "default__deduplicate"),
        ("dbt_utils", "duckdb__deduplicate"),
    }

    assert (
        model.render_query_or_raise().sql(dialect="duckdb")
        == """SELECT \'duckdb impl\' AS "duckdb impl" FROM "db"."sqlmesh_example"."full_model" AS "full_model\""""
    )


def test_load_migrated_dbt_adapter_dispatch_macros_in_different_packages(tmp_path: Path):
    # some things like dbt.current_timestamp() dispatch to macros in a different package
    init_example_project(tmp_path, dialect="duckdb")

    migrated_package_path_dbt = tmp_path / "macros" / c.MIGRATED_DBT_PACKAGES / "dbt"
    migrated_package_path_dbt_duckdb = tmp_path / "macros" / c.MIGRATED_DBT_PACKAGES / "dbt_duckdb"
    migrated_package_path_dbt.mkdir(parents=True)
    migrated_package_path_dbt_duckdb.mkdir(parents=True)

    (migrated_package_path_dbt / "current_timestamp.sql").write_text("""
    {%- macro current_timestamp(relation) -%}
        {{ return(adapter.dispatch('current_timestamp', 'dbt')()) }}
    {% endmacro %}
    """)

    (migrated_package_path_dbt / "default__current_timestamp.sql").write_text("""
    {% macro default__current_timestamp() -%}
        {{ exceptions.raise_not_implemented('current_timestamp macro not implemented') }}
    {%- endmacro %}
    """)

    (migrated_package_path_dbt_duckdb / "duckdb__current_timestamp.sql").write_text("""
    {%- macro duckdb__current_timestamp() -%}
        'duckdb current_timestamp impl'
    {% endmacro %}
    """)

    (tmp_path / "models" / "test_model.sql").write_text("""
    MODEL (
        name sqlmesh_example.test,
        kind FULL,
    );
JINJA_QUERY_BEGIN;
select {{ dbt.current_timestamp() }} as a
JINJA_END;
    """)

    config_path = tmp_path / "config.yaml"
    assert config_path.exists()
    config = load_config_from_yaml(config_path)
    config["variables"] = {}
    config["variables"][c.MIGRATED_DBT_PROJECT_NAME] = "test"

    config_path.write_text(dump(config))

    ctx = Context(paths=tmp_path)

    model = ctx.models['"db"."sqlmesh_example"."test"']
    assert model.dialect == "duckdb"
    assert {(package, name) for package, name, _ in model.jinja_macros.all_macros} == {
        ("dbt", "current_timestamp"),
        ("dbt", "default__current_timestamp"),
        ("dbt_duckdb", "duckdb__current_timestamp"),
    }

    assert (
        model.render_query_or_raise().sql(dialect="duckdb")
        == "SELECT 'duckdb current_timestamp impl' AS \"a\""
    )
