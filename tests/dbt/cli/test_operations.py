from pathlib import Path
import pytest
from sqlmesh_dbt.operations import create
from sqlmesh.utils import yaml
from sqlmesh.utils.errors import SQLMeshError
import time_machine

pytestmark = pytest.mark.slow


def test_create_sets_and_persists_default_start_date(jaffle_shop_duckdb: Path):
    with time_machine.travel("2020-01-02 00:00:00 UTC"):
        from sqlmesh.utils.date import yesterday_ds, to_ds

        assert yesterday_ds() == "2020-01-01"

        operations = create()

        assert operations.context.config.model_defaults.start
        assert to_ds(operations.context.config.model_defaults.start) == "2020-01-01"
        assert all(
            to_ds(model.start) if model.start else None == "2020-01-01"
            for model in operations.context.models.values()
            if not model.kind.is_seed
        )

    # check that the date set on the first invocation persists to future invocations
    from sqlmesh.utils.date import yesterday_ds, to_ds

    assert yesterday_ds() != "2020-01-01"

    operations = create()

    assert operations.context.config.model_defaults.start
    assert to_ds(operations.context.config.model_defaults.start) == "2020-01-01"
    assert all(
        to_ds(model.start) if model.start else None == "2020-01-01"
        for model in operations.context.models.values()
        if not model.kind.is_seed
    )


def test_create_uses_configured_start_date_if_supplied(jaffle_shop_duckdb: Path):
    sqlmesh_yaml = jaffle_shop_duckdb / "sqlmesh.yml"

    with sqlmesh_yaml.open("w") as f:
        yaml.dump({"model_defaults": {"start": "2023-12-12"}}, f)

    operations = create()

    assert operations.context.config.model_defaults.start == "2023-12-12"
    assert all(
        model.start == "2023-12-12"
        for model in operations.context.models.values()
        if not model.kind.is_seed
    )


def test_create_can_specify_profile_and_target(jaffle_shop_duckdb: Path):
    with pytest.raises(SQLMeshError, match=r"Profile 'foo' not found"):
        create(profile="foo")

    with pytest.raises(
        SQLMeshError, match=r"Target 'prod' not specified in profiles for 'jaffle_shop'"
    ):
        create(profile="jaffle_shop", target="prod")

    dbt_project = create(profile="jaffle_shop", target="dev").project

    assert dbt_project.context.profile_name == "jaffle_shop"
    assert dbt_project.context.target_name == "dev"


def test_create_can_set_project_variables(jaffle_shop_duckdb: Path):
    (jaffle_shop_duckdb / "models" / "test_model.sql").write_text("""
       select '{{ var('foo') }}' as a
    """)

    dbt_project = create(vars={"foo": "bar"})
    assert dbt_project.context.config.variables["foo"] == "bar"

    test_model = dbt_project.context.models['"jaffle_shop"."main"."test_model"']
    query = test_model.render_query()
    assert query is not None
    assert query.sql() == "SELECT 'bar' AS \"a\""
