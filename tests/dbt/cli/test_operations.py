from pathlib import Path
import pytest
from sqlmesh_dbt.operations import create
from sqlmesh.utils import yaml
import time_machine

pytestmark = pytest.mark.slow


def test_create_injects_default_start_date(jaffle_shop_duckdb: Path):
    with time_machine.travel("2020-01-02 00:00:00 UTC"):
        from sqlmesh.utils.date import yesterday_ds

        assert yesterday_ds() == "2020-01-01"

        operations = create()

        assert operations.context.config.model_defaults.start == "2020-01-01"
        assert all(
            model.start == "2020-01-01"
            for model in operations.context.models.values()
            if not model.kind.is_seed
        )

    # check that the date set on the first invocation persists to future invocations
    from sqlmesh.utils.date import yesterday_ds

    assert yesterday_ds() != "2020-01-01"

    operations = create()

    assert operations.context.config.model_defaults.start == "2020-01-01"
    assert all(
        model.start == "2020-01-01"
        for model in operations.context.models.values()
        if not model.kind.is_seed
    )


def test_create_uses_configured_start_date_if_supplied(jaffle_shop_duckdb: Path):
    dbt_project_yaml = jaffle_shop_duckdb / "dbt_project.yml"

    contents = yaml.load(dbt_project_yaml, render_jinja=False)

    contents["models"]["+start"] = "2023-12-12"

    with dbt_project_yaml.open("w") as f:
        yaml.dump(contents, f)

    operations = create()

    assert operations.context.config.model_defaults.start == "2023-12-12"
    assert all(
        model.start == "2023-12-12"
        for model in operations.context.models.values()
        if not model.kind.is_seed
    )
