import pytest
from pathlib import Path
from sqlmesh.utils.errors import SQLMeshError
from sqlmesh.cli.project_init import init_example_project, ProjectTemplate
from sqlmesh.utils import yaml
from sqlmesh.core.context import Context
from sqlmesh.core.config.common import VirtualEnvironmentMode


def test_project_init_dbt(tmp_path: Path):
    assert not len(list(tmp_path.glob("**/*")))

    with pytest.raises(SQLMeshError, match=r"Required dbt project file.*not found"):
        init_example_project(path=tmp_path, engine_type=None, template=ProjectTemplate.DBT)

    with (tmp_path / "dbt_project.yml").open("w") as f:
        yaml.dump({"name": "jaffle_shop"}, f)

    init_example_project(path=tmp_path, engine_type=None, template=ProjectTemplate.DBT)
    files = [f for f in tmp_path.glob("**/*") if f.is_file()]

    assert set([f.name for f in files]) == set(["sqlmesh.yaml", "dbt_project.yml"])

    sqlmesh_config = next(f for f in files if f.name == "sqlmesh.yaml")
    assert "model_defaults" in sqlmesh_config.read_text()
    assert "start: " in sqlmesh_config.read_text()

    with (tmp_path / "profiles.yml").open("w") as f:
        yaml.dump({"jaffle_shop": {"target": "dev", "outputs": {"dev": {"type": "duckdb"}}}}, f)

    ctx = Context(paths=tmp_path)
    assert ctx.config.model_defaults.start
    assert ctx.config.virtual_environment_mode == VirtualEnvironmentMode.DEV_ONLY
