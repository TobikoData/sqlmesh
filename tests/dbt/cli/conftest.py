import typing as t
from pathlib import Path
import os
import functools
from click.testing import CliRunner, Result
from sqlmesh_dbt.operations import init_project_if_required
import pytest
import uuid


class EmptyProjectCreator(t.Protocol):
    def __call__(
        self, project_name: t.Optional[str] = None, target_name: t.Optional[str] = None
    ) -> Path: ...


@pytest.fixture
def jaffle_shop_duckdb(copy_to_temp_path: t.Callable[..., t.List[Path]]) -> t.Iterable[Path]:
    fixture_path = Path(__file__).parent / "fixtures" / "jaffle_shop_duckdb"
    assert fixture_path.exists()

    current_path = os.getcwd()
    output_path = copy_to_temp_path(paths=fixture_path)[0]

    # so that we can invoke commands from the perspective of a user that is already in the correct directory
    os.chdir(output_path)

    yield output_path

    os.chdir(current_path)


@pytest.fixture
def create_empty_project(
    copy_to_temp_path: t.Callable[..., t.List[Path]],
) -> t.Iterable[t.Callable[..., Path]]:
    default_project_name = f"test_{str(uuid.uuid4())[:8]}"
    default_target_name = "duckdb"
    fixture_path = Path(__file__).parent / "fixtures" / "empty_project"
    assert fixture_path.exists()

    current_path = os.getcwd()

    def _create_empty_project(
        project_name: t.Optional[str] = None, target_name: t.Optional[str] = None
    ) -> Path:
        project_name = project_name or default_project_name
        target_name = target_name or default_target_name
        output_path = copy_to_temp_path(paths=fixture_path)[0]

        dbt_project_yml = output_path / "dbt_project.yml"
        profiles_yml = output_path / "profiles.yml"

        assert dbt_project_yml.exists()
        assert profiles_yml.exists()

        (output_path / "models").mkdir()
        (output_path / "seeds").mkdir()

        dbt_project_yml.write_text(
            dbt_project_yml.read_text().replace("empty_project", project_name)
        )
        profiles_yml.write_text(
            profiles_yml.read_text()
            .replace("empty_project", project_name)
            .replace("__DEFAULT_TARGET__", target_name)
        )

        init_project_if_required(output_path)

        # so that we can invoke commands from the perspective of a user that is already in the correct directory
        os.chdir(output_path)

        return output_path

    yield _create_empty_project

    # cleanup - switch cwd back to original
    os.chdir(current_path)


@pytest.fixture
def invoke_cli() -> t.Callable[..., Result]:
    from sqlmesh_dbt.cli import dbt

    return functools.partial(CliRunner().invoke, dbt)
