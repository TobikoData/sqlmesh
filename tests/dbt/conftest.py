from __future__ import annotations

import typing as t
import os
from pathlib import Path

import pytest

from sqlmesh.core.context import Context
from sqlmesh.core.selector import DbtSelector
from sqlmesh.dbt.context import DbtContext
from sqlmesh.dbt.project import Project
from sqlmesh.dbt.target import PostgresConfig
from sqlmesh_dbt.operations import init_project_if_required
import uuid


class EmptyProjectCreator(t.Protocol):
    def __call__(
        self,
        project_name: t.Optional[str] = None,
        target_name: t.Optional[str] = None,
        start: t.Optional[str] = None,
    ) -> t.Tuple[Path, Path]: ...


@pytest.fixture()
def sushi_test_project(sushi_test_dbt_context: Context) -> Project:
    return sushi_test_dbt_context._loaders[0]._load_projects()[0]  # type: ignore


@pytest.fixture
def create_empty_project(
    copy_to_temp_path: t.Callable[..., t.List[Path]],
) -> t.Iterable[EmptyProjectCreator]:
    default_project_name = f"test_{str(uuid.uuid4())[:8]}"
    default_target_name = "duckdb"
    fixture_path = Path(__file__).parent.parent / "fixtures" / "dbt" / "empty_project"
    assert fixture_path.exists()

    current_path = os.getcwd()

    def _create_empty_project(
        project_name: t.Optional[str] = None,
        target_name: t.Optional[str] = None,
        start: t.Optional[str] = None,
    ) -> t.Tuple[Path, Path]:
        project_name = project_name or default_project_name
        target_name = target_name or default_target_name
        output_path = copy_to_temp_path(paths=fixture_path)[0]

        dbt_project_yml = output_path / "dbt_project.yml"
        profiles_yml = output_path / "profiles.yml"

        assert dbt_project_yml.exists()
        assert profiles_yml.exists()

        models_path = output_path / "models"
        (models_path).mkdir()
        (output_path / "seeds").mkdir()

        dbt_project_yml.write_text(
            dbt_project_yml.read_text().replace("empty_project", project_name)
        )
        profiles_yml.write_text(
            profiles_yml.read_text()
            .replace("empty_project", project_name)
            .replace("__DEFAULT_TARGET__", target_name)
        )

        init_project_if_required(output_path, start)

        # so that we can invoke commands from the perspective of a user that is already in the correct directory
        os.chdir(output_path)

        return output_path, models_path

    yield _create_empty_project

    # cleanup - switch cwd back to original
    os.chdir(current_path)


@pytest.fixture
def jaffle_shop_duckdb(copy_to_temp_path: t.Callable[..., t.List[Path]]) -> t.Iterable[Path]:
    fixture_path = Path(__file__).parent.parent / "fixtures" / "dbt" / "jaffle_shop_duckdb"
    assert fixture_path.exists()

    current_path = os.getcwd()
    output_path = copy_to_temp_path(paths=fixture_path)[0]

    # so that we can invoke commands from the perspective of a user that is alrady in the correct directory
    os.chdir(output_path)

    yield output_path

    os.chdir(current_path)


@pytest.fixture
def jaffle_shop_duckdb_context(jaffle_shop_duckdb: Path) -> Context:
    init_project_if_required(jaffle_shop_duckdb)
    return Context(paths=[jaffle_shop_duckdb], selector=DbtSelector)


@pytest.fixture()
def runtime_renderer() -> t.Callable:
    def create_renderer(context: DbtContext, **kwargs: t.Any) -> t.Callable:
        environment = context.jinja_macros.build_environment(**{**context.jinja_globals, **kwargs})

        def render(value: str) -> str:
            return environment.from_string(value).render()

        return render

    return create_renderer


@pytest.fixture()
def dbt_dummy_postgres_config() -> PostgresConfig:
    return PostgresConfig(  # type: ignore
        name="postgres",
        host="host",
        user="user",
        password="password",
        dbname="dbname",
        port=5432,
        schema="schema",
    )


@pytest.fixture(scope="function", autouse=True)
def reset_dbt_globals():
    # This fixture is used to clear the memoized cache for _get_package_with_retries
    # in dbt.clients.registry. This is necessary because the cache is shared across
    # tests and can cause unexpected behavior if not cleared as some tests depend on
    # the deprecation warning that _get_package_with_retries fires
    yield
    # https://github.com/dbt-labs/dbt-core/blob/main/tests/functional/conftest.py#L9
    try:
        from dbt.clients.registry import _get_cached

        _get_cached.cache = {}
    except Exception:
        pass
    # https://github.com/dbt-labs/dbt-core/blob/main/core/dbt/tests/util.py#L82
    try:
        from dbt_common.events.functions import reset_metadata_vars

        reset_metadata_vars()
    except Exception:
        pass
