from __future__ import annotations

import typing as t

import pytest

from sqlmesh.core.context import Context
from sqlmesh.dbt.context import DbtContext
from sqlmesh.dbt.project import Project


@pytest.fixture()
def sushi_test_project(sushi_test_dbt_context: Context) -> Project:
    return sushi_test_dbt_context._loader._load_projects()[0]  # type: ignore


@pytest.fixture()
def runtime_renderer() -> t.Callable:
    def create_renderer(context: DbtContext, **kwargs: t.Any) -> t.Callable:
        environment = context.jinja_macros.build_environment(**context.jinja_globals, **kwargs)

        def render(value: str) -> str:
            return environment.from_string(value).render()

        return render

    return create_renderer
