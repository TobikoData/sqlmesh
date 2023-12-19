from __future__ import annotations

import typing as t

import pytest

from sqlmesh.core.context import Context
from sqlmesh.dbt.context import DbtContext
from sqlmesh.dbt.loader import DbtLoader
from sqlmesh.dbt.project import Project


@pytest.fixture()
def sushi_test_project(sushi_test_dbt_context: Context) -> Project:
    project = t.cast(DbtLoader, sushi_test_dbt_context._loader)._project
    assert project
    return project


@pytest.fixture()
def runtime_renderer() -> t.Callable:
    def create_renderer(context: DbtContext, **kwargs: t.Any) -> t.Callable:
        environment = context.jinja_macros.build_environment(**context.jinja_globals, **kwargs)

        def render(value: str) -> str:
            return environment.from_string(value).render()

        return render

    return create_renderer
