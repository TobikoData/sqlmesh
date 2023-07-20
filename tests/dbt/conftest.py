from __future__ import annotations

import typing as t
from pathlib import Path

import pytest

from sqlmesh.dbt.context import DbtContext
from sqlmesh.dbt.project import Project
from tests.conftest import delete_cache


@pytest.fixture()
def sushi_test_project() -> Project:
    project_root = "tests/fixtures/dbt/sushi_test"
    delete_cache(project_root)
    project = Project.load(DbtContext(project_root=Path(project_root)))
    for package_name, package in project.packages.items():
        project.context.jinja_macros.add_macros(package.macro_infos, package=package_name)
    return project


@pytest.fixture()
def runtime_renderer() -> t.Callable:
    def create_renderer(context: DbtContext, **kwargs: t.Any) -> t.Callable:
        environment = context.jinja_macros.build_environment(**context.jinja_globals, **kwargs)

        def render(value: str) -> str:
            return environment.from_string(value).render()

        return render

    return create_renderer
