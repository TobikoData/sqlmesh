from __future__ import annotations

from pathlib import Path

import pytest
from pytest_mock.plugin import MockerFixture

from sqlmesh.dbt.context import DbtContext
from sqlmesh.dbt.project import Project
from tests.conftest import delete_cache


@pytest.fixture()
def sushi_test_project(mocker: MockerFixture) -> Project:
    project_root = "tests/fixtures/dbt/sushi_test"
    delete_cache(project_root)
    project = Project.load(DbtContext(project_root=Path(project_root)))
    for package_name, package in project.packages.items():
        project.context.jinja_macros.add_macros(
            package.macro_infos,
            package=package_name if package_name != project.context.project_name else None,
        )
    return project
