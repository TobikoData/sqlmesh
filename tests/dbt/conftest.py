from __future__ import annotations

from pathlib import Path

import pytest
from pytest_mock.plugin import MockerFixture

from sqlmesh.dbt.common import DbtContext
from sqlmesh.dbt.project import Project


@pytest.fixture()
def sushi_test_project(mocker: MockerFixture) -> Project:
    project = Project.load(DbtContext(project_root=Path("tests/fixtures/dbt/sushi_test")))
    for package_name, package in project.packages.items():
        project.context.jinja_macros.add_macros(
            package.macros,
            package=package_name if package_name != project.context.project_name else None,
        )
    return project
