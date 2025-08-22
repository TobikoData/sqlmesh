from __future__ import annotations

import typing as t

import pytest

from sqlmesh.core.context import Context
from sqlmesh.dbt.context import DbtContext
from sqlmesh.dbt.project import Project
from sqlmesh.dbt.target import PostgresConfig


@pytest.fixture()
def sushi_test_project(sushi_test_dbt_context: Context) -> Project:
    return sushi_test_dbt_context._loaders[0]._load_projects()[0]  # type: ignore


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
