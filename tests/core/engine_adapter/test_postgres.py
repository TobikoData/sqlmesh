import typing as t

import pytest
from pytest_mock import MockFixture
from sqlglot.helper import ensure_list

from sqlmesh.core.engine_adapter import PostgresEngineAdapter
from sqlmesh.utils.errors import UnsupportedCatalogOperationError
from tests.core.engine_adapter import to_sql_calls


@pytest.mark.parametrize(
    "kwargs, expected",
    [
        (
            {
                "schema_name": "test_schema",
            },
            'DROP SCHEMA IF EXISTS "test_schema"',
        ),
        (
            {
                "schema_name": "test_schema",
                "ignore_if_not_exists": False,
            },
            'DROP SCHEMA "test_schema"',
        ),
        (
            {
                "schema_name": "test_schema",
                "cascade": True,
            },
            'DROP SCHEMA IF EXISTS "test_schema" CASCADE',
        ),
        (
            {
                "schema_name": "test_schema",
                "cascade": True,
                "ignore_if_not_exists": False,
            },
            'DROP SCHEMA "test_schema" CASCADE',
        ),
    ],
)
def test_drop_schema(kwargs, expected, make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(PostgresEngineAdapter)

    adapter.drop_schema(**kwargs)

    assert to_sql_calls(adapter) == ensure_list(expected)


def test_drop_schema_with_catalog(make_mocked_engine_adapter: t.Callable, mocker: MockFixture):
    adapter = make_mocked_engine_adapter(PostgresEngineAdapter)

    adapter.get_current_catalog = mocker.MagicMock(return_value="test_catalog")

    with pytest.raises(UnsupportedCatalogOperationError):
        adapter.drop_schema("test_catalog.test_schema")
