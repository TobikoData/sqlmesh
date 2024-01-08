import typing as t

import pytest

from sqlmesh.core.engine_adapter import TrinoEngineAdapter
from tests.core.engine_adapter import to_sql_calls

pytestmark = [pytest.mark.engine, pytest.mark.trino]


def test_set_current_catalog(make_mocked_engine_adapter: t.Callable, duck_conn):
    adapter = make_mocked_engine_adapter(TrinoEngineAdapter)
    adapter.set_current_catalog("test_catalog")

    assert to_sql_calls(adapter) == [
        'USE "test_catalog"."information_schema"',
    ]
