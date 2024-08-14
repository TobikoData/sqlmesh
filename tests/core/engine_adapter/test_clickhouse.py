import pytest
from sqlmesh.core.engine_adapter import ClickhouseEngineAdapter
from tests.core.engine_adapter import to_sql_calls
from sqlglot import exp

pytestmark = [pytest.mark.clickhouse, pytest.mark.engine]


@pytest.fixture
def adapter(make_mocked_engine_adapter) -> ClickhouseEngineAdapter:
    return make_mocked_engine_adapter(ClickhouseEngineAdapter)


def test_create_schema(adapter: ClickhouseEngineAdapter):
    adapter.create_schema("foo")
    adapter.create_schema("foo", properties=[exp.OnCluster(this=exp.to_identifier("cluster1"))])

    assert to_sql_calls(adapter) == [
        'CREATE DATABASE IF NOT EXISTS "foo"',
        # will fail until https://github.com/tobymao/sqlglot/commit/c22f41129985ecfd3b3906b9594ca1692b91708c makes it to a sqlglot release
        'CREATE DATABASE IF NOT EXISTS "foo" ON CLUSTER "cluster1"',
    ]


def test_drop_schema(adapter: ClickhouseEngineAdapter):
    adapter.drop_schema("foo")
    adapter.drop_schema("foo", cluster=exp.OnCluster(this=exp.to_identifier("cluster1")))

    assert to_sql_calls(adapter) == [
        'DROP DATABASE IF EXISTS "foo"',
        'DROP DATABASE IF EXISTS "foo" ON CLUSTER "cluster1"',
    ]
