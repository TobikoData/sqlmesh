import typing as t

import pytest
from pytest_mock.plugin import MockerFixture

from sqlmesh.core.dialect import normalize_model_name
from sqlmesh.core.engine_adapter import SnowflakeEngineAdapter

pytestmark = [pytest.mark.engine, pytest.mark.snowflake]


def test_get_temp_table(mocker: MockerFixture, make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(SnowflakeEngineAdapter)

    mocker.patch("sqlmesh.core.engine_adapter.base.random_id", return_value="abcdefgh")

    value = adapter._get_temp_table(
        normalize_model_name("catalog.db.test_table", default_catalog=None, dialect=adapter.dialect)
    )

    assert value.sql(dialect=adapter.dialect) == '"CATALOG"."DB"."__temp_TEST_TABLE_abcdefgh"'
