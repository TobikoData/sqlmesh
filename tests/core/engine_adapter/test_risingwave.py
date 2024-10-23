# type: ignore
import typing as t
from unittest.mock import call

import pytest
from sqlglot import parse_one
from sqlmesh.core.engine_adapter.risingwave import RisingwaveEngineAdapter
from sqlmesh.core.model.risingwavesink import PropertiesSettings, RwSinkSettings, FormatSettings

pytestmark = [pytest.mark.engine, pytest.mark.postgres, pytest.mark.redshift]


def test_create_view(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(RisingwaveEngineAdapter)

    adapter.create_view("db.view", parse_one("SELECT 1"), replace=True)
    adapter.create_view("db.view", parse_one("SELECT 1"), replace=False)

    adapter.cursor.execute.assert_has_calls(
        [
            # 1st call
            call('DROP VIEW IF EXISTS "db"."view" CASCADE'),
            call('CREATE VIEW "db"."view" AS SELECT 1'),
            # 2nd call
            call('CREATE VIEW "db"."view" AS SELECT 1'),
        ]
    )


def test_create_sink(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(RisingwaveEngineAdapter)

    properties_settings = PropertiesSettings(
        connector="kafka",
        topic="my_topic",
    )
    format_settings = FormatSettings(
        format="json",
    )
    rwsink_settings = RwSinkSettings(
        properties=properties_settings,
        format=format_settings,
    )

    # view_name = 'sqlmesh__sqlmesh.sqlmesh__mv_sales_test01__3707292608'
    view_name = "db.sink"
    adapter.create_view(
        view_name, parse_one("SELECT 1"), replace=True, sink=True, connections_str=rwsink_settings
    )
    #@TODO: Fix this test
    print("has following calls:", adapter.cursor.execute.mock_calls, flush=True)
    # adapter.cursor.execute.assert_has_calls(
    #     [
    #         # 1st call
    #         call('DROP VIEW IF EXISTS "sqlmesh__sqlmesh"."sqlmesh__mv_sales_test01__3707292608" CASCADE'),
    #         call('CREATE VIEW "sqlmesh__sqlmesh"."sqlmesh__mv_sales_test01__3707292608" AS SELECT 1'),
    #     ]
    # )


def test_drop_view(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(RisingwaveEngineAdapter)
    adapter.SUPPORTS_MATERIALIZED_VIEWS = True

    adapter.drop_view("db.view")
    adapter.drop_view("db.view", materialized=True)
    adapter.drop_view("db.view", cascade=False)

    adapter.cursor.execute.assert_has_calls(
        [
            # 1st call
            call('DROP VIEW IF EXISTS "db"."view" CASCADE'),
            # 2nd call
            call('DROP MATERIALIZED VIEW IF EXISTS "db"."view" CASCADE'),
            # 3rd call
            call('DROP VIEW IF EXISTS "db"."view"'),
        ]
    )
