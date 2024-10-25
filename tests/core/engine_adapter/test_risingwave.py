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

    sink_name = "db.sink"
    view_name = "db.view"
    adapter._create_rw_sink(sink_name, view_name, rwsink_settings, _is_sink_need_drop=False)
    adapter._create_rw_sink(sink_name, view_name, rwsink_settings, _is_sink_need_drop=True)
    print("has following calls:", adapter.cursor.execute.mock_calls, flush=True)
    adapter.cursor.execute.assert_has_calls(
        [
            # 1st call
            call(
                "CREATE SINK IF NOT EXISTS db.sink FROM db.view \nWITH (\n\tconnector='kafka',\n\ttopic='my_topic'\n) FORMAT json \tschema_registry_name_strategy='None',\n \tschema_registry='None',\n \tforce_append_only='None'\n)"
            ),
            # 2nd call
            call("DROP SINK IF EXISTS db.sink"),
            call(
                "CREATE SINK IF NOT EXISTS db.sink FROM db.view \nWITH (\n\tconnector='kafka',\n\ttopic='my_topic'\n) FORMAT json \tschema_registry_name_strategy='None',\n \tschema_registry='None',\n \tforce_append_only='None'\n)"
            ),
        ]
    )


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
