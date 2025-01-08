# type: ignore
import typing as t
from unittest.mock import call

import pytest
from sqlglot import parse_one
from sqlmesh.core.engine_adapter.risingwave import RisingwaveEngineAdapter

pytestmark = [pytest.mark.engine, pytest.mark.risingwave]


@pytest.fixture
def adapter(make_mocked_engine_adapter):
    adapter = make_mocked_engine_adapter(RisingwaveEngineAdapter)
    return adapter


def test_create_view(adapter: t.Callable):
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


def test_drop_view(adapter: t.Callable):
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
