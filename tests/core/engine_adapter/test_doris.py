import typing as t

import pytest
from pytest_mock import MockFixture
from sqlglot import exp

from sqlmesh.core.engine_adapter.doris import DorisEngineAdapter
from tests.core.engine_adapter import to_sql_calls


pytestmark = pytest.mark.doris


@pytest.fixture
def make_mocked_engine_adapter(mocker: MockFixture) -> t.Callable[..., DorisEngineAdapter]:
    def _make(server_version: t.Optional[str] = None) -> DorisEngineAdapter:
        connection_mock = mocker.MagicMock()
        cursor_mock = mocker.MagicMock()
        connection_mock.cursor.return_value = cursor_mock

        adapter = DorisEngineAdapter(lambda: connection_mock, "doris")
        return adapter

    return _make


@pytest.mark.parametrize(
    "kwargs, expected",
    [
        (
            {"schema_name": "test_schema"},
            "DROP DATABASE IF EXISTS `test_schema`",
        ),
        (
            {"schema_name": "test_schema", "ignore_if_not_exists": False},
            "DROP DATABASE `test_schema`",
        ),
        (
            {"schema_name": "test_schema", "cascade": True},
            "DROP DATABASE IF EXISTS `test_schema`",
        ),
        (
            {"schema_name": "test_schema", "cascade": True, "ignore_if_not_exists": False},
            "DROP DATABASE `test_schema`",
        ),
    ],
)
def test_drop_schema(
    kwargs: t.Dict[str, t.Any],
    expected: str,
    make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter],
) -> None:
    adapter = make_mocked_engine_adapter()

    adapter.drop_schema(**kwargs)

    sql_calls = to_sql_calls(adapter)
    assert sql_calls == [expected]


@pytest.mark.parametrize(
    "kwargs, expected",
    [
        (
            {
                "table_name": "test_table",
                "index_name": "test_index",
                "columns": ("a",),
                "index_type": "INVERTED",
            },
            "CREATE INDEX IF NOT EXISTS test_index ON test_table (`a`) USING INVERTED",
        ),
        (
            {
                "table_name": "test_table",
                "index_name": "test_index",
                "columns": ("a", "b"),
                "index_type": "BLOOMFILTER",
                "comment": "test comment",
            },
            "CREATE INDEX IF NOT EXISTS test_index ON test_table (`a`, `b`) USING BLOOMFILTER COMMENT 'test comment'",
        ),
        (
            {
                "table_name": "test_table",
                "index_name": "test_index",
                "columns": ("a",),
                "index_type": "NGRAM_BF",
                "properties": {"gram_size": "4", "bf_size": "2048"},
            },
            'CREATE INDEX IF NOT EXISTS test_index ON test_table (`a`) USING NGRAM_BF PROPERTIES ("gram_size" = "4", "bf_size" = "2048")',
        ),
    ],
)
def test_create_index(
    kwargs: t.Dict[str, t.Any],
    expected: str,
    make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter],
) -> None:
    adapter = make_mocked_engine_adapter()

    adapter.create_index(**kwargs)

    sql_calls = to_sql_calls(adapter)
    assert sql_calls[0].startswith(expected)


def test_create_table_with_comments(
    make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter],
) -> None:
    adapter = make_mocked_engine_adapter()

    adapter.create_table(
        "test_table",
        {"a": exp.DataType.build("INT"), "b": exp.DataType.build("VARCHAR")},
        table_description="test table comment",
        column_descriptions={"a": "test column comment"},
    )

    sql_calls = to_sql_calls(adapter)
    assert sql_calls == [
        "CREATE TABLE IF NOT EXISTS `test_table` (\n  `a` INT COMMENT 'test column comment',\n  `b` VARCHAR\n) COMMENT 'test table comment'"
    ]


def test_merge(make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter]) -> None:
    adapter = make_mocked_engine_adapter()

    adapter.merge(
        target_table="target",
        source_table=exp.to_table("source"),
        columns_to_types={
            "id": exp.DataType.build("int"),
            "ts": exp.DataType.build("timestamp"),
            "val": exp.DataType.build("int"),
        },
        unique_key=[exp.to_identifier("id")],
    )

    sql_calls = to_sql_calls(adapter)
    assert sql_calls == [
        "INSERT OVERWRITE `target` (`id`, `ts`, `val`) SELECT `id`, `ts`, `val` FROM `source`"
    ]


def test_create_table_like(make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter]) -> None:
    adapter = make_mocked_engine_adapter()

    adapter.create_table_like("target_table", "source_table")
    adapter.cursor.execute.assert_called_once_with(
        "CREATE TABLE IF NOT EXISTS `target_table` LIKE `source_table`"
    )


def test_comment_truncation(
    make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter],
) -> None:
    adapter = make_mocked_engine_adapter()
    allowed_table_comment_length = DorisEngineAdapter.MAX_TABLE_COMMENT_LENGTH
    truncated_table_comment = "a" * allowed_table_comment_length
    long_table_comment = truncated_table_comment + "b"

    allowed_column_comment_length = DorisEngineAdapter.MAX_COLUMN_COMMENT_LENGTH
    truncated_column_comment = "c" * allowed_column_comment_length
    long_column_comment = truncated_column_comment + "d"

    adapter.create_table(
        "test_table",
        {"a": exp.DataType.build("INT"), "b": exp.DataType.build("INT")},
        table_description=long_table_comment,
        column_descriptions={"a": long_column_comment},
    )

    sql_calls = to_sql_calls(adapter)
    # Table and column comments should be truncated to Doris limits
    assert any(truncated_table_comment in sql for sql in sql_calls)
    assert any(truncated_column_comment in sql for sql in sql_calls)
