from unittest.mock import call

from pytest_mock.plugin import MockerFixture
from sqlglot import expressions as exp
from sqlglot import parse_one

from sqlmesh.core.engine_adapter import BigQueryEngineAdapter


def test_insert_overwrite_by_time_partition(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = BigQueryEngineAdapter(lambda: connection_mock)  # type: ignore
    adapter.insert_overwrite_by_time_partition(
        "test_table",
        parse_one("SELECT a, ds FROM tbl"),  # type: ignore
        start="2022-01-01",
        end="2022-01-05",
        time_formatter=lambda x: exp.Literal.string(x.strftime("%Y-%m-%d")),  # type: ignore
        time_column="ds",
        columns_to_types={
            "a": exp.DataType.build("int"),
            "ds": exp.DataType.build("string"),
        },
    )

    cursor_mock.execute.assert_has_calls(
        [
            call(
                "DELETE FROM `test_table` WHERE `ds` BETWEEN '2022-01-01' AND '2022-01-05'",
                job_config=None,
            ),
            call(
                "INSERT INTO `test_table` (`a`, `ds`) SELECT `a`, `ds` FROM `tbl`",
                job_config=None,
            ),
        ]
    )
