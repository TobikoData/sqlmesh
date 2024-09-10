import logging
import pathlib
import typing as t
from unittest.mock import MagicMock

import pytest
from bs4 import BeautifulSoup
from freezegun import freeze_time
from hyperscript import h
from IPython.core.error import UsageError
from IPython.testing.globalipapp import start_ipython
from IPython.utils.capture import CapturedIO, capture_output
from pytest_mock.plugin import MockerFixture
from rich.console import Console as RichConsole

from sqlmesh import Context, RuntimeEnv
from sqlmesh.magics import register_magics

logger = logging.getLogger(__name__)


SUSHI_EXAMPLE_PATH = pathlib.Path("./examples/sushi")
SUCCESS_STYLE = "color: #008000; text-decoration-color: #008000"
NEUTRAL_STYLE = "color: #008080; text-decoration-color: #008080"
RICH_PRE_STYLE = "white-space:pre;overflow-x:auto;line-height:normal;font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace"
FREEZE_TIME = "2023-01-01 00:00:00"

pytestmark = pytest.mark.jupyter


@pytest.fixture(scope="session")
def ip():
    logger.root.handlers.clear()
    return start_ipython()


@pytest.fixture
def notebook(mocker: MockerFixture, ip):
    mocker.patch("sqlmesh.RuntimeEnv.get", MagicMock(side_effect=lambda: RuntimeEnv.JUPYTER))
    mocker.patch(
        "sqlmesh.core.console.RichConsole",
        MagicMock(return_value=RichConsole(force_jupyter=True, color_system="truecolor")),
    )
    register_magics()
    return ip


@pytest.fixture
def sushi_context(copy_to_temp_path, notebook, tmp_path) -> Context:
    sushi_dir = copy_to_temp_path(SUSHI_EXAMPLE_PATH)[0]
    notebook.run_line_magic(
        magic_name="context", line=f"{str(sushi_dir)} --log-file-dir {tmp_path}"
    )
    return notebook.user_ns["context"]


@pytest.fixture
@freeze_time(FREEZE_TIME)
def loaded_sushi_context(sushi_context) -> Context:
    with capture_output():
        sushi_context.plan(no_prompts=True, auto_apply=True)
    return sushi_context


@pytest.fixture
def convert_all_html_output_to_text():
    def _convert(output: CapturedIO) -> t.List[str]:
        return [
            BeautifulSoup(output.data["text/html"]).get_text().strip() for output in output.outputs
        ]

    return _convert


@pytest.fixture
def convert_all_html_output_to_tags():
    def _convert_html_to_tags(html: str) -> t.List[str]:
        # BS4 automatically adds html and body tags so we remove those since they are not actually part of the output
        return [
            tag.name
            for tag in BeautifulSoup(html, "html").find_all()
            if tag.name not in {"html", "body"}
        ]

    def _convert(output: CapturedIO) -> t.List[t.List[str]]:
        return [_convert_html_to_tags(output.data["text/html"]) for output in output.outputs]

    return _convert


@pytest.fixture
def get_all_html_output():
    def _convert(output: CapturedIO) -> t.List[str]:
        # We remove newlines to make it easier to compare the output to expected output
        return [output.data["text/html"].replace("\n", "") for output in output.outputs]

    return _convert


def test_context(notebook, convert_all_html_output_to_text, get_all_html_output, tmp_path):
    with capture_output() as output:
        notebook.run_line_magic(
            magic_name="context", line=f"{str(SUSHI_EXAMPLE_PATH)}  --log-file-dir {tmp_path}"
        )

    assert output.stdout == ""
    assert output.stderr == ""
    assert len(output.outputs) == 1
    assert convert_all_html_output_to_text(output) == [
        "SQLMesh project context set to: examples/sushi"
    ]
    assert get_all_html_output(output) == [
        str(
            h(
                "pre",
                {"style": RICH_PRE_STYLE},
                h(
                    "span",
                    {"style": SUCCESS_STYLE},
                    "SQLMesh project context set to: examples/sushi",
                    autoescape=False,
                ),
                autoescape=False,
            )
        )
    ]


def test_init(tmp_path, notebook, convert_all_html_output_to_text, get_all_html_output):
    with pytest.raises(UsageError, match="the following arguments are required: path"):
        notebook.run_line_magic(magic_name="init", line="")
    with pytest.raises(UsageError, match="the following arguments are required: sql_dialect"):
        notebook.run_line_magic(magic_name="init", line="foo")
    with capture_output() as output:
        notebook.run_line_magic(magic_name="init", line=f"{tmp_path} duckdb")

    assert output.stdout == ""
    assert output.stderr == ""
    assert len(output.outputs) == 1
    assert convert_all_html_output_to_text(output) == ["SQLMesh project scaffold created"]
    assert get_all_html_output(output) == [
        str(
            h(
                "div",
                h(
                    "span",
                    {"style": "color: green; font-weight: bold"},
                    "SQLMesh project scaffold created",
                    autoescape=False,
                ),
                autoescape=False,
            )
        )
    ]


@pytest.mark.slow
def test_render(
    notebook, sushi_context, convert_all_html_output_to_text, convert_all_html_output_to_tags
):
    with capture_output() as output:
        notebook.run_line_magic(magic_name="render", line="sushi.top_waiters")

    assert output.stdout == ""
    assert output.stderr == ""
    assert len(output.outputs) == 1
    assert len(convert_all_html_output_to_text(output)[0]) > 2200
    assert len(convert_all_html_output_to_tags(output)[0]) > 150


@pytest.mark.slow
def test_render_no_format(
    notebook, sushi_context, convert_all_html_output_to_text, convert_all_html_output_to_tags
):
    with capture_output() as output:
        notebook.run_line_magic(magic_name="render", line="sushi.top_waiters --no-format")

    assert output.stdout == ""
    assert output.stderr == ""
    assert len(output.outputs) == 1
    assert len(convert_all_html_output_to_text(output)[0]) >= 700
    assert len(convert_all_html_output_to_tags(output)[0]) >= 50


@pytest.mark.slow
@freeze_time(FREEZE_TIME)
def test_evaluate(notebook, loaded_sushi_context):
    with capture_output() as output:
        notebook.run_line_magic(magic_name="evaluate", line="sushi.top_waiters")

    assert not output.stdout
    assert not output.stderr
    assert len(output.outputs) == 1
    # TODO: Add console method to display pandas dataframe and then use that instead of calling display directly
    assert not output.outputs[0].data.get("text/html")
    # Since there is no ordering in the query, we can't assert the exact output, but we can assert the columns
    # and it starts with the index
    assert output.outputs[0].data["text/plain"].startswith("   waiter_id  revenue\n0")


def test_format(notebook, sushi_context):
    with capture_output():
        test_model_path = sushi_context.path / "models" / "test_model.sql"
        test_model_path.write_text("MODEL(name db.test); SELECT 1 AS foo FROM t")
        sushi_context.load()
    assert test_model_path.read_text() == "MODEL(name db.test); SELECT 1 AS foo FROM t"
    with capture_output() as output:
        notebook.run_line_magic(magic_name="format", line="")

    assert not output.stdout
    assert not output.stderr
    assert len(output.outputs) == 0
    assert (
        test_model_path.read_text()
        == """MODEL (
  name db.test
);

SELECT
  1 AS foo
FROM t"""
    )


@pytest.mark.slow
def test_diff(sushi_context, notebook, convert_all_html_output_to_text, get_all_html_output):
    with capture_output():
        test_model_path = sushi_context.path / "models" / "test_model.sql"
        test_model_path.write_text("MODEL(name sqlmesh_example.test); SELECT 1 AS foo")
        sushi_context.load()
        notebook.run_line_magic(magic_name="plan", line="--no-prompts --auto-apply")
    test_model_path.write_text("MODEL(name sqlmesh_example.test); SELECT 1 AS foo, 2 AS bar")
    with capture_output() as output:
        notebook.run_line_magic(magic_name="diff", line="prod")

    assert not output.stdout
    assert not output.stderr
    assert len(output.outputs) == 2
    assert convert_all_html_output_to_text(output) == [
        "Summary of differences against `prod`:",
        "Models:\n└── Directly Modified:\n    └── sqlmesh_example.test",
    ]
    assert get_all_html_output(output) == [
        str(
            h(
                "pre",
                {"style": RICH_PRE_STYLE},
                h(
                    "span",
                    {"style": "font-weight: bold"},
                    "Summary of differences against `prod`:",
                    autoescape=False,
                ),
                autoescape=False,
            )
        ),
        str(
            h(
                "pre",
                {"style": RICH_PRE_STYLE},
                str(
                    h(
                        "span",
                        {"style": "font-weight: bold"},
                        "Models:",
                        autoescape=False,
                    )
                )
                + "└── "
                + str(
                    h(
                        "span",
                        {"style": "font-weight: bold"},
                        "Directly Modified:",
                        autoescape=False,
                    )
                )
                + "    └── sqlmesh_example.test",
                autoescape=False,
            )
        ),
    ]


@pytest.mark.slow
def test_plan(
    notebook, sushi_context, convert_all_html_output_to_text, convert_all_html_output_to_tags
):
    with capture_output() as output:
        notebook.run_line_magic(magic_name="plan", line="--no-prompts --auto-apply")

    # TODO: Should this be going to stdout? This is printing the status updates for when each batch finishes for
    # the models and how long it took
    assert len(output.stdout.strip().split("\n")) == 22
    assert not output.stderr
    assert len(output.outputs) == 4
    text_output = convert_all_html_output_to_text(output)
    # TODO: Is this what we expect?
    # This has minor differences between CI/CD and local.
    assert "[2K" in text_output[0]
    assert text_output[1].startswith(
        "Virtually Updating 'prod' ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0%"
    )
    # TODO: Is this what we expect?
    assert text_output[2] == ""
    assert text_output[3] == "The target environment has been updated successfully"
    assert convert_all_html_output_to_tags(output) == [
        ["pre", "span"],
        ["pre"] + ["span"] * 4,
        ["pre"],
        ["pre", "span"],
    ]


@pytest.mark.slow
@freeze_time("2023-01-03 00:00:00")
def test_run_dag(
    notebook, loaded_sushi_context, convert_all_html_output_to_text, get_all_html_output
):
    with capture_output() as output:
        notebook.run_line_magic(magic_name="run_dag", line="")

    assert not output.stdout.startswith(
        "'Evaluating models ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% • 18/18"
    )
    assert not output.stderr
    assert len(output.outputs) == 2
    assert convert_all_html_output_to_text(output) == [
        "All model batches have been executed successfully",
        "Run finished for environment 'prod'",
    ]
    assert get_all_html_output(output) == [
        str(
            h(
                "pre",
                {"style": RICH_PRE_STYLE},
                h(
                    "span",
                    {"style": SUCCESS_STYLE},
                    "All model batches have been executed successfully",
                    autoescape=False,
                ),
                autoescape=False,
            )
        ),
        str(
            h(
                "pre",
                {"style": RICH_PRE_STYLE},
                h(
                    "span",
                    {"style": SUCCESS_STYLE},
                    "Run finished for environment ",
                    autoescape=False,
                ),
                h(
                    "span",
                    {"style": SUCCESS_STYLE},
                    "'prod'",
                    autoescape=False,
                ),
                autoescape=False,
            )
        ),
    ]


@pytest.mark.slow
@freeze_time(FREEZE_TIME)
def test_invalidate(
    notebook, loaded_sushi_context, convert_all_html_output_to_text, get_all_html_output
):
    with capture_output():
        notebook.run_line_magic(
            magic_name="plan", line="dev --include-unmodified --no-prompts --auto-apply"
        )
    with capture_output() as output:
        notebook.run_line_magic(magic_name="invalidate", line="dev")

    assert not output.stdout
    assert not output.stderr
    assert len(output.outputs) == 1
    assert convert_all_html_output_to_text(output) == [
        "Environment 'dev' has been invalidated.",
    ]
    assert get_all_html_output(output) == [
        str(
            h(
                "pre",
                {"style": RICH_PRE_STYLE},
                str(
                    h(
                        "span",
                        {"style": SUCCESS_STYLE},
                        "Environment ",
                        autoescape=False,
                    )
                )
                + str(
                    h(
                        "span",
                        {"style": SUCCESS_STYLE},
                        "'dev'",
                        autoescape=False,
                    )
                )
                + str(
                    h(
                        "span",
                        {"style": SUCCESS_STYLE},
                        " has been invalidated.",
                        autoescape=False,
                    )
                ),
                autoescape=False,
            )
        )
    ]


@pytest.mark.slow
def test_janitor(
    notebook, loaded_sushi_context, convert_all_html_output_to_text, get_all_html_output
):
    with capture_output():
        notebook.run_line_magic(
            magic_name="plan", line="dev --include-unmodified --no-prompts --auto-apply"
        )
        notebook.run_line_magic(magic_name="invalidate", line="dev")

    with capture_output() as output:
        notebook.run_line_magic(magic_name="janitor", line="")

    assert not output.stdout
    assert not output.stderr
    assert convert_all_html_output_to_text(output) == [
        "Deleted object memory.sushi__dev",
        "Cleanup complete.",
    ]


def test_dag(tmp_path_factory, notebook, sushi_context):
    temp_dir = tmp_path_factory.mktemp("dag")
    dag_file = temp_dir / "dag.html"
    with capture_output() as output:
        notebook.run_line_magic(magic_name="dag", line=f"--file {str(dag_file)}")

    assert not output.stdout
    assert not output.stderr
    assert len(output.outputs) == 1
    output_plain = output.outputs[0].data["text/plain"]
    # When we capture output it just captures the repr of the class, but it gets displayed fine in a notebook
    assert "GraphHTML" in output_plain
    file_contents = dag_file.read_text()
    assert '<div id="sqlglot-lineage">' in file_contents
    assert "waiter_revenue_by_day" in file_contents


def test_create_test(notebook, sushi_context):
    with capture_output():
        notebook.run_line_magic(
            magic_name="create_test",
            line='''sushi.top_waiters --query sushi.waiter_revenue_by_day "SELECT 1 as waiter_id"''',
        )

    test_file = sushi_context.path / "tests" / "test_top_waiters.yaml"
    assert test_file.exists()
    assert (
        test_file.read_text()
        == """test_top_waiters:
  model: sushi.top_waiters
  inputs:
    sushi.waiter_revenue_by_day:
    - waiter_id: 1
  outputs:
    query: []
"""
    )


def test_test(notebook, sushi_context):
    """
    Not sure how to test the cell contents. If we can figure that out, then it would be great to test
    starting with a line magic, output the test to the cell, update cell contents, and then run the cell
    and verify the new contents get written to the file. That is a common workflow a user would go through.

    Also easier test is testing the ls command
    """
    with capture_output() as output:
        notebook.run_cell_magic(
            magic_name="test",
            line="sushi.customer_revenue_by_day test_customer_revenue_by_day",
            cell="TESTING",
        )
    assert not output.stdout
    assert not output.stderr
    assert not output.outputs
    test_file = sushi_context.path / "tests" / "test_customer_revenue_by_day.yaml"
    assert test_file.exists()
    assert test_file.read_text() == """test_customer_revenue_by_day: TESTING\n"""


def test_run_test(notebook, sushi_context):
    with capture_output() as output:
        notebook.run_line_magic(
            magic_name="run_test",
            line=f"{sushi_context.path / 'tests' / 'test_customer_revenue_by_day.yaml'}::test_customer_revenue_by_day",
        )

    assert not output.stdout
    # TODO: Does it make sense for this to go to stderr?
    assert "Ran 1 test" in output.stderr
    assert "OK" in output.stderr
    assert not output.outputs


@pytest.mark.slow
def test_audit(notebook, loaded_sushi_context, convert_all_html_output_to_text):
    with capture_output() as output:
        notebook.run_line_magic(magic_name="audit", line="sushi.top_waiters")

    assert not output.stdout
    assert not output.stderr
    assert len(output.outputs) == 4
    assert convert_all_html_output_to_text(output) == [
        "Found 1 audit(s).",
        "unique_values on model sushi.top_waiters ✅ PASS.",
        "Finished with 0 audit errors and 0 audits skipped.",
        "Done.",
    ]


def test_fetchdf(notebook, sushi_context):
    with capture_output() as output:
        notebook.run_cell_magic(magic_name="fetchdf", line="my_result", cell="SELECT 1 AS foo")

    assert not output.stdout
    assert not output.stderr
    assert len(output.outputs) == 1
    assert output.outputs[0].data["text/plain"] == """   foo\n0    1"""
    assert notebook.user_ns["my_result"].to_dict() == {"foo": {0: 1}}


def test_info(notebook, sushi_context, convert_all_html_output_to_text, get_all_html_output):
    with capture_output() as output:
        notebook.run_line_magic(magic_name="info", line="")

    assert not output.stdout
    assert not output.stderr
    assert len(output.outputs) == 3
    assert convert_all_html_output_to_text(output) == [
        "Models: 17",
        "Macros: 6",
        "Data warehouse connection succeeded",
    ]
    assert get_all_html_output(output) == [
        str(
            h(
                "pre",
                {"style": RICH_PRE_STYLE},
                "Models: "
                + str(
                    h(
                        "span",
                        # "color: #008000; text-decoration-color: #008000"
                        {"style": f"{NEUTRAL_STYLE}; font-weight: bold"},
                        "17",
                        autoescape=False,
                    )
                ),
                autoescape=False,
            )
        ),
        str(
            h(
                "pre",
                {"style": RICH_PRE_STYLE},
                "Macros: "
                + str(
                    h(
                        "span",
                        {"style": f"{NEUTRAL_STYLE}; font-weight: bold"},
                        "6",
                        autoescape=False,
                    )
                ),
                autoescape=False,
            )
        ),
        str(
            h(
                "pre",
                {"style": RICH_PRE_STYLE},
                "Data warehouse connection "
                + str(
                    h(
                        "span",
                        {"style": SUCCESS_STYLE},
                        "succeeded",
                        autoescape=False,
                    )
                ),
                autoescape=False,
            )
        ),
    ]


@pytest.mark.slow
def test_migrate(
    notebook, loaded_sushi_context, convert_all_html_output_to_text, get_all_html_output
):
    with capture_output() as output:
        notebook.run_line_magic(magic_name="migrate", line="")

    assert not output.stdout
    assert not output.stderr
    assert len(output.outputs) == 1
    assert convert_all_html_output_to_text(output) == [
        "Migration complete",
    ]
    assert get_all_html_output(output) == [
        str(
            h(
                "pre",
                {"style": RICH_PRE_STYLE},
                h(
                    "span",
                    {"style": SUCCESS_STYLE},
                    "Migration complete",
                    autoescape=False,
                ),
                autoescape=False,
            )
        )
    ]


# TODO: Add test for rollback
# def test_rollback(notebook, loaded_sushi_context):
#     with capture_output() as output:
#         notebook.run_line_magic(magic_name="rollback", line="")
#


@pytest.mark.slow
def test_create_external_models(notebook, loaded_sushi_context):
    external_model_file = loaded_sushi_context.path / "external_models.yaml"
    external_model_file.unlink()
    assert not external_model_file.exists()
    loaded_sushi_context.load()
    with capture_output() as output:
        notebook.run_line_magic(magic_name="create_external_models", line="")

    assert not output.stdout
    assert not output.stderr
    assert not output.outputs
    assert external_model_file.exists()
    assert (
        external_model_file.read_text()
        == """- name: '"memory"."raw"."demographics"'
  columns:
    customer_id: INT
    zip: TEXT
"""
    )


@pytest.mark.slow
@freeze_time(FREEZE_TIME)
def test_table_diff(notebook, loaded_sushi_context, convert_all_html_output_to_text):
    with capture_output():
        loaded_sushi_context.plan("dev", no_prompts=True, auto_apply=True, include_unmodified=True)
    with capture_output() as output:
        notebook.run_line_magic(magic_name="table_diff", line="dev:prod --model sushi.top_waiters")

    assert not output.stdout
    assert not output.stderr
    assert len(output.outputs) == 4
    assert convert_all_html_output_to_text(output) == [
        """Schema Diff Between 'DEV' and 'PROD' environments for model 'sushi.top_waiters':
└── Schemas match""",
        """Row Counts:
└──  FULL MATCH: 8 rows (100.0%)""",
        """COMMON ROWS column comparison stats:""",
        """pct_match
revenue      100.0""",
    ]


@pytest.mark.slow
@freeze_time(FREEZE_TIME)
def test_table_name(notebook, loaded_sushi_context, convert_all_html_output_to_text):
    with capture_output() as output:
        notebook.run_line_magic(magic_name="table_name", line="sushi.orders")

    assert not output.stdout
    assert not output.stderr
    assert len(output.outputs) == 1
    assert convert_all_html_output_to_text(output)[0].startswith(
        "memory.sqlmesh__sushi.sushi__orders__"
    )
