from __future__ import annotations

import typing as t
from collections import defaultdict

from hyperscript import h
from IPython.core.display import HTML, display
from IPython.core.magic import (
    Magics,
    cell_magic,
    line_cell_magic,
    line_magic,
    magics_class,
)
from IPython.core.magic_arguments import argument, magic_arguments, parse_argstring

from sqlmesh.cli.example_project import ProjectTemplate, init_example_project
from sqlmesh.core import constants as c
from sqlmesh.core.console import get_console
from sqlmesh.core.context import Context
from sqlmesh.core.dialect import format_model_expressions, parse
from sqlmesh.core.model import load_sql_based_model
from sqlmesh.core.test import ModelTestMetadata, get_all_model_tests
from sqlmesh.utils import sqlglot_dialects, yaml
from sqlmesh.utils.errors import MagicError, MissingContextException

CONTEXT_VARIABLE_NAMES = [
    "context",
    "ctx",
    "sqlmesh",
]


@magics_class
class SQLMeshMagics(Magics):
    @property
    def display(self) -> t.Callable:
        from sqlmesh import runtime_env

        if runtime_env.is_databricks:
            # Use Databricks' special display instead of the normal IPython display
            return self._shell.user_ns["display"]
        return display

    @property
    def _context(self) -> Context:
        for variable_name in CONTEXT_VARIABLE_NAMES:
            context = self._shell.user_ns.get(variable_name)
            if context:
                return context
        raise MissingContextException(
            f"Context must be defined and initialized with one of these names: {', '.join(CONTEXT_VARIABLE_NAMES)}"
        )

    def success_message(self, messages: t.Dict[str, str]) -> HTML:
        unstyled = messages.get("unstyled")
        msg = str(
            h(
                "div",
                h(
                    "span",
                    messages.get("green-bold"),
                    {"style": {"color": "green", "font-weight": "bold"}},
                ),
                h("span", unstyled) if unstyled else "",
            )
        )
        return HTML(msg)

    @magic_arguments()
    @argument(
        "paths",
        type=str,
        nargs="+",
        default="",
        help="The path(s) to the SQLMesh project(s).",
    )
    @line_magic
    def context(self, line: str) -> None:
        """Sets the context in the user namespace."""
        args = parse_argstring(self.context, line)
        self._shell.user_ns["context"] = Context(paths=args.paths)
        message = self.success_message(
            {
                "green-bold": "SQLMesh project context set to:",
                "unstyled": "<br>&emsp;" + "<br>&emsp;".join(args.paths),
            }
        )

        self.display(message)

    @magic_arguments()
    @argument("path", type=str, help="The path where the new SQLMesh project should be created.")
    @argument(
        "sql_dialect",
        type=str,
        nargs="?",
        default=None,
        help=f"Default model SQL dialect. Supported values: {sqlglot_dialects()}.",
    )
    @argument(
        "--template",
        "-t",
        type=str,
        help="Project template. Supported values: airflow, dbt, default.",
    )
    @line_magic
    def init(self, line: str) -> None:
        """Creates a SQLMesh project scaffold with a default SQL dialect."""
        args = parse_argstring(self.init, line)
        try:
            project_template = ProjectTemplate(
                args.template.lower() if args.template else "default"
            )
        except ValueError:
            raise MagicError(f"Invalid project template '{args.template}'")
        init_example_project(args.path, args.sql_dialect, project_template)
        self.display(self.success_message({"green-bold": "SQLMesh project scaffold created"}))

    @magic_arguments()
    @argument("model", type=str, help="The model.")
    @argument("--start", "-s", type=str, help="Start date to render.")
    @argument("--end", "-e", type=str, help="End date to render.")
    @argument("--execution-time", type=str, help="Execution time.")
    @argument("--dialect", "-d", type=str, help="The rendered dialect.")
    @line_cell_magic
    def model(self, line: str, sql: t.Optional[str] = None) -> None:
        """Renders the model and automatically fills in an editable cell with the model definition."""
        args = parse_argstring(self.model, line)
        model = self._context.get_model(args.model, raise_if_missing=True)

        if sql:
            config = self._context.config_for_model(model)
            loaded = load_sql_based_model(
                parse(sql, default_dialect=config.dialect),
                macros=self._context._macros,
                jinja_macros=self._context._jinja_macros,
                path=model._path,
                dialect=config.dialect,
                time_column_format=config.time_column_format,
                physical_schema_override=self._context.config.physical_schema_override,
            )

            if loaded.name == args.model:
                model = loaded

        self._context.upsert_model(model)
        expressions = model.render_definition(include_python=False)

        formatted = format_model_expressions(expressions, model.dialect)

        self._shell.set_next_input(
            "\n".join(
                [
                    " ".join(["%%model", line]),
                    formatted,
                ]
            ),
            replace=True,
        )

        with open(model._path, "w", encoding="utf-8") as file:
            file.write(formatted)

        if sql:
            self.display(self.success_message({"green-bold": f"Model `{args.model}` updated"}))

        self._context.upsert_model(model)
        self._context.console.show_sql(
            self._context.render(
                model.name,
                start=args.start,
                end=args.end,
                execution_time=args.execution_time,
            ).sql(pretty=True, dialect=args.dialect or model.dialect)
        )

    @magic_arguments()
    @argument("model", type=str, help="The model.")
    @argument("test_name", type=str, nargs="?", default=None, help="The test name to display")
    @argument("--ls", action="store_true", help="List tests associated with a model")
    @line_cell_magic
    def test(self, line: str, test_def_raw: t.Optional[str] = None) -> None:
        """Allow the user to list tests for a model, output a specific test, and then write their changes back"""
        args = parse_argstring(self.test, line)
        if not args.test_name and not args.ls:
            raise MagicError("Must provide either test name or `--ls` to list tests")

        test_meta = []

        for path, config in self._context.configs.items():
            test_meta.extend(
                get_all_model_tests(
                    path / c.TESTS,
                    ignore_patterns=config.ignore_patterns,
                )
            )

        tests: t.Dict[str, t.Dict[str, ModelTestMetadata]] = defaultdict(dict)
        for model_test_metadata in test_meta:
            model = model_test_metadata.body.get("model")
            if not model:
                self._context.console.log_error(
                    f"Test found that does not have `model` defined: {model_test_metadata.path}"
                )
            tests[model][model_test_metadata.test_name] = model_test_metadata

        model = self._context.get_model(args.model, raise_if_missing=True)

        if args.ls:
            # TODO: Provide better UI for displaying tests
            for test_name in tests[model.name]:
                self._context.console.log_status_update(test_name)
            return

        test = tests[model.name][args.test_name]
        test_def = yaml.load(test_def_raw) if test_def_raw else test.body
        test_def_output = yaml.dump(test_def)

        self._shell.set_next_input(
            "\n".join(
                [
                    " ".join(["%%test", line]),
                    test_def_output,
                ]
            ),
            replace=True,
        )

        with open(test.path, "r+", encoding="utf-8") as file:
            content = yaml.load(file.read())
            content[args.test_name] = test_def
            file.seek(0)
            yaml.dump(content, file)
            file.truncate()

    @magic_arguments()
    @argument(
        "environment",
        nargs="?",
        type=str,
        help="The environment to run the plan against",
    )
    @argument("--start", "-s", type=str, help="Start date to backfill.")
    @argument("--end", "-e", type=str, help="End date to backfill.")
    @argument("--execution-time", type=str, help="Execution time.")
    @argument(
        "--create-from",
        type=str,
        help="The environment to create the target environment from if it doesn't exist. Default: prod.",
    )
    @argument(
        "--skip-tests",
        "-t",
        action="store_true",
        help="Skip the unit tests defined for the model.",
    )
    @argument(
        "--restate-model",
        "-r",
        type=str,
        nargs="*",
        help="Restate data for specified models (and models downstream from the one specified). For production environment, all related model versions will have their intervals wiped, but only the current versions will be backfilled. For development environment, only the current model versions will be affected.",
    )
    @argument(
        "--no-gaps",
        "-g",
        action="store_true",
        help="Ensure that new snapshots have no data gaps when comparing to existing snapshots for matching models in the target environment.",
    )
    @argument(
        "--skip-backfill",
        action="store_true",
        help="Skip the backfill step.",
    )
    @argument(
        "--forward-only",
        action="store_true",
        help="Create a plan for forward-only changes.",
    )
    @argument(
        "--effective-from",
        type=str,
        help="The effective date from which to apply forward-only changes on production.",
    )
    @argument(
        "--no-prompts",
        action="store_true",
        help="Disables interactive prompts for the backfill time range. Please note that if this flag is set and there are uncategorized changes, plan creation will fail.",
    )
    @argument(
        "--auto-apply",
        action="store_true",
        help="Automatically applies the new plan after creation.",
    )
    @argument(
        "--no-auto-categorization",
        action="store_true",
        help="Disable automatic change categorization.",
        default=None,
    )
    @argument(
        "--include-unmodified",
        action="store_true",
        help="Include unmodified models in the target environment.",
        default=None,
    )
    @line_magic
    def plan(self, line: str) -> None:
        """Goes through a set of prompts to both establish a plan and apply it"""
        self._context.refresh()
        args = parse_argstring(self.plan, line)

        # Since the magics share a context we want to clear out any state before generating a new plan
        console = self._context.console
        self._context.console = get_console(display=self.display)

        self._context.plan(
            args.environment,
            start=args.start,
            end=args.end,
            execution_time=args.execution_time,
            create_from=args.create_from,
            skip_tests=args.skip_tests,
            restate_models=args.restate_model,
            no_gaps=args.no_gaps,
            skip_backfill=args.skip_backfill,
            forward_only=args.forward_only,
            no_prompts=args.no_prompts,
            auto_apply=args.auto_apply,
            no_auto_categorization=args.no_auto_categorization,
            effective_from=args.effective_from,
            include_unmodified=args.include_unmodified,
        )
        self._context.console = console

    @magic_arguments()
    @argument(
        "environment",
        nargs="?",
        type=str,
        help="The environment to run against",
    )
    @argument("--start", "-s", type=str, help="Start date to evaluate.")
    @argument("--end", "-e", type=str, help="End date to evaluate.")
    @argument("--skip-janitor", action="store_true", help="Skip the janitor task.")
    @argument(
        "--ignore-cron",
        action="store_true",
        help="Run for all missing intervals, ignoring individual cron schedules.",
    )
    @line_magic
    def run_dag(self, line: str) -> None:
        """Evaluate the DAG of models using the built-in scheduler."""
        args = parse_argstring(self.run_dag, line)

        # Since the magics share a context we want to clear out any state before generating a new plan
        console = self._context.console
        self._context.console = get_console(display=self.display)

        self._context.run(
            args.environment,
            start=args.start,
            end=args.end,
            skip_janitor=args.skip_janitor,
            ignore_cron=args.ignore_cron,
        )
        self._context.console = console

    @magic_arguments()
    @argument("model", type=str, help="The model.")
    @argument("--start", "-s", type=str, help="Start date to render.")
    @argument("--end", "-e", type=str, help="End date to render.")
    @argument("--execution-time", type=str, help="Execution time.")
    @argument(
        "--limit",
        type=int,
        help="The number of rows which the query should be limited to.",
    )
    @line_magic
    def evaluate(self, line: str) -> None:
        """Evaluate a model query and fetches a dataframe."""
        self._context.refresh()
        args = parse_argstring(self.evaluate, line)

        df = self._context.evaluate(
            args.model,
            start=args.start,
            end=args.end,
            execution_time=args.execution_time,
            limit=args.limit,
        )
        self.display(df)

    @magic_arguments()
    @argument("model", type=str, help="The model.")
    @argument("--start", "-s", type=str, help="Start date to render.")
    @argument("--end", "-e", type=str, help="End date to render.")
    @argument("--execution-time", type=str, help="Execution time.")
    @argument(
        "--expand",
        type=t.Union[bool, t.Iterable[str]],
        help="Whether or not to use expand materialized models, defaults to False. If True, all referenced models are expanded as raw queries. If a list, only referenced models are expanded as raw queries.",
    )
    @argument("--dialect", type=str, help="SQL dialect to render.")
    @line_magic
    def render(self, line: str) -> None:
        """Renders a model's query, optionally expanding referenced models."""
        self._context.refresh()
        args = parse_argstring(self.render, line)

        query = self._context.render(
            args.model,
            start=args.start,
            end=args.end,
            execution_time=args.execution_time,
            expand=args.expand,
        )

        self._context.console.show_sql(query.sql(pretty=True, dialect=args.dialect))

    @magic_arguments()
    @argument(
        "df_var",
        default=None,
        nargs="?",
        type=str,
        help="An optional variable name to store the resulting dataframe.",
    )
    @cell_magic
    def fetchdf(self, line: str, sql: str) -> None:
        """Fetches a dataframe from sql, optionally storing it in a variable."""
        args = parse_argstring(self.fetchdf, line)
        df = self._context.fetchdf(sql)
        if args.df_var:
            self._shell.user_ns[args.df_var] = df
        self.display(df)

    @magic_arguments()
    @line_magic
    def dag(self, line: str) -> None:
        """Displays the dag"""
        self._context.refresh()
        dag = self._context.get_dag()
        self.display(HTML(dag.pipe().decode("utf-8")))

    @magic_arguments()
    @line_magic
    def migrate(self, line: str) -> None:
        """Migrate SQLMesh to the current running version."""
        self._context.migrate()
        self.display("Migration complete")

    @magic_arguments()
    @line_magic
    def create_external_models(self, line: str) -> None:
        """Create a schema file containing external model schemas."""
        self._context.create_external_models()

    @magic_arguments()
    @argument(
        "--source",
        "-s",
        type=str,
        required=True,
        help="The source environment or table.",
    )
    @argument(
        "--target",
        "-t",
        type=str,
        required=True,
        help="The target environment or table.",
    )
    @argument(
        "--on",
        type=str,
        nargs="+",
        required=True,
        help='The SQL join condition or list of columns to use as keys. Table aliases must be "s" and "t" for source and target.',
    )
    @argument(
        "--model",
        type=str,
        help="The model to diff against when source and target are environments and not tables.",
    )
    @argument(
        "--where",
        type=str,
        help="An optional where statement to filter results.",
    )
    @argument(
        "--limit",
        type=int,
        help="The limit of the sample dataframe.",
    )
    @line_magic
    def table_diff(self, line: str) -> None:
        """Show the diff between two tables.

        Can either be two tables or two environments and a model.
        """
        args = parse_argstring(self.table_diff, line)

        self._context.table_diff(
            source=args.source,
            target=args.target,
            on=args.on,
            model_or_snapshot=args.model,
            where=args.where,
            limit=args.limit,
        )

    @property
    def _shell(self) -> t.Any:
        # Make mypy happy.
        if not self.shell:
            raise RuntimeError("IPython Magics are in invalid state")
        return self.shell


def register_magics() -> None:
    try:
        shell = get_ipython()  # type: ignore
        shell.register_magics(SQLMeshMagics)
    except NameError:
        pass
