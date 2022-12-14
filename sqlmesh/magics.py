from __future__ import annotations

import typing as t
from collections import defaultdict

from IPython.core.display import HTML, display
from IPython.core.magic import Magics, line_cell_magic, line_magic, magics_class
from IPython.core.magic_arguments import argument, magic_arguments, parse_argstring

from sqlmesh.core.console import NotebookMagicConsole
from sqlmesh.core.context import Context
from sqlmesh.core.dialect import format_model_expressions, parse_model
from sqlmesh.core.model import Model
from sqlmesh.core.test import ModelTestMetadata, get_all_model_tests
from sqlmesh.utils.errors import MagicError, MissingContextException, SQLMeshError
from sqlmesh.utils.yaml import dumps as yaml_dumps
from sqlmesh.utils.yaml import yaml

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
            # Use Databrick's special display instead of the normal IPython display
            return self.shell.user_ns["display"]
        return display

    @property
    def context(self) -> Context:
        for variable_name in CONTEXT_VARIABLE_NAMES:
            context = self.shell.user_ns.get(variable_name)
            if context:
                return context
        raise MissingContextException(
            f"Context must be defined and initialized with one of these names: {', '.join(CONTEXT_VARIABLE_NAMES)}"
        )

    @magic_arguments()
    @argument("model", type=str, help="The model.")
    @argument("--start", "-s", type=str, help="Start date to render.")
    @argument("--end", "-e", type=str, help="End date to render.")
    @argument("--latest", "-l", type=str, help="Latest date to render.")
    @argument("--dialect", "-d", type=str, help="The rendered dialect.")
    @line_cell_magic
    def model(self, line: str, sql: t.Optional[str] = None):
        """Render's the model and automatically fills in an editable cell with the model definition."""
        args = parse_argstring(self.model, line)
        model = self.context.models.get(args.model)

        if not model:
            raise SQLMeshError(f"Cannot find {model}")

        if sql:
            loaded = Model.load(
                parse_model(sql, default_dialect=self.context.dialect),
                macros=self.context.macros,
                path=model._path,
                dialect=self.context.dialect,
                time_column_format=self.context.config.time_column_format,
            )

            if loaded.name == args.model:
                model = loaded

        self.context.upsert_model(model)
        expressions = model.render()

        formatted = format_model_expressions(expressions, model.dialect)

        self.shell.set_next_input(
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

        self.context.models.update({model.name: model})
        self.context.console.show_sql(
            self.context.render(
                model.name,
                start=args.start,
                end=args.end,
                latest=args.latest,
            ).sql(pretty=True, dialect=args.dialect or model.dialect)
        )

    @magic_arguments()
    @argument("model", type=str, help="The model.")
    @argument(
        "test_name", type=str, nargs="?", default=None, help="The test name to display"
    )
    @argument("--ls", action="store_true", help="List tests associated with a model")
    @line_cell_magic
    def test(self, line: str, test_def_raw: t.Optional[str] = None):
        """Allow the user to list tests for a model, output a specific test and then write their changes back"""
        args = parse_argstring(self.test, line)
        if not args.test_name and not args.ls:
            raise MagicError("Must provide either test name or `--ls` to list tests")

        model_test_metadatas = get_all_model_tests(
            self.context.test_directory_path,
            ignore_patterns=self.context._ignore_patterns,
        )
        tests: t.Dict[str, t.Dict[str, ModelTestMetadata]] = defaultdict(dict)
        for model_test_metadata in model_test_metadatas:
            model = model_test_metadata.body.get("model")
            if not model:
                self.context.console.log_error(
                    f"Test found that does not have `model` defined: {model_test_metadata.path}"
                )
            tests[model][model_test_metadata.test_name] = model_test_metadata
        if args.ls:
            # TODO: Provide better UI for displaying tests
            for test_name in tests[args.model]:
                self.context.console.log_status_update(test_name)
            return

        test = tests[args.model][args.test_name]
        test_def = yaml.load(test_def_raw) if test_def_raw else test.body
        test_def_output = yaml_dumps(test_def)

        self.shell.set_next_input(
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
    @argument(
        "--from",
        "-f",
        dest="from_",
        type=str,
        help="The environment to base the plan on instead of local files.",
    )
    @argument(
        "--skip-tests",
        "-t",
        action="store_true",
        help="Skip the unit tests defined for the model",
    )
    @argument(
        "--restate-from",
        "-r",
        type=str,
        nargs="*",
        help="Restate all models that depend on these upstream tables. All snapshots that depend on these upstream tables will have their intervals wiped but only the current snapshots will be backfilled.",
    )
    @argument(
        "--no-gaps",
        "-g",
        action="store_true",
        help="Ensure that new snapshots have no data gaps when comparing to existing snapshots for matching models in the target environment.",
    )
    @argument(
        "--no-prompts",
        action="store_true",
        help="Disables interactive prompts for the backfill time range. Please note that if this flag is set and there are uncategorized changes the plan creation will fail.",
    )
    @argument(
        "--auto-apply",
        action="store_true",
        help="Automatically applies the new plan after creation.",
    )
    @line_magic
    def plan(self, line) -> None:
        """Goes through a set of prompts to both establish a plan and apply it"""
        self.context.refresh()
        args = parse_argstring(self.plan, line)

        # Since the magics share a context we want to clear out any state before generating a new plan
        console = self.context.console
        self.context.console = NotebookMagicConsole(self.display)

        self.context.plan(
            args.environment,
            start=args.start,
            end=args.end,
            from_=args.from_,
            skip_tests=args.skip_tests,
            restate_from=args.restate_from,
            no_gaps=args.no_gaps,
            no_prompts=args.no_prompts,
            auto_apply=args.auto_apply,
        )
        self.context.console = console

    @magic_arguments()
    @argument("model", type=str, help="The model.")
    @argument("--start", "-s", type=str, help="Start date to render.")
    @argument("--end", "-e", type=str, help="End date to render.")
    @argument("--latest", "-l", type=str, help="Latest date to render.")
    @argument(
        "--limit",
        type=int,
        help="The number of rows which the query should be limited to.",
    )
    @line_magic
    def evaluate(self, line):
        """Evaluate a model query and fetches a dataframe."""
        self.context.refresh()
        args = parse_argstring(self.evaluate, line)

        df = self.context.evaluate(
            args.model,
            start=args.start,
            end=args.end,
            latest=args.latest,
            limit=args.limit,
        )
        self.display(df)

    @magic_arguments()
    @argument(
        "df_var",
        default=None,
        nargs="?",
        type=str,
        help="An optional variable name to the store the resulting dataframe in.",
    )
    @line_cell_magic
    def fetchdf(self, line, sql: str):
        """Fetches a dataframe from sql, optionally storing it in a variable."""
        args = parse_argstring(self.fetchdf, line)
        df = self.context.fetchdf(sql)
        if args.df_var:
            self.shell.user_ns[args.df_var] = df
        self.display(df)

    @magic_arguments()
    @line_magic
    def dag(self, line):
        """Displays the dag"""
        self.context.refresh()
        dag = self.context.get_dag()
        self.display(HTML(dag.pipe().decode("utf-8")))


def register_magics() -> None:
    try:
        shell = get_ipython()  # type: ignore
        shell.register_magics(SQLMeshMagics)
    except NameError:
        pass
