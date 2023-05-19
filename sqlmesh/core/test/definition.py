from __future__ import annotations

import difflib
import pathlib
import typing as t
import unittest

import pandas as pd
from sqlglot import Expression, exp, parse_one

from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.core.model import Model, PythonModel, SqlModel
from sqlmesh.utils.errors import SQLMeshError


class TestError(SQLMeshError):
    """Test error"""


class ModelTest(unittest.TestCase):
    view_names: list[str] = []

    def __init__(
        self,
        body: dict[str, t.Any],
        test_name: str,
        model: Model,
        models: dict[str, Model],
        engine_adapter: EngineAdapter,
        path: pathlib.Path | None,
    ) -> None:
        """ModelTest encapsulates a unit test for a model.

        Args:
            body: A dictionary that contains test metadata like inputs and outputs.
            test_name: The name of the test.
            model: The model that is being tested.
            models: All models to use for expansion and mapping of physical locations.
            engine_adapter: The engine adapter to use.
            path: An optional path to the test definition yaml file
        """
        self.body = body
        self.test_name = test_name
        self.model = model
        self.engine_adapter = engine_adapter
        self.path = path

        inputs = self.body.get("inputs", {})
        for depends_on in self.model.depends_on:
            if depends_on not in inputs:
                _raise_error(f"Incomplete test, missing input for table {depends_on}", path)

        super().__init__()

    def setUp(self) -> None:
        """Load all input tables"""
        inputs = {name: table["rows"] for name, table in self.body.get("inputs", {}).items()}

        for table, rows in inputs.items():
            df = pd.DataFrame.from_records(rows)  # noqa
            columns_to_types: dict[str, exp.DataType] = {}
            for i, v in rows[0].items():
                # convert ruamel into python
                v = v.real if hasattr(v, "real") else v
                columns_to_types[i] = parse_one(type(v).__name__, into=exp.DataType)
            self.engine_adapter.create_schema(table)
            self.engine_adapter.create_view(_test_fixture_name(table), df, columns_to_types)

    def tearDown(self) -> None:
        """Drop all input tables"""
        for table in self.body.get("inputs", {}):
            self.engine_adapter.drop_view(table)

    def assert_equal(self, df1: pd.DataFrame, df2: pd.DataFrame) -> None:
        """Compare two DataFrames"""
        try:
            pd.testing.assert_frame_equal(
                df1, df2, check_dtype=False, check_datetimelike_compat=True
            )
        except AssertionError as e:
            diff = "\n".join(
                difflib.ndiff(
                    [str(x) for x in df1.to_dict("records")],
                    [str(x) for x in df2.to_dict("records")],
                )
            )
            e.args = (f"Data differs\n{diff}",)
            raise e

    def runTest(self) -> None:
        raise NotImplementedError

    @staticmethod
    def create_test(
        body: dict[str, t.Any],
        test_name: str,
        models: dict[str, Model],
        engine_adapter: EngineAdapter,
        path: pathlib.Path | None,
    ) -> ModelTest:
        """Create a SqlModelTest or a PythonModelTest.

        Args:
            body: A dictionary that contains test metadata like inputs and outputs.
            test_name: The name of the test.
            models: All models to use for expansion and mapping of physical locations.
            engine_adapter: The engine adapter to use.
            path: An optional path to the test definition yaml file
        """
        if "model" not in body:
            _raise_error("Incomplete test, missing model name", path)

        if "outputs" not in body:
            _raise_error("Incomplete test, missing outputs", path)

        model_name = body["model"]
        if model_name not in models:
            _raise_error(f"Model '{model_name}' was not found", path)

        model = models[model_name]
        if isinstance(model, SqlModel):
            return SqlModelTest(body, test_name, model, models, engine_adapter, path)
        if isinstance(model, PythonModel):
            return PythonModelTest(body, test_name, model, models, engine_adapter, path)
        raise TestError(f"Model '{model_name}' is an unsupported model type for testing at {path}")

    def __str__(self) -> str:
        return f"{self.test_name} ({self.path}:{self.body.lc.line})"  # type: ignore


class SqlModelTest(ModelTest):
    def __init__(
        self,
        body: dict[str, t.Any],
        test_name: str,
        model: SqlModel,
        models: dict[str, Model],
        engine_adapter: EngineAdapter,
        path: pathlib.Path | None,
    ) -> None:
        """SqlModelTest encapsulates a unit test for a SQL model.

        Args:
            body: A dictionary that contains test metadata like inputs and outputs.
            test_name: The name of the test.
            model: The SQL model that is being tested.
            models: All models to use for expansion and mapping of physical locations.
            engine_adapter: The engine adapter to use.
            path: An optional path to the test definition yaml file
        """
        super().__init__(body, test_name, model, models, engine_adapter, path)

        self.query = model.render_query(
            add_incremental_filter=True,
            **self.body.get("vars", {}),
            engine_adapter=engine_adapter,
        )
        # For tests we just use the model name for the table reference and we don't want to expand
        mapping = {name: _test_fixture_name(name) for name in models}
        if mapping:
            self.query = exp.replace_tables(self.query, mapping)

        self.ctes = {cte.alias: cte for cte in self.query.ctes}

    def execute(self, query: Expression) -> pd.DataFrame:
        """Execute the query with the engine adapter and return a DataFrame"""
        return self.engine_adapter.fetchdf(query)

    def test_ctes(self) -> None:
        """Run CTE queries and compare output to expected output"""
        for cte_name, value in self.body["outputs"].get("ctes", {}).items():
            with self.subTest(cte=cte_name):
                if cte_name not in self.ctes:
                    _raise_error(
                        f"No CTE named {cte_name} found in model {self.model.name}", self.path
                    )
                expected_df = pd.DataFrame.from_records(value["rows"])
                actual_df = self.execute(self.ctes[cte_name].this)
                self.assert_equal(expected_df, actual_df)

    def runTest(self) -> None:
        self.test_ctes()

        # Test model query
        if "rows" in self.body["outputs"].get("query", {}):
            expected_df = pd.DataFrame.from_records(self.body["outputs"]["query"]["rows"])
            actual_df = self.execute(self.query)
            self.assert_equal(expected_df, actual_df)


class PythonModelTest(ModelTest):
    def __init__(
        self,
        body: dict[str, t.Any],
        test_name: str,
        model: PythonModel,
        models: dict[str, Model],
        engine_adapter: EngineAdapter,
        path: pathlib.Path | None,
    ) -> None:
        """PythonModelTest encapsulates a unit test for a Python model.

        Args:
            body: A dictionary that contains test metadata like inputs and outputs.
            test_name: The name of the test.
            model: The Python model that is being tested.
            models: All models to use for expansion and mapping of physical locations.
            engine_adapter: The engine adapter to use.
            path: An optional path to the test definition yaml file
        """
        from sqlmesh.core.test.context import TestExecutionContext

        super().__init__(body, test_name, model, models, engine_adapter, path)

        self.context = TestExecutionContext(
            engine_adapter=engine_adapter,
            models=models,
        )

    def runTest(self) -> None:
        if "rows" in self.body["outputs"].get("query", {}):
            expected_df = pd.DataFrame.from_records(self.body["outputs"]["query"]["rows"])
            actual_df = next(
                self.model.render(
                    context=self.context,
                    **self.body.get("vars", {}),
                )
            )
            actual_df = t.cast(pd.DataFrame, actual_df)
            actual_df.reset_index(drop=True, inplace=True)
            self.assert_equal(expected_df, actual_df)


def _test_fixture_name(name: str) -> str:
    return f"{name}__fixture"


def _raise_error(msg: str, path: pathlib.Path | None) -> None:
    if path:
        raise TestError(f"{msg} at {path}")
    raise TestError(msg)
