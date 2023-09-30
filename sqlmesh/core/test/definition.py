from __future__ import annotations

import difflib
import pathlib
import typing as t
import unittest

import numpy as np
import pandas as pd
from sqlglot import exp, parse_one
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers

from sqlmesh.core.dialect import normalize_model_name
from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.core.model import Model, PythonModel, SqlModel
from sqlmesh.utils.errors import SQLMeshError

Row = t.Dict[str, t.Any]


class TestError(SQLMeshError):
    """Test error"""


class ModelTest(unittest.TestCase):
    __test__ = False
    view_names: list[str] = []

    def __init__(
        self,
        body: dict[str, t.Any],
        test_name: str,
        model: Model,
        models: dict[str, Model],
        engine_adapter: EngineAdapter,
        dialect: str | None,
        path: pathlib.Path | None,
    ) -> None:
        """ModelTest encapsulates a unit test for a model.

        Args:
            body: A dictionary that contains test metadata like inputs and outputs.
            test_name: The name of the test.
            model: The model that is being tested.
            models: All models to use for expansion and mapping of physical locations.
            engine_adapter: The engine adapter to use.
            dialect: The models' dialect, used for normalization purposes.
            path: An optional path to the test definition yaml file.
        """
        self.body = body
        self.test_name = test_name
        self.model = model
        self.models = models
        self.engine_adapter = engine_adapter
        self.path = path

        self._normalize_test(dialect)

        inputs = self.body.get("inputs", {})
        for depends_on in self.model.depends_on:
            if depends_on not in inputs:
                _raise_error(f"Incomplete test, missing input for table {depends_on}", path)

        super().__init__()

    def setUp(self) -> None:
        """Load all input tables"""
        for table_name, rows in self.body.get("inputs", {}).items():
            df = pd.DataFrame.from_records(rows)  # noqa
            columns_to_types: dict[str, exp.DataType] = {}
            if table_name in self.models:
                columns_to_types = self.models[table_name].columns_to_types or {}

            if not columns_to_types:
                for i, v in rows[0].items():
                    # convert ruamel into python
                    v = v.real if hasattr(v, "real") else v
                    columns_to_types[i] = parse_one(type(v).__name__, into=exp.DataType)

            columns_to_types = {k: v for k, v in columns_to_types.items() if k in df}
            table = exp.to_table(table_name)

            if table.db:
                self.engine_adapter.create_schema(table.db, catalog_name=table.catalog)

            self.engine_adapter.create_view(_test_fixture_name(table_name), df, columns_to_types)

    def tearDown(self) -> None:
        """Drop all input tables"""
        for table in self.body.get("inputs", {}):
            self.engine_adapter.drop_view(table)

    def assert_equal(self, expected: pd.DataFrame, actual: pd.DataFrame) -> None:
        """Compare two DataFrames"""
        expected = expected.astype(actual.dtypes.to_dict())
        expected = expected.replace({np.nan: None, "nan": None})
        actual = actual.replace({np.nan: None, "nan": None})

        try:
            pd.testing.assert_frame_equal(
                expected.sort_index(axis=1),
                actual.sort_index(axis=1),
                check_dtype=False,
                check_datetimelike_compat=True,
            )
        except AssertionError as e:
            diff = "\n".join(
                difflib.ndiff(
                    [str(x) for x in expected.to_dict("records")],
                    [str(x) for x in actual.to_dict("records")],
                )
            )
            e.args = (f"Data differs\n{diff}",)
            raise e

    def runTest(self) -> None:
        raise NotImplementedError

    def path_relative_to(self, other: pathlib.Path) -> pathlib.Path | None:
        """Compute a version of this test's path relative to the `other` path"""
        return self.path.relative_to(other) if self.path else None

    @staticmethod
    def create_test(
        body: dict[str, t.Any],
        test_name: str,
        models: dict[str, Model],
        engine_adapter: EngineAdapter,
        dialect: str | None,
        path: pathlib.Path | None,
    ) -> ModelTest:
        """Create a SqlModelTest or a PythonModelTest.

        Args:
            body: A dictionary that contains test metadata like inputs and outputs.
            test_name: The name of the test.
            models: All models to use for expansion and mapping of physical locations.
            engine_adapter: The engine adapter to use.
            dialect: The models' dialect, used for normalization purposes.
            path: An optional path to the test definition yaml file.
        """
        if "model" not in body:
            _raise_error("Incomplete test, missing model name", path)

        if "outputs" not in body:
            _raise_error("Incomplete test, missing outputs", path)

        model_name = normalize_model_name(body["model"], dialect=dialect)
        if model_name not in models:
            _raise_error(f"Model '{model_name}' was not found", path)

        model = models[model_name]
        if isinstance(model, SqlModel):
            return SqlModelTest(body, test_name, model, models, engine_adapter, dialect, path)
        if isinstance(model, PythonModel):
            return PythonModelTest(body, test_name, model, models, engine_adapter, dialect, path)

        raise TestError(f"Model '{model_name}' is an unsupported model type for testing at {path}")

    def __str__(self) -> str:
        return f"{self.test_name} ({self.path})"

    def _normalize_test(self, dialect: str | None) -> None:
        """Normalizes all identifiers in this test according to the given dialect."""

        def _normalize_rows(rows: list[Row] | dict[str, list[Row]]) -> t.List[Row]:
            if isinstance(rows, dict):
                if "rows" not in rows:
                    _raise_error("Incomplete test, missing row data for table", self.path)
                rows = rows["rows"]

            return [
                {
                    normalize_identifiers(column, dialect=dialect).name: value
                    for column, value in row.items()
                }
                for row in rows
            ]

        def _normalize_sources(sources: t.Dict[str, t.Any]) -> t.Dict[str, t.Any]:
            return {
                normalize_model_name(name, dialect=dialect): _normalize_rows(rows)
                for name, rows in sources.items()
            }

        inputs = self.body.get("inputs", {})
        outputs = self.body["outputs"]
        ctes = outputs.get("ctes", {})
        query = outputs.get("query", [])

        if inputs:
            self.body["inputs"] = _normalize_sources(inputs)
        if ctes:
            outputs["ctes"] = _normalize_sources(ctes)
        if query:
            outputs["query"] = _normalize_rows(query)

        self.body["model"] = normalize_model_name(self.body["model"], dialect=dialect)


class SqlModelTest(ModelTest):
    def execute(self, query: exp.Expression) -> pd.DataFrame:
        """Execute the query with the engine adapter and return a DataFrame"""
        return self.engine_adapter.fetchdf(query)

    def test_ctes(self, ctes: t.Dict[str, exp.Expression]) -> None:
        """Run CTE queries and compare output to expected output"""
        for cte_name, value in self.body["outputs"].get("ctes", {}).items():
            with self.subTest(cte=cte_name):
                if cte_name not in ctes:
                    _raise_error(
                        f"No CTE named {cte_name} found in model {self.model.name}", self.path
                    )

                cte_query = ctes[cte_name].this
                for alias, cte in ctes.items():
                    cte_query = cte_query.with_(alias, cte.this)

                expected_df = pd.DataFrame.from_records(value)
                actual_df = self.execute(cte_query)
                self.assert_equal(expected_df, actual_df)

    def runTest(self) -> None:
        # For tests we just use the model name for the table reference and we don't want to expand
        mapping = {
            name: _test_fixture_name(name)
            for name in self.models.keys() | self.body.get("inputs", {}).keys()
        }
        query = self.model.render_query_or_raise(
            **self.body.get("vars", {}),
            engine_adapter=self.engine_adapter,
            table_mapping=mapping,
        )

        self.test_ctes({cte.alias: cte for cte in query.ctes})

        # Test model query
        if "query" in self.body["outputs"]:
            expected_df = pd.DataFrame.from_records(self.body["outputs"]["query"])
            actual_df = self.execute(query)
            self.assert_equal(expected_df, actual_df)


class PythonModelTest(ModelTest):
    def __init__(
        self,
        body: dict[str, t.Any],
        test_name: str,
        model: PythonModel,
        models: dict[str, Model],
        engine_adapter: EngineAdapter,
        dialect: str | None,
        path: pathlib.Path | None,
    ) -> None:
        """PythonModelTest encapsulates a unit test for a Python model.

        Args:
            body: A dictionary that contains test metadata like inputs and outputs.
            test_name: The name of the test.
            model: The Python model that is being tested.
            models: All models to use for expansion and mapping of physical locations.
            engine_adapter: The engine adapter to use.
            dialect: The models' dialect, used for normalization purposes.
            path: An optional path to the test definition yaml file.
        """
        from sqlmesh.core.test.context import TestExecutionContext

        super().__init__(body, test_name, model, models, engine_adapter, dialect, path)

        self.context = TestExecutionContext(
            engine_adapter=engine_adapter,
            models=models,
        )

    def runTest(self) -> None:
        if "query" in self.body["outputs"]:
            expected_df = pd.DataFrame.from_records(self.body["outputs"]["query"])
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
