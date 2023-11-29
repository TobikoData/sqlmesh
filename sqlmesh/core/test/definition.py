from __future__ import annotations

import typing as t
import unittest
from pathlib import Path

import numpy as np
import pandas as pd
from sqlglot import exp, parse_one
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers

from sqlmesh.core import constants as c
from sqlmesh.core.dialect import normalize_model_name, schema_
from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.core.model import Model, PythonModel, SqlModel
from sqlmesh.utils import yaml
from sqlmesh.utils.errors import ConfigError, SQLMeshError

Row = t.Dict[str, t.Any]


class TestError(SQLMeshError):
    """Test error"""


class ModelTest(unittest.TestCase):
    __test__ = False
    view_names: t.List[str] = []

    def __init__(
        self,
        body: t.Dict[str, t.Any],
        test_name: str,
        model: Model,
        models: t.Dict[str, Model],
        engine_adapter: EngineAdapter,
        dialect: str | None,
        path: Path | None,
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
            columns_to_types: t.Dict[str, exp.DataType] = {}
            if table_name in self.models:
                columns_to_types = self.models[table_name].columns_to_types or {}

            if not columns_to_types and rows:
                for i, v in rows[0].items():
                    # convert ruamel into python
                    v = v.real if hasattr(v, "real") else v
                    columns_to_types[i] = parse_one(type(v).__name__, into=exp.DataType)

            table = exp.to_table(table_name)
            if table.db:
                self.engine_adapter.create_schema(
                    schema_(table.args["db"], table.args.get("catalog"))
                )

            self._add_missing_columns(df, columns_to_types)
            self.engine_adapter.create_view(_test_fixture_name(table_name), df, columns_to_types)

    def tearDown(self) -> None:
        """Drop all fixture tables."""
        for table in self.body.get("inputs", {}):
            self.engine_adapter.drop_view(_test_fixture_name(table))

    def assert_equal(self, expected: pd.DataFrame, actual: pd.DataFrame) -> None:
        """Compare two DataFrames"""
        self._add_missing_columns(expected, actual)

        # Two astypes are necessary, pandas converts strings to times as NS,
        # but if the actual is US, it doesn't take effect until the 2nd try!
        actual_types = actual.dtypes.to_dict()
        expected = expected.astype(actual_types, errors="ignore").astype(
            actual_types, errors="ignore"
        )

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
            if expected.empty and actual.empty and all(expected.columns == actual.columns):
                # Only the index differs, so we treat the two DataFrames as equivalent in this case
                return

            diff = expected.compare(actual).rename(columns={"self": "exp", "other": "act"})
            e.args = (f"Data differs (exp: expected, act: actual)\n\n{diff}",)
            raise e

    def runTest(self) -> None:
        raise NotImplementedError

    def path_relative_to(self, other: Path) -> Path | None:
        """Compute a version of this test's path relative to the `other` path"""
        return self.path.relative_to(other) if self.path else None

    @staticmethod
    def create_test(
        body: t.Dict[str, t.Any],
        test_name: str,
        models: t.Dict[str, Model],
        engine_adapter: EngineAdapter,
        dialect: str | None,
        path: Path | None,
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

    def _add_missing_columns(self, df: pd.DataFrame, columns: t.Iterable) -> None:
        """Add missing columns to a given dataframe with None values."""
        for index, column in enumerate(columns):
            if column not in df:
                df.insert(index, column, None)  # type: ignore

    def _normalize_test(self, dialect: str | None) -> None:
        """Normalizes all identifiers in this test according to the given dialect."""

        def _normalize_rows(rows: t.List[Row] | t.Dict[str, t.List[Row]]) -> t.List[Row]:
            if isinstance(rows, dict) and rows:
                if "rows" not in rows:
                    _raise_error("Incomplete test, missing row data for table", self.path)
                rows = rows["rows"]

            return [
                {
                    normalize_identifiers(column, dialect=dialect).name: value
                    for column, value in t.cast(Row, row).items()
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
    def _execute(self, query: exp.Expression) -> pd.DataFrame:
        """Executes the query with the engine adapter and returns a DataFrame."""
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
                actual_df = self._execute(cte_query)
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
            actual_df = self._execute(query)
            self.assert_equal(expected_df, actual_df)


class PythonModelTest(ModelTest):
    def __init__(
        self,
        body: t.Dict[str, t.Any],
        test_name: str,
        model: PythonModel,
        models: t.Dict[str, Model],
        engine_adapter: EngineAdapter,
        dialect: str | None,
        path: Path | None,
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

    def _execute_model(self) -> pd.DataFrame:
        """Executes the python model and returns a DataFrame."""
        return t.cast(
            pd.DataFrame,
            next(self.model.render(context=self.context, **self.body.get("vars", {}))),
        )

    def runTest(self) -> None:
        if "query" in self.body["outputs"]:
            expected_df = pd.DataFrame.from_records(self.body["outputs"]["query"])
            actual_df = self._execute_model()
            actual_df.reset_index(drop=True, inplace=True)
            self.assert_equal(expected_df, actual_df)


def generate_test(
    model: Model,
    input_queries: t.Dict[str, str],
    models: t.Dict[str, Model],
    engine_adapter: EngineAdapter,
    project_path: Path,
    overwrite: bool = False,
    variables: t.Optional[t.Dict[str, str]] = None,
    path: t.Optional[str] = None,
    name: t.Optional[str] = None,
) -> None:
    """Generate a unit test fixture for a given model.

    Args:
        model: The model to test.
        input_queries: Mapping of model names to queries. Each model included in this mapping
            will be populated in the test based on the results of the corresponding query.
        models: The context's models.
        engine_adapter: The target engine adapter.
        project_path: The path pointing to the project's root directory.
        overwrite: Whether to overwrite the existing test in case of a file path collision.
            When set to False, an error will be raised if there is such a collision.
        variables: Key-value pairs that will define variables needed by the model.
        path: The file path corresponding to the fixture, relative to the test directory.
            By default, the fixture will be created under the test directory and the file name
            will be inferred from the test's name.
        name: The name of the test. This is inferred from the model name by default.
    """
    test_name = name or f"test_{model.view_name}"
    path = path or f"{test_name}.yaml"

    extension = path.split(".")[-1].lower()
    if extension not in ("yaml", "yml"):
        path = f"{path}.yaml"

    fixture_path = project_path / c.TESTS / path
    if not overwrite and fixture_path.exists():
        raise ConfigError(
            f"Fixture '{fixture_path}' already exists, make sure to set --overwrite if it can be safely overwritten."
        )

    inputs = {
        dep: engine_adapter.fetchdf(query).to_dict(orient="records")
        for dep, query in input_queries.items()
    }
    outputs: t.Dict[str, t.Any] = {"query": {}}
    variables = variables or {}
    test_body = {"model": model.name, "inputs": inputs, "outputs": outputs}

    if variables:
        test_body["vars"] = variables

    test = ModelTest.create_test(
        body=test_body,
        test_name=test_name,
        models=models,
        engine_adapter=engine_adapter,
        dialect=model.dialect,
        path=fixture_path,
    )

    test.setUp()

    if isinstance(model, SqlModel):
        mapping = {name: _test_fixture_name(name) for name in models.keys() | inputs.keys()}
        model_query = model.render_query_or_raise(
            **t.cast(t.Dict[str, t.Any], variables),
            engine_adapter=engine_adapter,
            table_mapping=mapping,
        )
        output = t.cast(SqlModelTest, test)._execute(model_query)
    else:
        output = t.cast(PythonModelTest, test)._execute_model()

    outputs["query"] = output.to_dict(orient="records")

    test.tearDown()

    fixture_path.parent.mkdir(exist_ok=True, parents=True)
    with open(fixture_path, "w", encoding="utf-8") as file:
        yaml.dump({test_name: test_body}, file)


def _test_fixture_name(name: str) -> str:
    return f"{name}__fixture"


def _raise_error(msg: str, path: Path | None) -> None:
    if path:
        raise TestError(f"{msg} at {path}")
    raise TestError(msg)
