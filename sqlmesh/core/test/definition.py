from __future__ import annotations

import typing as t
import unittest
from pathlib import Path

import numpy as np
import pandas as pd
from sqlglot import exp
from sqlglot.optimizer.annotate_types import annotate_types
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers

from sqlmesh.core import constants as c
from sqlmesh.core.dialect import normalize_model_name, schema_
from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.core.model import Model, PythonModel, SqlModel
from sqlmesh.utils import UniqueKeyDict, yaml
from sqlmesh.utils.date import pandas_timestamp_to_pydatetime
from sqlmesh.utils.errors import ConfigError, SQLMeshError

Row = t.Dict[str, t.Any]


class TestError(SQLMeshError):
    """Test error"""


class ModelTest(unittest.TestCase):
    __test__ = False

    def __init__(
        self,
        body: t.Dict[str, t.Any],
        test_name: str,
        model: Model,
        models: UniqueKeyDict[str, Model],
        engine_adapter: EngineAdapter,
        dialect: str | None,
        path: Path | None,
        default_catalog: str | None = None,
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
        self.default_catalog = default_catalog
        self.dialect = dialect

        self._normalize_test(dialect)

        inputs = [
            normalize_model_name(input, default_catalog=default_catalog, dialect=dialect)
            for input in self.body.get("inputs", {})
        ]
        for depends_on in self.model.depends_on:
            if depends_on not in inputs:
                _raise_error(f"Incomplete test, missing input for table {depends_on}", path)

        super().__init__()

    def setUp(self) -> None:
        """Load all input tables"""
        for table_name, values in self.body.get("inputs", {}).items():
            columns_to_types: t.Dict[str, exp.DataType] = {}
            if table_name in self.models:
                model = self.models[table_name]
                columns_to_types = model.columns_to_types if model.annotated else {}

            rows = values["rows"]
            if not columns_to_types and rows:
                for i, v in rows[0].items():
                    # convert ruamel into python
                    v = v.real if hasattr(v, "real") else v
                    v_type = annotate_types(exp.convert(v)).type or type(v).__name__
                    columns_to_types[i] = exp.maybe_parse(v_type, into=exp.DataType)

            test_fixture_table = _fully_qualified_test_fixture_table(table_name, self.dialect)
            if test_fixture_table.db:
                self.engine_adapter.create_schema(
                    schema_(test_fixture_table.args["db"], test_fixture_table.args.get("catalog"))
                )

            df = self._create_df(rows, columns=columns_to_types)
            self.engine_adapter.create_view(test_fixture_table, df, columns_to_types)

    def tearDown(self) -> None:
        """Drop all fixture tables."""
        for table in self.body.get("inputs", {}):
            self.engine_adapter.drop_view(
                _fully_qualified_test_fixture_name(table, self.dialect)
                if table in self.models
                else table
            )

    def assert_equal(
        self,
        expected: pd.DataFrame,
        actual: pd.DataFrame,
        sort: bool,
        partial: t.Optional[bool] = False,
    ) -> None:
        """Compare two DataFrames"""
        if partial:
            intersection = actual[actual.columns.intersection(expected.columns)]
            if not intersection.empty:
                actual = intersection

        # Two astypes are necessary, pandas converts strings to times as NS,
        # but if the actual is US, it doesn't take effect until the 2nd try!
        actual_types = actual.dtypes.to_dict()
        expected = expected.astype(actual_types, errors="ignore").astype(
            actual_types, errors="ignore"
        )

        actual = actual.replace({None: np.nan})
        expected = expected.replace({None: np.nan})

        def _to_hashable(x: t.Any) -> t.Any:
            return tuple(x) if isinstance(x, list) else x

        if sort:
            actual = (
                actual.apply(_to_hashable)
                .sort_values(by=actual.columns.to_list())
                .reset_index(drop=True)
            )
            expected = (
                expected.apply(_to_hashable)
                .sort_values(by=expected.columns.to_list())
                .reset_index(drop=True)
            )

        try:
            pd.testing.assert_frame_equal(
                expected,
                actual,
                check_dtype=False,
                check_datetimelike_compat=True,
                check_like=True,  # ignore column order
            )
        except AssertionError as e:
            diff = expected.compare(actual).rename(columns={"self": "exp", "other": "act"})
            description = self.body.get("description")
            description = f"\n\n\nTest description: {description}" if description else ""
            e.args = (f"Data differs (exp: expected, act: actual)\n\n{diff}{description}",)
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
        models: UniqueKeyDict[str, Model],
        engine_adapter: EngineAdapter,
        dialect: str | None,
        path: Path | None,
        default_catalog: str | None = None,
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

        model_name = normalize_model_name(
            body["model"], default_catalog=default_catalog, dialect=dialect
        )
        if model_name not in models:
            _raise_error(f"Model '{model_name}' was not found", path)

        model = models[model_name]
        if isinstance(model, SqlModel):
            return SqlModelTest(
                body, test_name, model, models, engine_adapter, dialect, path, default_catalog
            )
        if isinstance(model, PythonModel):
            return PythonModelTest(
                body, test_name, model, models, engine_adapter, dialect, path, default_catalog
            )

        raise TestError(f"Model '{model_name}' is an unsupported model type for testing at {path}")

    def __str__(self) -> str:
        return f"{self.test_name} ({self.path})"

    def _normalize_test(self, dialect: str | None) -> None:
        """Normalizes all identifiers in this test according to the given dialect."""

        def _normalize_rows(values: t.List[Row] | t.Dict) -> t.Dict:
            if not isinstance(values, dict):
                values = {"rows": values}

            if "rows" not in values:
                _raise_error("Incomplete test, missing row data for table", self.path)

            values["rows"] = [
                {
                    normalize_identifiers(column, dialect=dialect).name: value
                    for column, value in row.items()
                }
                for row in values["rows"]
            ]
            return values

        def _normalize_sources(sources: t.Dict) -> t.Dict:
            return {
                normalize_model_name(
                    name, default_catalog=self.default_catalog, dialect=dialect
                ): _normalize_rows(values)
                for name, values in sources.items()
            }

        inputs = self.body.get("inputs")
        outputs = self.body["outputs"]
        ctes = outputs.get("ctes")
        query = outputs.get("query")

        if inputs:
            self.body["inputs"] = _normalize_sources(inputs)
        if ctes:
            outputs["ctes"] = _normalize_sources(ctes)
        if query or query == []:
            outputs["query"] = _normalize_rows(query)

        self.body["model"] = normalize_model_name(
            self.body["model"], default_catalog=self.default_catalog, dialect=dialect
        )

    def _create_df(
        self,
        rows: t.List[Row],
        columns: t.Optional[t.Iterable] = None,
        partial: t.Optional[bool] = False,
    ) -> pd.DataFrame:
        if columns:
            referenced_columns = {col for row in rows for col in row}
            unknown_columns = [col for col in referenced_columns if col not in columns]
            if unknown_columns:
                expected_cols = f"Expected column(s): {', '.join(columns)}\n"
                unknown_cols = f"Unknown column(s): {', '.join(unknown_columns)}"
                _raise_error(f"Detected unknown column(s)\n\n{expected_cols}{unknown_cols}")

            if partial:
                columns = list(referenced_columns)

        return pd.DataFrame.from_records(rows, columns=columns)


class SqlModelTest(ModelTest):
    def _execute(self, query: exp.Expression) -> pd.DataFrame:
        """Executes the query with the engine adapter and returns a DataFrame."""
        return self.engine_adapter.fetchdf(query)

    def test_ctes(self, ctes: t.Dict[str, exp.Expression]) -> None:
        """Run CTE queries and compare output to expected output"""
        for cte_name, values in self.body["outputs"].get("ctes", {}).items():
            with self.subTest(cte=cte_name):
                if cte_name not in ctes:
                    _raise_error(
                        f"No CTE named {cte_name} found in model {self.model.name}", self.path
                    )

                cte_query = ctes[cte_name].this
                for alias, cte in ctes.items():
                    cte_query = cte_query.with_(alias, cte.this)

                rows = values["rows"]
                partial = values.get("partial")
                sort = cte_query.args.get("order") is None

                actual = self._execute(cte_query)
                expected = self._create_df(rows, columns=cte_query.named_selects, partial=partial)

                self.assert_equal(expected, actual, sort=sort, partial=partial)

    def runTest(self) -> None:
        # For tests we just use the model name for the table reference and we don't want to expand
        mapping = {
            name: _fully_qualified_test_fixture_name(name, dialect=self.dialect)
            for name in [
                normalize_model_name(name, self.default_catalog, self.dialect)
                for name in self.models.keys() | self.body.get("inputs", {}).keys()
            ]
        }
        query = self.model.render_query_or_raise(
            **self.body.get("vars", {}),
            engine_adapter=self.engine_adapter,
            table_mapping=mapping,
        )

        self.test_ctes(
            {normalize_model_name(cte.alias, None, self.dialect): cte for cte in query.ctes}
        )

        values = self.body["outputs"].get("query")
        if values is not None:
            rows = values["rows"]
            partial = values.get("partial")
            sort = query.args.get("order") is None

            actual = self._execute(query)
            expected = self._create_df(rows, columns=self.model.columns_to_types, partial=partial)

            self.assert_equal(expected, actual, sort=sort, partial=partial)


class PythonModelTest(ModelTest):
    def __init__(
        self,
        body: t.Dict[str, t.Any],
        test_name: str,
        model: PythonModel,
        models: UniqueKeyDict[str, Model],
        engine_adapter: EngineAdapter,
        dialect: str | None,
        path: Path | None,
        default_catalog: str | None = None,
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

        super().__init__(
            body, test_name, model, models, engine_adapter, dialect, path, default_catalog
        )

        self.context = TestExecutionContext(
            engine_adapter=engine_adapter,
            models=models,
            default_dialect=dialect,
            default_catalog=default_catalog,
        )

    def _execute_model(self) -> pd.DataFrame:
        """Executes the python model and returns a DataFrame."""
        return t.cast(
            pd.DataFrame,
            next(self.model.render(context=self.context, **self.body.get("vars", {}))),
        )

    def runTest(self) -> None:
        values = self.body["outputs"].get("query")
        if values is not None:
            rows = values["rows"]
            partial = values.get("partial")

            actual_df = self._execute_model()
            actual_df.reset_index(drop=True, inplace=True)
            expected = self._create_df(rows, columns=self.model.columns_to_types, partial=partial)

            self.assert_equal(expected, actual_df, sort=False, partial=partial)


def generate_test(
    model: Model,
    input_queries: t.Dict[str, str],
    models: UniqueKeyDict[str, Model],
    engine_adapter: EngineAdapter,
    test_engine_adapter: EngineAdapter,
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
        test_engine_adapter: The test engine adapter.
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

    # ruamel.yaml does not support pandas Timestamps, so we must convert them to python
    # datetime or datetime.date objects based on column type
    inputs = {
        models[dep]
        .name: pandas_timestamp_to_pydatetime(
            engine_adapter.fetchdf(query), models[dep].columns_to_types
        )
        .to_dict(orient="records")
        for dep, query in input_queries.items()
    }
    outputs: t.Dict[str, t.Any] = {"query": {}}
    variables = variables or {}
    test_body = {"model": model.name, "inputs": inputs, "outputs": outputs}

    if variables:
        test_body["vars"] = variables

    test = ModelTest.create_test(
        body=test_body.copy(),
        test_name=test_name,
        models=models,
        engine_adapter=test_engine_adapter,
        dialect=model.dialect,
        path=fixture_path,
        default_catalog=model.default_catalog,
    )

    test.setUp()

    if isinstance(model, SqlModel):
        mapping = {
            name: _fully_qualified_test_fixture_name(name, test_engine_adapter.dialect)
            for name in models.keys() | inputs.keys()
        }
        model_query = model.render_query_or_raise(
            **t.cast(t.Dict[str, t.Any], variables),
            engine_adapter=test_engine_adapter,
            table_mapping=mapping,
        )
        output = t.cast(SqlModelTest, test)._execute(model_query)
    else:
        output = t.cast(PythonModelTest, test)._execute_model()

    outputs["query"] = pandas_timestamp_to_pydatetime(output, model.columns_to_types).to_dict(
        orient="records"
    )

    test.tearDown()

    fixture_path.parent.mkdir(exist_ok=True, parents=True)
    with open(fixture_path, "w", encoding="utf-8") as file:
        yaml.dump({test_name: test_body}, file)


def _fully_qualified_test_fixture_table(name: str, dialect: str | None) -> exp.Table:
    fqt = exp.to_table(name, dialect=dialect)
    fqt.this.set("this", f"{fqt.this.this}__fixture")
    return fqt


def _fully_qualified_test_fixture_name(name: str, dialect: str | None) -> str:
    return _fully_qualified_test_fixture_table(name, dialect).sql()


def _raise_error(msg: str, path: Path | None = None) -> None:
    if path:
        raise TestError(f"{msg} at {path}")
    raise TestError(msg)
