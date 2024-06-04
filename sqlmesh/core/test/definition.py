from __future__ import annotations

import datetime
import typing as t
import unittest
from collections import Counter
from contextlib import AbstractContextManager, nullcontext
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd
from io import StringIO
from freezegun import freeze_time
from pandas.api.types import is_object_dtype
from sqlglot import Dialect, exp
from sqlglot.optimizer.annotate_types import annotate_types
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers

from sqlmesh.core import constants as c
from sqlmesh.core.dialect import normalize_model_name, schema_
from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.core.macros import RuntimeStage
from sqlmesh.core.model import Model, PythonModel, SqlModel
from sqlmesh.utils import UniqueKeyDict, random_id, type_is_known, yaml
from sqlmesh.utils.date import pandas_timestamp_to_pydatetime
from sqlmesh.utils.errors import ConfigError, TestError
from sqlmesh.utils.yaml import load as yaml_load

if t.TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    Row = t.Dict[str, t.Any]


class ModelTest(unittest.TestCase):
    __test__ = False

    def __init__(
        self,
        body: t.Dict[str, t.Any],
        test_name: str,
        model: Model,
        models: UniqueKeyDict[str, Model],
        engine_adapter: EngineAdapter,
        dialect: str | None = None,
        path: Path | None = None,
        preserve_fixtures: bool = False,
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
            preserve_fixtures: Preserve the fixture tables in the testing database, useful for debugging.
        """
        self.body = body
        self.test_name = test_name
        self.model = model
        self.models = models
        self.engine_adapter = engine_adapter
        self.path = path
        self.preserve_fixtures = preserve_fixtures
        self.default_catalog = default_catalog
        self.dialect = dialect

        self._fixture_table_cache: t.Dict[str, exp.Table] = {}
        self._normalized_column_name_cache: t.Dict[str, str] = {}
        self._normalized_model_name_cache: t.Dict[t.Tuple[str, bool], str] = {}

        self._test_adapter_dialect = Dialect.get_or_raise(self.engine_adapter.dialect)

        self._validate_and_normalize_test()

        if self.engine_adapter.default_catalog:
            self._fixture_catalog: t.Optional[exp.Identifier] = exp.parse_identifier(
                self.engine_adapter.default_catalog, dialect=self._test_adapter_dialect
            )
        else:
            self._fixture_catalog = None

        # The test schema name is randomized to avoid concurrency issues
        self._fixture_schema = exp.to_identifier(f"sqlmesh_test_{random_id(short=True)}")
        self._qualified_fixture_schema = schema_(self._fixture_schema, self._fixture_catalog)

        self._transforms = self._test_adapter_dialect.generator_class.TRANSFORMS
        self._execution_time = str(self.body.get("vars", {}).get("execution_time") or "")

        # When execution_time is set, we mock the CURRENT_* SQL expressions so they always return it
        if self._execution_time:
            exec_time = exp.Literal.string(self._execution_time)
            self._transforms = {
                **self._transforms,
                exp.CurrentDate: lambda self, _: self.sql(exp.cast(exec_time, "date")),
                exp.CurrentDatetime: lambda self, _: self.sql(exp.cast(exec_time, "datetime")),
                exp.CurrentTime: lambda self, _: self.sql(exp.cast(exec_time, "time")),
                exp.CurrentTimestamp: lambda self, _: self.sql(exp.cast(exec_time, "timestamp")),
            }

        super().__init__()

    def shortDescription(self) -> t.Optional[str]:
        return self.body.get("description")

    def setUp(self) -> None:
        """Load all input tables"""
        self.engine_adapter.create_schema(self._qualified_fixture_schema)

        for name, values in self.body.get("inputs", {}).items():
            all_types_are_known = False
            known_columns_to_types: t.Dict[str, exp.DataType] = {}

            model = self.models.get(name)
            if model:
                inferred_columns_to_types = model.columns_to_types or {}
                known_columns_to_types = {
                    c: t for c, t in inferred_columns_to_types.items() if type_is_known(t)
                }
                all_types_are_known = bool(inferred_columns_to_types) and (
                    len(known_columns_to_types) == len(inferred_columns_to_types)
                )

            # Types specified in the test will override the corresponding inferred ones
            known_columns_to_types.update(values.get("columns", {}))

            rows = values.get("rows")
            if not all_types_are_known and rows:
                for col, value in rows[0].items():
                    if col not in known_columns_to_types:
                        v_type = annotate_types(exp.convert(value)).type or type(value).__name__
                        v_type = exp.maybe_parse(
                            v_type, into=exp.DataType, dialect=self._test_adapter_dialect
                        )

                        if not type_is_known(v_type):
                            _raise_error(
                                f"Failed to infer the data type of column '{col}' for '{name}'. This issue can be "
                                "mitigated by casting the column in the model definition, setting its type in "
                                "external_models.yaml if it's an external model, setting the model's 'columns' property, "
                                "or setting its 'columns' mapping in the test itself",
                                self.path,
                            )

                        known_columns_to_types[col] = v_type

            if rows is None:
                query_or_df: exp.Query | pd.DataFrame = self._add_missing_columns(
                    values["query"], known_columns_to_types
                )
            else:
                query_or_df = self._create_df(values, columns=known_columns_to_types)

            self.engine_adapter.create_view(
                self._test_fixture_table(name), query_or_df, known_columns_to_types
            )

    def tearDown(self) -> None:
        """Drop all fixture tables."""
        if not self.preserve_fixtures:
            self.engine_adapter.drop_schema(self._qualified_fixture_schema, cascade=True)

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

        # The `actual` df's dtypes will almost always be pd.Timestamp for datetime values,
        # but in some scenarios (e.g., DuckDB >=0.10.2) it will be a pandas `object` type
        # containing python `datetime.xxx` values.
        #
        # Pandas `object` columns result in a noop for the `astype` call above. Because any
        # quoted YAML value is a string, we must manually convert the `expected` df string
        # values to the correct `datetime.xxx` type.
        #
        # We determine the type from a single sentinel value, but since the `actual` df is
        # coming from a database query, it is safe to assume that the column contains only
        # a single type.
        object_sentinel_values = {
            col: actual[col][0]
            for col in actual_types
            if is_object_dtype(actual_types[col]) and len(actual[col]) != 0
        }
        for col, value in object_sentinel_values.items():
            # can't use `isinstance()` here - https://stackoverflow.com/a/68743663/1707525
            if type(value) is datetime.date:
                expected[col] = pd.to_datetime(expected[col], errors="ignore").dt.date  # type: ignore
            elif type(value) is datetime.time:
                expected[col] = pd.to_datetime(expected[col], errors="ignore").dt.time  # type: ignore
            elif type(value) is datetime.datetime:
                expected[col] = pd.to_datetime(expected[col], errors="ignore").dt.to_pydatetime()  # type: ignore

        actual = actual.replace({np.nan: None})
        expected = expected.replace({np.nan: None})

        def _to_hashable(x: t.Any) -> t.Any:
            if isinstance(x, (list, np.ndarray)):
                return tuple(x)
            return str(x) if not isinstance(x, t.Hashable) else x

        if sort:
            actual = actual.apply(lambda col: col.map(_to_hashable))
            actual = actual.sort_values(by=actual.columns.to_list()).reset_index(drop=True)
            expected = expected.apply(lambda col: col.map(_to_hashable))
            expected = expected.sort_values(by=expected.columns.to_list()).reset_index(drop=True)

        try:
            pd.testing.assert_frame_equal(
                expected,
                actual,
                check_dtype=False,
                check_datetimelike_compat=True,
                check_like=True,  # Ignore column order
            )
        except AssertionError as e:
            if expected.shape != actual.shape:
                _raise_if_unexpected_columns(expected.columns, actual.columns)

                error_msg = "Data mismatch (rows are different)"

                missing_rows = _row_difference(expected, actual)
                if not missing_rows.empty:
                    error_msg += f"\n\nMissing rows:\n\n{missing_rows}"

                unexpected_rows = _row_difference(actual, expected)
                if not unexpected_rows.empty:
                    error_msg += f"\n\nUnexpected rows:\n\n{unexpected_rows}"

                e.args = (error_msg,)
            else:
                diff = expected.compare(actual).rename(columns={"self": "exp", "other": "act"})
                e.args = (f"Data mismatch (exp: expected, act: actual)\n\n{diff}",)

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
        preserve_fixtures: bool = False,
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
            preserve_fixtures: Preserve the fixture tables in the testing database, useful for debugging.
        """
        name = normalize_model_name(body["model"], default_catalog=default_catalog, dialect=dialect)
        model = models.get(name)
        if not model:
            _raise_error(f"Model '{name}' was not found", path)

        if isinstance(model, SqlModel):
            test_type: t.Type[ModelTest] = SqlModelTest
        elif isinstance(model, PythonModel):
            test_type = PythonModelTest
        else:
            _raise_error(f"Model '{name}' is an unsupported model type for testing", path)

        return test_type(
            body,
            test_name,
            t.cast(Model, model),
            models,
            engine_adapter,
            dialect,
            path,
            preserve_fixtures,
            default_catalog,
        )

    def __str__(self) -> str:
        return f"{self.test_name} ({self.path})"

    def _validate_and_normalize_test(self) -> None:
        inputs = self.body.get("inputs")
        outputs = self.body.get("outputs", {})

        if not outputs:
            _raise_error("Incomplete test, missing outputs", self.path)

        ctes = outputs.get("ctes")
        query = outputs.get("query")
        partial = outputs.pop("partial", None)

        def _normalize_rows(
            values: t.List[Row] | t.Dict,
            name: str,
            partial: bool = False,
            dialect: DialectType = None,
        ) -> t.Dict:
            if not isinstance(values, dict):
                values = {"rows": values}

            rows = values.get("rows")
            query = values.get("query")

            fmt = values.get("format")
            path = values.get("path")
            if fmt == "csv":
                csv_settings = values.get("csv_settings") or {}
                rows = pd.read_csv(path or StringIO(rows), **csv_settings).to_dict(orient="records")
            elif fmt in (None, "yaml"):
                if path:
                    input_rows = yaml_load(Path(path))
                    rows = input_rows.get("rows") if isinstance(input_rows, dict) else input_rows
            else:
                _raise_error(f"Unsupported data format '{fmt}' for '{name}'", self.path)

            if query is not None:
                if rows is not None:
                    _raise_error(
                        f"Invalid test, cannot set both 'query' and 'rows' for '{name}'", self.path
                    )

                # We parse the user-supplied query using the testing adapter dialect, but we
                # normalize its identifiers according to the model's dialect, so that, e.g.,
                # the projection names match those in its `columns_to_types` field
                values["query"] = normalize_identifiers(
                    exp.maybe_parse(query, dialect=self._test_adapter_dialect), dialect=dialect
                )
                return values

            if rows is None:
                _raise_error(f"Incomplete test, missing row data for '{name}'", self.path)

            assert isinstance(rows, list)
            values["rows"] = [
                {self._normalize_column_name(column): value for column, value in row.items()}
                for row in rows
            ]
            if partial:
                values["partial"] = True

            return values

        def _normalize_sources(
            sources: t.Dict, partial: bool = False, with_default_catalog: bool = True
        ) -> t.Dict:
            normalized_sources = {}
            for name, values in sources.items():
                normalized_name = self._normalize_model_name(
                    name, with_default_catalog=with_default_catalog
                )
                model = self.models.get(normalized_name)
                dialect = model.dialect if model else self.dialect

                normalized_sources[normalized_name] = _normalize_rows(
                    values, name, partial=partial, dialect=dialect
                )

            return normalized_sources

        normalized_model_name = self._normalize_model_name(self.body["model"])
        self.body["model"] = normalized_model_name

        if inputs:
            inputs = _normalize_sources(inputs)
            for name, values in inputs.items():
                columns = values.get("columns")
                if columns is None:
                    continue

                if not isinstance(columns, dict):
                    _raise_error(
                        f"Invalid 'columns' value for model '{name}', expected a mapping name -> type",
                        self.path,
                    )

                values["columns"] = {
                    self._normalize_column_name(c): exp.DataType.build(
                        t, dialect=self._test_adapter_dialect
                    )
                    for c, t in columns.items()
                }

            for depends_on in self.model.depends_on:
                if depends_on not in inputs:
                    _raise_error(f"Incomplete test, missing input model '{depends_on}'", self.path)

            if self.model.depends_on_self and normalized_model_name not in inputs:
                inputs[normalized_model_name] = {"rows": []}

            self.body["inputs"] = inputs

        if ctes:
            outputs["ctes"] = _normalize_sources(ctes, partial=partial, with_default_catalog=False)

        if query or query == []:
            outputs["query"] = _normalize_rows(
                query, self.model.name, partial=partial, dialect=self.model.dialect
            )

    def _test_fixture_table(self, name: str) -> exp.Table:
        table = self._fixture_table_cache.get(name)
        if not table:
            table = exp.to_table(name, dialect=self._test_adapter_dialect)

            # We change the table path below, so this ensures there are no name clashes
            table.this.set("this", "__".join(part.name for part in table.parts))

            table.set("db", self._fixture_schema.copy())
            if self._fixture_catalog:
                table.set("catalog", self._fixture_catalog.copy())

            self._fixture_table_cache[name] = table

        return table

    def _normalize_model_name(self, name: str, with_default_catalog: bool = True) -> str:
        normalized_name = self._normalized_model_name_cache.get((name, with_default_catalog))
        if normalized_name is None:
            default_catalog = self.default_catalog if with_default_catalog else None
            normalized_name = normalize_model_name(
                name, default_catalog=default_catalog, dialect=self.dialect
            )
            self._normalized_model_name_cache[(name, with_default_catalog)] = normalized_name

        return normalized_name

    def _normalize_column_name(self, name: str) -> str:
        normalized_name = self._normalized_column_name_cache.get(name)
        if normalized_name is None:
            normalized_name = normalize_identifiers(name, dialect=self.dialect).name
            self._normalized_column_name_cache[name] = normalized_name

        return normalized_name

    def _execute(self, query: exp.Query) -> pd.DataFrame:
        """Executes the given query using the testing engine adapter and returns a DataFrame."""
        with patch.dict(self._test_adapter_dialect.generator_class.TRANSFORMS, self._transforms):
            return self.engine_adapter.fetchdf(query)

    def _create_df(
        self,
        values: t.Dict[str, t.Any],
        columns: t.Optional[t.Collection] = None,
        partial: t.Optional[bool] = False,
    ) -> pd.DataFrame:
        query = values.get("query")
        if query:
            return self._execute(self._add_missing_columns(query, columns))

        rows = values["rows"]
        if columns:
            referenced_columns = list(dict.fromkeys(col for row in rows for col in row))
            _raise_if_unexpected_columns(columns, referenced_columns)

            if partial:
                columns = referenced_columns

        return pd.DataFrame.from_records(rows, columns=columns)

    def _add_missing_columns(
        self, query: exp.Query, all_columns: t.Optional[t.Collection[str]] = None
    ) -> exp.Query:
        if not all_columns or query.is_star:
            return query

        query_columns = set(query.named_selects)
        missing_columns = [col for col in all_columns if col not in query_columns]
        if missing_columns:
            query.select(*[exp.null().as_(col) for col in missing_columns], copy=False)

        return query


class SqlModelTest(ModelTest):
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

                partial = values.get("partial")
                sort = cte_query.args.get("order") is None

                actual = self._execute(cte_query)
                expected = self._create_df(values, columns=cte_query.named_selects, partial=partial)

                self.assert_equal(expected, actual, sort=sort, partial=partial)

    def runTest(self) -> None:
        query = self._render_model_query()

        self.test_ctes(
            {
                self._normalize_model_name(cte.alias, with_default_catalog=False): cte
                for cte in query.ctes
            }
        )

        values = self.body["outputs"].get("query")
        if values is not None:
            partial = values.get("partial")
            sort = query.args.get("order") is None

            actual = self._execute(query)
            expected = self._create_df(values, columns=self.model.columns_to_types, partial=partial)

            self.assert_equal(expected, actual, sort=sort, partial=partial)

    def _render_model_query(self) -> exp.Query:
        return self.model.render_query_or_raise(
            **self.body.get("vars", {}),
            engine_adapter=self.engine_adapter,
            table_mapping={
                name: self._test_fixture_table(name).sql() for name in self.body.get("inputs", {})
            },
            runtime_stage=RuntimeStage.TESTING,
        )


class PythonModelTest(ModelTest):
    def __init__(
        self,
        body: t.Dict[str, t.Any],
        test_name: str,
        model: Model,
        models: UniqueKeyDict[str, Model],
        engine_adapter: EngineAdapter,
        dialect: str | None = None,
        path: Path | None = None,
        preserve_fixtures: bool = False,
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
            preserve_fixtures: Preserve the fixture tables in the testing database, useful for debugging.
        """
        from sqlmesh.core.test.context import TestExecutionContext

        super().__init__(
            body,
            test_name,
            model,
            models,
            engine_adapter,
            dialect,
            path,
            preserve_fixtures,
            default_catalog,
        )

        self.context = TestExecutionContext(
            engine_adapter=engine_adapter,
            models=models,
            test=self,
            default_dialect=dialect,
            default_catalog=default_catalog,
        )

    def runTest(self) -> None:
        values = self.body["outputs"].get("query")
        if values is not None:
            partial = values.get("partial")

            actual_df = self._execute_model()
            actual_df.reset_index(drop=True, inplace=True)
            expected = self._create_df(values, columns=self.model.columns_to_types, partial=partial)

            self.assert_equal(expected, actual_df, sort=False, partial=partial)

    def _execute_model(self) -> pd.DataFrame:
        """Executes the python model and returns a DataFrame."""
        time_ctx = freeze_time(self._execution_time) if self._execution_time else nullcontext()
        with patch.dict(self._test_adapter_dialect.generator_class.TRANSFORMS, self._transforms):
            with t.cast(AbstractContextManager, time_ctx):
                return t.cast(
                    pd.DataFrame,
                    next(self.model.render(context=self.context, **self.body.get("vars", {}))),
                )


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
    include_ctes: bool = False,
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
        include_ctes: When true, CTE fixtures will also be generated.
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
        models[dep].name: pandas_timestamp_to_pydatetime(
            engine_adapter.fetchdf(query).apply(lambda col: col.map(_normalize_df_value)),
            models[dep].columns_to_types,
        )
        .replace({np.nan: None})
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
        assert isinstance(test, SqlModelTest)
        model_query = test._render_model_query()

        if include_ctes:
            ctes = {}
            previous_ctes: t.List[exp.CTE] = []
            for cte in model_query.ctes:
                cte_query = cte.this
                for prev in previous_ctes:
                    cte_query = cte_query.with_(prev.alias, prev.this)

                cte_output = test._execute(cte_query)
                ctes[cte.alias] = (
                    pandas_timestamp_to_pydatetime(
                        cte_output.apply(lambda col: col.map(_normalize_df_value)),
                        cte_query.named_selects,
                    )
                    .replace({np.nan: None})
                    .to_dict(orient="records")
                )

                previous_ctes.append(cte)

            if ctes:
                outputs["ctes"] = ctes

        output = test._execute(model_query)
    else:
        output = t.cast(PythonModelTest, test)._execute_model()

    outputs["query"] = (
        pandas_timestamp_to_pydatetime(
            output.apply(lambda col: col.map(_normalize_df_value)), model.columns_to_types
        )
        .replace({np.nan: None})
        .to_dict(orient="records")
    )

    test.tearDown()

    fixture_path.parent.mkdir(exist_ok=True, parents=True)
    with open(fixture_path, "w", encoding="utf-8") as file:
        yaml.dump({test_name: test_body}, file)


def _raise_if_unexpected_columns(
    expected_cols: t.Collection[str], actual_cols: t.Collection[str]
) -> None:
    unique_expected_cols = set(expected_cols)
    unknown_cols = [col for col in actual_cols if col not in unique_expected_cols]

    if unknown_cols:
        expected = f"Expected column(s): {', '.join(list(expected_cols))}\n"
        unknown = f"Unknown column(s): {', '.join(unknown_cols)}"
        _raise_error(f"Detected unknown column(s)\n\n{expected}{unknown}")


def _row_difference(left: pd.DataFrame, right: pd.DataFrame) -> pd.DataFrame:
    """Returns all rows in `left` that don't appear in `right`."""
    rows_missing_from_right = []

    # `None` replaces `np.nan` because `np.nan != np.nan` and this would affect the mapping lookup
    right_row_count: t.MutableMapping[t.Tuple, int] = Counter(
        right.replace({np.nan: None}).itertuples(index=False, name=None)
    )
    for left_row in left.replace({np.nan: None}).itertuples(index=False):
        left_row_tuple = tuple(left_row)
        if right_row_count[left_row_tuple] <= 0:
            rows_missing_from_right.append(left_row)
        else:
            right_row_count[left_row_tuple] -= 1

    return pd.DataFrame(rows_missing_from_right)


def _raise_error(msg: str, path: Path | None = None) -> None:
    if path:
        raise TestError(f"{msg} at {path}")
    raise TestError(msg)


def _normalize_df_value(value: t.Any) -> t.Any:
    """Normalize data in a pandas dataframe so ruamel and sqlglot can deal with it."""
    if isinstance(value, (list, np.ndarray)):
        return [_normalize_df_value(v) for v in value]
    if isinstance(value, dict):
        if "key" in value and "value" in value:
            # Maps returned by DuckDB look like: {'key': ['key1', 'key2'], 'value': [10, 20]}
            # so we convert to {'key1': 10, 'key2': 20} (TODO: handle more dialects here)
            return {k: _normalize_df_value(v) for k, v in zip(value["key"], value["value"])}
        return {k: _normalize_df_value(v) for k, v in value.items()}
    return value
