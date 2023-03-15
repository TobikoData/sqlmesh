import difflib
import fnmatch
import itertools
import pathlib
import types
import typing as t
import unittest

import pandas as pd
import ruamel
from sqlglot import Expression, exp, parse_one

from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.core.snapshot import Snapshot
from sqlmesh.utils import unique
from sqlmesh.utils.errors import SQLMeshError
from sqlmesh.utils.pydantic import PydanticModel
from sqlmesh.utils.yaml import load as yaml_load


class ModelTestMetadata(PydanticModel):
    path: pathlib.Path
    test_name: str
    body: ruamel.yaml.comments.CommentedMap

    @property
    def fully_qualified_test_name(self) -> str:
        return f"{self.path}::{self.test_name}"

    def __hash__(self) -> int:
        return self.fully_qualified_test_name.__hash__()


class TestError(SQLMeshError):
    """Test error"""


class ModelTest(unittest.TestCase):
    view_names: t.List[str] = []

    def __init__(
        self,
        body: t.Dict[str, t.Any],
        test_name: str,
        snapshots: t.Dict[str, Snapshot],
        engine_adapter: EngineAdapter,
        path: t.Optional[pathlib.Path],
    ) -> None:
        """ModelTest encapsulates a unit test for a model.

        Args:
            body: A dictionary that contains test metadata like inputs and outputs.
            test_name: The name of the test.
            snapshots: All snapshots to use for expansion and mapping of physical locations.
            engine_adapter: The engine adapter to use.
            path: An optional path to the test definition yaml file
        """
        self.body = body
        self.path = path

        self.test_name = test_name
        self.engine_adapter = engine_adapter

        if "model" not in body:
            self._raise_error("Incomplete test, missing model name")

        if "outputs" not in body:
            self._raise_error("Incomplete test, missing outputs")

        self.model_name = body["model"]
        if self.model_name not in snapshots:
            self._raise_error(f"Model '{self.model_name}' was not found")

        self.snapshot = snapshots[self.model_name]

        inputs = self.body.get("inputs", {})
        for snapshot_id in self.snapshot.parents:
            if snapshot_id.name not in inputs:
                self._raise_error(f"Incomplete test, missing input for table {snapshot_id.name}")

        self.query = self.snapshot.model.render_query(**self.body.get("vars", {}))
        # For tests we just use the model name for the table reference and we don't want to expand
        mapping = {name: _test_fixture_name(name) for name in snapshots}
        if mapping:
            self.query = exp.replace_tables(self.query, mapping)

        self.ctes = {cte.alias: cte for cte in self.query.ctes}

        super().__init__()

    def setUp(self) -> None:
        """Load all input tables"""
        inputs = {name: table["rows"] for name, table in self.body.get("inputs", {}).items()}
        self.engine_adapter.create_schema(self.snapshot.physical_schema)
        for table, rows in inputs.items():
            df = pd.DataFrame.from_records(rows)  # noqa
            columns_to_types: t.Dict[str, exp.DataType] = {}
            for i, v in rows[0].items():
                # convert ruamel into python
                v = v.real if hasattr(v, "real") else v
                columns_to_types[i] = parse_one(type(v).__name__, into=exp.DataType)  # type: ignore
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

    def execute(self, query: Expression) -> pd.DataFrame:
        """Execute the query with the engine adapter and return a DataFrame"""
        return self.engine_adapter.fetchdf(query)

    def test_ctes(self) -> None:
        """Run CTE queries and compare output to expected output"""
        for cte_name, value in self.body["outputs"].get("ctes", {}).items():
            with self.subTest(cte=cte_name):
                if cte_name not in self.ctes:
                    self._raise_error(f"No CTE named {cte_name} found in model {self.model_name}")
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

    def __str__(self) -> str:
        return f"{self.test_name} ({self.path}:{self.body.lc.line})"  # type: ignore

    def _raise_error(self, msg: str) -> None:
        raise TestError(f"{msg} at {self.path}")


class ModelTextTestResult(unittest.TextTestResult):
    def addFailure(
        self,
        test: unittest.TestCase,
        err: t.Union[
            t.Tuple[t.Type[BaseException], BaseException, types.TracebackType],
            t.Tuple[None, None, None],
        ],
    ) -> None:
        """Called when the test case test signals a failure.

        The traceback is suppressed because it is redundant and not useful.

        Args:
            test: The test case.
            err: A tuple of the form returned by sys.exc_info(), i.e., (type, value, traceback).
        """
        exctype, value, tb = err
        return super().addFailure(test, (exctype, value, None))  # type: ignore


def load_model_test_file(
    path: pathlib.Path,
) -> t.Dict[str, ModelTestMetadata]:
    """Load a single model test file.

    Args:
        path: The path to the test file

    returns:
        A list of ModelTestMetadata named tuples.
    """
    model_test_metadata = {}
    contents = yaml_load(path)

    for test_name, value in contents.items():
        model_test_metadata[test_name] = ModelTestMetadata(
            path=path, test_name=test_name, body=value
        )
    return model_test_metadata


def discover_model_tests(
    path: pathlib.Path, ignore_patterns: t.Optional[t.List[str]] = None
) -> t.Generator[ModelTestMetadata, None, None]:
    """Discover model tests.

    Model tests are defined in YAML files and contain the inputs and outputs used to test model queries.

    Args:
        path: A path to search for tests.
        ignore_patterns: An optional list of patterns to ignore.

    Returns:
        A list of ModelTestMetadata named tuples.
    """
    search_path = pathlib.Path(path)

    for yaml_file in itertools.chain(
        search_path.glob("**/test*.yaml"),
        search_path.glob("**/test*.yml"),
    ):
        for ignore_pattern in ignore_patterns or []:
            if yaml_file.match(ignore_pattern):
                break
        else:
            for model_test_metadata in load_model_test_file(yaml_file).values():
                yield model_test_metadata


def filter_tests_by_patterns(
    tests: t.List[ModelTestMetadata], patterns: t.List[str]
) -> t.List[ModelTestMetadata]:
    """Filter out tests whose filename or name does not match a pattern.

    Args:
        tests: A list of ModelTestMetadata named tuples to match.
        patterns: A list of patterns to match against.

    Returns:
        A list of ModelTestMetadata named tuples.
    """
    return unique(
        test
        for test, pattern in itertools.product(tests, patterns)
        if ("*" in pattern and fnmatch.fnmatchcase(test.fully_qualified_test_name, pattern))
        or pattern in test.fully_qualified_test_name
    )


def run_tests(
    model_test_metadata: t.List[ModelTestMetadata],
    snapshots: t.Dict[str, Snapshot],
    engine_adapter: EngineAdapter,
    verbosity: int = 1,
) -> unittest.result.TestResult:
    """Create a test suite of ModelTest objects and run it.

    Args:
        model_test_metadata: A list of ModelTestMetadata named tuples.
        snapshots: All snapshots to use for expansion and mapping of physical locations.
        engine_adapter: The engine adapter to use.
        patterns: A list of patterns to match against.
        verbosity: The verbosity level.
    """
    suite = unittest.TestSuite(
        ModelTest(
            body=metadata.body,
            test_name=metadata.test_name,
            snapshots=snapshots,
            engine_adapter=engine_adapter,
            path=metadata.path,
        )
        for metadata in model_test_metadata
    )
    return unittest.TextTestRunner(verbosity=verbosity, resultclass=ModelTextTestResult).run(suite)


def get_all_model_tests(
    path: pathlib.Path,
    patterns: t.Optional[t.List[str]] = None,
    ignore_patterns: t.Optional[t.List[str]] = None,
) -> t.List[ModelTestMetadata]:
    model_test_metadatas = list(discover_model_tests(pathlib.Path(path), ignore_patterns))
    if patterns:
        model_test_metadatas = filter_tests_by_patterns(model_test_metadatas, patterns)
    return model_test_metadatas


def run_all_model_tests(
    path: pathlib.Path,
    snapshots: t.Dict[str, Snapshot],
    engine_adapter: EngineAdapter,
    verbosity: int = 1,
    patterns: t.Optional[t.List[str]] = None,
    ignore_patterns: t.Optional[t.List[str]] = None,
) -> unittest.result.TestResult:
    """Discover and run all model tests found in path.

    Args:
        path: A path to search for tests.
        snapshots: All snapshots to use for expansion and mapping of physical locations.
        engine_adapter: The engine adapter to use.
        verbosity: The verbosity level.
        patterns: A list of patterns to match against.
        ignore_patterns: An optional list of patterns to ignore.
    """
    model_tests = get_all_model_tests(path, patterns, ignore_patterns)
    return run_tests(model_tests, snapshots, engine_adapter, verbosity)


def run_model_tests(
    tests: t.List[str],
    snapshots: t.Dict[str, Snapshot],
    engine_adapter: EngineAdapter,
    verbosity: int = 1,
    patterns: t.Optional[t.List[str]] = None,
    ignore_patterns: t.Optional[t.List[str]] = None,
) -> unittest.result.TestResult:
    """Load and run tests.

    Args
        tests: A list of tests to run, e.g. [tests/test_orders.yaml::test_single_order]
        snapshots: All snapshots to use for expansion and mapping of physical locations.
        engine_adapter: The engine adapter to use.
        patterns: A list of patterns to match against.
        verbosity: The verbosity level.
        ignore_patterns: An optional list of patterns to ignore.
    """
    loaded_tests = []
    for test in tests:
        filename, test_name = test.split("::", maxsplit=1) if "::" in test else (test, "")
        path = pathlib.Path(filename)
        for ignore_pattern in ignore_patterns or []:
            if path.match(ignore_pattern):
                break
        else:
            if test_name:
                loaded_tests.append(load_model_test_file(path)[test_name])
            else:
                loaded_tests.extend(load_model_test_file(path).values())
    if patterns:
        loaded_tests = filter_tests_by_patterns(loaded_tests, patterns)
    return run_tests(loaded_tests, snapshots, engine_adapter, verbosity)


def _test_fixture_name(name: str) -> str:
    return f"{name}__fixture"
