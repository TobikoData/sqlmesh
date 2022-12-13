"""
# Model Tests
Model tests are one of the tools SQLMesh provides to validate your models. Along with `sqlmesh.core.audit`,
they are a great way to ensure the quality of your data and to build trust in it across your organization.
Similar to unit tests in software engineering, model tests take some input and compare the expected output with
actual output from your model query. A comprehensive suite of tests can empower your data engineers and analysts
to work with confidence because downstream models are behaving as expcted when they make changes to models.

# What exactly are model tests
Model tests are input and output data fixtures defined in YAML files in a test directory in your project. SQLMesh
even allows you to test individual CTEs in your model queries.
```yaml
test_customer_revenue_by_day:
  model: sushi.customer_revenue_by_day
  inputs:
    sushi.orders:
      rows:
        - id: 1
          customer_id: 1
          waiter_id: 1
          start_ts: 2022-01-01 01:59:00
          end_ts: 2022-01-01 02:29:00
          ds: 2022-01-01
        - id: 2
          customer_id: 1
          waiter_id: 2
          start_ts: 2022-01-01 03:59:00
          end_ts: 2022-01-01 03:29:00
          ds: 2022-01-01
    sushi.order_items:
      rows:
        - id: 1
          order_id: 1
          item_id: 1
          quantity: 2
          ds: 2022-01-01
        - id: 2
          order_id: 1
          item_id: 2
          quantity: 3
          ds: 2022-01-01
        - id: 3
          order_id: 2
          item_id: 1
          quantity: 4
          ds: 2022-01-01
        - id: 4
          order_id: 2
          item_id: 2
          quantity: 5
          ds: 2022-01-01
    sushi.items:
      rows:
        - id: 1
          name: maguro
          price: 1.23
          ds: 2022-01-01
        - id: 2
          name: ika
          price: 2.34
          ds: 2022-01-01
  outputs:
    vars:
      start: 2022-01-01
      end: 2022-01-01
      latest: 2022-01-01
    ctes:
      order_total:
        rows:
        - order_id: 1
          total: 9.48
          ds: 2022-01-01
        - order_id: 2
          total: 16.62
          ds: 2022-01-01
    query:
      rows:
      - customer_id: 1
        revenue: 26.1
        ds: '2022-01-01'
```
In the above example, we defined a model test for the `sushi.customer_revenue_by_day` model to ensure the model query
behaves as expcted. The test provides upstream data as input for the model as well as expected output for the model
and a CTE used by the model. SQLMesh will load the input rows, execute your model's CTE and query, and compare them to
the output rows.

# Running model tests
## The CLI test command
You can execute your model tests with the `sqlmesh test` command.
```
% sqlmesh --path example test
...F
======================================================================
FAIL: test_customer_revenue_by_day (example/models/tests/test_customer_revenue_by_day.yaml:1)
----------------------------------------------------------------------
AssertionError: Data differs
- {'customer_id': 1, 'revenue': 26.2, 'ds': '2022-01-01'}
?                                  ^

+ {'customer_id': 1, 'revenue': 26.1, 'ds': '2022-01-01'}
?                                  ^


----------------------------------------------------------------------
Ran 4 tests in 0.030s

FAILED (failures=1)
```
SQLMesh will run your model tests and identify any that fail.

You can run a specific model test by passing in the module followed by `::` and the name of the test, e.g.
`project.tests.test_order_items::test_single_order_item`. You can also run tests that match a pattern or
substring using a glob pathname expansion syntax. For example, `project.tests.test_order*` will match
`project.tests.test_orders` and `project.tests.test_order_items`.

# Advanced usage
## Reusable Data Fixtures
TODO
"""
import difflib
import fnmatch
import itertools
import pathlib
import typing as t
import unittest

import duckdb
import pandas as pd
import ruamel
from sqlglot import Expression, exp, parse_one

from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.core.snapshot import Snapshot, table_name
from sqlmesh.utils import unique
from sqlmesh.utils.errors import SQLMeshError
from sqlmesh.utils.pydantic import PydanticModel
from sqlmesh.utils.yaml import yaml


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
        # For tests we just use the model name for the table reference and we don't want to expand
        mapping = {name: name for name in snapshots}
        self.query = self.snapshot.model.render_query(
            snapshots=snapshots, mapping=mapping, **self.body["outputs"].get("vars", {})
        )
        self.ctes = {cte.alias: cte for cte in self.query.ctes}

        super().__init__()

    def setUp(self) -> None:
        """Load all input tables"""
        inputs = {
            name: table["rows"] for name, table in self.body.get("inputs", {}).items()
        }
        conn = duckdb.connect()
        self.engine_adapter.create_schema(self.snapshot.physical_schema)

        for table, rows in inputs.items():
            self.engine_adapter.create_schema(table)
            df = pd.DataFrame.from_records(rows)  # noqa
            self.engine_adapter.create_and_insert(
                table,
                {
                    column_name: exp.DataType.build(column_type)
                    for column_name, column_type, *_ in conn.execute(
                        "DESCRIBE SELECT * FROM df"
                    ).fetchall()
                },
                exp.values([tuple(row.values()) for row in rows]),
            )

        for snapshot_id in self.snapshot.parents:
            if snapshot_id.name not in inputs:
                self._raise_error(
                    f"Incomplete test, missing input for table {snapshot_id.name}"
                )
            self.view_names.append(
                table_name(
                    self.snapshot.physical_schema,
                    snapshot_id.name,
                    snapshot_id.fingerprint,
                )
            )
            self.engine_adapter.create_view(
                self.view_names[-1],
                parse_one(f"SELECT * FROM {snapshot_id.name}"),  # type: ignore
            )

    def tearDown(self) -> None:
        """Drop all input tables"""
        for table in self.body.get("inputs", {}):
            self.engine_adapter.execute(f"DROP TABLE {table}")
        for view in self.view_names:
            self.engine_adapter.drop_view(view)

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
        self.engine_adapter.execute(query)
        return self.engine_adapter.cursor.df()

    def test_ctes(self) -> None:
        """Run CTE queries and compare output to expected output"""
        for cte_name, value in self.body["outputs"].get("ctes", {}).items():
            with self.subTest(cte=cte_name):
                if cte_name not in self.ctes:
                    self._raise_error(
                        f"No CTE named {cte_name} found in model {self.model_name}"
                    )
                expected_df = pd.DataFrame.from_records(value["rows"])
                actual_df = self.execute(self.ctes[cte_name].this)
                self.assert_equal(expected_df, actual_df)

    def runTest(self) -> None:
        self.test_ctes()

        # Test model query
        if "rows" in self.body["outputs"].get("query", {}):
            expected_df = pd.DataFrame.from_records(
                self.body["outputs"]["query"]["rows"]
            )
            actual_df = self.execute(self.query)
            self.assert_equal(expected_df, actual_df)

    def __str__(self):
        return f"{self.test_name} ({self.path}:{self.body.lc.line})"

    def _raise_error(self, msg: str) -> None:
        raise TestError(f"{msg} at {self.path}")


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
    with open(path) as f:
        contents = yaml.load(f.read())
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
    for yaml_file in search_path.glob("**/test*.yaml"):
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
        if (
            "*" in pattern
            and fnmatch.fnmatchcase(test.fully_qualified_test_name, pattern)
        )
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
    return unittest.TextTestRunner(verbosity=verbosity).run(suite)


def get_all_model_tests(
    path: pathlib.Path,
    patterns: t.Optional[t.List[str]] = None,
    ignore_patterns: t.Optional[t.List[str]] = None,
) -> t.List[ModelTestMetadata]:
    model_test_metadatas = list(
        discover_model_tests(pathlib.Path(path), ignore_patterns)
    )
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
        filename, test_name = (
            test.split("::", maxsplit=1) if "::" in test else (test, "")
        )
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
