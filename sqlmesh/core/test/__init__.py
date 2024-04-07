from __future__ import annotations

import pathlib
import typing as t
import unittest

from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.core.model import Model
from sqlmesh.core.test.definition import ModelTest, generate_test
from sqlmesh.core.test.discovery import (
    ModelTestMetadata,
    filter_tests_by_patterns,
    get_all_model_tests,
    load_model_test_file,
)
from sqlmesh.core.test.result import ModelTextTestResult
from sqlmesh.utils import UniqueKeyDict


def run_tests(
    model_test_metadata: list[ModelTestMetadata],
    models: UniqueKeyDict[str, Model],
    engine_adapter: EngineAdapter,
    dialect: str | None = None,
    verbosity: int = 1,
    persist_fixtures: bool = False,
    stream: t.TextIO | None = None,
    default_catalog: str | None = None,
) -> ModelTextTestResult:
    """Create a test suite of ModelTest objects and run it.

    Args:
        model_test_metadata: A list of ModelTestMetadata named tuples.
        models: All models to use for expansion and mapping of physical locations.
        engine_adapter: The engine adapter to use.
        verbosity: The verbosity level.
        persist_fixtures: Persist the fixture tables in the testing database, useful for debugging.
    """
    suite = unittest.TestSuite(
        ModelTest.create_test(
            body=metadata.body,
            test_name=metadata.test_name,
            models=models,
            engine_adapter=engine_adapter,
            dialect=dialect,
            path=metadata.path,
            default_catalog=default_catalog,
            persist_fixtures=persist_fixtures,
        )
        for metadata in model_test_metadata
    )

    return t.cast(
        ModelTextTestResult,
        unittest.TextTestRunner(
            stream=stream, verbosity=verbosity, resultclass=ModelTextTestResult
        ).run(suite),
    )


def run_model_tests(
    tests: list[str],
    models: UniqueKeyDict[str, Model],
    engine_adapter: EngineAdapter,
    dialect: str | None = None,
    verbosity: int = 1,
    patterns: list[str] | None = None,
    persist_fixtures: bool = False,
    stream: t.TextIO | None = None,
    default_catalog: t.Optional[str] = None,
) -> ModelTextTestResult:
    """Load and run tests.

    Args:
        tests: A list of tests to run, e.g. [tests/test_orders.yaml::test_single_order]
        models: All models to use for expansion and mapping of physical locations.
        engine_adapter: The engine adapter to use.
        verbosity: The verbosity level.
        patterns: A list of patterns to match against.
        persist_fixtures: Persist the fixture tables in the testing database, useful for debugging.
    """
    loaded_tests = []
    for test in tests:
        filename, test_name = test.split("::", maxsplit=1) if "::" in test else (test, "")
        path = pathlib.Path(filename)

        if test_name:
            loaded_tests.append(load_model_test_file(path)[test_name])
        else:
            loaded_tests.extend(load_model_test_file(path).values())

    if patterns:
        loaded_tests = filter_tests_by_patterns(loaded_tests, patterns)

    return run_tests(
        loaded_tests,
        models,
        engine_adapter,
        dialect=dialect,
        verbosity=verbosity,
        persist_fixtures=persist_fixtures,
        stream=stream,
        default_catalog=default_catalog,
    )
