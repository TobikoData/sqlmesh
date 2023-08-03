from __future__ import annotations

import pathlib
import typing as t
import unittest

from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.core.model import Model
from sqlmesh.core.test.definition import ModelTest
from sqlmesh.core.test.discovery import (
    ModelTestMetadata,
    filter_tests_by_patterns,
    get_all_model_tests,
    load_model_test_file,
)
from sqlmesh.core.test.result import ModelTextTestResult


def run_tests(
    model_test_metadata: list[ModelTestMetadata],
    models: dict[str, Model],
    engine_adapter: EngineAdapter,
    dialect: str | None = None,
    verbosity: int = 1,
    stream: t.TextIO | None = None,
) -> unittest.result.TestResult:
    """Create a test suite of ModelTest objects and run it.

    Args:
        model_test_metadata: A list of ModelTestMetadata named tuples.
        models: All models to use for expansion and mapping of physical locations.
        engine_adapter: The engine adapter to use.
        verbosity: The verbosity level.
    """
    suite = unittest.TestSuite(
        ModelTest.create_test(
            body=metadata.body,
            test_name=metadata.test_name,
            models=models,
            engine_adapter=engine_adapter,
            dialect=dialect,
            path=metadata.path,
        )
        for metadata in model_test_metadata
    )

    return unittest.TextTestRunner(
        stream=stream, verbosity=verbosity, resultclass=ModelTextTestResult
    ).run(suite)


def run_model_tests(
    tests: list[str],
    models: dict[str, Model],
    engine_adapter: EngineAdapter,
    dialect: str | None = None,
    verbosity: int = 1,
    patterns: list[str] | None = None,
    stream: t.TextIO | None = None,
) -> unittest.result.TestResult:
    """Load and run tests.

    Args
        tests: A list of tests to run, e.g. [tests/test_orders.yaml::test_single_order]
        models: All models to use for expansion and mapping of physical locations.
        engine_adapter: The engine adapter to use.
        verbosity: The verbosity level.
        patterns: A list of patterns to match against.
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

    return run_tests(loaded_tests, models, engine_adapter, dialect, verbosity, stream)
