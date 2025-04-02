from __future__ import annotations

import sys
import time
import pathlib
import threading
import typing as t
import unittest

import concurrent
from concurrent.futures import ThreadPoolExecutor

from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.core.model import Model
from sqlmesh.core.test.definition import ModelTest as ModelTest, generate_test as generate_test
from sqlmesh.core.test.discovery import (
    ModelTestMetadata as ModelTestMetadata,
    filter_tests_by_patterns as filter_tests_by_patterns,
    get_all_model_tests as get_all_model_tests,
    load_model_test_file as load_model_test_file,
)
from sqlmesh.core.config.connection import BaseDuckDBConnectionConfig

from sqlmesh.core.test.result import ModelTextTestResult as ModelTextTestResult
from sqlmesh.utils import UniqueKeyDict, Verbosity


if t.TYPE_CHECKING:
    from sqlmesh.core.config.loader import C


class ModelTextTestRunner(unittest.TextTestRunner):
    def __init__(
        self,
        **kwargs: t.Any,
    ) -> None:
        # StringIO is used to capture the output of the tests since we'll
        # run them in parallel and we don't want to mix the output streams
        from io import StringIO

        super().__init__(
            stream=StringIO(),
            resultclass=ModelTextTestResult,
            **kwargs,
        )


def create_testing_engine_adapters(
    model_test_metadata: list[ModelTestMetadata],
    config: C,
    default_gateway: str,
    default_catalog: str | None = None,
    default_catalog_dialect: str = "",
) -> t.Dict[ModelTestMetadata, EngineAdapter]:
    testing_adapter_by_gateway: t.Dict[str, EngineAdapter] = {}
    metadata_to_adapter = {}

    for metadata in model_test_metadata:
        gateway = metadata.body.get("gateway") or default_gateway
        test_connection = config.get_test_connection(
            gateway, default_catalog, default_catalog_dialect
        )

        concurrent_tasks = test_connection.concurrent_tasks

        is_duckdb_connection = isinstance(test_connection, BaseDuckDBConnectionConfig)
        adapter = None
        if is_duckdb_connection:
            # Ensure DuckDB connections are fully isolated from each other
            # by forcing the creation of a new adapter with SingletonConnectionPool
            test_connection.concurrent_tasks = 1
            adapter = test_connection.create_engine_adapter(register_comments_override=False)
            test_connection.concurrent_tasks = concurrent_tasks
        elif gateway not in testing_adapter_by_gateway:
            # All other engines can share connections between threads
            testing_adapter_by_gateway[gateway] = test_connection.create_engine_adapter(
                register_comments_override=False
            )

        metadata_to_adapter[metadata] = adapter or testing_adapter_by_gateway[gateway]

    return metadata_to_adapter


def run_tests(
    model_test_metadata: list[ModelTestMetadata],
    models: UniqueKeyDict[str, Model],
    config: C,
    gateway: t.Optional[str] = None,
    dialect: str | None = None,
    verbosity: Verbosity = Verbosity.DEFAULT,
    preserve_fixtures: bool = False,
    stream: t.TextIO | None = None,
    default_catalog: str | None = None,
    default_catalog_dialect: str = "",
) -> ModelTextTestResult:
    """Create a test suite of ModelTest objects and run it.

    Args:
        model_test_metadata: A list of ModelTestMetadata named tuples.
        models: All models to use for expansion and mapping of physical locations.
        verbosity: The verbosity level.
        preserve_fixtures: Preserve the fixture tables in the testing database, useful for debugging.
    """
    default_gateway = gateway or config.default_gateway_name

    default_test_connection = config.get_test_connection(
        gateway_name=default_gateway,
        default_catalog=default_catalog,
        default_catalog_dialect=default_catalog_dialect,
    )

    lock = threading.Lock()

    combined_results = ModelTextTestResult(
        stream=unittest.runner._WritelnDecorator(stream or sys.stderr),  # type: ignore
        verbosity=2 if verbosity >= Verbosity.VERBOSE else 1,
        descriptions=True,
    )

    metadata_to_adapter = create_testing_engine_adapters(
        model_test_metadata=model_test_metadata,
        config=config,
        default_gateway=default_gateway,
        default_catalog=default_catalog,
        default_catalog_dialect=default_catalog_dialect,
    )

    def _run_single_test(
        metadata: ModelTestMetadata, engine_adapter: EngineAdapter
    ) -> ModelTextTestResult:
        test = ModelTest.create_test(
            body=metadata.body,
            test_name=metadata.test_name,
            models=models,
            engine_adapter=engine_adapter,
            dialect=dialect,
            path=metadata.path,
            default_catalog=default_catalog,
            preserve_fixtures=preserve_fixtures,
        )

        result = t.cast(
            ModelTextTestResult,
            ModelTextTestRunner().run(t.cast(unittest.TestCase, test)),
        )

        with lock:
            if result.successes:
                combined_results.addSuccess(result.successes[0])
            elif result.errors:
                combined_results.addError(result.original_err[0], result.original_err[1])
            elif result.failures:
                combined_results.addFailure(result.original_err[0], result.original_err[1])
            elif result.skipped:
                skipped_args = result.skipped[0]
                combined_results.addSkip(skipped_args[0], skipped_args[1])
        return result

    test_results = []

    # Ensure workers are not greater than the number of tests
    num_workers = min(len(model_test_metadata) or 1, default_test_connection.concurrent_tasks)

    start_time = time.perf_counter()
    try:
        with ThreadPoolExecutor(max_workers=num_workers) as pool:
            futures = [
                pool.submit(_run_single_test, metadata=metadata, engine_adapter=engine_adapter)
                for metadata, engine_adapter in metadata_to_adapter.items()
            ]

            for future in concurrent.futures.as_completed(futures):
                test_results.append(future.result())
    finally:
        for engine_adapter in set(metadata_to_adapter.values()):
            # The engine adapters list might have duplicates, so we ensure that we close each adapter once
            if engine_adapter:
                engine_adapter.close()

    end_time = time.perf_counter()

    combined_results.testsRun = len(test_results)

    combined_results.log_test_report(test_duration=end_time - start_time)

    return combined_results


def run_model_tests(
    tests: list[str],
    models: UniqueKeyDict[str, Model],
    config: C,
    gateway: t.Optional[str] = None,
    dialect: str | None = None,
    verbosity: Verbosity = Verbosity.DEFAULT,
    patterns: list[str] | None = None,
    preserve_fixtures: bool = False,
    stream: t.TextIO | None = None,
    default_catalog: t.Optional[str] = None,
    default_catalog_dialect: str = "",
) -> ModelTextTestResult:
    """Load and run tests.

    Args:
        tests: A list of tests to run, e.g. [tests/test_orders.yaml::test_single_order]
        models: All models to use for expansion and mapping of physical locations.
        verbosity: The verbosity level.
        patterns: A list of patterns to match against.
        preserve_fixtures: Preserve the fixture tables in the testing database, useful for debugging.
    """
    loaded_tests = []
    for test in tests:
        filename, test_name = test.split("::", maxsplit=1) if "::" in test else (test, "")
        path = pathlib.Path(filename)

        if test_name:
            loaded_tests.append(load_model_test_file(path, variables=config.variables)[test_name])
        else:
            loaded_tests.extend(load_model_test_file(path, variables=config.variables).values())

    if patterns:
        loaded_tests = filter_tests_by_patterns(loaded_tests, patterns)

    return run_tests(
        loaded_tests,
        models,
        config,
        gateway=gateway,
        dialect=dialect,
        verbosity=verbosity,
        preserve_fixtures=preserve_fixtures,
        stream=stream,
        default_catalog=default_catalog,
        default_catalog_dialect=default_catalog_dialect,
    )
