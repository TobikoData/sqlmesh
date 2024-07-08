from __future__ import annotations

import pathlib
import typing as t
import unittest

from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.core.model import Model
from sqlmesh.core.test.definition import ModelTest as ModelTest, generate_test as generate_test
from sqlmesh.core.test.discovery import (
    ModelTestMetadata as ModelTestMetadata,
    filter_tests_by_patterns as filter_tests_by_patterns,
    get_all_model_tests as get_all_model_tests,
    load_model_test_file as load_model_test_file,
)
from sqlmesh.core.test.result import ModelTextTestResult as ModelTextTestResult
from sqlmesh.utils import UniqueKeyDict

if t.TYPE_CHECKING:
    from sqlmesh.core.config.loader import C


def run_tests(
    model_test_metadata: list[ModelTestMetadata],
    models: UniqueKeyDict[str, Model],
    config: C,
    gateway: t.Optional[str] = None,
    dialect: str | None = None,
    verbosity: int = 1,
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
    testing_adapter_by_gateway: t.Dict[str, EngineAdapter] = {}
    default_gateway = gateway or config.default_gateway_name

    try:
        tests = []
        for metadata in model_test_metadata:
            body = metadata.body
            gateway = body.get("gateway") or default_gateway
            testing_engine_adapter = testing_adapter_by_gateway.get(gateway)
            if not testing_engine_adapter:
                testing_engine_adapter = config.get_test_connection(
                    gateway,
                    default_catalog,
                    default_catalog_dialect,
                ).create_engine_adapter(register_comments_override=False)
                testing_adapter_by_gateway[gateway] = testing_engine_adapter

            tests.append(
                ModelTest.create_test(
                    body=body,
                    test_name=metadata.test_name,
                    models=models,
                    engine_adapter=testing_engine_adapter,
                    dialect=dialect,
                    path=metadata.path,
                    default_catalog=default_catalog,
                    preserve_fixtures=preserve_fixtures,
                )
            )

        result = t.cast(
            ModelTextTestResult,
            unittest.TextTestRunner(
                stream=stream, verbosity=verbosity, resultclass=ModelTextTestResult
            ).run(unittest.TestSuite(tests)),
        )
    finally:
        for testing_engine_adapter in testing_adapter_by_gateway.values():
            testing_engine_adapter.close()

    return result


def run_model_tests(
    tests: list[str],
    models: UniqueKeyDict[str, Model],
    config: C,
    gateway: t.Optional[str] = None,
    dialect: str | None = None,
    verbosity: int = 1,
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
