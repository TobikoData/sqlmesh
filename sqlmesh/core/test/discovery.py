from __future__ import annotations

import fnmatch
import itertools
import pathlib
import typing as t
from collections.abc import Iterator

import ruamel

from sqlmesh.core import constants as c
from sqlmesh.utils import unique
from sqlmesh.utils.pydantic import PydanticModel
from sqlmesh.utils.yaml import load as yaml_load

if t.TYPE_CHECKING:
    from sqlmesh.core.config.loader import C


class ModelTestMetadata(PydanticModel):
    path: pathlib.Path
    test_name: str
    body: t.Union[t.Dict, ruamel.yaml.comments.CommentedMap]

    @property
    def fully_qualified_test_name(self) -> str:
        return f"{self.path}::{self.test_name}"

    def __hash__(self) -> int:
        return self.fully_qualified_test_name.__hash__()


def load_model_test_file(
    path: pathlib.Path, variables: dict[str, t.Any] | None = None
) -> dict[str, ModelTestMetadata]:
    """Load a single model test file.

    Args:
        path: The path to the test file

    returns:
        A list of ModelTestMetadata named tuples.
    """
    model_test_metadata = {}
    contents = yaml_load(path, variables=variables)

    for test_name, value in contents.items():
        model_test_metadata[test_name] = ModelTestMetadata(
            path=path, test_name=test_name, body=value
        )
    return model_test_metadata


def discover_model_tests(
    path: pathlib.Path,
    ignore_patterns: list[str] | None = None,
    variables: dict[str, t.Any] | None = None,
) -> Iterator[ModelTestMetadata]:
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
            for model_test_metadata in load_model_test_file(
                yaml_file, variables=variables
            ).values():
                yield model_test_metadata


def filter_tests_by_patterns(
    tests: list[ModelTestMetadata], patterns: list[str]
) -> list[ModelTestMetadata]:
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


def get_all_model_tests(
    *paths: pathlib.Path,
    patterns: list[str] | None = None,
    ignore_patterns: list[str] | None = None,
    variables: dict[str, t.Any] | None = None,
) -> list[ModelTestMetadata]:
    model_test_metadatas = [
        meta
        for path in paths
        for meta in discover_model_tests(pathlib.Path(path), ignore_patterns, variables=variables)
    ]
    if patterns:
        model_test_metadatas = filter_tests_by_patterns(model_test_metadatas, patterns)
    return model_test_metadatas


def load_model_tests(
    configs: dict[pathlib.Path, C],
    tests: t.Optional[t.List[str]] = None,
    patterns: list[str] | None = None,
    variables: dict[str, t.Any] | None = None,
) -> list[ModelTestMetadata]:
    """Load model tests into a list of ModelTestMetadata which will be propagated to the test runner.

    Args:
        tests: A list of tests to load; If not specified, all tests are loaded
        patterns: A list of patterns to match against.
        variables: A dictionary of variables to use when loading the tests.
        configs: A dictionary of configs to use when loading all the tests.
    """
    test_meta = []

    if tests:
        for test in tests:
            filename, test_name = test.split("::", maxsplit=1) if "::" in test else (test, "")

            test_file = load_model_test_file(pathlib.Path(filename), variables=variables)
            if test_name:
                test_meta.append(test_file[test_name])
            else:
                test_meta.extend(test_file.values())

        if patterns:
            test_meta = filter_tests_by_patterns(test_meta, patterns)

    else:
        for path, config in configs.items():
            test_meta.extend(
                get_all_model_tests(
                    path / c.TESTS,
                    patterns=patterns,
                    ignore_patterns=config.ignore_patterns,
                    variables=variables,
                )
            )

    return test_meta
