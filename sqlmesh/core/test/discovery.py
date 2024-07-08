from __future__ import annotations

import fnmatch
import itertools
import pathlib
import re
import typing as t
from collections.abc import Iterator

import ruamel

from sqlmesh.utils import unique
from sqlmesh.utils.pydantic import PydanticModel
from sqlmesh.utils.yaml import load as yaml_load


class ModelTestMetadata(PydanticModel):
    path: pathlib.Path
    test_name: str
    body: t.Union[t.Dict, ruamel.yaml.comments.CommentedMap]

    @property
    def fully_qualified_test_name(self) -> str:
        return f"{self.path}::{self.test_name}"

    def __hash__(self) -> int:
        return self.fully_qualified_test_name.__hash__()


def replace_placeholder_identifiers(file_contents: str, variables: dict[str, str]) -> str:
    replaced_file_contents = file_contents
    for m in re.finditer(r"@{(.*)}", file_contents):
        thing_to_replace = file_contents[m.span()[0]:m.span()[1]]
        thing_to_replace_with = variables.get(m.group(1))
        if thing_to_replace_with:
            replaced_file_contents = re.sub(thing_to_replace, thing_to_replace_with, replaced_file_contents)
        else:
            raise ValueError(f"Could not find value for {thing_to_replace}")
    return replaced_file_contents


def load_model_test_file(path: pathlib.Path, variables: dict[str, str]| None) -> dict[str, ModelTestMetadata]:
    """Load a single model test file.

    Args:
        path: The path to the test file

    returns:
        A list of ModelTestMetadata named tuples.
    """
    model_test_metadata = {}
    with open(path) as f:
        file_contents = f.read()

    if variables:
        file_contents = replace_placeholder_identifiers(file_contents, variables)

    contents = yaml_load(file_contents)

    for test_name, value in contents.items():
        model_test_metadata[test_name] = ModelTestMetadata(
            path=path, test_name=test_name, body=value
        )
    return model_test_metadata


def discover_model_tests(
        path: pathlib.Path, ignore_patterns: list[str] | None = None, variables: dict[str, str] | None = None
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
            for model_test_metadata in load_model_test_file(yaml_file, variables).values():
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
    variables: dict[str, str] | None = None,
) -> list[ModelTestMetadata]:
    model_test_metadatas = [
        meta for path in paths for meta in discover_model_tests(pathlib.Path(path), ignore_patterns, variables)
    ]
    if patterns:
        model_test_metadatas = filter_tests_by_patterns(model_test_metadatas, patterns)
    return model_test_metadatas
