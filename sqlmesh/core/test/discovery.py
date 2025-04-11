from __future__ import annotations

import fnmatch
import itertools
import pathlib
import typing as t

import ruamel

from sqlmesh.utils import unique
from sqlmesh.utils.pydantic import PydanticModel


class ModelTestMetadata(PydanticModel):
    path: pathlib.Path
    test_name: str
    body: t.Union[t.Dict, ruamel.yaml.comments.CommentedMap]

    @property
    def fully_qualified_test_name(self) -> str:
        return f"{self.path}::{self.test_name}"

    def __hash__(self) -> int:
        return self.fully_qualified_test_name.__hash__()


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
