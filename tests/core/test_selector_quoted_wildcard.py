from __future__ import annotations

import fnmatch

import pytest

from sqlmesh.core.selector import parse


@pytest.mark.parametrize(
    ("selector", "expected"),
    [
        ("foo.bar", "foo.bar"),
        ("foo*", "foo*"),
        ("*bar", "*bar"),
        ("'biquery-project'.schema.table", "'biquery-project'.schema.table"),
        ("'biquery-project'.schema*", "'biquery-project'.schema*"),
        ('"biquery-project".schema*', '"biquery-project".schema*'),
        ("`biquery-project`.schema*", "`biquery-project`.schema*"),
    ],
)
def test_parse_preserves_quotes_in_var(selector: str, expected: str) -> None:
    node = parse(selector)
    assert node.this == expected


def test_fnmatch_works_with_quotes_in_pattern() -> None:
    model_name = "'biquery-project'.schema.table"
    assert fnmatch.fnmatchcase(model_name, "'biquery-project'.schema*")
