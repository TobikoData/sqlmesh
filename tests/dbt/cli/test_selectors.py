import typing as t
import pytest
from sqlmesh_dbt import selectors


@pytest.mark.parametrize(
    "dbt_select,expected",
    [
        ([], None),
        (["main.model_a"], "main.model_a"),
        (["main.model_a main.model_b"], "main.model_a | main.model_b"),
        (["main.model_a", "main.model_b"], "main.model_a | main.model_b"),
        (["(main.model_a & ^main.model_b)"], "(main.model_a & ^main.model_b)"),
        (
            ["(+main.model_a & ^main.model_b)", "main.model_c"],
            "(+main.model_a & ^main.model_b) | main.model_c",
        ),
    ],
)
def test_selection(dbt_select: t.List[str], expected: t.Optional[str]):
    assert selectors.to_sqlmesh(dbt_select=dbt_select, dbt_exclude=[]) == expected


@pytest.mark.parametrize(
    "dbt_exclude,expected",
    [
        ([], None),
        (["main.model_a"], "^(main.model_a)"),
        (["(main.model_a & main.model_b)"], "^(main.model_a & main.model_b)"),
        (["main.model_a,main.model_b"], "^(main.model_a & main.model_b)"),
        (["main.model_a +main.model_b"], "^(main.model_a | +main.model_b)"),
        (
            ["(+main.model_a & ^main.model_b)", "main.model_c"],
            "^((+main.model_a & ^main.model_b) | main.model_c)",
        ),
    ],
)
def test_exclusion(dbt_exclude: t.List[str], expected: t.Optional[str]):
    assert selectors.to_sqlmesh(dbt_select=[], dbt_exclude=dbt_exclude) == expected


@pytest.mark.parametrize(
    "dbt_select,dbt_exclude,expected",
    [
        ([], [], None),
        (["+main.model_a"], ["raw.src_data"], "+main.model_a & ^(raw.src_data)"),
        (
            ["+main.model_a", "main.*b+"],
            ["raw.src_data"],
            "(+main.model_a | main.*b+) & ^(raw.src_data)",
        ),
        (
            ["+main.model_a", "main.*b+"],
            ["raw.src_data", "tag:disabled"],
            "(+main.model_a | main.*b+) & ^(raw.src_data | tag:disabled)",
        ),
    ],
)
def test_selection_and_exclusion(
    dbt_select: t.List[str], dbt_exclude: t.List[str], expected: t.Optional[str]
):
    assert selectors.to_sqlmesh(dbt_select=dbt_select, dbt_exclude=dbt_exclude) == expected


@pytest.mark.parametrize(
    "expression,expected",
    [
        ("", ([], [])),
        ("model_a", (["model_a"], [])),
        ("model_a model_b", (["model_a", "model_b"], [])),
        ("model_a,model_b", ([], ["model_a", "model_b"])),
        ("model_a model_b,model_c", (["model_a"], ["model_b", "model_c"])),
        ("model_a,model_b model_c", (["model_c"], ["model_a", "model_b"])),
    ],
)
def test_split_unions_and_intersections(
    expression: str, expected: t.Tuple[t.List[str], t.List[str]]
):
    assert selectors._split_unions_and_intersections(expression) == expected
