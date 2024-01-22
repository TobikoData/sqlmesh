from sqlmesh.utils import groupby


def test_groupby() -> None:
    assert groupby([1, 2, 5, 3, 4], lambda x: x % 2) == {0: [2, 4], 1: [1, 5, 3]}
    assert groupby(["apple", "banana", "avocado", "orange"], lambda x: x[0]) == {
        "a": ["apple", "avocado"],
        "b": ["banana"],
        "o": ["orange"],
    }
