import pytest

from sqlmesh.utils import sanitize_name


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("simple", "simple"),
        ("snake_case", "snake_case"),
        ("客户数据", "客户数据"),  # pure Chinese kept
        ("客户-数据 v2", "客户_数据_v2"),  # dash/space -> underscore
        ("中文，逗号", "中文_逗号"),  # full-width comma -> underscore
        ("a/b", "a_b"),  # slash -> underscore
        ("spaces\tand\nnewlines", "spaces_and_newlines"),
        ("data📦2025", "data_2025"),
        ("MiXeD123_名字", "MiXeD123_名字"),
        ("", ""),
    ],
)
def test_sanitize_known_cases(raw, expected):
    assert sanitize_name(raw) == expected
