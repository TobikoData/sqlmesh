import pytest

from sqlmesh.utils import sanitize_name


@pytest.mark.parametrize(
    "raw,exclude_unicode,include_unicode",
    [
        ("simple", "simple", "simple"),
        ("snake_case", "snake_case", "snake_case"),
        ("客户数据", "____", "客户数据"),
        ("客户-数据 v2", "______v2", "客户_数据_v2"),
        ("中文，逗号", "_____", "中文_逗号"),
        ("a/b", "a_b", "a_b"),
        ("spaces\tand\nnewlines", "spaces_and_newlines", "spaces_and_newlines"),
        ("data📦2025", "data_2025", "data_2025"),
        ("MiXeD123_名字", "MiXeD123___", "MiXeD123_名字"),
        ("", "", ""),
    ],
)
def test_sanitize_name_no_(raw, exclude_unicode, include_unicode):
    assert sanitize_name(raw) == exclude_unicode
    assert sanitize_name(raw, include_unicode=True) == include_unicode
