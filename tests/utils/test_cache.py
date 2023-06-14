from pathlib import Path

from pytest_mock.plugin import MockerFixture
from sqlglot import parse_one

from sqlmesh.core.model import SqlModel
from sqlmesh.core.model.cache import OptimizedQueryCache
from sqlmesh.utils.cache import FileCache
from sqlmesh.utils.pydantic import PydanticModel


class _TestEntry(PydanticModel):
    value: str


def test_file_cache(tmp_path: Path, mocker: MockerFixture):
    cache = FileCache(tmp_path, _TestEntry)

    test_entry_a = _TestEntry(value="value_a")
    test_entry_b = _TestEntry(value="value_b")

    loader = mocker.Mock(return_value=test_entry_a)

    assert cache.get("test_name", "test_entry_a") is None

    assert cache.get_or_load("test_name", "test_entry_a", loader) == test_entry_a
    assert cache.get_or_load("test_name", "test_entry_a", loader) == test_entry_a
    assert cache.get("test_name", "test_entry_a") == test_entry_a

    cache.put("test_name", "test_entry_b", test_entry_b)
    assert cache.get("test_name", "test_entry_b") == test_entry_b
    assert cache.get_or_load("test_name", "test_entry_b", loader) == test_entry_b
    assert cache.get("test_name", "test_entry_a") is None

    assert cache.get("different_name", "test_entry_b") is None

    loader.assert_called_once()


def test_optimized_query_cache(tmp_path: Path, mocker: MockerFixture):
    model = SqlModel(
        name="test_model",
        query=parse_one("SELECT a FROM tbl"),
        mapping_schema={"tbl": {"a": "int"}},
    )

    cache = OptimizedQueryCache(tmp_path)

    assert not cache.with_optimized_query(model)
    assert cache.with_optimized_query(model)
