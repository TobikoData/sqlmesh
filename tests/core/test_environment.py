import pytest

from sqlmesh.core.environment import Environment


def test_normalize_name():
    assert Environment.normalize_name("12foo!#%@") == "12foo____"
    assert Environment.normalize_name("__test@$@%") == "__test____"
    assert Environment.normalize_name("test") == "test"
    assert Environment.normalize_name("-test_") == "_test_"
    with pytest.raises(TypeError, match="Expected str or Environment, got int"):
        Environment.normalize_name(123)
