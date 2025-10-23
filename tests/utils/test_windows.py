import pytest
from pathlib import Path
from sqlmesh.utils.windows import IS_WINDOWS, WINDOWS_LONGPATH_PREFIX, fix_windows_path


@pytest.mark.skipif(
    not IS_WINDOWS, reason="pathlib.Path only produces WindowsPath objects on Windows"
)
def test_fix_windows_path():
    short_path = Path("c:\\foo")
    short_path_prefixed = Path(WINDOWS_LONGPATH_PREFIX + "c:\\foo")

    segments = "\\".join(["bar", "baz", "bing"] * 50)
    long_path = Path("c:\\" + segments)
    long_path_prefixed = Path(WINDOWS_LONGPATH_PREFIX + "c:\\" + segments)

    assert len(str(short_path.absolute)) < 260
    assert len(str(long_path.absolute)) > 260

    # paths less than 260 chars are still prefixed because they may be being used as a base path
    assert fix_windows_path(short_path) == short_path_prefixed

    # paths greater than 260 characters don't work at all without the prefix
    assert fix_windows_path(long_path) == long_path_prefixed

    # multiple calls dont keep appending the same prefix
    assert (
        fix_windows_path(fix_windows_path(fix_windows_path(long_path_prefixed)))
        == long_path_prefixed
    )

    # paths with relative sections need to have relative sections resolved before they can be used
    # since the \\?\ prefix doesnt work for paths with relative sections
    assert fix_windows_path(Path("c:\\foo\\..\\bar")) == Path(WINDOWS_LONGPATH_PREFIX + "c:\\bar")

    # also check that relative sections are still resolved if they are added to a previously prefixed path
    base = fix_windows_path(Path("c:\\foo"))
    assert base == Path(WINDOWS_LONGPATH_PREFIX + "c:\\foo")
    assert fix_windows_path(base / ".." / "bar") == Path(WINDOWS_LONGPATH_PREFIX + "c:\\bar")
