import platform
from pathlib import Path

IS_WINDOWS = platform.system() == "Windows"

WINDOWS_LONGPATH_PREFIX = "\\\\?\\"


def fix_windows_path(path: Path) -> Path:
    """
    Windows paths are limited to 260 characters: https://learn.microsoft.com/en-us/windows/win32/fileio/maximum-file-path-limitation
    Users can change this by updating a registry entry but we cant rely on that.

    SQLMesh quite commonly generates cache file paths that exceed 260 characters and thus cause a FileNotFound error.
    If we prefix paths with "\\?\" then we can have paths up to 32,767 characters.

    Note that this prefix also means that relative paths no longer work. From the above docs:
     > Because you cannot use the "\\?\" prefix with a relative path, relative paths are always limited to a total of MAX_PATH characters.

    So we also call path.resolve() to resolve the relative sections so that operations like `path.read_text()` continue to work
    """
    if path.parts and not path.parts[0].startswith(WINDOWS_LONGPATH_PREFIX):
        path = Path(WINDOWS_LONGPATH_PREFIX + str(path.absolute()))
    return path.resolve()
