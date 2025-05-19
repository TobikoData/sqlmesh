import platform
from pathlib import Path

IS_WINDOWS = platform.system() == "Windows"


def fix_windows_path(path: Path) -> Path:
    """
    Windows paths are limited to 260 characters: https://learn.microsoft.com/en-us/windows/win32/fileio/maximum-file-path-limitation
    Users can change this by updating a registry entry but we cant rely on that.
    We can quite commonly generate a cache file path that exceeds 260 characters which causes a FileNotFound error.
    If we prefix the path with "\\?\" then we can have paths up to 32,767 characters
    """
    return Path("\\\\?\\" + str(path.absolute()))
