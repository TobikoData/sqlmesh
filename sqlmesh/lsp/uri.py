from pathlib import Path
from pygls.uris import from_fs_path, to_fs_path
import typing as t


class URI:
    """
    A URI is a unique identifier for a file used in the LSP.
    """

    def __init__(self, uri: str):
        self.value: str = uri

    def __hash__(self) -> int:
        return hash(self.value)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, URI):
            return False
        return self.value == other.value

    def __repr__(self) -> str:
        return f"URI({self.value})"

    def to_path(self) -> Path:
        p = to_fs_path(self.value)
        return Path(p)

    @staticmethod
    def from_path(path: t.Union[str, Path]) -> "URI":
        if isinstance(path, Path):
            path = path.as_posix()
        return URI(from_fs_path(path))
