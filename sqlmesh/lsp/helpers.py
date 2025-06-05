from lsprotocol.types import Range, Position

from sqlmesh.core.linter.helpers import (
    Range as SQLMeshRange,
    Position as SQLMeshPosition,
)


def to_lsp_range(
    range: SQLMeshRange,
) -> Range:
    """
    Converts a SQLMesh Range to an LSP Range.
    """
    return Range(
        start=Position(line=range.start.line, character=range.start.character),
        end=Position(line=range.end.line, character=range.end.character),
    )


def to_lsp_position(
    position: SQLMeshPosition,
) -> Position:
    """
    Converts a SQLMesh Position to an LSP Position.
    """
    return Position(line=position.line, character=position.character)
