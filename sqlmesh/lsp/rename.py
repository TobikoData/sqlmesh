import typing as t
from lsprotocol.types import (
    Position,
    TextEdit,
    WorkspaceEdit,
    PrepareRenameResult_Type1,
    DocumentHighlight,
    DocumentHighlightKind,
)

from sqlmesh.lsp.context import LSPContext
from sqlmesh.lsp.helpers import to_sqlmesh_position, to_lsp_range
from sqlmesh.lsp.reference import (
    _position_within_range,
    get_cte_references,
    CTEReference,
)
from sqlmesh.lsp.uri import URI


def prepare_rename(
    lsp_context: LSPContext, document_uri: URI, lsp_position: Position
) -> t.Optional[PrepareRenameResult_Type1]:
    """
    Prepare for rename operation by checking if the symbol at the position can be renamed.

    Args:
        lsp_context: The LSP context
        document_uri: The URI of the document
        position: The position in the document

    Returns:
        PrepareRenameResult if the symbol can be renamed, None otherwise
    """
    # Check if there's a CTE at this position
    position = to_sqlmesh_position(lsp_position)
    cte_references = get_cte_references(lsp_context, document_uri, position)
    if cte_references:
        # Find the target CTE definition to get its range
        target_range = None
        for ref in cte_references:
            # Check if cursor is on a CTE usage
            if _position_within_range(position, ref.range):
                target_range = ref.target_range
                break
            # Check if cursor is on the CTE definition
            elif _position_within_range(position, ref.target_range):
                target_range = ref.target_range
                break
        if target_range:
            return PrepareRenameResult_Type1(
                range=to_lsp_range(target_range), placeholder="cte_name"
            )

    # For now, only CTEs are supported
    return None


def rename_symbol(
    lsp_context: LSPContext, document_uri: URI, lsp_position: Position, new_name: str
) -> t.Optional[WorkspaceEdit]:
    """
    Perform rename operation on the symbol at the given position.

    Args:
        lsp_context: The LSP context
        document_uri: The URI of the document
        position: The position in the document
        new_name: The new name for the symbol

    Returns:
        WorkspaceEdit with the changes, or None if no symbol to rename
    """
    # Check if there's a CTE at this position
    cte_references = get_cte_references(
        lsp_context, document_uri, to_sqlmesh_position(lsp_position)
    )
    if cte_references:
        return _rename_cte(cte_references, new_name)

    # For now, only CTEs are supported
    return None


def _rename_cte(cte_references: t.List[CTEReference], new_name: str) -> WorkspaceEdit:
    """
    Create a WorkspaceEdit for renaming a CTE.

    Args:
        cte_references: List of CTE references (definition and usages)
        new_name: The new name for the CTE

    Returns:
        WorkspaceEdit with the text edits for renaming the CTE
    """
    changes: t.Dict[str, t.List[TextEdit]] = {}

    for ref in cte_references:
        uri = URI.from_path(ref.path).value
        if uri not in changes:
            changes[uri] = []

        # Create a text edit for this reference
        text_edit = TextEdit(range=to_lsp_range(ref.range), new_text=new_name)
        changes[uri].append(text_edit)

    return WorkspaceEdit(changes=changes)


def get_document_highlights(
    lsp_context: LSPContext, document_uri: URI, position: Position
) -> t.Optional[t.List[DocumentHighlight]]:
    """
    Get document highlights for all occurrences of the symbol at the given position.

    This function finds all occurrences of a symbol (CTE) within the current document
    and returns them as DocumentHighlight objects for "Change All Occurrences" feature.

    Args:
        lsp_context: The LSP context
        document_uri: The URI of the document
        position: The position in the document to find highlights for

    Returns:
        List of DocumentHighlight objects or None if no symbol found
    """
    # Check if there's a CTE at this position
    cte_references = get_cte_references(lsp_context, document_uri, to_sqlmesh_position(position))
    if cte_references:
        highlights = []
        for ref in cte_references:
            # Determine the highlight kind based on whether it's a definition or usage
            kind = (
                DocumentHighlightKind.Write
                if ref.range == ref.target_range
                else DocumentHighlightKind.Read
            )

            highlights.append(DocumentHighlight(range=to_lsp_range(ref.range), kind=kind))
        return highlights

    # For now, only CTEs are supported
    return None
