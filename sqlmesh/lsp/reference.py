from lsprotocol.types import Range, Position
import typing as t

from sqlmesh.core.dialect import normalize_model_name
from sqlmesh.core.model.definition import SqlModel
from sqlmesh.lsp.context import LSPContext, ModelTarget, AuditTarget
from sqlglot import exp

from sqlmesh.utils.pydantic import PydanticModel


class Reference(PydanticModel):
    """
    A reference to a model.

    Attributes:
        range: The range of the reference in the source file
        uri: The uri of the referenced model
        description: The description of the referenced model
    """

    range: Range
    uri: str
    description: t.Optional[str] = None


def by_position(position: Position) -> t.Callable[[Reference], bool]:
    """
    Filter reference to only filter references that contain the given position.

    Args:
        position: The cursor position to check

    Returns:
        A function that returns True if the reference contains the position, False otherwise
    """

    def contains_position(r: Reference) -> bool:
        return (
            r.range.start.line < position.line
            or (
                r.range.start.line == position.line
                and r.range.start.character <= position.character
            )
        ) and (
            r.range.end.line > position.line
            or (r.range.end.line == position.line and r.range.end.character >= position.character)
        )

    return contains_position


def get_references(
    lint_context: LSPContext, document_uri: str, position: Position
) -> t.List[Reference]:
    """
    Get references at a specific position in a document.

    Used for hover information.

    Args:
        lint_context: The LSP context
        document_uri: The URI of the document
        position: The position to check for references

    Returns:
        A list of references at the given position
    """
    references = get_model_definitions_for_a_path(lint_context, document_uri)
    filtered_references = list(filter(by_position(position), references))
    return filtered_references


def get_model_definitions_for_a_path(
    lint_context: LSPContext, document_uri: str
) -> t.List[Reference]:
    """
    Get the model references for a given path.

    Works for models and standalone audits.
    Works for targeting sql and python models.

    Steps:
    - Get the parsed query
    - Find all table objects using find_all exp.Table
        - Match the string against all model names
    - Need to normalize it before matching
    - Try get_model before normalization
    - Match to models that the model refers to
    """
    # Ensure the path is a sql file
    if not document_uri.endswith(".sql"):
        return []

    # Get the file info from the context map
    if document_uri not in lint_context.map:
        return []

    file_info = lint_context.map[document_uri]

    # Process based on whether it's a model or standalone audit
    if isinstance(file_info, ModelTarget):
        # It's a model
        model = lint_context.context.get_model(
            model_or_snapshot=file_info.names[0], raise_if_missing=False
        )
        if model is None or not isinstance(model, SqlModel):
            return []

        query = model.query
        dialect = model.dialect
        depends_on = model.depends_on
        file_path = model._path
    elif isinstance(file_info, AuditTarget):
        # It's a standalone audit
        audit = lint_context.context.standalone_audits.get(file_info.name)
        if audit is None:
            return []

        query = audit.query
        dialect = audit.dialect
        depends_on = audit.depends_on
        file_path = audit._path
    else:
        return []

    # Find all possible references
    references = []

    # Get SQL query and find all table references
    tables = list(query.find_all(exp.Table))
    if len(tables) == 0:
        return []

    with open(file_path, "r", encoding="utf-8") as file:
        read_file = file.readlines()

    for table in tables:
        # Normalize the table reference
        unaliased = table.copy()
        if unaliased.args.get("alias") is not None:
            unaliased.set("alias", None)
        reference_name = unaliased.sql(dialect=dialect)
        try:
            normalized_reference_name = normalize_model_name(
                reference_name,
                default_catalog=lint_context.context.default_catalog,
                dialect=dialect,
            )
            if normalized_reference_name not in depends_on:
                continue
        except Exception:
            # Skip references that cannot be normalized
            continue

        # Get the referenced model uri
        referenced_model = lint_context.context.get_model(
            model_or_snapshot=normalized_reference_name, raise_if_missing=False
        )
        if referenced_model is None:
            continue
        referenced_model_path = referenced_model._path
        # Check whether the path exists
        if not referenced_model_path.is_file():
            continue
        referenced_model_uri = f"file://{referenced_model_path}"

        # Extract metadata for positioning
        table_meta = TokenPositionDetails.from_meta(table.this.meta)
        table_range = _range_from_token_position_details(table_meta, read_file)
        start_pos = table_range.start
        end_pos = table_range.end

        # If there's a catalog or database qualifier, adjust the start position
        catalog_or_db = table.args.get("catalog") or table.args.get("db")
        if catalog_or_db is not None:
            catalog_or_db_meta = TokenPositionDetails.from_meta(catalog_or_db.meta)
            catalog_or_db_range = _range_from_token_position_details(catalog_or_db_meta, read_file)
            start_pos = catalog_or_db_range.start

        references.append(
            Reference(
                uri=referenced_model_uri,
                range=Range(start=start_pos, end=end_pos),
                description=referenced_model.description,
            )
        )

    return references


class TokenPositionDetails(PydanticModel):
    """
    Details about a token's position in the source code.

    Attributes:
        line (int): The line that the token ends on.
        col (int): The column that the token ends on.
        start (int): The start index of the token.
        end (int): The ending index of the token.
    """

    line: int
    col: int
    start: int
    end: int

    @staticmethod
    def from_meta(meta: t.Dict[str, int]) -> "TokenPositionDetails":
        return TokenPositionDetails(
            line=meta["line"],
            col=meta["col"],
            start=meta["start"],
            end=meta["end"],
        )


def _range_from_token_position_details(
    token_position_details: TokenPositionDetails, read_file: t.List[str]
) -> Range:
    """
    Convert a TokenPositionDetails object to a Range object.

    :param token_position_details: Details about a token's position
    :param read_file: List of lines from the file
    :return: A Range object representing the token's position
    """
    # Convert from 1-indexed to 0-indexed for line only
    end_line_0 = token_position_details.line - 1
    end_col_0 = token_position_details.col

    # Find the start line and column by counting backwards from the end position
    start_pos = token_position_details.start
    end_pos = token_position_details.end

    # Initialize with the end position
    start_line_0 = end_line_0
    start_col_0 = end_col_0 - (end_pos - start_pos + 1)

    # If start_col_0 is negative, we need to go back to previous lines
    while start_col_0 < 0 and start_line_0 > 0:
        start_line_0 -= 1
        start_col_0 += len(read_file[start_line_0])
        # Account for newline character
        if start_col_0 >= 0:
            break
        start_col_0 += 1  # For the newline character

    # Ensure we don't have negative values
    start_col_0 = max(0, start_col_0)
    return Range(
        start=Position(line=start_line_0, character=start_col_0),
        end=Position(line=end_line_0, character=end_col_0),
    )
