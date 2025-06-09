from lsprotocol.types import Range, Position
import typing as t
from pathlib import Path
from pydantic import Field

from sqlmesh.core.audit import StandaloneAudit
from sqlmesh.core.dialect import normalize_model_name
from sqlmesh.core.linter.helpers import (
    TokenPositionDetails,
)
from sqlmesh.core.model.definition import SqlModel
from sqlmesh.lsp.context import LSPContext, ModelTarget, AuditTarget
from sqlglot import exp
from sqlmesh.lsp.description import generate_markdown_description
from sqlglot.optimizer.scope import build_scope

from sqlmesh.lsp.helpers import to_lsp_range, to_lsp_position
from sqlmesh.lsp.uri import URI
from sqlmesh.utils.pydantic import PydanticModel
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers
import ast
from sqlmesh.core.model import Model
from sqlmesh import macro
import inspect


class LSPModelReference(PydanticModel):
    """A LSP reference to a model."""

    type: t.Literal["model"] = "model"
    uri: str
    range: Range
    markdown_description: t.Optional[str] = None


class LSPCteReference(PydanticModel):
    """A LSP reference to a CTE."""

    type: t.Literal["cte"] = "cte"
    uri: str
    range: Range
    target_range: Range


class LSPMacroReference(PydanticModel):
    """A LSP reference to a macro."""

    type: t.Literal["macro"] = "macro"
    uri: str
    range: Range
    target_range: Range
    markdown_description: t.Optional[str] = None


Reference = t.Annotated[
    t.Union[LSPModelReference, LSPCteReference, LSPMacroReference], Field(discriminator="type")
]


def by_position(position: Position) -> t.Callable[[Reference], bool]:
    """
    Filter reference to only filter references that contain the given position.

    Args:
        position: The cursor position to check

    Returns:
        A function that returns True if the reference contains the position, False otherwise
    """

    def contains_position(r: Reference) -> bool:
        return _position_within_range(position, r.range)

    return contains_position


def get_references(
    lint_context: LSPContext, document_uri: URI, position: Position
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

    # Get macro references before filtering by position
    macro_references = get_macro_definitions_for_a_path(lint_context, document_uri)
    references.extend(macro_references)

    filtered_references = list(filter(by_position(position), references))
    return filtered_references


def get_model_definitions_for_a_path(
    lint_context: LSPContext, document_uri: URI
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
    - Also find CTE references within the query
    """
    path = document_uri.to_path()
    if path.suffix != ".sql":
        return []
    # Get the file info from the context map
    if path not in lint_context.map:
        return []

    file_info = lint_context.map[path]
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
    references: t.List[Reference] = []

    with open(file_path, "r", encoding="utf-8") as file:
        read_file = file.readlines()

    # Build a scope tree to properly handle nested CTEs
    try:
        query = normalize_identifiers(query.copy(), dialect=dialect)
        root_scope = build_scope(query)
    except Exception:
        root_scope = None

    if root_scope:
        # Traverse all scopes to find CTE definitions and table references
        for scope in root_scope.traverse():
            for table in scope.tables:
                table_name = table.name

                # Check if this table reference is a CTE in the current scope
                if cte_scope := scope.cte_sources.get(table_name):
                    cte = cte_scope.expression.parent
                    alias = cte.args["alias"]
                    if isinstance(alias, exp.TableAlias):
                        identifier = alias.this
                        if isinstance(identifier, exp.Identifier):
                            target_range_sqlmesh = TokenPositionDetails.from_meta(
                                identifier.meta
                            ).to_range(read_file)
                            table_range_sqlmesh = TokenPositionDetails.from_meta(
                                table.this.meta
                            ).to_range(read_file)

                            # Convert SQLMesh Range to LSP Range
                            target_range = to_lsp_range(target_range_sqlmesh)
                            table_range = to_lsp_range(table_range_sqlmesh)

                            references.append(
                                LSPCteReference(
                                    uri=document_uri.value,  # Same file
                                    range=table_range,
                                    target_range=target_range,
                                )
                            )
                    continue

                # For non-CTE tables, process as before (external model references)
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
                referenced_model_uri = URI.from_path(referenced_model_path)

                # Extract metadata for positioning
                table_meta = TokenPositionDetails.from_meta(table.this.meta)
                table_range_sqlmesh = table_meta.to_range(read_file)
                start_pos_sqlmesh = table_range_sqlmesh.start
                end_pos_sqlmesh = table_range_sqlmesh.end

                # If there's a catalog or database qualifier, adjust the start position
                catalog_or_db = table.args.get("catalog") or table.args.get("db")
                if catalog_or_db is not None:
                    catalog_or_db_meta = TokenPositionDetails.from_meta(catalog_or_db.meta)
                    catalog_or_db_range_sqlmesh = catalog_or_db_meta.to_range(read_file)
                    start_pos_sqlmesh = catalog_or_db_range_sqlmesh.start

                description = generate_markdown_description(referenced_model)

                references.append(
                    LSPModelReference(
                        uri=referenced_model_uri.value,
                        range=Range(
                            start=to_lsp_position(start_pos_sqlmesh),
                            end=to_lsp_position(end_pos_sqlmesh),
                        ),
                        markdown_description=description,
                    )
                )

    return references


def get_macro_definitions_for_a_path(
    lsp_context: LSPContext, document_uri: URI
) -> t.List[Reference]:
    """
    Get macro references for a given path.

    This function finds all macro invocations (e.g., @ADD_ONE, @MULTIPLY) in a SQL file
    and creates references to their definitions in the Python macro files.

    Args:
        lsp_context: The LSP context containing macro definitions
        document_uri: The URI of the document to search for macro invocations

    Returns:
        A list of Reference objects for each macro invocation found
    """
    path = document_uri.to_path()
    if path.suffix != ".sql":
        return []

    # Get the file info from the context map
    if path not in lsp_context.map:
        return []

    file_info = lsp_context.map[path]
    # Process based on whether it's a model or standalone audit
    if isinstance(file_info, ModelTarget):
        # It's a model
        target: t.Optional[t.Union[Model, StandaloneAudit]] = lsp_context.context.get_model(
            model_or_snapshot=file_info.names[0], raise_if_missing=False
        )
        if target is None or not isinstance(target, SqlModel):
            return []
        query = target.query
        file_path = target._path
    elif isinstance(file_info, AuditTarget):
        # It's a standalone audit
        target = lsp_context.context.standalone_audits.get(file_info.name)
        if target is None:
            return []
        query = target.query
        file_path = target._path
    else:
        return []

    references = []
    _, config_path = lsp_context.context.config_for_path(
        file_path,
    )

    with open(file_path, "r", encoding="utf-8") as file:
        read_file = file.readlines()

    for node in query.find_all(exp.Anonymous):
        macro_name = node.name.lower()
        reference = get_macro_reference(
            node=node,
            target=target,
            read_file=read_file,
            config_path=config_path,
            macro_name=macro_name,
        )
        if reference is not None:
            references.append(reference)

    return references


def get_macro_reference(
    target: t.Union[Model, StandaloneAudit],
    read_file: t.List[str],
    config_path: t.Optional[Path],
    node: exp.Expression,
    macro_name: str,
) -> t.Optional[Reference]:
    # Get the file path where the macro is defined
    try:
        # Get the position of the macro invocation in the source file first
        if hasattr(node, "meta") and node.meta:
            macro_range = TokenPositionDetails.from_meta(node.meta).to_range(read_file)

            # Check if it's a built-in method
            if builtin := get_built_in_macro_reference(macro_name, to_lsp_range(macro_range)):
                return builtin
        else:
            # Skip if we can't get the position
            return None

        # Find the macro definition information
        macro_def = target.python_env.get(macro_name)
        if macro_def is None:
            return None

        function_name = macro_def.name
        if not function_name:
            return None
        if not macro_def.path:
            return None
        if not config_path:
            return None
        path = Path(config_path).joinpath(macro_def.path)

        # Parse the Python file to find the function definition
        with open(path, "r") as f:
            tree = ast.parse(f.read())
        with open(path, "r") as f:
            output_read_line = f.readlines()

        # Find the function definition by name
        start_line = None
        end_line = None
        get_length_of_end_line = None
        docstring = None
        for ast_node in ast.walk(tree):
            if isinstance(ast_node, ast.FunctionDef) and ast_node.name == function_name:
                start_line = ast_node.lineno
                end_line = ast_node.end_lineno
                get_length_of_end_line = (
                    len(output_read_line[end_line - 1])
                    if end_line is not None and end_line - 1 < len(read_file)
                    else 0
                )
                # Extract docstring if present
                docstring = ast.get_docstring(ast_node)
                break

        if start_line is None or end_line is None or get_length_of_end_line is None:
            return None

        # Create a reference to the macro definition
        macro_uri = URI.from_path(path)

        return LSPMacroReference(
            uri=macro_uri.value,
            range=to_lsp_range(macro_range),
            target_range=Range(
                start=Position(line=start_line - 1, character=0),
                end=Position(line=end_line - 1, character=get_length_of_end_line),
            ),
            markdown_description=docstring,
        )
    except Exception:
        return None


def get_built_in_macro_reference(macro_name: str, macro_range: Range) -> t.Optional[Reference]:
    """
    Get a reference to a built-in macro by its name.

    Args:
        macro_name: The name of the built-in macro (e.g., 'each', 'sql_literal')
        macro_range: The range of the macro invocation in the source file
    """
    built_in_macros = macro.get_registry()
    built_in_macro = built_in_macros.get(macro_name)
    if built_in_macro is None:
        return None

    func = built_in_macro.func
    filename = inspect.getfile(func)
    source_lines, line_number = inspect.getsourcelines(func)

    # Calculate the end line number by counting the number of source lines
    end_line_number = line_number + len(source_lines) - 1

    return LSPMacroReference(
        uri=URI.from_path(Path(filename)).value,
        range=macro_range,
        target_range=Range(
            start=Position(line=line_number - 1, character=0),
            end=Position(line=end_line_number - 1, character=0),
        ),
        markdown_description=func.__doc__ if func.__doc__ else None,
    )


def get_model_find_all_references(
    lint_context: LSPContext, document_uri: URI, position: Position
) -> t.List[LSPModelReference]:
    """
    Get all references to a model across the entire project.

    This function finds all usages of a model in other files by searching through
    all models in the project and checking their dependencies.

    Args:
        lint_context: The LSP context
        document_uri: The URI of the document
        position: The position to check for model references

    Returns:
        A list of references to the model across all files
    """
    # First, get the references in the current file to determine what model we're looking for
    current_file_references = [
        ref
        for ref in get_model_definitions_for_a_path(lint_context, document_uri)
        if isinstance(ref, LSPModelReference)
    ]

    # Find the model reference at the cursor position
    target_model_uri: t.Optional[str] = None
    for ref in current_file_references:
        if _position_within_range(position, ref.range):
            # This is a model reference, get the target model URI
            target_model_uri = ref.uri
            break

    if target_model_uri is None:
        return []

    # Start with the model definition
    all_references: t.List[LSPModelReference] = [
        LSPModelReference(
            uri=ref.uri,
            range=Range(
                start=Position(line=0, character=0),
                end=Position(line=0, character=0),
            ),
            markdown_description=ref.markdown_description,
        )
    ]

    # Then add the original reference
    for ref in current_file_references:
        if ref.uri == target_model_uri and isinstance(ref, LSPModelReference):
            all_references.append(
                LSPModelReference(
                    uri=document_uri.value,
                    range=ref.range,
                    markdown_description=ref.markdown_description,
                )
            )

    # Search through the models in the project
    for path, target in lint_context.map.items():
        if not isinstance(target, (ModelTarget, AuditTarget)):
            continue

        file_uri = URI.from_path(path)

        # Skip current file, already processed
        if file_uri.value == document_uri.value:
            continue

        # Get model references for this file
        file_references = [
            ref
            for ref in get_model_definitions_for_a_path(lint_context, file_uri)
            if isinstance(ref, LSPModelReference)
        ]

        # Add references that point to the target model file
        for ref in file_references:
            if ref.uri == target_model_uri and isinstance(ref, LSPModelReference):
                all_references.append(
                    LSPModelReference(
                        uri=file_uri.value,
                        range=ref.range,
                        markdown_description=ref.markdown_description,
                    )
                )

    return all_references


def get_cte_references(
    lint_context: LSPContext, document_uri: URI, position: Position
) -> t.List[LSPCteReference]:
    """
    Get all references to a CTE at a specific position in a document.

    This function finds both the definition and all usages of a CTE within the same file.

    Args:
        lint_context: The LSP context
        document_uri: The URI of the document
        position: The position to check for CTE references

    Returns:
        A list of references to the CTE (including its definition and all usages)
    """

    # Filter to get the CTE references
    cte_references: t.List[LSPCteReference] = [
        ref
        for ref in get_model_definitions_for_a_path(lint_context, document_uri)
        if isinstance(ref, LSPCteReference)
    ]

    if not cte_references:
        return []

    target_cte_definition_range = None
    for ref in cte_references:
        # Check if cursor is on a CTE usage
        if _position_within_range(position, ref.range):
            target_cte_definition_range = ref.target_range
            break
        # Check if cursor is on the CTE definition
        elif _position_within_range(position, ref.target_range):
            target_cte_definition_range = ref.target_range
            break

    if target_cte_definition_range is None:
        return []

    # Add the CTE definition
    matching_references = [
        LSPCteReference(
            uri=document_uri.value,
            range=target_cte_definition_range,
            target_range=target_cte_definition_range,
        )
    ]

    # Add all usages
    for ref in cte_references:
        if ref.target_range == target_cte_definition_range:
            matching_references.append(
                LSPCteReference(
                    uri=document_uri.value,
                    range=ref.range,
                    target_range=ref.target_range,
                )
            )

    return matching_references


def get_all_references(
    lint_context: LSPContext, document_uri: URI, position: Position
) -> t.Sequence[Reference]:
    """
    Get all references of a symbol at a specific position in a document.

    This function determines the type of reference (CTE, model for now) at the cursor
    position and returns all references to that symbol across the project.

    Args:
        lint_context: The LSP context
        document_uri: The URI of the document
        position: The position to check for references

    Returns:
        A list of references to the symbol at the given position
    """
    # First try CTE references (within same file)
    if cte_references := get_cte_references(lint_context, document_uri, position):
        return cte_references

    # Then try model references (across files)
    if model_references := get_model_find_all_references(lint_context, document_uri, position):
        return model_references

    return []


def _position_within_range(position: Position, range: Range) -> bool:
    """Check if a position is within a given range."""
    return (
        range.start.line < position.line
        or (range.start.line == position.line and range.start.character <= position.character)
    ) and (
        range.end.line > position.line
        or (range.end.line == position.line and range.end.character >= position.character)
    )
