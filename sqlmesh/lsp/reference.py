from lsprotocol.types import Range, Position
import typing as t
from pathlib import Path
from pydantic import Field

from sqlmesh.core.audit import StandaloneAudit
from sqlmesh.core.dialect import normalize_model_name
from sqlmesh.core.linter.helpers import (
    TokenPositionDetails,
)
from sqlmesh.core.model.definition import SqlModel, ExternalModel
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
from ruamel.yaml import YAML


class LSPModelReference(PydanticModel):
    """A LSP reference to a model, excluding external models."""

    type: t.Literal["model"] = "model"
    uri: str
    range: Range
    markdown_description: t.Optional[str] = None


class LSPExternalModelReference(PydanticModel):
    """A LSP reference to an external model."""

    type: t.Literal["external_model"] = "external_model"
    uri: str
    range: Range
    markdown_description: t.Optional[str] = None
    target_range: t.Optional[Range] = None


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
    t.Union[LSPModelReference, LSPCteReference, LSPMacroReference, LSPExternalModelReference],
    Field(discriminator="type"),
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

                            column_references = _process_column_references(
                                scope=scope,
                                reference_name=table.name,
                                read_file=read_file,
                                referenced_model_uri=document_uri,
                                description="",
                                reference_type="cte",
                                cte_target_range=target_range,
                            )
                            references.extend(column_references)
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

                # For external models in YAML files, find the specific model block
                if isinstance(referenced_model, ExternalModel):
                    yaml_target_range: t.Optional[Range] = None
                    if (
                        referenced_model_path.suffix in (".yaml", ".yml")
                        and referenced_model_path.is_file()
                    ):
                        yaml_target_range = _get_yaml_model_range(
                            referenced_model_path, referenced_model.name
                        )
                    references.append(
                        LSPExternalModelReference(
                            uri=referenced_model_uri.value,
                            range=Range(
                                start=to_lsp_position(start_pos_sqlmesh),
                                end=to_lsp_position(end_pos_sqlmesh),
                            ),
                            markdown_description=description,
                            target_range=yaml_target_range,
                        )
                    )

                    column_references = _process_column_references(
                        scope=scope,
                        reference_name=normalized_reference_name,
                        read_file=read_file,
                        referenced_model_uri=referenced_model_uri,
                        description=description,
                        yaml_target_range=yaml_target_range,
                        reference_type="external_model",
                        default_catalog=lint_context.context.default_catalog,
                        dialect=dialect,
                    )
                    references.extend(column_references)
                else:
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

                    column_references = _process_column_references(
                        scope=scope,
                        reference_name=normalized_reference_name,
                        read_file=read_file,
                        referenced_model_uri=referenced_model_uri,
                        description=description,
                        reference_type="model",
                        default_catalog=lint_context.context.default_catalog,
                        dialect=dialect,
                    )
                    references.extend(column_references)

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
    # Find the model reference at the cursor position
    model_at_position = next(
        filter(
            lambda ref: isinstance(ref, LSPModelReference)
            and _position_within_range(position, ref.range),
            get_model_definitions_for_a_path(lint_context, document_uri),
        ),
        None,
    )

    if not model_at_position:
        return []

    assert isinstance(model_at_position, LSPModelReference)  # for mypy

    target_model_uri = model_at_position.uri

    # Start with the model definition
    all_references: t.List[LSPModelReference] = [
        LSPModelReference(
            uri=model_at_position.uri,
            range=Range(
                start=Position(line=0, character=0),
                end=Position(line=0, character=0),
            ),
            markdown_description=model_at_position.markdown_description,
        )
    ]

    # Then add references from the current file
    current_file_refs = filter(
        lambda ref: isinstance(ref, LSPModelReference) and ref.uri == target_model_uri,
        get_model_definitions_for_a_path(lint_context, document_uri),
    )

    for ref in current_file_refs:
        assert isinstance(ref, LSPModelReference)  # for mypy

        all_references.append(
            LSPModelReference(
                uri=document_uri.value,
                range=ref.range,
                markdown_description=ref.markdown_description,
            )
        )

    # Search through the models in the project
    for path, _ in lint_context.map.items():
        file_uri = URI.from_path(path)

        # Skip current file, already processed
        if file_uri.value == document_uri.value:
            continue

        # Get model references that point to the target model
        matching_refs = filter(
            lambda ref: isinstance(ref, LSPModelReference) and ref.uri == target_model_uri,
            get_model_definitions_for_a_path(lint_context, file_uri),
        )

        for ref in matching_refs:
            assert isinstance(ref, LSPModelReference)  # for mypy

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


def get_macro_find_all_references(
    lsp_context: LSPContext, document_uri: URI, position: Position
) -> t.List[LSPMacroReference]:
    """
    Get all references to a macro at a specific position in a document.

    This function finds all usages of a macro across the entire project.

    Args:
        lsp_context: The LSP context
        document_uri: The URI of the document
        position: The position to check for macro references

    Returns:
        A list of references to the macro across all files
    """
    # Find the macro reference at the cursor position
    macro_at_position = next(
        filter(
            lambda ref: isinstance(ref, LSPMacroReference)
            and _position_within_range(position, ref.range),
            get_macro_definitions_for_a_path(lsp_context, document_uri),
        ),
        None,
    )

    if not macro_at_position:
        return []

    assert isinstance(macro_at_position, LSPMacroReference)  # for mypy

    target_macro_uri = macro_at_position.uri
    target_macro_target_range = macro_at_position.target_range

    # Start with the macro definition
    all_references: t.List[LSPMacroReference] = [
        LSPMacroReference(
            uri=target_macro_uri,
            range=target_macro_target_range,
            target_range=target_macro_target_range,
            markdown_description=None,
        )
    ]

    # Search through all SQL and audit files in the project
    for path, _ in lsp_context.map.items():
        file_uri = URI.from_path(path)

        # Get macro references that point to the same macro definition
        matching_refs = filter(
            lambda ref: isinstance(ref, LSPMacroReference)
            and ref.uri == target_macro_uri
            and ref.target_range == target_macro_target_range,
            get_macro_definitions_for_a_path(lsp_context, file_uri),
        )

        for ref in matching_refs:
            assert isinstance(ref, LSPMacroReference)  # for mypy
            all_references.append(
                LSPMacroReference(
                    uri=file_uri.value,
                    range=ref.range,
                    target_range=ref.target_range,
                    markdown_description=ref.markdown_description,
                )
            )

    return all_references


def get_all_references(
    lint_context: LSPContext, document_uri: URI, position: Position
) -> t.Sequence[Reference]:
    """
    Get all references of a symbol at a specific position in a document.

    This function determines the type of reference (CTE, model or macro) at the cursor
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

    # Finally try macro references (across files)
    if macro_references := get_macro_find_all_references(lint_context, document_uri, position):
        return macro_references

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


def _get_column_table_range(column: exp.Column, read_file: t.List[str]) -> Range:
    """
    Get the range for a column's table reference, handling both simple and qualified table names.

    Args:
        column: The column expression
        read_file: The file content as list of lines

    Returns:
        The Range covering the table reference in the column
    """

    table_parts = column.parts[:-1]

    start_range = TokenPositionDetails.from_meta(table_parts[0].meta).to_range(read_file)
    end_range = TokenPositionDetails.from_meta(table_parts[-1].meta).to_range(read_file)

    return Range(
        start=to_lsp_position(start_range.start),
        end=to_lsp_position(end_range.end),
    )


def _process_column_references(
    scope: t.Any,
    reference_name: str,
    read_file: t.List[str],
    referenced_model_uri: URI,
    description: t.Optional[str] = None,
    yaml_target_range: t.Optional[Range] = None,
    reference_type: t.Literal["model", "external_model", "cte"] = "model",
    default_catalog: t.Optional[str] = None,
    dialect: t.Optional[str] = None,
    cte_target_range: t.Optional[Range] = None,
) -> t.List[Reference]:
    """
    Process column references for a given table and create appropriate reference objects.

    Args:
        scope: The SQL scope to search for columns
        reference_name: The full reference name (may include database/catalog)
        read_file: The file content as list of lines
        referenced_model_uri: URI of the referenced model
        description: Markdown description for the reference
        yaml_target_range: Target range for external models (YAML files)
        reference_type: Type of reference - "model", "external_model", or "cte"
        default_catalog: Default catalog for normalization
        dialect: SQL dialect for normalization
        cte_target_range: Target range for CTE references

    Returns:
        List of table references for column usages
    """

    references: t.List[Reference] = []
    for column in scope.find_all(exp.Column):
        if column.table:
            if reference_type == "cte":
                if column.table == reference_name:
                    table_range = _get_column_table_range(column, read_file)
                    references.append(
                        LSPCteReference(
                            uri=referenced_model_uri.value,
                            range=table_range,
                            target_range=cte_target_range,
                        )
                    )
            else:
                table_parts = [part.sql(dialect) for part in column.parts[:-1]]
                table_ref = ".".join(table_parts)
                normalized_reference_name = normalize_model_name(
                    table_ref,
                    default_catalog=default_catalog,
                    dialect=dialect,
                )
                if normalized_reference_name == reference_name:
                    table_range = _get_column_table_range(column, read_file)
                    if reference_type == "external_model":
                        references.append(
                            LSPExternalModelReference(
                                uri=referenced_model_uri.value,
                                range=table_range,
                                markdown_description=description,
                                target_range=yaml_target_range,
                            )
                        )
                    else:
                        references.append(
                            LSPModelReference(
                                uri=referenced_model_uri.value,
                                range=table_range,
                                markdown_description=description,
                            )
                        )

    return references


def _get_yaml_model_range(path: Path, model_name: str) -> t.Optional[Range]:
    """
    Find the range of a specific model block in a YAML file.

    Args:
        yaml_path: Path to the YAML file
        model_name: Name of the model to find

    Returns:
        The Range of the model block in the YAML file, or None if not found
    """
    yaml = YAML()
    with path.open("r", encoding="utf-8") as f:
        data = yaml.load(f)

    if not isinstance(data, list):
        return None

    for item in data:
        if isinstance(item, dict) and item.get("name") == model_name:
            # Get size of block by taking the earliest line/col in the items block and the last line/col of the block
            position_data = item.lc.data["name"]  # type: ignore
            start = Position(line=position_data[2], character=position_data[3])
            end = Position(line=position_data[2], character=position_data[3] + len(item["name"]))
            return Range(start=start, end=end)
    return None
