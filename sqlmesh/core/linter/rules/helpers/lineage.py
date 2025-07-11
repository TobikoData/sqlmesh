import typing as t
from pathlib import Path

from pydantic import Field
from sqlglot.optimizer import build_scope
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers

from sqlmesh.core.dialect import normalize_model_name
from sqlmesh.core.linter.helpers import TokenPositionDetails
from sqlmesh.core.linter.rule import Range, Position
from sqlmesh.core.linter.rules.helpers.yaml import _get_yaml_model_range
from sqlmesh.core.model import SqlModel, ExternalModel
from sqlglot import exp

from sqlmesh.utils.pydantic import PydanticModel

if t.TYPE_CHECKING:
    from sqlmesh.core.context import GenericContext, Context


class LSPModelReference(PydanticModel):
    """A LSP reference to a model, excluding external models."""

    type: t.Literal["model"] = "model"
    path: Path
    range: Range
    markdown_description: t.Optional[str] = None


class LSPExternalModelReference(PydanticModel):
    """A LSP reference to an external model."""

    type: t.Literal["external_model"] = "external_model"

    model_name: str
    path: Path
    range: Range
    markdown_description: t.Optional[str] = None
    target_range: t.Optional[Range] = None


class LSPCteReference(PydanticModel):
    """A LSP reference to a CTE."""

    type: t.Literal["cte"] = "cte"
    path: Path
    range: Range
    target_range: Range


class LSPMacroReference(PydanticModel):
    """A LSP reference to a macro."""

    type: t.Literal["macro"] = "macro"
    path: Path
    range: Range
    target_range: Range
    markdown_description: t.Optional[str] = None


Reference = t.Annotated[
    t.Union[LSPModelReference, LSPCteReference, LSPMacroReference, LSPExternalModelReference],
    Field(discriminator="type"),
]


def find_external_model_ranges(
    context: "GenericContext",
    external_models_not_registered: t.Set[str],
    model: SqlModel,
) -> t.Optional[t.Dict[str, t.List[Range]]]:
    """Returns a map of external model names to their ranges found in the query.

    It returns a dictionary of fqn to a list of ranges where the external model
    """
    path = model._path
    if path is None or not str(path).endswith(".sql"):
        return None

    depends_on = model.depends_on
    query = model.query
    with open(path, "r", encoding="utf-8") as file:
        content = file.readlines()

    references = extract_references_from_query(
        context=context,  # type: ignore
        read_file=content,
        query=query,
        dialect=model.dialect,
        depends_on=depends_on,
        document_path=path,
    )
    external_model_references = [
        reference for reference in references if isinstance(reference, LSPExternalModelReference)
    ]
    if not external_model_references:
        return None

    external_model_references_filtered = [
        reference
        for reference in external_model_references
        if reference.model_name in external_models_not_registered
    ]
    if not external_model_references_filtered:
        return None
    external_model_ranges: t.Dict[str, t.List[Range]] = {}
    for reference in external_model_references_filtered:
        if reference.model_name not in external_model_ranges:
            external_model_ranges[reference.model_name] = []
        # Convert LSP Range to linter Range
        lsp_range = reference.range
        linter_range = Range(
            start=Position(line=lsp_range.start.line, character=lsp_range.start.character),
            end=Position(line=lsp_range.end.line, character=lsp_range.end.character),
        )
        external_model_ranges[reference.model_name].append(linter_range)
    return external_model_ranges


def extract_references_from_query(
    query: exp.Expression,
    context: "Context",
    document_path: Path,
    read_file: t.List[str],
    depends_on: t.Set[str],
    dialect: t.Optional[str] = None,
) -> t.List[Reference]:
    """
    Extract references from a SQL query, including CTEs and external
    models.
    """
    references: t.List[Reference] = []
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
                            target_range = TokenPositionDetails.from_meta(identifier.meta).to_range(
                                read_file
                            )
                            table_range = TokenPositionDetails.from_meta(table.this.meta).to_range(
                                read_file
                            )

                            references.append(
                                LSPCteReference(
                                    path=document_path,  # Same file
                                    range=table_range,
                                    target_range=target_range,
                                )
                            )

                            column_references = _process_column_references(
                                scope=scope,
                                reference_name=table.name,
                                read_file=read_file,
                                referenced_model_path=document_path,
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
                        default_catalog=context.default_catalog,
                        dialect=dialect,
                    )
                    if normalized_reference_name not in depends_on:
                        continue
                except Exception:
                    # Skip references that cannot be normalized
                    continue

                # Get the referenced model uri
                referenced_model = context.get_model(
                    model_or_snapshot=normalized_reference_name, raise_if_missing=False
                )
                if referenced_model is None:
                    continue
                referenced_model_path = referenced_model._path
                if referenced_model_path is None:
                    continue
                # Check whether the path exists
                if not referenced_model_path.is_file():
                    continue

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
                            model_name=normalized_reference_name,
                            path=referenced_model_path,
                            range=Range(
                                start=start_pos_sqlmesh,
                                end=end_pos_sqlmesh,
                            ),
                            markdown_description=description,
                            target_range=yaml_target_range,
                        )
                    )

                    column_references = _process_column_references(
                        scope=scope,
                        reference_name=normalized_reference_name,
                        read_file=read_file,
                        referenced_model_path=referenced_model_path,
                        description=description,
                        yaml_target_range=yaml_target_range,
                        reference_type="external_model",
                        default_catalog=context.default_catalog,
                        dialect=dialect,
                    )
                    references.extend(column_references)
                else:
                    references.append(
                        LSPModelReference(
                            path=referenced_model_path,
                            range=Range(
                                start=start_pos_sqlmesh,
                                end=end_pos_sqlmesh,
                            ),
                            markdown_description=description,
                        )
                    )

                    column_references = _process_column_references(
                        scope=scope,
                        reference_name=normalized_reference_name,
                        read_file=read_file,
                        referenced_model_path=referenced_model_path,
                        description=description,
                        reference_type="model",
                        default_catalog=context.default_catalog,
                        dialect=dialect,
                    )
                    references.extend(column_references)

    return references


def _process_column_references(
    scope: t.Any,
    reference_name: str,
    read_file: t.List[str],
    referenced_model_path: Path,
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
                            path=referenced_model_path,
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
                                model_name=reference_name,
                                path=referenced_model_path,
                                range=table_range,
                                markdown_description=description,
                                target_range=yaml_target_range,
                            )
                        )
                    else:
                        references.append(
                            LSPModelReference(
                                path=referenced_model_path,
                                range=table_range,
                                markdown_description=description,
                            )
                        )

    return references


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
        start=start_range.start,
        end=end_range.end,
    )


from sqlmesh.core.model.definition import (
    ExternalModel,
    PythonModel,
    SeedModel,
    SqlModel,
)
import typing as t


def generate_markdown_description(
    model: t.Union[SqlModel, ExternalModel, PythonModel, SeedModel],
) -> t.Optional[str]:
    description = model.description
    columns = model.columns_to_types
    column_descriptions = model.column_descriptions

    if columns is None:
        return description or None

    columns_table = "\n".join(
        [
            f"| {column} | {column_type} | {column_descriptions.get(column, '')} |"
            for column, column_type in columns.items()
        ]
    )

    table_header = "| Column | Type | Description |\n|--------|------|-------------|\n"
    columns_text = table_header + columns_table
    return f"{description}\n\n{columns_text}" if description else columns_text
