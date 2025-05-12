import typing as t
from dataclasses import dataclass

from lsprotocol.types import Range

from sqlmesh.core.model.definition import SqlModel
from sqlglot import exp

from sqlmesh.lsp.reference import range_from_token_position_details, TokenPositionDetails


@dataclass
class ColumnDescriptionMap:
    range: Range
    column_name: str
    data_type: t.Optional[str] = None
    description: t.Optional[str] = None


def get_columns_and_ranges_for_model(model: SqlModel) -> t.Optional[t.List[ColumnDescriptionMap]]:
    """
    Get the top level columns and their position in the file to be able to provide hover information.

    If the column information is not available, return None.
    """
    type_definitions = model.columns_to_types
    columns = model.column_descriptions
    query = model.query

    if not isinstance(query, exp.Query):
        return None

    path = model._path
    if not path.is_file():
        return None
    with open(path, "r") as f:
        lines = f.readlines()

    # Get the top-level columns from the SELECT
    outs = []
    top_level_columns = query.expressions
    for projection in top_level_columns:
        if isinstance(projection, exp.Alias):
            column = projection.get('alias')
        elif isinstance(projection, exp.Column):
            column = projection
        else:
            continue

        if not isinstance(column, exp.Column):
            continue

        column_name = column.name
        data_type = type_definitions[column_name] if type_definitions is not None else None
        description = columns[column_name] if column_name in columns else None
        token_details = TokenPositionDetails.from_meta(column.this.meta)
        column_range = range_from_token_position_details(token_details, lines)
        column_description_map = ColumnDescriptionMap(
            range=column_range,
            column_name=column_name,
            data_type=str(data_type) if data_type else None,
            description=description,
        )
        outs.append(column_description_map)

    return outs
