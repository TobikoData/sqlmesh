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
