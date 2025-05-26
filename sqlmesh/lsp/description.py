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
    columns = model.columns_to_types
    column_descriptions = model.column_descriptions

    columns_table = (
        "\n".join(
            [
                f"| {column} | {column_type} | {column_descriptions.get(column, '')} |"
                for column, column_type in columns.items()
            ]
        )
        if columns
        else ""
    )

    table_header = "\n\n| Column | Type | Description |\n|--------|------|-------------|\n"
    return (
        f"{model.description}{table_header}{columns_table}" if columns_table else model.description
    )
