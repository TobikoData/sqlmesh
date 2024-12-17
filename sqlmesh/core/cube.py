from __future__ import annotations

import json
import typing as t
from pathlib import Path

from sqlglot import parse
from sqlmesh.core.dialect import parse_one
from sqlmesh.core.lineage import lineage
from sqlmesh.core.model import SqlModel
from sqlmesh.core.loader import SqlMeshLoader
from sqlmesh.core.config import Config, ModelDefaultsConfig
from sqlmesh.core.config.connection import DuckDBConnectionConfig
from sqlmesh.core.config.loader import load_config_from_yaml


def get_column_lineage(model: SqlModel, column: str) -> dict:
    """Get lineage information for a single column."""
    try:
        node = lineage(column, model)
        sources = set()
        references = set()

        if hasattr(node, 'expressions'):
            for expr in node.expressions:
                if hasattr(expr, 'table') and hasattr(expr, 'name'):
                    table_name = expr.table.name if expr.table else None
                    column_name = expr.name
                    if table_name and column_name:
                        sources.add(f"{table_name}.{column_name}")
                        references.add(table_name)

        return {
            "sources": list(sources),
            "references": list(references),
            "is_derived": bool(sources) or any(expr.is_aggregate for expr in node.expressions) if hasattr(node, 'expressions') else False,
            "expression": str(node.expression) if hasattr(node, 'expression') else None,
            "data_type": str(model.columns_to_types.get(column)) if hasattr(model, 'columns_to_types') else None,
            "description": model.column_descriptions.get(column) if hasattr(model, 'column_descriptions') else None,
        }
    except Exception as e:
        return {
            "sources": [],
            "references": [],
            "is_derived": False,
            "error": str(e)
        }


def get_model_dependencies(model: SqlModel) -> dict:
    """Get model-level dependencies."""
    depends_on = set()
    referenced_by = set()

    # Extract dependencies from references
    if hasattr(model, 'references'):
        depends_on.update(str(ref) for ref in model.references)

    return {
        "depends_on": list(depends_on),
        "referenced_by": list(referenced_by)  # This would need to be populated by scanning other models
    }


def generate_model_lineage(model: SqlModel) -> dict:
    """Generate comprehensive lineage information for a single model."""
    model_deps = get_model_dependencies(model)
    columns = {}

    for column in model.columns_to_types.keys():
        columns[column] = get_column_lineage(model, column)
    
    lineage_data = {
        "model": model.name,
        "dialect": str(model.dialect),
        "depends_on": model_deps["depends_on"],
        "referenced_by": model_deps["referenced_by"],
        "columns": columns,
        "query": str(model.query),
        "metadata": {
            "tags": list(model.tags) if hasattr(model, 'tags') else [],
            "grain": model.grain if hasattr(model, 'grain') else None,
            "description": model.description if hasattr(model, 'description') else None,
            "owners": model.owners if hasattr(model, 'owners') else []
        }
    }

    return lineage_data


def generate_cube_data(models: t.List[SqlModel]) -> t.List[dict]:
    """Generate cube data for a list of models."""
    return [generate_model_lineage(model) for model in models]


def load_models_from_directory(directory_path: Path) -> t.List[SqlModel]:
    """Load SQL models from a directory."""
    # Create a context with the project config
    from sqlmesh.core.context import Context

    project_root = directory_path
    while not (project_root / "config.yaml").exists():
        if project_root == project_root.parent:
            raise FileNotFoundError("Could not find config.yaml in any parent directory")
        project_root = project_root.parent

    context = Context(paths=[project_root])
    return [model for model in context.models.values() if isinstance(model, SqlModel)]


def write_cube_data(cube_data: t.List[dict], output_file: t.Optional[Path] = None) -> None:
    """Write cube data to a file or stdout."""
    json_str = json.dumps(cube_data, indent=2)
    if output_file:
        with open(output_file, "w") as f:
            f.write(json_str)
    else:
        print(json_str)


def main(model_dir: Path, output_file: t.Optional[Path] = None, models: t.Optional[t.List[SqlModel]] = None) -> None:
    """Main function to generate cube data.
    
    Args:
        model_dir: Directory containing SQL models
        output_file: Optional output file path. If not provided, prints to stdout.
        models: Optional list of pre-filtered SQL models. If not provided, all models
            in the directory will be loaded.
    """
    if models is None:
        models = load_models_from_directory(model_dir)
    cube_data = generate_cube_data(models)
    write_cube_data(cube_data, output_file)


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python -m sqlmesh.core.cube <model_dir> [output_file]")
        sys.exit(1)
    
    model_dir = Path(sys.argv[1])
    output_file = Path(sys.argv[2]) if len(sys.argv) > 2 else None
    main(model_dir, output_file)
