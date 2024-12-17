from __future__ import annotations

import json
import typing as t
from pathlib import Path
from sqlmesh.core.model import SqlModel
from sqlmesh.core.loader import SqlMeshLoader
from sqlmesh.core.config import Config, ModelDefaultsConfig
from sqlmesh.core.config.loader import load_config_from_yaml
from sqlglot import exp


def extract_joins(query: exp.Expression) -> list[dict]:
    """Extract all joins from a query."""
    joins = []
    
    # Find the FROM clause to get the base table
    base_table = None
    
    if isinstance(query, exp.Select) and query.args.get('from'):
        from_expr = query.args['from']
        if isinstance(from_expr, exp.From) and from_expr.this:
            base_table = from_expr.this.sql(pretty=True)
    
    # Then find all joins
    for join in query.find_all(exp.Join):
        # For the first join, use the base table as the left side
        # For subsequent joins, use the previous right side as the left side
        join_info = {
            "type": join.args.get("side", "INNER"),  # JOIN type (LEFT, RIGHT, INNER, etc.)
            "left": base_table,  # Left side of join (base table or previous join)
            "right": join.this.sql(pretty=True),  # Right side of join (table being joined)
            "condition": join.args.get("on", "").sql(pretty=True) if join.args.get("on") else None  # Join condition
        }
        joins.append(join_info)
        # Update base_table for subsequent joins
        base_table = join.this.sql(pretty=True)
    return joins


def generate_model_lineage(model: SqlModel) -> dict:
    """Generate join information for a model."""
    joins = extract_joins(model.query)
    return {
        "model": model.name,
        "joins": joins
    }


def generate_cube_data(models: t.List[SqlModel]) -> t.List[dict]:
    """Generate cube data for a list of models."""
    return [generate_model_lineage(model) for model in models]


def write_cube_data(cube_data: t.List[dict], output_file: t.Optional[Path] = None) -> None:
    """Write cube data to a file or stdout."""
    if output_file:
        with open(output_file, "w") as f:
            json.dump(cube_data, f, indent=2)
    else:
        print(json.dumps(cube_data, indent=2))


def load_models_from_directory(directory_path: Path, tag: t.Optional[str] = None) -> t.List[SqlModel]:
    """Load SQL models from a directory.
    
    Args:
        directory_path: Path to the directory containing models
        tag: Optional tag to filter models by
    """
    # Create a context with the project config
    from sqlmesh.core.context import Context

    project_root = directory_path
    while not (project_root / "config.yaml").exists():
        if project_root == project_root.parent:
            raise FileNotFoundError("Could not find config.yaml in any parent directory")
        project_root = project_root.parent

    context = Context(paths=[project_root])
    models = [model for model in context.models.values() if isinstance(model, SqlModel)]
    
    if tag:
        models = [model for model in models if hasattr(model, 'tags') and tag in model.tags]
    
    return models


def main(model_dir: Path, output_file: t.Optional[Path] = None, models: t.Optional[t.List[SqlModel]] = None, tag: t.Optional[str] = None) -> None:
    """Main function to generate cube data.
    
    Args:
        model_dir: Directory containing SQL models
        output_file: Optional output file path. If not provided, prints to stdout.
        models: Optional list of pre-filtered SQL models. If not provided, all models
            in the directory will be loaded.
        tag: Optional tag to filter models by
    """
    if models is None:
        models = load_models_from_directory(model_dir, tag)
    cube_data = generate_cube_data(models)
    write_cube_data(cube_data, output_file)


if __name__ == "__main__":
    import sys
    import argparse

    parser = argparse.ArgumentParser(description="Generate cube data from SQL models")
    parser.add_argument("model_dir", type=Path, help="Directory containing SQL models")
    parser.add_argument("--output", "-o", type=Path, help="Output file path")
    parser.add_argument("--tag", "-t", help="Filter models by tag")

    args = parser.parse_args()
    main(args.model_dir, args.output, tag=args.tag)
