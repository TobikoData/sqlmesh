from __future__ import annotations

import json
import typing as t
from pathlib import Path
from sqlmesh.core.model import SqlModel
from sqlmesh.core.loader import SqlMeshLoader
from sqlmesh.core.config import Config, ModelDefaultsConfig
from sqlmesh.core.config.loader import load_config_from_yaml
import sqlglot


def extract_joins(query: sqlglot.exp.Expression) -> list[dict]:
    """Extract all joins from a query."""
    joins = []
    
    # Find the FROM clause to get the base table
    base_table = None
    
    if isinstance(query, sqlglot.exp.Select) and query.args.get('from'):
        from_expr = query.args['from']
        if isinstance(from_expr, sqlglot.exp.From) and from_expr.this:
            base_table = from_expr.this.sql(pretty=True)
    
    # Then find all joins
    for join in query.find_all(sqlglot.exp.Join):
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


def extract_fields(query: sqlglot.exp.Expression, model: SqlModel) -> list[dict]:
    """Extract all fields from a query."""
    fields = []
    
    def clean_type(type_str: str) -> str:
        """Remove TYPE. prefix from type strings if present."""
        if type_str.startswith("TYPE."):
            return type_str[5:]
        return type_str

    if isinstance(query, sqlglot.exp.Select):
        # Get the model's column types
        column_types = model.columns_to_types_or_raise
        
        for expr in query.expressions:
            # Get the field name (either from alias or column name)
            field_name = None
            sql_expr = None
            data_type = "UNKNOWN"
            
            if isinstance(expr, sqlglot.exp.Alias):
                if isinstance(expr.alias, sqlglot.exp.Identifier):
                    field_name = expr.alias.this
                else:
                    field_name = str(expr.alias)
                sql_expr = expr.this.sql(pretty=True)
                # Try to infer type from expression
                if isinstance(expr.this, sqlglot.exp.Count):
                    data_type = "BIGINT"
                elif isinstance(expr.this, sqlglot.exp.Sum):
                    # For SUM, try to get the type from the column being summed
                    if expr.this.this and isinstance(expr.this.this, sqlglot.exp.Column):
                        col = expr.this.this
                        col_name = col.this.this if isinstance(col.this, sqlglot.exp.Identifier) else str(col.this)
                        if col_name in column_types:
                            base_type = column_types[col_name]
                            if base_type.is_type(*sqlglot.exp.DataType.INTEGER_TYPES):
                                data_type = clean_type(str(base_type.this).upper())
                            elif base_type.is_type(*sqlglot.exp.DataType.REAL_TYPES):
                                if base_type.this == sqlglot.exp.DataType.Type.DECIMAL:
                                    params = [p.this for p in base_type.find_all(sqlglot.exp.DataTypeParam)]
                                    data_type = f"DECIMAL({','.join(map(str, params))})" if params else "DECIMAL"
                                else:
                                    data_type = clean_type(str(base_type.this).upper())
                            elif base_type.is_type(*sqlglot.exp.DataType.TEMPORAL_TYPES):
                                data_type = clean_type(str(base_type.this).upper())
                            else:
                                data_type = clean_type(str(base_type.this).upper())
                    else:
                        data_type = "DECIMAL"
                elif isinstance(expr.this, sqlglot.exp.Avg):
                    data_type = "DOUBLE"
                elif isinstance(expr.this, sqlglot.exp.DateDiff):
                    data_type = "INT"
                elif isinstance(expr.this, sqlglot.exp.Cast):
                    data_type = clean_type(str(expr.this.args["to"].this).upper())
                elif isinstance(expr.this, sqlglot.exp.Column):
                    col = expr.this
                    col_name = col.this.this if isinstance(col.this, sqlglot.exp.Identifier) else str(col.this)
                    if col_name in column_types:
                        base_type = column_types[col_name]
                        if base_type.is_type(*sqlglot.exp.DataType.INTEGER_TYPES):
                            data_type = clean_type(str(base_type.this).upper())
                        elif base_type.is_type(*sqlglot.exp.DataType.REAL_TYPES):
                            if base_type.this == sqlglot.exp.DataType.Type.DECIMAL:
                                params = [p.this for p in base_type.find_all(sqlglot.exp.DataTypeParam)]
                                data_type = f"DECIMAL({','.join(map(str, params))})" if params else "DECIMAL"
                            else:
                                data_type = clean_type(str(base_type.this).upper())
                        elif base_type.is_type(*sqlglot.exp.DataType.TEMPORAL_TYPES):
                            data_type = clean_type(str(base_type.this).upper())
                        else:
                            data_type = clean_type(str(base_type.this).upper())
                elif field_name in column_types:
                    # For direct column references, use the model's type
                    base_type = column_types[field_name]
                    if base_type.is_type(*sqlglot.exp.DataType.INTEGER_TYPES):
                        data_type = clean_type(str(base_type.this).upper())
                    elif base_type.is_type(*sqlglot.exp.DataType.REAL_TYPES):
                        if base_type.this == sqlglot.exp.DataType.Type.DECIMAL:
                            params = [p.this for p in base_type.find_all(sqlglot.exp.DataTypeParam)]
                            data_type = f"DECIMAL({','.join(map(str, params))})" if params else "DECIMAL"
                        else:
                            data_type = clean_type(str(base_type.this).upper())
                    elif base_type.is_type(*sqlglot.exp.DataType.TEMPORAL_TYPES):
                        data_type = clean_type(str(base_type.this).upper())
                    else:
                        data_type = clean_type(str(base_type.this).upper())
            else:
                if isinstance(expr, sqlglot.exp.Column):
                    field_name = expr.this.this if isinstance(expr.this, sqlglot.exp.Identifier) else str(expr.this)
                else:
                    field_name = str(expr)
                sql_expr = expr.sql(pretty=True)

                if field_name in column_types:
                    # For direct column references, use the model's type
                    base_type = column_types[field_name]
                    if base_type.is_type(*sqlglot.exp.DataType.INTEGER_TYPES):
                        data_type = clean_type(str(base_type.this).upper())
                    elif base_type.is_type(*sqlglot.exp.DataType.REAL_TYPES):
                        if base_type.this == sqlglot.exp.DataType.Type.DECIMAL:
                            params = [p.this for p in base_type.find_all(sqlglot.exp.DataTypeParam)]
                            data_type = f"DECIMAL({','.join(map(str, params))})" if params else "DECIMAL"
                        else:
                            data_type = clean_type(str(base_type.this).upper())
                    elif base_type.is_type(*sqlglot.exp.DataType.TEMPORAL_TYPES):
                        data_type = clean_type(str(base_type.this).upper())
                    else:
                        data_type = clean_type(str(base_type.this).upper())

            if field_name:
                field_info = {
                    "name": field_name,
                    "sql": sql_expr or field_name,
                    "type": data_type
                }
                fields.append(field_info)

    return fields


def extract_model_info(query: sqlglot.exp.Expression, model: SqlModel) -> dict:
    """Extract both joins and fields from a query."""
    return {
        "joins": extract_joins(query),
        "fields": extract_fields(query, model)
    }


def generate_model_lineage(model: SqlModel) -> dict:
    """Generate join information for a model."""
    query = model.query
    model_info = extract_model_info(query, model)
    return {
        "model": model.name,
        **model_info
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
