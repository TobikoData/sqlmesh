from __future__ import annotations

import json
import typing as t
from pathlib import Path
from sqlmesh.core.model import SqlModel
from sqlmesh.core.loader import SqlMeshLoader
from sqlmesh.core.config import Config, ModelDefaultsConfig
from sqlmesh.core.config.loader import load_config_from_yaml
from sqlmesh.core.openai_client import OpenAIClient
import sqlglot


def clean_type(type_str: str) -> str:
    """Remove TYPE. prefix from type strings if present."""
    if isinstance(type_str, str) and type_str.startswith("TYPE."):
        return type_str[5:]
    return type_str


def extract_table_from_expr(expr: sqlglot.exp.Expression) -> str:
    """Extract the actual table name from an expression."""
    if isinstance(expr, sqlglot.exp.Table):
        return expr.sql(pretty=True)
    elif isinstance(expr, sqlglot.exp.Alias):
        if isinstance(expr.this, sqlglot.exp.Table):
            return expr.this.sql(pretty=True)
        elif isinstance(expr.this, sqlglot.exp.Select):
            # For subqueries, try to get the base table
            if expr.this.args.get('from'):
                from_expr = expr.this.args['from']
                if isinstance(from_expr, sqlglot.exp.From) and from_expr.this:
                    return extract_table_from_expr(from_expr.this)
    return None


def extract_base_table_from_cte(cte: sqlglot.exp.CTE) -> str:
    """Extract the base table from a CTE."""
    if isinstance(cte.this, sqlglot.exp.Select) and cte.this.args.get('from'):
        from_expr = cte.this.args['from']
        if isinstance(from_expr, sqlglot.exp.From) and from_expr.this:
            return extract_table_from_expr(from_expr.this)
    return None


def extract_joins_recursive(query: sqlglot.exp.Expression) -> list[dict]:
    """Extract all joins from a query recursively, including CTEs."""
    joins = []
    cte_tables = {}  # Map CTE names to their base tables
    
    # First get base tables from CTEs
    if isinstance(query, sqlglot.exp.Select) and query.args.get('with'):
        with_clause = query.args['with']
        for cte in with_clause.expressions:
            if isinstance(cte, sqlglot.exp.CTE):
                cte_name = cte.alias.this if isinstance(cte.alias, sqlglot.exp.Identifier) else str(cte.alias)
                base_table = extract_base_table_from_cte(cte)
                if base_table:
                    cte_tables[cte_name] = base_table
                    # Get joins within the CTE
                    cte_query = cte.this
                    if isinstance(cte_query, sqlglot.exp.Select):
                        for join in cte_query.find_all(sqlglot.exp.Join):
                            right_table = extract_table_from_expr(join.this)
                            if right_table:
                                join_info = {
                                    "type": join.args.get("side", "INNER"),
                                    "left": base_table,
                                    "right": right_table,
                                    "condition": join.args.get("on", "").sql(pretty=True) if join.args.get("on") else None
                                }
                                joins.append(join_info)
                                base_table = right_table  # Update for next join
    
    # Then handle the main query
    base_table = None
    if isinstance(query, sqlglot.exp.Select) and query.args.get('from'):
        from_expr = query.args['from']
        if isinstance(from_expr, sqlglot.exp.From) and from_expr.this:
            table_expr = from_expr.this
            if isinstance(table_expr, sqlglot.exp.Alias):
                table_name = table_expr.alias.this if isinstance(table_expr.alias, sqlglot.exp.Identifier) else str(table_expr.alias)
                # If it's a CTE reference, use its base table
                if table_name in cte_tables:
                    base_table = cte_tables[table_name]
                else:
                    base_table = extract_table_from_expr(table_expr)
            else:
                base_table = extract_table_from_expr(table_expr)
    
    if base_table:
        for join in query.find_all(sqlglot.exp.Join):
            table_expr = join.this
            right_table = None
            if isinstance(table_expr, sqlglot.exp.Alias):
                table_name = table_expr.alias.this if isinstance(table_expr.alias, sqlglot.exp.Identifier) else str(table_expr.alias)
                # If it's a CTE reference, use its base table
                if table_name in cte_tables:
                    right_table = cte_tables[table_name]
                else:
                    right_table = extract_table_from_expr(table_expr)
            else:
                right_table = extract_table_from_expr(table_expr)
            
            if right_table:
                # If both sides are CTEs, check the parent node for the left table
                parent = join.find_ancestor(sqlglot.exp.From)
                if parent and isinstance(parent.this, sqlglot.exp.Alias):
                    left_name = parent.this.alias.this if isinstance(parent.this.alias, sqlglot.exp.Identifier) else str(parent.this.alias)
                    if left_name in cte_tables:
                        base_table = cte_tables[left_name]
                
                # Remove CTE aliases and use base tables
                left_table = base_table.split(" AS ")[0] if base_table else base_table
                right_table = right_table.split(" AS ")[0] if right_table else right_table
                
                # If either side is a CTE name, replace it with its base table
                if left_table in cte_tables:
                    left_table = cte_tables[left_table]
                if right_table in cte_tables:
                    right_table = cte_tables[right_table]
                
                join_info = {
                    "type": join.args.get("side", "INNER"),
                    "left": left_table,
                    "right": right_table,
                    "condition": join.args.get("on", "").sql(pretty=True) if join.args.get("on") else None
                }
                joins.append(join_info)
                base_table = right_table
    
    return joins


def extract_cte_info(query: sqlglot.exp.Expression) -> dict:
    """Extract information from CTEs including types and aggregations."""
    cte_info = {}
    
    if isinstance(query, sqlglot.exp.Select) and query.args.get('with'):
        with_clause = query.args['with']
        for cte in with_clause.expressions:
            if isinstance(cte, sqlglot.exp.CTE):
                cte_name = cte.alias.this if isinstance(cte.alias, sqlglot.exp.Identifier) else str(cte.alias)
                cte_query = cte.this
                cte_fields = {}
                
                if isinstance(cte_query, sqlglot.exp.Select):
                    for expr in cte_query.expressions:
                        field_name = None
                        field_type = "UNKNOWN"
                        sql_expr = None
                        is_agg = False
                        
                        if isinstance(expr, sqlglot.exp.Alias):
                            field_name = expr.alias.this if isinstance(expr.alias, sqlglot.exp.Identifier) else str(expr.alias)
                            sql_expr = expr.this.sql(pretty=True)
                            # Infer type and check for aggregation
                            if isinstance(expr.this, (sqlglot.exp.Count, sqlglot.exp.Sum, sqlglot.exp.Avg)):
                                is_agg = True
                                if isinstance(expr.this, sqlglot.exp.Count):
                                    field_type = "BIGINT"
                                elif isinstance(expr.this, sqlglot.exp.Sum):
                                    field_type = "DECIMAL"
                                elif isinstance(expr.this, sqlglot.exp.Avg):
                                    field_type = "DOUBLE"
                            elif isinstance(expr.this, sqlglot.exp.Cast):
                                field_type = clean_type(str(expr.this.args["to"].this).upper())
                            elif isinstance(expr.this, (sqlglot.exp.DateTrunc, sqlglot.exp.Date)):
                                field_type = "DATE"
                            # Check for expressions containing aggregates
                            elif any(isinstance(node, (sqlglot.exp.Count, sqlglot.exp.Sum, sqlglot.exp.Avg)) 
                                   for node in expr.this.find_all((sqlglot.exp.Count, sqlglot.exp.Sum, sqlglot.exp.Avg))):
                                is_agg = True
                                field_type = "DECIMAL"  # Default to DECIMAL for complex aggregates
                        
                        if field_name:
                            cte_fields[field_name] = {
                                "type": field_type,
                                "sql": sql_expr,
                                "is_agg": is_agg,
                                "original_cte": cte_name  # Track which CTE this field came from
                            }
                
                cte_info[cte_name] = cte_fields
    
    return cte_info


def extract_fields(query: sqlglot.exp.Expression, model: SqlModel) -> list[dict]:
    """Extract all fields from a query."""
    fields = []
    
    if isinstance(query, sqlglot.exp.Select):
        # First get CTE information to use for type inference
        cte_info = extract_cte_info(query)
        
        # Get table alias mapping
        table_aliases = extract_table_aliases(query)
        
        # Build a map of CTE fields for faster lookup
        cte_field_map = {}
        for cte_name, cte_fields in cte_info.items():
            for field_name, field_info in cte_fields.items():
                cte_field_map[f"{cte_name}.{field_name}"] = field_info
                # Add CTE fields to the output with real table alias
                base_table = table_aliases.get(cte_name)
                if base_table:
                    fields.append({
                        "name": f"{base_table}.{field_name}",
                        "type": field_info["type"],
                        "sql": field_info["sql"],
                        "is_agg": field_info["is_agg"]
                    })
                else:
                    fields.append({
                        "name": field_name,
                        "type": field_info["type"],
                        "sql": field_info["sql"],
                        "is_agg": field_info["is_agg"]
                    })

        # Get the model's column types
        column_types = model.columns_to_types_or_raise
        
        for expr in query.expressions:
            field_name = None
            sql_expr = None
            data_type = "UNKNOWN"
            is_agg = False
            
            if isinstance(expr, sqlglot.exp.Alias):
                field_name = expr.alias.this if isinstance(expr.alias, sqlglot.exp.Identifier) else str(expr.alias)
                
                # Try to get the original SQL expression from CTE if available
                if isinstance(expr.this, sqlglot.exp.Column):
                    table_name = None
                    if hasattr(expr.this, 'table') and expr.this.table:
                        table_name = expr.this.table.this if isinstance(expr.this.table, sqlglot.exp.Identifier) else str(expr.this.table)
                    col_name = expr.this.this.this if isinstance(expr.this.this, sqlglot.exp.Identifier) else str(expr.this.this)
                    
                    # Check if this is a CTE field reference
                    if table_name:
                        cte_field_key = f"{table_name}.{col_name}"
                        if cte_field_key in cte_field_map:
                            cte_field = cte_field_map[cte_field_key]
                            sql_expr = cte_field["sql"]
                            data_type = cte_field["type"]
                            is_agg = cte_field["is_agg"]  # Preserve aggregation status from CTE
                
                if not sql_expr:
                    sql_expr = expr.this.sql(pretty=True)
                    # Check if this is an aggregation or contains aggregations
                    if isinstance(expr.this, (sqlglot.exp.Count, sqlglot.exp.Sum, sqlglot.exp.Avg)):
                        is_agg = True
                    elif isinstance(expr.this, sqlglot.exp.Binary) and any(isinstance(node, (sqlglot.exp.Count, sqlglot.exp.Sum, sqlglot.exp.Avg)) 
                                                                      for node in expr.this.find_all((sqlglot.exp.Count, sqlglot.exp.Sum, sqlglot.exp.Avg))):
                        is_agg = True  # Binary operation with aggregates is also an aggregate
                    else:
                        # Check for nested aggregations in the expression
                        is_agg = any(isinstance(node, (sqlglot.exp.Count, sqlglot.exp.Sum, sqlglot.exp.Avg))
                                   for node in expr.this.find_all((sqlglot.exp.Count, sqlglot.exp.Sum, sqlglot.exp.Avg)))
                        
                        # Check if this is a calculation using aggregated CTE fields
                        for col in expr.this.find_all(sqlglot.exp.Column):
                            table_name = None
                            if hasattr(col, 'table') and col.table:
                                table_name = col.table.this if isinstance(col.table, sqlglot.exp.Identifier) else str(col.table)
                            col_name = col.this.this if isinstance(col.this, sqlglot.exp.Identifier) else str(col.this)
                            
                            if table_name:
                                cte_field_key = f"{table_name}.{col_name}"
                                if cte_field_key in cte_field_map:
                                    cte_field = cte_field_map[cte_field_key]
                                    if cte_field["is_agg"]:
                                        is_agg = True
                                        break
                        
                        # For division operations, default to DOUBLE type
                        if isinstance(expr.this, sqlglot.exp.Div):
                            data_type = "DOUBLE"
                        else:
                            data_type = "DECIMAL"  # Default for other binary operations
                    
                    # If we still don't have a type but it's an aggregation, default to DECIMAL
                    if data_type == "UNKNOWN" and is_agg:
                        data_type = "DECIMAL"
            else:
                if isinstance(expr, sqlglot.exp.Column):
                    field_name = expr.this.this if isinstance(expr.this, sqlglot.exp.Identifier) else str(expr.this)
                    sql_expr = expr.sql(pretty=True)
                    
                    # Try to get the original SQL expression from CTE if available
                    table_name = None
                    if hasattr(expr, 'table') and expr.table:
                        table_name = expr.table.this if isinstance(expr.table, sqlglot.exp.Identifier) else str(expr.table)
                    if table_name:
                        cte_field_key = f"{table_name}.{field_name}"
                        if cte_field_key in cte_field_map:
                            cte_field = cte_field_map[cte_field_key]
                            sql_expr = cte_field["sql"]
                            data_type = cte_field["type"]
                            is_agg = cte_field["is_agg"]  # Preserve aggregation status from CTE
                    elif field_name in column_types:
                        base_type = column_types[field_name]
                        data_type = clean_type(str(base_type.this).upper())
            
            if field_name and data_type != "UNKNOWN":
                # Get the base table name if this field is from a CTE
                base_table = None
                if isinstance(expr.this, sqlglot.exp.Column) and hasattr(expr.this, 'table') and expr.this.table:
                    table_name = expr.this.table.this if isinstance(expr.this.table, sqlglot.exp.Identifier) else str(expr.this.table)
                    base_table = table_aliases.get(table_name)

                # Only add non-CTE fields here since we already added CTE fields above
                if not any(f["name"] == field_name for f in fields):
                    fields.append({
                        "name": f"{base_table}.{field_name}" if base_table else field_name,
                        "type": data_type,
                        "sql": sql_expr,
                        "is_agg": is_agg
                    })
    
    return fields


def extract_table_aliases(query: sqlglot.exp.Expression) -> dict:
    """Extract mapping of CTE names to their base table aliases."""
    alias_map = {}
    
    if isinstance(query, sqlglot.exp.Select):
        # First get CTE base tables
        for cte in query.find_all(sqlglot.exp.CTE):
            cte_name = cte.alias.this if isinstance(cte.alias, sqlglot.exp.Identifier) else str(cte.alias)
            base_table = extract_base_table_from_cte(cte)
            if base_table:
                alias_map[cte_name] = base_table
        
        # Then get table aliases from joins
        for join in query.find_all(sqlglot.exp.Join):
            if join.this:
                table = join.this
                if isinstance(table, sqlglot.exp.Table):
                    table_name = table.this.this if isinstance(table.this, sqlglot.exp.Identifier) else str(table.this)
                    if hasattr(table, 'alias') and table.alias:
                        alias = table.alias.this if isinstance(table.alias, sqlglot.exp.Identifier) else str(table.alias)
                        alias_map[alias] = table_name
    
    return alias_map


def extract_model_info(query: sqlglot.exp.Expression, model: SqlModel) -> dict:
    """Extract both joins and fields from a query."""
    return {
        "joins": extract_joins_recursive(query),
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


def generate_model_sql(model: SqlModel) -> str:
    """Generate SQL definition for a model.
    
    Args:
        model: SqlModel to generate SQL for
        
    Returns:
        SQL definition string
    """
    rendered_query = model.render_query()
    return f"-- Model: {model.name}\n{rendered_query.sql(pretty=True) if rendered_query else ''}\n\n"


def write_cube_data(
    cube_data: t.List[dict],
    models: t.List[SqlModel],
    output_file: t.Optional[Path] = None,
    openai_key: t.Optional[str] = None,
    openai_org: t.Optional[str] = None,
    openai_model: t.Optional[str] = None,
    generate_yaml: bool = False,
) -> None:
    """Write cube data to a file or stdout.
    
    Args:
        cube_data: List of cube data dictionaries
        models: List of SqlModels to include SQL definitions from
        output_file: Optional output file path. If not provided, prints to stdout.
        openai_key: Optional OpenAI API key for YAML generation
        openai_org: Optional OpenAI organization ID
        openai_model: Optional OpenAI model to use
        generate_yaml: If True, converts JSON to YAML using OpenAI
    """
    output_data = cube_data
    
    if generate_yaml:
        # Initialize OpenAI client
        client = OpenAIClient(
            api_key=openai_key,
            organization=openai_org,
            model=openai_model,
        )
        
        # Load prompt template
        prompt_path = Path(__file__).parent / "prompts" / "cube_generate.txt"
        with open(prompt_path) as f:
            prompt_template = f.read()
            
        # Generate SQL definitions
        sql_definitions = "\n".join(generate_model_sql(model) for model in models)
            
        # Generate YAML using OpenAI
        output_data = client.generate_yaml(cube_data, sql_definitions, prompt_template)
    else:
        # Convert to JSON if not generating YAML
        output_data = json.dumps(cube_data, indent=2)

    if output_file:
        # Write to file with appropriate extension
        ext = ".yaml" if generate_yaml else ".json"
        output_path = Path(output_file).with_suffix(ext)
        with open(output_path, "w") as f:
            f.write(output_data)
    else:
        print(output_data)


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
    if models is None and model_dir:
        models = load_models_from_directory(model_dir, tag)
    if not models:
        return
    cube_data = generate_cube_data(models)
    write_cube_data(cube_data, models, output_file)


if __name__ == "__main__":
    import sys
    import argparse

    parser = argparse.ArgumentParser(description="Generate cube data from SQL models")
    parser.add_argument("model_dir", type=Path, help="Directory containing SQL models")
    parser.add_argument("--output", "-o", type=Path, help="Output file path")
    parser.add_argument("--tag", "-t", help="Filter models by tag")

    args = parser.parse_args()
    main(args.model_dir, args.output, tag=args.tag)
