from __future__ import annotations

import collections
import typing as t

import sqlglot
from fastapi import APIRouter, Depends
from sqlglot import exp
from sqlglot.lineage import Node, lineage

from sqlmesh.core.context import Context
from sqlmesh.core.dialect import normalize_model_name
from sqlmesh.utils.errors import SQLMeshError
from web.server.exceptions import ApiException
from web.server.models import LineageColumn
from web.server.settings import get_loaded_context

if t.TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlmesh.core.model import Model

router = APIRouter()


def _get_table(node: Node, dialect: t.Optional[DialectType] = None) -> str:
    """Get a node's table/source"""
    # Default to node name
    table: t.Union[exp.Table, str] = node.name
    if node.alias:
        # Use node alias if available
        table = node.alias
    elif isinstance(node.expression, exp.Table):
        table = node.expression
    elif isinstance(node.expression, exp.Alias):
        # Extract source table from alias
        sources_from_comments = [
            comment[len("source :") :]
            for comment in node.source.args["from"].this.comments or []
            if comment.startswith("source: ")
        ]
        if sources_from_comments:
            table = sources_from_comments[0]
        else:
            for source_table in node.source.find_all(exp.Table):
                for column in node.expression.this.find_all(exp.Column):
                    if source_table.alias == column.table:
                        table = source_table

    try:
        return normalize_model_name(table, None, dialect=dialect)
    except sqlglot.errors.ParseError:
        # Cannot extract table from node. One reason this can happen is node is
        # '*' because a model selects * from an external model for which we do
        # not know the schema.
        return ""


def _get_column(node: Node, dialect: t.Optional[DialectType] = None) -> str:
    if isinstance(node.expression, exp.Alias) and isinstance(node.expression.this, exp.Column):
        return node.expression.this.alias_or_name
    return exp.to_column(node.name).name


def _get_node_source(node: Node, dialect: DialectType) -> str:
    """Get a node's source"""
    if isinstance(node.expression, exp.Table):
        source = f"SELECT {node.name} FROM {node.expression.this}"
    else:
        source = node.source.sql(pretty=True, dialect=dialect)
    return source


def _process_downstream(
    downstream: t.List[Node], parent_table: str, dialect: DialectType
) -> t.Dict[str, t.List[str]]:
    """Aggregate a list of downstream nodes by table/source"""
    graph = collections.defaultdict(list)
    for node in downstream:
        table = _get_table(node, dialect=dialect)
        if not table or table == parent_table:
            continue

        column = _get_column(node)
        if column:
            graph[table].append(column)
    return graph


def render_query(model: Model) -> exp.Subqueryable:
    """Render a model's query, adding in managed columns"""
    query = model.render_query_or_raise()
    if model.managed_columns:
        query.select(
            *[
                exp.alias_(exp.cast(exp.Null(), to=col_type), col)
                for col, col_type in model.managed_columns.items()
                if col not in query.named_selects
            ],
            append=True,
            copy=False,
        )
    return query


@router.get("/{model_name:str}/{column_name:str}")
async def column_lineage(
    column_name: str,
    model_name: str,
    context: Context = Depends(get_loaded_context),
) -> t.Dict[str, t.Dict[str, LineageColumn]]:
    """Get a column's lineage"""
    try:
        model = context.get_model(model_name)
        dialect = model.dialect
        sources: t.Dict[str, str | exp.Subqueryable] = {}
        for m in context.dag.upstream(model.fqn):
            if m in context.models:
                try:
                    sources[m] = render_query(context.models[m])
                except SQLMeshError:
                    continue

        node = lineage(
            column=column_name,
            sql=render_query(model),
            sources=sources,
            dialect=dialect,
        )
    except Exception:
        raise ApiException(
            message="Unable to get a column lineage",
            origin="API -> lineage -> column_lineage",
        )

    graph: t.Dict[str, t.Dict[str, LineageColumn]] = collections.defaultdict(dict)

    for i, node in enumerate(node.walk()):
        if i == 0:
            table = model.fqn
        else:
            table = _get_table(node, dialect)
        if not table:
            continue

        column_name = _get_column(node, dialect)
        if column_name in graph.get(table, []):
            continue

        # At this point node_name should be fqn/normalized/quoted
        graph[table][column_name] = LineageColumn(
            expression=node.expression.sql(pretty=True, dialect=dialect),
            source=_get_node_source(node=node, dialect=dialect),
            models=_process_downstream(node.downstream, parent_table=table, dialect=dialect),
        )

    return graph


@router.get("/{model_name:str}")
async def model_lineage(
    model_name: str,
    context: Context = Depends(get_loaded_context),
) -> t.Dict[str, t.Set[str]]:
    """Get a model's lineage"""
    try:
        model_name = context.get_model(model_name).fqn
    except Exception:
        raise ApiException(
            message="Unable to get a model lineage",
            origin="API -> lineage -> model_lineage",
        )

    return context.dag.lineage(model_name).graph
