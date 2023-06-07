from __future__ import annotations

import typing as t

from fastapi import APIRouter, Depends
from sqlglot import exp

from sqlmesh.core.context import Context
from web.server.models import RowDiff, SchemaDiff, TableDiff
from web.server.settings import get_loaded_context

router = APIRouter()


@router.get("")
def get_table_diff(
    source: str,
    target: str,
    on: str,
    model_or_snapshot: t.Optional[str] = None,
    where: t.Optional[str] = None,
    limit: int = 20,
    context: Context = Depends(get_loaded_context),
) -> TableDiff:
    """Calculate differences between tables, taking into account schema and row level differences."""
    diff = context.table_diff(
        source=source,
        target=target,
        on=exp.condition(on),
        model_or_snapshot=model_or_snapshot,
        where=where,
        limit=limit,
        show=False,
    )
    _schema_diff = diff.schema_diff()
    _row_diff = diff.row_diff()
    schema_diff = SchemaDiff(
        source=_schema_diff.source,
        target=_schema_diff.target,
        source_schema=_schema_diff.source_schema,
        target_schema=_schema_diff.target_schema,
        added=_schema_diff.added,
        removed=_schema_diff.removed,
        modified=_schema_diff.modified,
    )
    row_diff = RowDiff(
        source=_row_diff.source,
        target=_row_diff.target,
        stats=_row_diff.stats,
        sample=_row_diff.sample.to_dict(),
        source_count=_row_diff.source_count,
        target_count=_row_diff.target_count,
        count_pct_change=_row_diff.count_pct_change,
    )
    return TableDiff(schema_diff=schema_diff, row_diff=row_diff)
