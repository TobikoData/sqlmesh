from __future__ import annotations

import math
import typing as t

import pandas as pd
from sqlglot import exp
from sqlglot.dialects.dialect import DialectType

from sqlmesh.utils.pydantic import PydanticModel

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import TableName
    from sqlmesh.core.engine_adapter import EngineAdapter


class SchemaDiff(PydanticModel, frozen=True):
    """An object containing the schema difference between a source and target table."""

    source: str
    target: str
    source_schema: t.Dict[str, exp.DataType]
    target_schema: t.Dict[str, exp.DataType]
    source_alias: t.Optional[str] = None
    target_alias: t.Optional[str] = None
    model_name: t.Optional[str] = None

    @property
    def added(self) -> t.List[t.Tuple[str, exp.DataType]]:
        """Added columns."""
        return [(c, t) for c, t in self.target_schema.items() if c not in self.source_schema]

    @property
    def removed(self) -> t.List[t.Tuple[str, exp.DataType]]:
        """Removed columns."""
        return [(c, t) for c, t in self.source_schema.items() if c not in self.target_schema]

    @property
    def modified(self) -> t.Dict[str, t.Tuple[exp.DataType, exp.DataType]]:
        """Columns with modified types."""
        modified = {}
        for column in self.source_schema.keys() & self.target_schema.keys():
            source_type = self.source_schema[column]
            target_type = self.target_schema[column]

            if source_type != target_type:
                modified[column] = (source_type, target_type)
        return modified


class RowDiff(PydanticModel, frozen=True):
    """Summary statistics and a sample dataframe."""

    source: str
    target: str
    stats: t.Dict[str, float]
    sample: pd.DataFrame
    source_alias: t.Optional[str] = None
    target_alias: t.Optional[str] = None
    model_name: t.Optional[str] = None

    @property
    def source_count(self) -> int:
        """Count of the source."""
        return int(self.stats["s_count"])

    @property
    def target_count(self) -> int:
        """Count of the target."""
        return int(self.stats["t_count"])

    @property
    def count_pct_change(self) -> float:
        """The percentage change of the counts."""
        if self.source_count == 0:
            return math.inf
        return ((self.target_count - self.source_count) / self.source_count) * 100


class TableDiff:
    """Calculates differences between tables, taking into account schema and row level differences."""

    def __init__(
        self,
        adapter: EngineAdapter,
        source: TableName,
        target: TableName,
        on: t.List[str] | exp.Condition,
        where: t.Optional[str | exp.Condition] = None,
        dialect: DialectType = None,
        limit: int = 20,
        source_alias: t.Optional[str] = None,
        target_alias: t.Optional[str] = None,
        model_name: t.Optional[str] = None,
    ):
        self.adapter = adapter
        self.source = source
        self.target = target
        self.where = exp.condition(where, dialect=dialect) if where else None
        self.limit = limit
        # Support environment aliases for diff output improvement in certain cases
        self.source_alias = source_alias
        self.target_alias = target_alias
        self.model_name = model_name

        if isinstance(on, (list, tuple)):
            self.on: exp.Condition = exp.and_(
                *(
                    exp.column(c, "s").eq(exp.column(c, "t"))
                    | (exp.column(c, "s").is_(exp.null()) & exp.column(c, "t").is_(exp.null()))
                    for c in on
                )
            )
        else:
            self.on = on

        self._source_schema: t.Optional[t.Dict[str, exp.DataType]] = None
        self._target_schema: t.Optional[t.Dict[str, exp.DataType]] = None
        self._row_diff: t.Optional[RowDiff] = None

    @property
    def source_schema(self) -> t.Dict[str, exp.DataType]:
        if self._source_schema is None:
            self._source_schema = self.adapter.columns(self.source)
        return self._source_schema

    @property
    def target_schema(self) -> t.Dict[str, exp.DataType]:
        if self._target_schema is None:
            self._target_schema = self.adapter.columns(self.target)
        return self._target_schema

    def schema_diff(self) -> SchemaDiff:
        return SchemaDiff(
            source=self.source,
            target=self.target,
            source_schema=self.source_schema,
            target_schema=self.target_schema,
            source_alias=self.source_alias,
            target_alias=self.target_alias,
            model_name=self.model_name,
        )

    def row_diff(self) -> RowDiff:
        if self._row_diff is None:
            s_selects = {c: exp.column(c, "s").as_(f"s__{c}") for c in self.source_schema}
            t_selects = {c: exp.column(c, "t").as_(f"t__{c}") for c in self.target_schema}

            s_index = []
            t_index = []

            for col in self.on.find_all(exp.Column):
                col.not_().is_(exp.Null())
                if col.table == "s":
                    s_index.append(col)
                elif col.table == "t":
                    t_index.append(col)

            comparisons = [
                exp.func("IF", exp.column(c, "s").eq(exp.column(c, "t")), 1, 0).as_(f"{c}_matches")
                for c, t in self.source_schema.items()
                if t == self.target_schema.get(c)
            ]

            query = (
                exp.select(
                    *s_selects.values(),
                    *t_selects.values(),
                    exp.func("IF", exp.or_(*(c.not_().is_(exp.Null()) for c in s_index)), 1, 0).as_(
                        "s_exists"
                    ),
                    exp.func("IF", exp.or_(*(c.not_().is_(exp.Null()) for c in t_index)), 1, 0).as_(
                        "t_exists"
                    ),
                    *comparisons,
                )
                .from_(exp.alias_(self.source, "s"))
                .join(
                    self.target,
                    on=self.on,
                    join_type="FULL",
                    join_alias="t",
                )
                .where(self.where)
            )

            with self.adapter.temp_table(query, name="sqlmesh_temp.diff") as table:
                summary_query = exp.select(
                    exp.func("SUM", "s_exists").as_("s_count"),
                    exp.func("SUM", "t_exists").as_("t_count"),
                    *(exp.func("SUM", c.alias).as_(c.alias) for c in comparisons),
                ).from_(table)

                sample_query = (
                    exp.select(
                        *(c.alias for c in s_selects.values()),
                        *(c.alias for c in t_selects.values()),
                    )
                    .from_(table)
                    .where(exp.or_(*(exp.column(c.alias).eq(0) for c in comparisons)))
                    .order_by(
                        *(s_selects[c.name].alias for c in s_index),
                        *(t_selects[c.name].alias for c in t_index),
                    )
                    .limit(self.limit)
                )

                self._row_diff = RowDiff(
                    source=self.source,
                    target=self.target,
                    stats=self.adapter.fetchdf(summary_query).iloc[0].to_dict(),
                    sample=self.adapter.fetchdf(sample_query),
                    source_alias=self.source_alias,
                    target_alias=self.target_alias,
                    model_name=self.model_name,
                )
        return self._row_diff
