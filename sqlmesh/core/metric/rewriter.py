from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.dialects.dialect import DialectType
from sqlglot.optimizer import find_all_in_scope, optimize
from sqlglot.optimizer.qualify import qualify

from sqlmesh.core import dialect as d
from sqlmesh.core.metric.definition import Metric, remove_namespace

if t.TYPE_CHECKING:
    from sqlmesh.core.reference import ReferenceGraph


AggsAndJoins = t.Tuple[t.Set[exp.AggFunc], t.Set[str]]


class Rewriter:
    def __init__(
        self,
        graph: ReferenceGraph,
        metrics: t.Dict[str, Metric],
        dialect: DialectType = "",
        join_type: str = "FULL",
        semantic_prefix: str = "__semantic",
    ):
        self.graph = graph
        self.metrics = metrics
        self.dialect = dialect
        self.join_type = join_type
        self.semantic_prefix = semantic_prefix
        self.semantic_table = f"{self.semantic_prefix}.__table"

    def rewrite(self, expression: exp.Expression) -> exp.Expression:
        for select in list(expression.find_all(exp.Select)):
            self._expand(select)

        return expression

    def _expand(self, select: exp.Select) -> None:
        base = select.args["from"].this
        base_name = d.normalize_model_name(base.find(exp.Table), dialect=self.dialect)
        base_alias = base.alias_or_name

        sources: t.Dict[str, AggsAndJoins] = {}

        for projection in select.selects:
            for ref in find_all_in_scope(projection, d.MetricAgg):
                metric = self.metrics[ref.this.name]
                ref.replace(metric.formula.this)

                for agg, (measure, dims) in metric.aggs.items():
                    if base_name.lower() == self.semantic_table:
                        base_name = measure
                        base = exp.to_table(base_name)

                    aggs, joins = sources.setdefault(measure, (set(), set()))
                    aggs.add(agg)
                    joins.update(dims)

        def remove_table(node: exp.Expression) -> exp.Expression:
            for column in find_all_in_scope(node, exp.Column):
                if column.table == base_alias:
                    column.args["table"].pop()
            return node

        def replace_table(node: exp.Expression, table: str) -> exp.Expression:
            for column in find_all_in_scope(node, exp.Column):
                if column.table == base_alias:
                    column.args["table"] = exp.to_identifier(table)
            return node

        group = select.args.pop("group", None)
        group_by = group.expressions if group else []

        for name, (aggs, joins) in sources.items():
            if name == base_name:
                source = base
            else:
                source = exp.to_table(name)

            table_name = remove_namespace(name)

            if not isinstance(source, exp.Subqueryable):
                source = exp.Select().from_(
                    exp.alias_(source, table_name, table=True, copy=False), copy=False
                )

            self._add_joins(source, name, joins)

            grain = [replace_table(e.copy(), table_name) for e in group_by]

            query = source.select(
                *grain,
                *sorted(aggs, key=str),
                copy=False,
            ).group_by(*grain, copy=False)

            if not query.selects:
                query.select("*", copy=False)

            if name == base_name:
                where = select.args.pop("where", None)

                if where:
                    query.where(remove_table(where.this), copy=False)

                select.from_(query.subquery(base_alias, copy=False), copy=False)
            else:
                select.join(
                    query,
                    on=[e.eq(replace_table(e.copy(), table_name)) for e in group_by],  # type: ignore
                    join_type=self.join_type,
                    join_alias=table_name,
                    copy=False,
                )

    def _add_joins(self, source: exp.Select, name: str, joins: t.Collection[str]) -> None:
        for join in joins:
            path = self.graph.find_path(name, join)
            for i in range(len(path) - 1):
                a_ref = path[i]
                b_ref = path[i + 1]
                a_model_alias = remove_namespace(a_ref.model_name)
                b_model_alias = remove_namespace(b_ref.model_name)

                a = a_ref.expression.copy()
                a.set("table", exp.to_identifier(a_model_alias))
                b = b_ref.expression.copy()
                b.set("table", exp.to_identifier(b_model_alias))

                source.join(
                    b_ref.model_name,
                    on=a.eq(b),
                    join_type="LEFT",
                    join_alias=b_model_alias,
                    dialect=self.dialect,
                    copy=False,
                )


def rewrite(
    sql: str | exp.Expression,
    graph: ReferenceGraph,
    metrics: t.Dict[str, Metric],
    dialect: str = "",
) -> exp.Expression:
    rewriter = Rewriter(graph=graph, metrics=metrics, dialect=dialect)

    return optimize(
        d.parse_one(sql, dialect=dialect) if isinstance(sql, str) else sql,
        dialect=dialect,
        rules=(
            qualify,
            rewriter.rewrite,
        ),
    )
