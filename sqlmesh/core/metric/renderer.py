from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.helper import ensure_collection

from sqlmesh.core.metric.definition import Metric, remove_namespace

if t.TYPE_CHECKING:
    from sqlmesh.core.reference import ReferenceGraph


AggsAndJoins = t.Tuple[t.Set[exp.AggFunc], t.Set[str]]


class Renderer:
    def __init__(
        self,
        metrics: t.Collection[Metric],
        graph: ReferenceGraph,
        dialect: str = "",
        sources: t.Optional[t.Dict[str, exp.Expression]] = None,
    ):
        self.metrics = metrics
        self.graph = graph
        self.dialect = dialect
        self.sources = sources or {}

    def agg(
        self,
        group_by: str | t.Collection[str],
        where: t.Optional[exp.ExpOrStr | t.Dict[str, exp.ExpOrStr]] = None,
        having: t.Optional[exp.ExpOrStr] = None,
        join_type: str = "FULL",
    ) -> exp.Expression:
        """Returns an aggregate expression to compute metrics.

        Args:
            group_by: A collection of fields to group by and use as the grain of the query.
            where: An expression to filter sources by or a dictionary of source -> filter.
            having: An expression to filter the aggregation by.
            join_type: The method of joining derived metrics, by default this is a full join.

        Returns:
            A SQLGlot expression containing the aggregate query.
        """
        where = where or {}
        group_by = ensure_collection(group_by)
        formulas = []
        # ensure that relative order of sources stays the same if passed in
        sources: t.Dict[str, AggsAndJoins] = {name: (set(), set()) for name in self.sources}

        for metric in self.metrics:
            formulas.append(metric.formula)
            for agg, (measure, dims) in metric.aggs.items():
                aggs, joins = sources.setdefault(measure, (set(), set()))
                aggs.add(agg)
                joins.update(dims)

        if not isinstance(where, dict):
            where = exp.maybe_parse(where, dialect=self.dialect)
            where = {name: where for name in sources}

        queries = {}

        for name, (aggs, joins) in sources.items():
            source = self.sources[name].copy() if name in self.sources else exp.to_table(name)
            table_name = remove_namespace(name)

            if not isinstance(source, exp.Subqueryable):
                source = exp.Select().from_(
                    exp.alias_(source, table_name, table=True, copy=False), copy=False
                )

            source.where(where.get(name), copy=False)

            self._add_joins(source, name, joins)

            if source.selects:
                source = exp.Select().from_(source.subquery(table_name, copy=False), copy=False)

            queries[table_name] = source.select(
                *group_by,
                *sorted(aggs, key=str),
                copy=False,
            ).group_by(*group_by, copy=False)

        (name, head), *tail = queries.items()

        query = exp.select(*group_by)

        query.transform(
            lambda n: exp.column(n.this, table=name)
            if isinstance(n, exp.Column) and not n.table
            else n,
            copy=False,
        )

        query = query.select(
            *formulas,
            append=True,
            copy=False,
        ).from_(head.subquery(name, copy=False), copy=False)

        for name, table in tail:
            query.join(
                table,
                using=group_by,
                join_type=join_type,
                join_alias=name,
                copy=False,
            )

        return query.where(having, copy=False)

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
