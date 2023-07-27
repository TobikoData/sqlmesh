from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.helper import ensure_collection

from sqlmesh.core.metric.definition import Metric, remove_namespace


class Renderer:
    def __init__(self, metrics: t.Collection[Metric], sources: t.Dict[str, exp.Expression]):
        self.metrics = metrics
        self.sources = sources

    def agg(
        self,
        group_by: str | t.Collection[str],
        where: t.Optional[exp.ExpOrStr | t.Dict[str, exp.ExpOrStr]] = None,
        having: t.Optional[exp.ExpOrStr] = None,
        join_type: str = "FULL",
        dialect: str = "",
    ) -> exp.Expression:
        """Returns an aggregate expression to compute metrics.

        Args:
            group_by: A collection of fields to group by and use as the grain of the query.
            where: An expression to filter sources by or a dictionary of source -> filter.
            having: An expression to filter the aggregation by.
            join_type: The method of joining derived metrics, by default this is a full join.
            dialect: The sql dialect that where and having are if they are strings.

        Returns:
            A SQLGlot expression containing the aggregate query.
        """
        group_by = ensure_collection(group_by)
        sources: t.Dict[str, t.Set[exp.AggFunc]] = {name: set() for name in self.sources}
        formulas = []
        where = where or {}

        if not isinstance(where, dict):
            where = exp.maybe_parse(where, dialect=dialect)
            where = {name: where for name in sources}

        for metric in self.metrics:
            formulas.append(metric.formula)
            for agg, tables in metric.aggs.items():
                # TODO: sort these tables depending on relationships
                for table_name in tables:
                    sources[table_name].add(agg)

        queries = {}

        for name, aggs in sources.items():
            source = self.sources[name].copy()
            table_name = remove_namespace(name)

            if isinstance(source, exp.Subqueryable):
                source = source.subquery(table_name, copy=False)
            else:
                source = exp.alias_(source, table_name, table=True, copy=False)

            queries[table_name] = (
                exp.select(
                    *group_by,
                    *sorted(aggs, key=str),
                    copy=False,
                )
                .from_(source, copy=False)
                .where(where.get(name))
                .group_by(*group_by, copy=False)
            )

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
