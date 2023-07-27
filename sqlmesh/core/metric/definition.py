from __future__ import annotations

import typing as t
from pathlib import Path

from pydantic import validator
from sqlglot import exp
from sqlglot.helper import first

from sqlmesh.core import dialect as d
from sqlmesh.core.node import str_or_exp_to_str
from sqlmesh.utils import UniqueKeyDict
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.pydantic import PydanticModel


def load_metric_ddl(
    expression: exp.Expression, dialect: t.Optional[str], path: Path = Path(), **kwargs: t.Any
) -> MetricMeta:
    """Returns a MetricMeta from raw Metric DDL."""
    if not isinstance(expression, d.Metric):
        _raise_metric_config_error(
            f"Only METRIC(...) statements are allowed. Found {expression.sql(pretty=True)}", path
        )

    metric = MetricMeta(
        **{
            "dialect": dialect,
            "description": "\n".join(comment.strip() for comment in expression.comments)
            if expression.comments
            else None,
            **{prop.name.lower(): prop.args.get("value") for prop in expression.expressions},
            **kwargs,
        }
    )

    metric._path = path

    return metric


def expand_metrics(metas: UniqueKeyDict[str, MetricMeta]) -> UniqueKeyDict[str, Metric]:
    """Resolves all metas into standalone metrics."""
    metrics: UniqueKeyDict[str, Metric] = UniqueKeyDict("metrics")

    for name, meta in metas.items():
        if name not in metrics:
            metrics[name] = meta.to_metric(metas, metrics)

    return metrics


@t.overload
def remove_namespace(expression: str) -> str:
    ...


@t.overload
def remove_namespace(expression: exp.Column, dialect: str) -> str:
    ...


def remove_namespace(expression: str | exp.Column, dialect: t.Optional[str] = None) -> str:
    """Given a column or a string, rewrite table namespaces like catalog.db to catalog__db"""

    if not isinstance(expression, str):
        assert dialect is not None
        expression = first(_find_tables(expression, dialect=dialect))
    return expression.replace(".", "__")


class MetricMeta(PydanticModel, frozen=True):
    """Raw metric definition without relationships or expansion of derived metrics."""

    name: str
    dialect: str
    expression: exp.Expression
    description: t.Optional[str] = None
    owner: t.Optional[str] = None

    _path: Path = Path()

    @validator("name", pre=True)
    def _name_validator(cls, v: t.Any, values: t.Dict[str, t.Any]) -> str:
        return cls._string_validator(v).lower()

    @validator("dialect", "owner", "description", pre=True)
    def _string_validator(cls, v: t.Any) -> t.Optional[str]:
        return str_or_exp_to_str(v)

    @validator("expression", pre=True)
    def _validate_expression(
        cls,
        v: t.Any,
        values: t.Dict[str, t.Any],
    ) -> exp.Expression:
        if isinstance(v, str):
            return d.parse_one(v, dialect=values.get("dialect"))
        if isinstance(v, exp.Expression):
            return v
        return v

    def to_metric(
        self, metas: t.Dict[str, MetricMeta], metrics: UniqueKeyDict[str, Metric]
    ) -> Metric:
        """Converts a metric meta into a fully expanded and standalone metric."""
        metric_refs = {}
        agg_or_ref = False

        for node, *_ in self.expression.walk():
            if isinstance(node, exp.Alias):
                _raise_metric_config_error(
                    f"Alias found for metric '{self.name}' which is not allowed", self._path
                )
            elif isinstance(node, exp.AggFunc):
                agg_or_ref = True
            elif isinstance(node, exp.Column) and not node.table:
                agg_or_ref = True
                ref = d.normalize_model_name(node.sql(dialect=self.dialect), dialect=self.dialect)

                if ref not in metrics:
                    metrics[ref] = metas[ref].to_metric(metas, metrics)

                metric_refs[node] = metrics[ref]

        if not agg_or_ref:
            _raise_metric_config_error(
                f"Metric '{self.name}' missing an aggregation or metric ref", self._path
            )

        if metric_refs:
            expanded = self.expression.copy()
            for column in expanded.find_all(exp.Column):
                metric = metric_refs.get(column)

                if metric:
                    column.replace(metric.expanded.copy())
        else:
            expanded = exp.alias_(self.expression, self.name)

        metric = Metric(**self.dict(), expanded=expanded)
        metric._path = self._path
        return metric


class Metric(MetricMeta, frozen=True):
    expanded: exp.Expression

    @property
    def aggs(self) -> t.Dict[exp.AggFunc, t.Set[str]]:
        """Returns a dictionary of aggregation to referenced tables.

        This method removes catalog and schema information from columns.
        """
        return {
            t.cast(exp.Expression, agg.parent).transform(
                lambda node: exp.column(node.this, table=remove_namespace(node, self.dialect))
                if isinstance(node, exp.Column) and node.table
                else node
            ): _find_tables(agg, self.dialect)
            for agg in self.expanded.find_all(exp.AggFunc)
        }

    @property
    def formula(self) -> exp.Expression:
        """Returns the post aggregation formula of a metric.

        For simple metrics it is just the metric name. For derived metrics,
        it consists of the operations of the derived metrics without aggregations.
        """
        return exp.alias_(
            self.expanded.transform(
                lambda node: exp.column(node.args["alias"]) if isinstance(node, exp.Alias) else node
            ),
            self.name,
            copy=False,
        )


def _raise_metric_config_error(msg: str, path: Path) -> None:
    raise ConfigError(f"{msg}. '{path}'")


def _find_tables(expression: exp.Expression, dialect: str) -> t.Set[str]:
    """Finds all the table references in a metric definition."""
    return {
        d.normalize_model_name(exp.table_(*reversed(column.parts[:-1])), dialect=dialect)  # type: ignore
        for column in expression.find_all(exp.Column)
        if column.table
    }
