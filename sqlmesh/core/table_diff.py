from __future__ import annotations

import math
import typing as t
from functools import cached_property

import pandas as pd

from sqlmesh.core.dialect import to_schema
from sqlmesh.core.engine_adapter.mixins import RowDiffMixin
from sqlglot import exp, parse_one
from sqlglot.helper import ensure_list
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers
from sqlglot.optimizer.qualify_columns import quote_identifiers
from sqlglot.optimizer.scope import find_all_in_scope

from sqlmesh.utils.pydantic import PydanticModel

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import TableName
    from sqlmesh.core.engine_adapter import EngineAdapter

SQLMESH_JOIN_KEY_COL = "__sqlmesh_join_key"


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
    joined_sample: pd.DataFrame
    s_sample: pd.DataFrame
    t_sample: pd.DataFrame
    column_stats: pd.DataFrame
    source_alias: t.Optional[str] = None
    target_alias: t.Optional[str] = None
    model_name: t.Optional[str] = None
    decimals: int = 3

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

    @property
    def join_count(self) -> int:
        """Count of successfully joined rows."""
        return int(self.stats["join_count"])

    @property
    def full_match_count(self) -> int:
        """The number of rows for which shared columns have same values."""
        return int(self.stats["full_match_count"])

    @property
    def full_match_pct(self) -> float:
        """The percentage of rows for which shared columns have same values."""
        return self._pct(2 * self.full_match_count)

    @property
    def partial_match_count(self) -> int:
        """The number of rows for which some shared columns have same values."""
        return self.join_count - self.full_match_count

    @property
    def partial_match_pct(self) -> float:
        """The percentage of rows for which some shared columns have same values."""
        return self._pct(2 * self.partial_match_count)

    @property
    def s_only_count(self) -> int:
        """Count of rows only present in source."""
        return int(self.stats["s_only_count"])

    @property
    def s_only_pct(self) -> float:
        """The percentage of rows that are only present in source."""
        return self._pct(self.s_only_count)

    @property
    def t_only_count(self) -> int:
        """Count of rows only present in target."""
        return int(self.stats["t_only_count"])

    @property
    def t_only_pct(self) -> float:
        """The percentage of rows that are only present in target."""
        return self._pct(self.t_only_count)

    def _pct(self, numerator: int) -> float:
        return round((numerator / (self.source_count + self.target_count)) * 100, 2)


class TableDiff:
    """Calculates differences between tables, taking into account schema and row level differences."""

    def __init__(
        self,
        adapter: EngineAdapter,
        source: TableName,
        target: TableName,
        on: t.List[str] | exp.Condition,
        skip_columns: t.List[str] | None = None,
        where: t.Optional[str | exp.Condition] = None,
        limit: int = 20,
        source_alias: t.Optional[str] = None,
        target_alias: t.Optional[str] = None,
        model_name: t.Optional[str] = None,
        model_dialect: t.Optional[str] = None,
        decimals: int = 3,
    ):
        if not isinstance(adapter, RowDiffMixin):
            raise ValueError(f"Engine {adapter} doesnt support RowDiff")

        self.adapter = adapter
        self.source = source
        self.target = target
        self.dialect = adapter.dialect
        self.source_table = exp.to_table(self.source, dialect=self.dialect)
        self.target_table = exp.to_table(self.target, dialect=self.dialect)
        self.where = exp.condition(where, dialect=self.dialect) if where else None
        self.limit = limit
        self.model_name = model_name
        self.model_dialect = model_dialect
        self.decimals = decimals

        # Support environment aliases for diff output improvement in certain cases
        self.source_alias = source_alias
        self.target_alias = target_alias

        self.skip_columns = {
            normalize_identifiers(
                exp.parse_identifier(t.cast(str, col)),
                dialect=self.model_dialect or self.dialect,
            ).name
            for col in ensure_list(skip_columns)
        }

        self._on = on
        self._row_diff: t.Optional[RowDiff] = None

    @cached_property
    def source_schema(self) -> t.Dict[str, exp.DataType]:
        return self.adapter.columns(self.source_table)

    @cached_property
    def target_schema(self) -> t.Dict[str, exp.DataType]:
        return self.adapter.columns(self.target_table)

    @cached_property
    def key_columns(self) -> t.Tuple[t.List[exp.Column], t.List[exp.Column], t.List[str]]:
        dialect = self.model_dialect or self.dialect

        # If the columns to join on are explicitly specified, then just return them
        if isinstance(self._on, (list, tuple)):
            identifiers = [normalize_identifiers(c, dialect=dialect) for c in self._on]
            s_index = [exp.column(c, "s") for c in identifiers]
            t_index = [exp.column(c, "t") for c in identifiers]
            return s_index, t_index, [i.name for i in identifiers]

        # Otherwise, we need to parse them out of the supplied "on" condition
        index_cols = []
        s_index = []
        t_index = []

        normalize_identifiers(self._on, dialect=dialect)
        for col in self._on.find_all(exp.Column):
            index_cols.append(col.name)
            if col.table.lower() == "s":
                s_index.append(col)
            elif col.table.lower() == "t":
                t_index.append(col)

        index_cols = list(dict.fromkeys(index_cols))
        s_index = list(dict.fromkeys(s_index))
        t_index = list(dict.fromkeys(t_index))

        return s_index, t_index, index_cols

    @property
    def source_key_expression(self) -> exp.Expression:
        s_index, _, _ = self.key_columns
        return self._key_expression(s_index, self.source_schema)

    @property
    def target_key_expression(self) -> exp.Expression:
        _, t_index, _ = self.key_columns
        return self._key_expression(t_index, self.target_schema)

    def _key_expression(
        self, cols: t.List[exp.Column], schema: t.Dict[str, exp.DataType]
    ) -> exp.Expression:
        # if there is a single column, dont do anything fancy to it in order to allow existing indexes to be hit
        if len(cols) == 1:
            return exp.to_column(cols[0].name)

        # if there are multiple columns, turn them into a single column by stringify-ing/concatenating them together
        key_columns_to_types = {key.name: schema[key.name] for key in cols}
        return self.adapter.concat_columns(key_columns_to_types, self.decimals)

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

    def row_diff(
        self, temp_schema: t.Optional[str] = None, skip_grain_check: bool = False
    ) -> RowDiff:
        if self._row_diff is None:
            source_schema = {
                c: t for c, t in self.source_schema.items() if c not in self.skip_columns
            }
            target_schema = {
                c: t for c, t in self.target_schema.items() if c not in self.skip_columns
            }

            s_selects = {c: exp.column(c, "s").as_(f"s__{c}") for c in source_schema}
            t_selects = {c: exp.column(c, "t").as_(f"t__{c}") for c in target_schema}
            s_selects[SQLMESH_JOIN_KEY_COL] = exp.column(SQLMESH_JOIN_KEY_COL, "s").as_(
                f"s__{SQLMESH_JOIN_KEY_COL}"
            )
            t_selects[SQLMESH_JOIN_KEY_COL] = exp.column(SQLMESH_JOIN_KEY_COL, "t").as_(
                f"t__{SQLMESH_JOIN_KEY_COL}"
            )

            matched_columns = {c: t for c, t in source_schema.items() if t == target_schema.get(c)}

            s_index, t_index, index_cols = self.key_columns
            s_index_names = [c.name for c in s_index]
            t_index_names = [t.name for t in t_index]

            def _column_expr(name: str, table: str) -> exp.Expression:
                column_type = matched_columns[name]
                qualified_column = exp.column(name, table)

                if column_type.is_type(*exp.DataType.FLOAT_TYPES):
                    return exp.func("ROUND", qualified_column, exp.Literal.number(self.decimals))
                if column_type.is_type(*exp.DataType.NESTED_TYPES):
                    return self.adapter._normalize_nested_value(qualified_column)

                return qualified_column

            comparisons = [
                exp.Case()
                .when(_column_expr(c, "s").eq(_column_expr(c, "t")), exp.Literal.number(1))
                .when(
                    exp.column(c, "s").is_(exp.Null()) & exp.column(c, "t").is_(exp.Null()),
                    exp.Literal.number(1),
                )
                .when(
                    exp.column(c, "s").is_(exp.Null()) | exp.column(c, "t").is_(exp.Null()),
                    exp.Literal.number(0),
                )
                .else_(exp.Literal.number(0))
                .as_(f"{c}_matches")
                for c, t in matched_columns.items()
            ]

            def name(e: exp.Expression) -> str:
                return e.args["alias"].sql(identify=True)

            source_query = (
                exp.select(
                    *(exp.column(c) for c in source_schema),
                    self.source_key_expression.as_(SQLMESH_JOIN_KEY_COL),
                )
                .from_(self.source_table.as_("s"))
                .where(self.where)
            )
            target_query = (
                exp.select(
                    *(exp.column(c) for c in target_schema),
                    self.target_key_expression.as_(SQLMESH_JOIN_KEY_COL),
                )
                .from_(self.target_table.as_("t"))
                .where(self.where)
            )

            # Ensure every column is qualified with the alias in the source and target queries
            for col in find_all_in_scope(source_query, exp.Column):
                col.set("table", exp.to_identifier("s"))
            for col in find_all_in_scope(target_query, exp.Column):
                col.set("table", exp.to_identifier("t"))

            source_table = exp.table_("__source")
            target_table = exp.table_("__target")
            stats_table = exp.table_("__stats")

            stats_query = (
                exp.select(
                    *s_selects.values(),
                    *t_selects.values(),
                    exp.func("IF", exp.or_(*(c.is_(exp.Null()).not_() for c in s_index)), 1, 0).as_(
                        "s_exists"
                    ),
                    exp.func("IF", exp.or_(*(c.is_(exp.Null()).not_() for c in t_index)), 1, 0).as_(
                        "t_exists"
                    ),
                    exp.func(
                        "IF",
                        exp.and_(
                            exp.column(SQLMESH_JOIN_KEY_COL, "s").eq(
                                exp.column(SQLMESH_JOIN_KEY_COL, "t")
                            ),
                            exp.and_(
                                *(c.is_(exp.Null()).not_() for c in s_index + t_index),
                            ),
                        ),
                        1,
                        0,
                    ).as_("row_joined"),
                    exp.func(
                        "IF",
                        exp.or_(
                            *(
                                exp.and_(
                                    s.is_(exp.Null()),
                                    t.is_(exp.Null()),
                                )
                                for s, t in zip(s_index, t_index)
                            ),
                        ),
                        1,
                        0,
                    ).as_("null_grain"),
                    *comparisons,
                )
                .from_(source_table.as_("s"))
                .join(
                    target_table.as_("t"),
                    on=exp.column(SQLMESH_JOIN_KEY_COL, "s").eq(
                        exp.column(SQLMESH_JOIN_KEY_COL, "t")
                    ),
                    join_type="FULL",
                )
            )

            base_query = (
                exp.Select()
                .with_(source_table, source_query)
                .with_(target_table, target_query)
                .with_(stats_table, stats_query)
                .select(
                    "*",
                    exp.Case()
                    .when(
                        exp.and_(
                            *[
                                exp.column(f"{c}_matches").eq(exp.Literal.number(1))
                                for c in matched_columns
                            ]
                        ),
                        exp.Literal.number(1),
                    )
                    .else_(exp.Literal.number(0))
                    .as_("row_full_match"),
                )
                .from_(stats_table)
            )

            query = self.adapter.ensure_nulls_for_unmatched_after_join(
                quote_identifiers(base_query.copy(), dialect=self.model_dialect or self.dialect)
            )

            if not temp_schema:
                temp_schema = "sqlmesh_temp"

            schema = to_schema(temp_schema, dialect=self.dialect)
            temp_table = exp.table_("diff", db=schema.db, catalog=schema.catalog, quoted=True)

            with self.adapter.temp_table(query, name=temp_table) as table:
                summary_sums = [
                    exp.func("SUM", "s_exists").as_("s_count"),
                    exp.func("SUM", "t_exists").as_("t_count"),
                    exp.func("SUM", "row_joined").as_("join_count"),
                    exp.func("SUM", "null_grain").as_("null_grain_count"),
                    exp.func("SUM", "row_full_match").as_("full_match_count"),
                    *(exp.func("SUM", name(c)).as_(c.alias) for c in comparisons),
                ]

                if not skip_grain_check:
                    summary_sums.extend(
                        [
                            parse_one(f"COUNT(DISTINCT(s__{SQLMESH_JOIN_KEY_COL}))").as_(
                                "distinct_count_s"
                            ),
                            parse_one(f"COUNT(DISTINCT(t__{SQLMESH_JOIN_KEY_COL}))").as_(
                                "distinct_count_t"
                            ),
                        ]
                    )

                summary_query = exp.select(*summary_sums).from_(table)

                stats_df = self.adapter.fetchdf(summary_query, quote_identifiers=True)
                stats_df["s_only_count"] = stats_df["s_count"] - stats_df["join_count"]
                stats_df["t_only_count"] = stats_df["t_count"] - stats_df["join_count"]
                stats = stats_df.iloc[0].to_dict()

                column_stats_query = (
                    exp.select(
                        *(
                            exp.func(
                                "ROUND",
                                100
                                * (
                                    exp.cast(
                                        exp.func("SUM", name(c)), exp.DataType.build("NUMERIC")
                                    )
                                    / exp.func("COUNT", name(c))
                                ),
                                9,
                            ).as_(c.alias)
                            for c in comparisons
                        )
                    )
                    .from_(table)
                    .where(exp.column("row_joined").eq(exp.Literal.number(1)))
                )

                column_stats = (
                    self.adapter.fetchdf(column_stats_query, quote_identifiers=True)
                    .T.rename(
                        columns={0: "pct_match"},
                        index=lambda x: str(x).replace("_matches", "") if x else "",
                    )
                    # errors=ignore because all the index_cols might not be present in the DF if the `on` condition was something like "s.id == t.item_id"
                    # because these would not be present in the matching_cols (since they have different names) and thus no summary would be generated
                    .drop(index=index_cols, errors="ignore")
                )

                sample_filter_cols = ["s_exists", "t_exists", "row_joined", "row_full_match"]
                sample_query = (
                    exp.select(
                        *(sample_filter_cols),
                        *(name(c) for c in s_selects.values()),
                        *(name(c) for c in t_selects.values()),
                    )
                    .from_(table)
                    .where(exp.or_(*(exp.column(c.alias).eq(0) for c in comparisons)))
                    .order_by(
                        *(name(s_selects[c.name]) for c in s_index),
                        *(name(t_selects[c.name]) for c in t_index),
                    )
                    .limit(self.limit)
                )
                sample = self.adapter.fetchdf(sample_query, quote_identifiers=True)

                joined_sample_cols = [f"s__{c}" for c in s_index_names]
                comparison_cols = [
                    (f"s__{c}", f"t__{c}")
                    for c in column_stats[column_stats["pct_match"] < 100].index
                ]
                for cols in comparison_cols:
                    joined_sample_cols.extend(cols)
                joined_renamed_cols = {
                    c: c.split("__")[1] if c.split("__")[1] in index_cols else c
                    for c in joined_sample_cols
                }

                if (
                    self.source_alias
                    and self.target_alias
                    and self.source != self.source_alias
                    and self.target != self.target_alias
                ):
                    joined_renamed_cols = {
                        c: (
                            n.replace(
                                "s__",
                                f"{self.source_alias.upper()}__",
                            )
                            if n.startswith("s__")
                            else n
                        )
                        for c, n in joined_renamed_cols.items()
                    }
                    joined_renamed_cols = {
                        c: (
                            n.replace(
                                "t__",
                                f"{self.target_alias.upper()}__",
                            )
                            if n.startswith("t__")
                            else n
                        )
                        for c, n in joined_renamed_cols.items()
                    }
                joined_sample = sample[sample["row_joined"] == 1][joined_sample_cols]
                joined_sample.rename(
                    columns=joined_renamed_cols,
                    inplace=True,
                )

                s_sample = sample[(sample["s_exists"] == 1) & (sample["row_joined"] == 0)][
                    [
                        *[f"s__{c}" for c in s_index_names],
                        *[f"s__{c}" for c in source_schema if c not in s_index_names],
                    ]
                ]
                s_sample.rename(
                    columns={c: c.replace("s__", "") for c in s_sample.columns}, inplace=True
                )

                t_sample = sample[(sample["t_exists"] == 1) & (sample["row_joined"] == 0)][
                    [
                        *[f"t__{c}" for c in t_index_names],
                        *[f"t__{c}" for c in target_schema if c not in t_index_names],
                    ]
                ]
                t_sample.rename(
                    columns={c: c.replace("t__", "") for c in t_sample.columns}, inplace=True
                )

                sample.drop(
                    columns=sample_filter_cols
                    + [f"s__{SQLMESH_JOIN_KEY_COL}", f"t__{SQLMESH_JOIN_KEY_COL}"],
                    inplace=True,
                )

                self._row_diff = RowDiff(
                    source=self.source,
                    target=self.target,
                    stats=stats,
                    column_stats=column_stats,
                    sample=sample,
                    joined_sample=joined_sample,
                    s_sample=s_sample,
                    t_sample=t_sample,
                    source_alias=self.source_alias,
                    target_alias=self.target_alias,
                    model_name=self.model_name,
                    decimals=self.decimals,
                )
        return self._row_diff
