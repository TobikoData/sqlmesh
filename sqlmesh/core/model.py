"""
# Model

Models are comprised of metadata and queries that create tables and views which can be used by other models or even outside of SQLMesh.
They are defined in a the `models/` directory of your SQLMesh product and live in `.sql` files. SQLMesh will automatically understand
the relationships and lineage of your models by parsing your SQL so you don't have to worry about manually setting up dependencies.

# Example
Models can be defined simply in SQL. The first statement of a model.sql file should be the MODEL DDL. The last statement will be a `SELECT` statement
that defines the logic needed to create the table.

```sql
-- Customer revenue computed and stored daily.
MODEL (
  name sushi.customer_revenue_by_day,
  owner toby,
  cron '@daily',
);

SELECT
  c.customer_id::TEXT,
  SUM(o.amount)::DOUBLE AS revenue
  o.ds::TEXT
FROM sushi.orders AS o
JOIN sushi.customers AS c
  ON o.customer_id = c.customer_id
WHERE o.ds BETWEEN @start_ds and @end_ds
```

# Conventions
SQLMesh attempts to infer a lot your pipelines through SQL alone to reduce the cognitive overhead of switching to another format like YAML.
The `SELECT` expression of a model must adhere to certain conventions in order for SQLMesh to get the necessary metadata to operate.

## Unique Column Names
The final selects of a model's query must be unique.

## Explict Types
The final selects of a model's query must be explicitly casted to a type. This way, SQLMesh can automatically create tables with the appropriate schema. SQLMesh uses the
postgres `x::int` syntax for casting because it is elegant. These postgres style casts will be transpiled automatically to the appropriate format of the
execution engine.

```sql
WITH cte AS (
SELECT 1 AS foo -- don't need to cast here
)
SELECT foo::int -- need to cast here because it is in the final select statement
```

## Inferrable Names
The final selects of a model's query must have inferrable names or aliases. An explicit alias is preferable but not necessary. Aliases will be automatically added by the SQLMesh formatter.

```sql
SELECT
  1, -- not inferrable
  x + 1, -- not infererrable
  SUM(x), -- not infererrable
  x, -- inferrable as x
  x::int, -- inferrable as x
  x + 1 AS x, -- explictly x
  SUM(x) as x, -- explicitly x
```

# Properties
The MODEL statement takes various properties which are used for both metadata and controlling behavior.

## name
- Name specifies the name of the model. This name represents the production view name that the model outputs so it generally
takens on the form of `"schema"."view_name"`. The name of a model must be unique in a SQLMesh project. When models are used in development
environments, SQLMesh automatically prefixes the name with a prefix. So given a model named `"sushi"."customers"`, in production, the view is named
 `"sushi"."customers"` and in dev `"dev__sushi"."customers"`.
- Name is ***required***, and must be ***unique***.

## kind
- Kind specifies what [kind](#model-kinds) a model is. A model's kind determines how it is computed and stored. The default kind is `incremental` which means that a model processes and stores data incrementally by minute, hour, or day.

## dialect
- Dialect defines the SQL dialect of the file. By default, this uses the dialect of the SQLMesh `sqlmesh.core.config`.

## owner
- Owner specifies who the main POC is for this particular model. It is an important field for organizations that have many data collaborators.

## start
- Start is used to determine the earliest time needed to process the model. It can be an absolute date/time (2022-01-01) or relative (`1 year ago`).

## cron
- Cron is used to schedule your model to process or refresh at a certain interval. It uses [croniter](https://github.com/kiorky/croniter) under the hood
    so expressions like `@daily` can be used. A model's `IntervalUnit` is determined implicity by the cron expression.

## batch_size
- Batch size is used to optimize backfilling incremental data. It determines the maximum number of intervals to run in a single job. For example, if
    a model specifies a cron of @hourly and a batch_size of 12, when backfilling 3 days of data, the scheduler will spawn 6 jobs.

## storage_format
- Storage format is an optional property for engines like Spark/Hive that support various storage formats like `parquet` and `orc`.

## time_column
- Time column is a required property for incremental models. It is used to determine which records to overwrite when doing an incremental insert.
Engines that support partitioning like Spark and Hive also use it as the partition key. Additional partition key columns can be specified with the
partitioned_by property below. Time column can have an optional format string. The format should be in the dialect of the model.

## partitioned_by
- Partition by is an optional property for engines like Spark/Hive that support partitioning. Use this to add additional columns to the time column partition key.

Models can also have descriptions associated with them in the form of comments, like in the following example:

```sql
/* Customer revenue computed and stored daily. */
MODEL (
  name sushi.customer_revenue_by_day,
  owner toby,
  cron '@daily',
);
```

# Model Kinds

## incremental

Incremental load is the default model kind. It specifies that the data is incrementally computed. For example,
many models representing 'facts' or 'logs' should be incremental because new data is continuously added.

## full
Full refresh is used when the entire table needs to be recomputed from scratch every batch.

## snapshot
Snapshot means recomputing the entire history of a table as of the compute date and storing that in a partition. Snapshots are expensive to compute and store but allow you to look at the frozen snapshot at a certain point in time. An example of a snapshot model would be computing and storing lifetime revenue of a user daily.

## view
View models rely on datebase engine views and don't require any direct backfilling. Using a view will create a view in the same location as you may expect a physical table, but no table is computed. Other models that reference view models will incur compute cost because only the query is stored.

## embedded
Embedded models are like views except they don't interact with the data warehouse at all. They are embedded directly in models that reference them as expanded queries. They are an easy way to share logic across models.

# Macros
Macros can be used for passing in paramaterized arguments like dates as well as for making SQL less repetitive. By default, SQLMesh provides several predefined macro variables that can be used your SQL. Macros are used by prefixing with the `@` symbol.

- @start_date
```sql
-- The inclusive start interval of an execution casted to a DATETIME SQL object.
@start_date = CAST("2020-01-01 00:00:00.0" AS DATETIME)
```
- @end_date
```sql
-- The inclusive end interval of an execution casted to a DATETIME SQL object.
@end_date = CAST("2020-01-01 23:59:59.999000" AS DATETIME)
```
- @latest_date
```sql
-- The latest datetime or current run date of an execution. Used when you only care about the latest data.
@latest_date = CAST("2020-01-01 00:00:00.0" AS DATETIME)
```
- @start_ds
```sql
-- The inclusive start date string.
@start_ds = '2020-01-01'
```
- @end_ds
```sql
-- The inclusive end date string.
@end_ds = '2020-01-01'
```
- @latest_ds
```sql
-- The date string of the run date.
@end_ds = '2020-01-01'
```

Read more about `sqlmesh.core.macros`.


# Statements
Models can have additional statements the run before the main query. This can be useful for loading things like [UDFs](https://en.wikipedia.org/wiki/User-defined_function). In general, statements should only be used for preparing the main query. They should not be used for creating or altering tables as this could lead to unpredictable behavior.

```SQL
MODEL (
...
);

ADD JAR s3://special_udf.jar;
CREATE TEMPORARY FUNCTION UDF AS 'my.jar.udf';

SELECT UDF(x)::int AS x
FROM y
```


# Time Column
Models that are loaded incrementally require a time column to partition data. A time column is a column in a model with an optional format string in
the dialect of the model, e.g. '%Y-%m-%d' for DuckDB or 'yyyy-mm-dd' for Snowflake.
```sql
-- Orders are partitioned by the ds column
MODEL (
  name sushi.orders,
  dialect duckdb,
  kind incremental,
  time_column (ds, '%Y-%m-%d')
);

SELECT
  id::INT AS id, -- Primary key
  customer_id::INT AS customer_id, -- Id of customer who made the order
  waiter_id::INT AS waiter_id, -- Id of waiter who took the order
  start_ts::TEXT AS start_ts, -- Start timestamp
  end_ts::TEXT AS end_ts, -- End timestamp
  ds::TEXT AS ds -- Date of order
FROM raw.orders
WHERE
  ds BETWEEN @start_ds AND @end_ds
```
When SQLMesh incrementally inserts data for a partition, it will overwrite any existing data in that partition. For engines that support partitions,
it will use an `INSERT OVERWRITE` query. For other engines that do not, it will first delete the data in the partition before inserting.

## Format String Configuration
The format string tells SQLMesh how your dates are formatted so it can compare start and end dates correctly. You can configure a project wide default
format in your project configuration. A time column format string declared in a model will override the project wide default. If the model uses a
different dialect than the rest of your project, the format string will be automatically transpiled to the model dialect with SQLGlot. SQLMesh will use
`%Y-%m-%d` as the default if no default time column format is configured.
See `sqlmesh.core.config`.

## Advanced Usage
The column used as your model's time column is not limited to being a text or date type. In the following example, the time column, `di`, is an integer.
```sql
-- Orders are partitioned by the di int column
MODEL (
  name sushi.orders,
  dialect duckdb,
  kind incremental,
  time_column (di, '%Y%m%d')
);

SELECT
  id::INT AS id, -- Primary key
  customer_id::INT AS customer_id, -- Id of customer who made the order
  waiter_id::INT AS waiter_id, -- Id of waiter who took the order
  start_ts::TEXT AS start_ts, -- Start timestamp
  end_ts::TEXT AS end_ts, -- End timestamp
  di::INT AS di -- Date of order
FROM raw.orders
WHERE
  di BETWEEN @start_ds AND @end_ds
```
SQLMesh will handle casting the start and end dates to the type of your time column. The format is reflected in the time column format string.
"""
from __future__ import annotations

import ast
import types
import typing as t
from datetime import datetime
from enum import Enum
from itertools import zip_longest
from pathlib import Path

from astor import to_source
from croniter import croniter
from jinja2 import Environment
from pydantic import Field, root_validator, validator
from sqlglot import exp, maybe_parse, parse_one
from sqlglot.optimizer import optimize
from sqlglot.optimizer.annotate_types import annotate_types
from sqlglot.optimizer.qualify_columns import qualify_columns
from sqlglot.optimizer.qualify_tables import qualify_tables
from sqlglot.optimizer.scope import traverse_scope
from sqlglot.optimizer.simplify import simplify
from sqlglot.schema import MappingSchema
from sqlglot.time import format_time

from sqlmesh.core import constants as c
from sqlmesh.core import dialect as d
from sqlmesh.core.audit import Audit
from sqlmesh.core.macros import MacroEvaluator, MacroRegistry, macro
from sqlmesh.core.model_kind import (
    MODEL_KINDS,
    IncrementalByTimeRange,
    IncrementalByUniqueKey,
    ModelKind,
    ModelKindEnum,
    TimeColumn,
)
from sqlmesh.utils import UniqueKeyDict, registry_decorator, unique
from sqlmesh.utils.date import (
    TimeLike,
    date_dict,
    make_inclusive,
    preserve_time_like_kind,
    to_datetime,
)
from sqlmesh.utils.errors import ConfigError, SQLMeshError
from sqlmesh.utils.metaprogramming import (
    Executable,
    build_env,
    prepare_env,
    print_exception,
    serialize_env,
)
from sqlmesh.utils.pydantic import PydanticModel

if t.TYPE_CHECKING:
    from sqlmesh.core.context import ExecutionContext
    from sqlmesh.core.engine_adapter import DF
    from sqlmesh.core.snapshot import Snapshot

META_FIELD_CONVERTER: t.Dict[str, t.Callable] = {
    "name": lambda value: exp.to_table(value),
    "kind": lambda value: exp.to_identifier(value.name.lower()),
    "start": lambda value: exp.Literal.string(value),
    "cron": lambda value: exp.Literal.string(value),
    "batch_size": lambda value: exp.Literal.number(value),
    "partitioned_by_": lambda value: (
        exp.to_identifier(value[0]) if len(value) == 1 else exp.Tuple(expressions=value)
    ),
    "depends_on_": lambda value: exp.Tuple(expressions=value),
    "columns_": lambda value: exp.Schema(
        expressions=[
            exp.ColumnDef(this=exp.to_column(c), kind=t) for c, t in value.items()
        ]
    ),
}

EPOCH_DS = "1970-01-01"

RENDER_OPTIMIZER_RULES = (qualify_tables, qualify_columns, annotate_types)


class IntervalUnit(str, Enum):
    """IntervalUnit is the inferred granularity of an incremental model.

    IntervalUnit can be one of 4 types, DAY, HOUR, MINUTE. The unit is inferred
    based on the cron schedule of a model. The minimum time delta between a sample set of dates
    is used to determine which unit a model's schedule is.
    """

    DAY = "day"
    HOUR = "hour"
    MINUTE = "minute"


class ModelMeta(PydanticModel):
    """Metadata for models which can be defined in SQL."""

    name: str
    kind: MODEL_KINDS
    dialect: str = ""
    cron: str = "@daily"
    owner: t.Optional[str]
    description: t.Optional[str]
    start: t.Optional[TimeLike]
    batch_size: t.Optional[int]
    storage_format: t.Optional[str]
    partitioned_by_: t.Optional[t.List[str]] = Field(
        default=None, alias="partitioned_by"
    )
    depends_on_: t.Optional[t.Set[str]] = Field(default=None, alias="depends_on")
    columns_: t.Optional[t.Dict[str, exp.DataType]] = Field(
        default=None, alias="columns"
    )
    _croniter: t.Optional[croniter] = None

    @validator("partitioned_by_", pre=True)
    def _value_or_tuple_validator(cls, v):
        if isinstance(v, exp.Tuple):
            return [i.name for i in v.expressions]
        if isinstance(v, exp.Expression):
            return [v.name]
        return v

    @validator("kind", pre=True)
    def _enum_validator(cls, v: t.Any) -> MODEL_KINDS:
        if isinstance(v, ModelKind):
            return v

        if isinstance(v, d.ModelKind):
            kind = v.this.lower()
            props = {prop.name: prop.args.get("value") for prop in v.expressions}
            if kind == ModelKindEnum.INCREMENTAL_BY_TIME_RANGE:
                return IncrementalByTimeRange(
                    kind=kind,
                    **props,
                )
            elif kind == ModelKindEnum.INCREMENTAL_BY_UNIQUE_KEY:
                return IncrementalByUniqueKey(
                    kind=kind,
                    **props,
                )
            else:
                return ModelKind(
                    kind=kind,
                    **props,
                )

        if isinstance(v, dict):
            if v.get("kind") == ModelKindEnum.INCREMENTAL_BY_TIME_RANGE:
                return IncrementalByTimeRange(**v)
            if v.get("kind") == ModelKindEnum.INCREMENTAL_BY_UNIQUE_KEY:
                return IncrementalByUniqueKey(**v)
            return ModelKind(**v)

        name = v.name if isinstance(v, exp.Expression) else str(v)
        try:
            return ModelKind(kind=ModelKindEnum(name.lower()))
        except ValueError:
            _raise_config_error(f"Invalid model kind '{name}'")
            raise

    @validator("dialect", "owner", "storage_format", "description", pre=True)
    def _string_validator(cls, v: t.Any) -> t.Optional[str]:
        if isinstance(v, exp.Expression):
            return v.name
        return str(v) if v is not None else None

    @validator("cron", pre=True)
    def _cron_validator(cls, v: t.Any) -> t.Optional[str]:
        cron = cls._string_validator(v)
        if cron:
            try:
                croniter(cron)
            except Exception:
                raise ConfigError(f"Invalid cron expression '{cron}'")
        return cron

    @validator("columns_", pre=True)
    def _columns_validator(cls, v: t.Any) -> t.Optional[t.Dict[str, exp.DataType]]:
        if isinstance(v, exp.Schema):
            return {column.name: column.args["kind"] for column in v.expressions}
        if isinstance(v, dict):
            return {
                k: maybe_parse(data_type, into=exp.DataType)  # type: ignore
                for k, data_type in v.items()
            }
        return v

    @validator("depends_on_", pre=True)
    def _depends_on_validator(cls, v: t.Any) -> t.Optional[t.Set[str]]:
        if isinstance(v, (exp.Array, exp.Tuple)):
            return {
                exp.table_name(table.name if table.is_string else table.sql())
                for table in v.expressions
            }
        if isinstance(v, exp.Expression):
            return {exp.table_name(v.sql())}
        return v

    @validator("start", pre=True)
    def _date_validator(cls, v: t.Any) -> t.Optional[TimeLike]:
        if isinstance(v, exp.Expression):
            v = v.name
        if not to_datetime(v):
            raise ConfigError(f"'{v}' not a valid date time")
        return v

    @validator("batch_size", pre=True)
    def _int_validator(cls, v: t.Any) -> t.Optional[int]:
        if not isinstance(v, exp.Expression):
            batch_size = int(v) if v is not None else None
        else:
            batch_size = int(v.name)
        if batch_size is not None and batch_size <= 0:
            raise ConfigError(
                f"Invalid batch size {batch_size}. The value should be greater than 0"
            )
        return batch_size

    @root_validator
    def _kind_validator(cls, values: t.Dict[str, t.Any]) -> t.Dict[str, t.Any]:
        kind = values.get("kind")
        if kind and not kind.is_materialized:
            if values.get("partitioned_by_"):
                raise ValueError(
                    f"partitioned_by field cannot be set for {kind} models"
                )
        return values

    @property
    def time_column(self) -> t.Optional[TimeColumn]:
        if isinstance(self.kind, IncrementalByTimeRange):
            return self.kind.time_column
        return None

    @property
    def partitioned_by(self) -> t.List[str]:
        time_column = [self.time_column.column] if self.time_column else []
        return unique([*time_column, *(self.partitioned_by_ or [])])

    def interval_unit(self, sample_size: int = 10) -> IntervalUnit:
        """Returns the IntervalUnit of the model

        The interval unit is used to determine the lag applied to start_date and end_date for model rendering and intervals.

        Args:
            sample_size: The number of samples to take from the cron to infer the unit.

        Returns:
            The IntervalUnit enum.
        """
        schedule = croniter(self.cron)
        samples = [schedule.get_next() for _ in range(sample_size)]
        min_interval = min(b - a for a, b in zip(samples, samples[1:]))
        if min_interval >= 86400:
            return IntervalUnit.DAY
        elif min_interval >= 3600:
            return IntervalUnit.HOUR
        return IntervalUnit.MINUTE

    def normalized_cron(self) -> str:
        """Returns the UTC normalized cron based on sampling heuristics.

        SQLMesh supports 3 interval units, daily, hourly, and minutes. If a job is scheduled
        daily at 1PM, the actual intervals are shifted back to midnight UTC.

        Returns:
            The cron string representing either daily, hourly, or minutes.
        """
        unit = self.interval_unit()
        if unit == IntervalUnit.MINUTE:
            return "* * * * *"
        if unit == IntervalUnit.HOUR:
            return "0 * * * *"
        if unit == IntervalUnit.DAY:
            return "0 0 * * *"
        return ""

    def croniter(self, value: TimeLike) -> croniter:
        if self._croniter is None:
            self._croniter = croniter(self.normalized_cron())
        self._croniter.set_current(to_datetime(value))
        return self._croniter

    def cron_next(self, value: TimeLike) -> TimeLike:
        """
        Get the next timestamp given a time-like value and the model's cron.

        Args:
            value: A variety of date formats.

        Returns:
            The timestamp for the next run.
        """
        return preserve_time_like_kind(value, self.croniter(value).get_next())

    def cron_prev(self, value: TimeLike) -> TimeLike:
        """
        Get the previous timestamp given a time-like value and the model's cron.

        Args:
            value: A variety of date formats.

        Returns:
            The timestamp for the previous run.
        """
        return preserve_time_like_kind(value, self.croniter(value).get_prev())

    def cron_floor(self, value: TimeLike) -> TimeLike:
        """
        Get the floor timestamp given a time-like value and the model's cron.

        Args:
            value: A variety of date formats.

        Returns:
            The timestamp floor.
        """
        return preserve_time_like_kind(
            value, self.croniter(self.cron_next(value)).get_prev()
        )


class Model(ModelMeta, frozen=True):
    """Model is the core abstraction for user defined SQL logic.

    A model represents a single SQL query and metadata surrounding it. Models can be
    run on arbitrary cadences and can be incremental or full refreshes. Models can also
    be materialized into physical tables or shared across other models as temporary views.

    Example:
        MODEL (
            name           sushi.order_items,
            owner          jen,
            cron           '@daily',
            batch_size     30,
            start          '2020-01-01',
            partitioned_by ds
        );

        @DEF(var, 'my_var');

        SELECT
          1 AS column_a # my first column,
          @var AS my_column #my second column,
        ;
    Args:
        model: The name of the model, which is of the form [catalog].[db].table.
            The catalog and db are optional.
        dialect: The SQL dialect that the model's query is written in. By default,
            this is assumed to be the dialect of the context.
        owner: The owner of the model.
        cron: A cron string specifying how often the model should be refresh, leveraging the
            [croniter](https://github.com/kiorky/croniter) library.
        start: The earliest date that the model will be backfilled for. If this is None,
            then the date is inferred by taking the most recent start date's of its ancestors.
            The start date can be a static datetime or a realtive datetime like "1 year ago"
        batch_size: The maximum number of intervals that can be run per backfill job. If this is None,
            then backfilling this model will do all of history in one job. If this is set, a model's backfill
            will be chunked such that each individual job will only contain jobs with max `batch_size` intervals.
        storage_format: The storage format used to store the physical table, only applicable in certain engines.
            (eg. 'parquet')
        partitioned_by: The partition columns, only applicable in certain engines. (eg. (ds, hour))
        query: The main query representing the model.
        expressions: All of the expressions between the model definition and final query, used for setting certain variables or environments.
        python_env: Dictionary containing all global variables needed to render the model's macros.
    """

    query: t.Union[exp.Subqueryable, d.MacroVar, d.Jinja]
    expressions_: t.Optional[t.List[exp.Expression]] = Field(
        default=None, alias="expressions"
    )
    python_env_: t.Optional[t.Dict[str, Executable]] = Field(
        default=None, alias="python_env"
    )
    _path: Path = Path()
    _depends_on: t.Optional[t.Set[str]] = None
    _columns: t.Optional[t.Dict[str, exp.DataType]] = None
    _column_descriptions: t.Optional[t.Dict[str, str]] = None
    _query_cache: t.Dict[
        t.Tuple[str, datetime, datetime, datetime], exp.Subqueryable
    ] = {}
    _schema: t.Optional[MappingSchema] = None
    audits: t.Dict[str, Audit] = UniqueKeyDict("audits")

    @validator("query", "expressions_", pre=True)
    def _parse_expression(
        cls,
        v: t.Union[
            t.List[str], t.List[exp.Expression], str, exp.Expression, t.Callable
        ],
    ) -> t.List[exp.Expression] | exp.Expression | t.Callable:
        """Helper method to deserialize SQLGlot expressions in Pydantic Models."""
        if callable(v):
            return v

        if isinstance(v, list):
            return [e for e in (maybe_parse(i) for i in v) if e]
        expression = maybe_parse(v)
        if not expression:
            raise ConfigError(f"Could not parse {v}")
        return expression

    @classmethod
    def load(
        cls,
        expressions: t.List[exp.Expression],
        *,
        path: Path = Path(),
        module_path: Path = Path(),
        time_column_format: str = c.DEFAULT_TIME_COLUMN_FORMAT,
        macros: t.Optional[MacroRegistry] = None,
        python_env: t.Optional[t.Dict[str, Executable]] = None,
        dialect: t.Optional[str] = None,
        **kwargs,
    ) -> Model:
        """Load a model from a parsed SQLMesh model file.

        Args:
            expressions: Model, *Statements, Query.
            module_path: The python module path to serialize macros for.
            path: An optional path to the file.
            dialect: The default dialect if no model dialect is configured.
            time_column_format: The default time column format to use if no model time column is configured.
                The format must adhere to Python's strftime codes.
        """
        if len(expressions) < 2:
            _raise_config_error(
                "Incomplete model definition, missing MODEL and QUERY", path
            )

        meta, *statements, query = expressions

        if not isinstance(meta, d.Model):
            _raise_config_error(
                "MODEL statement is required as the first statement in the definition",
                path,
            )

        provided_meta_fields = {p.name for p in meta.expressions}

        missing_required_fields = ModelMeta.missing_required_fields(
            provided_meta_fields
        )
        if missing_required_fields:
            _raise_config_error(
                f"Missing required fields {missing_required_fields} in the model definition",
                path,
            )

        extra_fields = ModelMeta.extra_fields(provided_meta_fields)
        if extra_fields:
            _raise_config_error(
                f"Invalid extra fields {extra_fields} in the model definition", path
            )

        if not isinstance(query, (exp.Subqueryable, d.MacroVar, d.Jinja)):
            _raise_config_error(
                "A query is required and must be a SELECT or UNION statement.",
                path,
            )

        if not query.expressions and not isinstance(query, d.MacroVar):
            _raise_config_error("Query missing select statements", path)

        if not python_env:
            python_env = _python_env(query, module_path, macros or macro.get_registry())

        try:
            model_meta = ModelMeta(
                **{
                    prop.name.lower(): prop.args.get("value")
                    for prop in meta.expressions
                },
                description="\n".join(comment.strip() for comment in meta.comments)
                if meta.comments
                else None,
                **kwargs,
            )

            # find dependencies for python models by parsing code if they are not explicitly defined
            if model_meta.depends_on_ is None and isinstance(query, d.MacroVar):
                depends_on = _parse_depends_on(query, python_env)
            else:
                depends_on = None

            model = cls(
                query=query,
                expressions=statements,
                python_env=python_env,
                **{
                    "dialect": dialect or "",
                    "depends_on": depends_on,
                    **model_meta.dict(),
                },
            )
        except Exception as ex:
            _raise_config_error(str(ex), location=path)

        model._path = path
        model.set_time_format(time_column_format)
        model.validate_definition()

        return model

    @property
    def depends_on(self) -> t.Set[str]:
        """All of the upstream dependencies referenced in the model's query, excluding self references.

        Returns:
            A list of all the upstream table names.
        """
        if self.depends_on_ is not None:
            return self.depends_on_

        if self._depends_on is None:
            self._depends_on = find_tables(self._render_query()) - {self.name}
        return self._depends_on

    @property
    def column_descriptions(self) -> t.Dict[str, str]:
        """A dictionary of column names to annotation comments."""
        if self._column_descriptions is None:
            self._column_descriptions = {
                select.alias: "\n".join(comment.strip() for comment in select.comments)
                for select in self._render_query().expressions
                if select.comments
            }
        return self._column_descriptions

    @property
    def columns(self) -> t.Dict[str, exp.DataType]:
        """Returns the mapping of column names to types of this model."""
        if self.columns_:
            return self.columns_

        if self._columns is None:
            query = annotate_types(self._render_query())
            self._columns = {
                expression.alias_or_name: expression.type
                for expression in query.expressions
            }

        return self._columns

    @property
    def contains_star_query(self) -> bool:
        """Returns True if the model's query contains a star projection."""
        return any(
            isinstance(expression, exp.Star)
            for expression in self.render_query().expressions
        )

    @property
    def annotated(self) -> bool:
        """Checks if all column projection types of this model are known."""
        return all(
            column_type.this != exp.DataType.Type.UNKNOWN
            for column_type in self.columns.values()
        )

    def render(self) -> t.List[exp.Expression]:
        """Returns the original list of sql expressions comprising the model."""
        expressions = []
        comment = None
        for field in ModelMeta.__fields__.values():
            field_value = getattr(self, field.name)

            if field_value is not None:
                if field.name == "description":
                    comment = field_value
                elif field.name == "time_column":
                    expression = field_value.expression

                    # time_column.format is stored as python format in memory
                    # convert it back to the model dialect
                    if field_value.format:
                        expression.expressions.pop()
                        expression.append(
                            "expressions",
                            exp.Literal.string(
                                format_time(
                                    field_value.format,
                                    d.Dialect.get_or_raise(
                                        self.dialect
                                    ).inverse_time_mapping,
                                )
                            ),
                        )
                    expressions.append(
                        exp.Property(this="time_column", value=expression)
                    )
                else:
                    expressions.append(
                        exp.Property(
                            this=field.alias or field.name,
                            value=META_FIELD_CONVERTER.get(
                                field.name, exp.to_identifier
                            )(field_value),
                        )
                    )

        model = d.Model(expressions=expressions)
        model.comments = [comment] if comment else None
        return [model, *self.expressions, self.query]

    def update_schema(self, schema: MappingSchema) -> None:
        self._schema = schema

        if self.contains_star_query:
            # We need to re-render in order to expand the star projection
            self._query_cache.clear()
            self._render_query()

    def _render_query(
        self,
        query_: t.Optional[exp.Expression] = None,
        *,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        latest: t.Optional[TimeLike] = None,
        add_incremental_filter: bool = False,
        snapshots: t.Optional[t.Dict[str, Snapshot]] = None,
        mapping: t.Optional[t.Dict[str, str]] = None,
        expand: t.Iterable[str] = tuple(),
        audit_name: t.Optional[str] = None,
        dialect: t.Optional[str] = None,
        **kwargs,
    ) -> exp.Subqueryable:
        """Renders a query, expanding macros with provided kwargs, and optionally expanding referenced models.

        Args:
            query_: The query to render, defaults to self.query.
            start: The start datetime to render. Defaults to epoch start.
            end: The end datetime to render. Defaults to epoch start.
            latest: The latest datetime to use for non-incremental queries. Defaults to epoch start.
            add_incremental_filter: Add an incremental filter to the query if the model is incremental.
            snapshots: All upstream snapshots to use for expansion and mapping of physical locations.
                If passing snapshots is undesirable, mapping can be used instead to manually map tables.
            mapping: Mapping to replace table names, if not set, the mapping will be created from snapshots.
            expand: Expand referenced models as subqueries. This is used to bypass backfills when running queries
                that depend on materialized tables.  Model definitions are inlined and can thus be run end to
                end on the fly.
            audit_name: The name of audit if the query to render is for an audit.
            kwargs: Additional kwargs to pass to the renderer.

        Returns:
            The rendered expression.
        """
        dates = (
            *make_inclusive(start or EPOCH_DS, end or EPOCH_DS),
            to_datetime(latest or EPOCH_DS),
        )
        key = (audit_name or "", *dates)

        snapshots = snapshots or {}
        mapping = mapping or {
            name: snapshot.table_name
            for name, snapshot in snapshots.items()
            if snapshot.version and not snapshot.is_embedded_kind
        }
        # if a snapshot is provided but not mapped, we need to expand it or the query
        # won't be valid
        expand = set(expand) | {name for name in snapshots if name not in mapping}

        if key not in self._query_cache:
            if query_ or self.is_sql:
                query_ = query_ or self.query
                render_kwargs = {
                    **date_dict(*dates, only_latest=self.kind.only_latest),
                    **kwargs,
                }

                if isinstance(query_, d.Jinja):
                    env = prepare_env(self.python_env)

                    query_ = parse_one(
                        Environment()
                        .from_string(query_.name)
                        .render(**env, **render_kwargs),
                        read=self.dialect,
                    )

                macro_evaluator = MacroEvaluator(
                    dialect or self.dialect, self.python_env
                )
                macro_evaluator.locals.update(render_kwargs)

                for definition in self.macro_definitions:
                    macro_evaluator.evaluate(definition)

                self._query_cache[key] = macro_evaluator.transform(query_)  # type: ignore
            else:
                self._query_cache[key] = exp.select(
                    *(
                        exp.alias_(f"NULL::{column_type}", name)
                        for name, column_type in self.columns.items()
                    )
                )

            if self._schema:
                # This takes care of expanding star projections
                self._query_cache[key] = optimize(
                    self._query_cache[key],
                    schema=self._schema,
                    rules=RENDER_OPTIMIZER_RULES,
                )

                self._columns = {
                    expression.alias_or_name: expression.type
                    for expression in self._query_cache[key].expressions
                }

                self.validate_definition()

        query = self._query_cache[key]

        if expand:

            def _expand(node: exp.Expression) -> exp.Expression:
                if isinstance(node, exp.Table) and snapshots:
                    name = exp.table_name(node)
                    model = snapshots[name].model if name in snapshots else None
                    if name in expand and model:
                        return model._render_query(
                            start=start,
                            end=end,
                            latest=latest,
                            snapshots=snapshots,
                            mapping=mapping,
                            expand=expand,
                            **kwargs,
                        ).subquery(
                            alias=node.alias or model.view_name,
                            copy=False,
                        )
                return node

            query = query.transform(_expand)

        # Ensure there is no data leakage in incremental mode by filtering out all
        # events that have data outside the time window of interest.
        if add_incremental_filter and isinstance(self.kind, IncrementalByTimeRange):
            # expansion copies the query for us. if it doesn't occur, make sure to copy.
            if not expand:
                query = query.copy()
            for node, _, _ in query.walk(prune=lambda n, *_: isinstance(n, exp.Select)):
                if isinstance(node, exp.Select):
                    self._filter_time_column(node, *dates[0:2])

        if mapping:
            return exp.replace_tables(query, mapping)

        if not isinstance(query, exp.Subqueryable):
            raise SQLMeshError(f"Query needs to be a SELECT or a UNION {query}")

        return query

    def ctas_query(self, snapshots: t.Dict[str, Snapshot]) -> exp.Subqueryable:
        """Return a dummy query to do a CTAS.

        If a model's column types are unknown, the only way to create the table is to
        run the fully expanded query. This can be expensive so we add a WHERE FALSE to all
        SELECTS and hopefully the optimizer is smart enough to not do anything.

        Args:
            snapshots: All upstream snapshots of this model so queries can be expanded.
        Return:
            The mocked out ctas query.
        """
        query = self._render_query(snapshots=snapshots, expand=snapshots)
        # the query is expanded so it's been copied, it's safe to mutate.
        for select in query.find_all(exp.Select):
            select.where("FALSE", copy=False)

        return query

    def render_query(
        self,
        *,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        latest: t.Optional[TimeLike] = None,
        snapshots: t.Optional[t.Dict[str, Snapshot]] = None,
        mapping: t.Optional[t.Dict[str, str]] = None,
        expand: t.Iterable[str] = tuple(),
        **kwargs,
    ) -> exp.Subqueryable:
        """Renders a model's query, expanding macros with provided kwargs, and optionally expanding referenced models.

        Args:
            start: The start datetime to render. Defaults to epoch start.
            end: The end datetime to render. Defaults to epoch start.
            latest: The latest datetime to use for non-incremental queries. Defaults to epoch start.
            snapshots: All snapshots to use for expansion and mapping of physical locations.
                If passing snapshots is undesirable, mapping can be used instead to manually map tables.
            mapping: Mapping to replace table names, if not set, the mapping wil be created from snapshots.
            expand: Expand referenced models as subqueries. This is used to bypass backfills when running queries
                that depend on materialized tables.  Model definitions are inlined and can thus be run end to
                end on the fly.
            audit_name: The name of audit if the query to render is for an audit.
            kwargs: Additional kwargs to pass to the renderer.

        Returns:
            The rendered expression.
        """
        return self._render_query(
            start=start,
            end=end,
            latest=latest,
            add_incremental_filter=True,
            snapshots=snapshots,
            mapping=mapping,
            expand=expand,
            **kwargs,
        )

    def exec_python(
        self,
        context: ExecutionContext,
        *,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        latest: t.Optional[TimeLike] = None,
        **kwargs,
    ) -> t.Generator[DF, None, None]:
        """Executes this model's python script.

        A python model is expected to return a dataframe or a generator that yields dataframes.

        Args:
            context: The execution context used for fetching data.
            start: The start date/time of the run.
            end: The end date/time of the run.
            latest: The latest date/time to use for the run.

        Returns:
            The return type must be a supported dataframe.
        """
        if self.is_sql:
            raise SQLMeshError(
                f"Model '{self.name}' is a SQL model and cannot be executed as a Python script."
            )

        from sqlmesh.core.engine_adapter import pyspark

        env = prepare_env(self.python_env)
        start, end = make_inclusive(start or EPOCH_DS, end or EPOCH_DS)
        latest = to_datetime(latest or EPOCH_DS)
        try:
            df_or_iter = env[self.query.name](
                context=context, start=start, end=end, latest=latest, **kwargs
            )

            if not isinstance(df_or_iter, types.GeneratorType):
                df_or_iter = [df_or_iter]

            for df in df_or_iter:
                if self.kind.is_incremental_by_time_range:
                    assert self.time_column

                    if pyspark and isinstance(df, pyspark.sql.DataFrame):
                        df = df.where(
                            pyspark.sql.functions.col(self.time_column.column).between(
                                pyspark.sql.functions.lit(
                                    self.convert_to_time_column(start).sql("spark")
                                ),
                                pyspark.sql.functions.lit(
                                    self.convert_to_time_column(end).sql("spark")
                                ),
                            )
                        )
                    else:
                        if self.time_column.format:
                            start_format: TimeLike = start.strftime(
                                self.time_column.format
                            )
                            end_format: TimeLike = end.strftime(self.time_column.format)
                        else:
                            start_format = start
                            end_format = end

                        df_time = df[self.time_column.column]
                        df = df[(df_time >= start_format) & (df_time <= end_format)]
                yield df
        except Exception as e:
            print_exception(e, self.python_env)
            raise SQLMeshError(
                f"Error executing Python model '{self.name}::{self.query.name}'"
            )

    def render_audit_queries(
        self,
        *,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        latest: t.Optional[TimeLike] = None,
        snapshots: t.Optional[t.Dict[str, Snapshot]] = None,
        mapping: t.Optional[t.Dict[str, str]] = None,
        **kwargs,
    ) -> t.Generator[t.Tuple[Audit, exp.Subqueryable], None, None]:
        """Renders this model's audit queries, expanding macros with provided kwargs.

        Args:
            start: The start datetime to render. Defaults to epoch start.
            end: The end datetime to render. Defaults to epoch start.
            latest: The latest datetime to use for non-incremental queries. Defaults to epoch start.
            snapshots: All snapshots to use for expansion and mapping of physical locations.
                If passing snapshots is undesirable, mapping can be used instead to manually map tables.
            mapping: Mapping to replace table names, if not set, the mapping wil be created from snapshots.
            kwargs: Additional kwargs to pass to the renderer.

        Returns:
            The rendered expression.
        """
        for audit in self.audits.values():
            query = self._render_query(
                audit.query,
                start=start,
                end=end,
                latest=latest,
                snapshots=snapshots,
                mapping=mapping,
                audit_name=audit.name,
                dialect=audit.dialect,
                **kwargs,
            )
            yield audit, query

    def text_diff(self, other: Model) -> str:
        """Produce a text diff against another model.

        Args:
            other: The model to diff against.

        Returns:
            A unified text diff showing additions and deletions.
        """
        meta_a, *statements_a, query_a = self.render()
        meta_b, *statements_b, query_b = other.render()
        return "\n".join(
            (
                d.text_diff(meta_a, meta_b, self.dialect),
                *(
                    d.text_diff(sa, sb, self.dialect)
                    for sa, sb in zip_longest(statements_a, statements_b)
                ),
                d.text_diff(query_a, query_b, self.dialect),
            )
        ).strip()

    def set_time_format(
        self, default_time_format: str = c.DEFAULT_TIME_COLUMN_FORMAT
    ) -> None:
        """Sets the default time format for a model.

        Args:
            default_time_format: A python time format used as the default format when none is provided.
        """
        if not self.time_column:
            return

        if self.time_column.format:
            # Transpile the time column format into the generic dialect
            self.time_column.format = format_time(
                self.time_column.format,
                d.Dialect.get_or_raise(self.dialect).time_mapping,
            )
        else:
            self.time_column.format = default_time_format

    def convert_to_time_column(self, time: TimeLike) -> exp.Expression:
        """Convert a TimeLike object to the same time format and type as the model's time column."""
        if self.time_column:
            if self.time_column.format:
                time = to_datetime(time).strftime(self.time_column.format)

            time_column_type = self.columns[self.time_column.column]
            if time_column_type.this in exp.DataType.TEXT_TYPES:
                return exp.Literal.string(time)
            elif time_column_type.this in exp.DataType.NUMERIC_TYPES:
                return exp.Literal.number(time)
            elif time_column_type.this in exp.DataType.TEMPORAL_TYPES:
                return exp.Cast(this=exp.Literal.string(time), to=time_column_type)
        return exp.convert(time)

    @property
    def macro_definitions(self) -> t.List[d.MacroDef]:
        """All macro definitions from the list of expressions."""
        return [s for s in (self.expressions or []) if isinstance(s, d.MacroDef)]

    @property
    def sql_statements(self) -> t.List[exp.Expression]:
        """All sql statements from the list of expressions."""
        return [s for s in (self.expressions or []) if not isinstance(s, d.MacroDef)]

    @property
    def view_name(self) -> str:
        return parse_model_name(self.name)[2]

    @property
    def expressions(self) -> t.List[exp.Expression]:
        return self.expressions_ or []

    @property
    def python_env(self) -> t.Dict[str, Executable]:
        return self.python_env_ or {}

    def validate_definition(self) -> None:
        """Validates the model's definition.

        Model's are not allowed to have duplicate column names, non-explicitly casted columns,
        or non infererrable column names.

        Raises:
            ConfigError
        """
        name_counts: t.Dict[str, int] = {}
        query = self._render_query()

        if not isinstance(query, exp.Subqueryable):
            _raise_config_error(
                "Missing SELECT query in the model definition", self._path
            )

        if not query.expressions:
            _raise_config_error("Query missing select statements", self._path)

        for expression in query.expressions:
            alias = expression.alias_or_name
            name_counts[alias] = name_counts.get(alias, 0) + 1

            if not alias:
                _raise_config_error(
                    f"Outer projection `{expression}` must have inferrable names or explicit aliases.",
                    self._path,
                )

        for name, count in name_counts.items():
            if count > 1:
                _raise_config_error(
                    f"Found duplicate outer select name `{name}`", self._path
                )

        if self.partitioned_by:
            unique_partition_keys = {k.strip().lower() for k in self.partitioned_by}
            if len(self.partitioned_by) != len(unique_partition_keys):
                _raise_config_error(
                    "All partition keys must be unique in the model definition",
                    self._path,
                )

            projections = {p.lower() for p in query.named_selects}
            missing_keys = unique_partition_keys - projections
            if missing_keys:
                missing_keys_str = ", ".join(f"'{k}'" for k in sorted(missing_keys))
                _raise_config_error(
                    f"Partition keys [{missing_keys_str}] are missing in the query in the model definition",
                    self._path,
                )

        if isinstance(self.kind, IncrementalByTimeRange) and not self.time_column:
            _raise_config_error(
                "Incremental models must have a time_column field.",
                self._path,
            )

    def _validate_view(self, query: exp.Expression) -> None:
        pass

    def _filter_time_column(
        self, query: exp.Select, start: TimeLike, end: TimeLike
    ) -> None:
        """Filters a query on the time column to ensure no data leakage when running in incremental mode."""
        if not self.time_column:
            return

        low = self.convert_to_time_column(start)
        high = self.convert_to_time_column(end)

        time_column_projection = next(
            select
            for select in query.selects
            if select.alias_or_name == self.time_column.column
        )

        if isinstance(time_column_projection, exp.Alias):
            time_column_projection = time_column_projection.this

        between = exp.Between(this=time_column_projection.copy(), low=low, high=high)

        if not query.args.get("group"):
            query.where(between, copy=False)
        else:
            query.having(between, copy=False)

        simplify(query)

    def __repr__(self):
        return f"Model<name: {self.name}, query: {str(self.query)[0:30]}>"

    @property
    def is_sql(self) -> bool:
        return not isinstance(self.query, d.MacroVar)


class model(registry_decorator):
    """Specifies a function is a python based model."""

    registry_name = "python_models"

    def __init__(self, definition: str = "", **kwargs):
        self.kwargs = kwargs
        self.expressions = d.parse_model(
            definition, default_dialect=self.kwargs.get("dialect")
        )

        if not self.expressions:
            self.expressions.insert(
                0,
                d.Model(
                    expressions=[
                        exp.Property(this="name", value=self.kwargs.pop("name", None))
                    ]
                ),
            )

        self.name = ""
        columns = "columns" in self.kwargs

        for prop in self.expressions[0].expressions:
            prop_name = prop.name.lower()
            if prop_name == "name":
                self.name = prop.text("value")
            elif prop_name == "columns":
                columns = True

        if not self.name:
            raise ConfigError(f"Python model must have a name.")
        if not columns:
            raise ConfigError(f"Python model must define column schema.")

    def model(
        self,
        *,
        module_path: Path,
        path: Path,
        time_column_format: str = c.DEFAULT_TIME_COLUMN_FORMAT,
    ) -> Model:
        """Get the model registered by this function."""
        env: t.Dict[str, t.Any] = {}
        name = self.func.__name__

        build_env(
            self.func,
            env=env,
            name=name,
            path=module_path,
        )

        expressions = [
            *self.expressions,
            d.MacroVar(this=name),
        ]

        return Model.load(
            expressions,
            path=path,
            time_column_format=time_column_format,
            python_env=serialize_env(env, path=module_path),
            **self.kwargs,
        )


def parse_model_name(name: str) -> t.Tuple[t.Optional[str], t.Optional[str], str]:
    """Convert a model name into table parts.

    Args:
        name: model name.

    Returns:
        A tuple consisting of catalog, schema, table name.
    """
    splits = name.split(".")
    if len(splits) == 3:
        return (splits[0], splits[1], splits[2])
    if len(splits) == 2:
        return (None, splits[0], splits[1])
    return (None, None, name)


def find_tables(query: exp.Expression) -> t.Set[str]:
    """Find all tables referenced in a query.

    Args:
        query: The expression to find tables for.

    Returns:
        A Set of all the table names.
    """
    return {
        exp.table_name(table)
        for scope in traverse_scope(query)
        for table in scope.tables
        if isinstance(table.this, exp.Identifier)
        and exp.table_name(table) not in scope.cte_sources
    }


def _python_env(
    query: exp.Expression, module_path: Path, macros: MacroRegistry
) -> t.Dict[str, Executable]:
    python_env: t.Dict[str, Executable] = {}

    used_macros = {}

    if isinstance(query, d.Jinja):
        for var in query.expressions:
            if var in macros:
                used_macros[var] = macros[var]
    else:
        for macro_func in query.find_all(d.MacroFunc):
            if macro_func.__class__ is d.MacroFunc:
                name = macro_func.this.name.lower()
                used_macros[name] = macros[name]

    for name, macro in used_macros.items():
        if macro.serialize:
            build_env(
                macro.func,
                env=python_env,
                name=name,
                path=module_path,
            )

    return serialize_env(python_env, path=module_path)


def _parse_depends_on(
    model_func: d.MacroVar, python_env: t.Dict[str, Executable]
) -> t.Set[str]:
    """Parses the source of a model function and finds upstream dependencies based on calls to context."""
    env = prepare_env(python_env)
    depends_on = set()
    executable = python_env[model_func.name]

    for node in ast.walk(ast.parse(executable.payload)):
        if not isinstance(node, ast.Call):
            continue

        func = node.func

        if (
            isinstance(func, ast.Attribute)
            and isinstance(func.value, ast.Name)
            and func.value.id == "context"
            and func.attr == "table"
        ):
            if node.args:
                table: t.Optional[ast.expr] = node.args[0]
            else:
                table = next(
                    (
                        keyword.value
                        for keyword in node.keywords
                        if keyword.arg == "model_name"
                    ),
                    None,
                )

            try:
                expression = to_source(table)
                depends_on.add(eval(expression, env))
            except Exception:
                raise ConfigError(
                    f"Error resolving dependencies for '{executable.path}'. References to context must be resolvable at parse time.\n\n{expression}"
                )

    return depends_on


def _raise_config_error(msg: str, location: t.Optional[str | Path] = None) -> None:
    if location:
        raise ConfigError(f"{msg} at '{location}'")
    raise ConfigError(msg)
