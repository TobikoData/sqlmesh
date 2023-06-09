from __future__ import annotations

import pathlib
import typing as t
from pathlib import Path

from pydantic import Field, validator
from sqlglot import exp

from sqlmesh.core import constants as c
from sqlmesh.core import dialect as d
from sqlmesh.core.model.definition import Model, _Model, expression_validator
from sqlmesh.core.renderer import QueryRenderer
from sqlmesh.utils.date import TimeLike
from sqlmesh.utils.errors import AuditConfigError, raise_config_error
from sqlmesh.utils.jinja import JinjaMacroRegistry
from sqlmesh.utils.pydantic import PydanticModel

if t.TYPE_CHECKING:
    from sqlmesh.core.snapshot import Snapshot


class AuditMeta(PydanticModel):
    """Metadata for audits which can be defined in SQL."""

    name: str
    """The name of this audit."""
    dialect: str = ""
    """The dialect of the audit query."""
    skip: bool = False
    """Setting this to `true` will cause this audit to be skipped. Defaults to `false`."""
    blocking: bool = True
    """Setting this to `true` will cause the pipeline execution to stop if this audit fails. Defaults to `true`."""

    @validator("name", "dialect", pre=True)
    def _string_validator(cls, v: t.Any) -> t.Optional[str]:
        if isinstance(v, exp.Expression):
            return v.name.lower()
        return str(v).lower() if v is not None else None

    @validator("skip", "blocking", pre=True)
    def _bool_validator(cls, v: t.Any) -> bool:
        if isinstance(v, exp.Boolean):
            return v.this
        if isinstance(v, exp.Expression):
            return v.name.lower() not in ("false", "no")
        return bool(v)


class Audit(AuditMeta, frozen=True):
    """Audit is an assertion made about your SQLMesh models.

    An audit is a SQL query that returns bad records.
    """

    query: t.Union[exp.Subqueryable, d.JinjaQuery]
    expressions_: t.Optional[t.List[exp.Expression]] = Field(default=None, alias="expressions")
    jinja_macros: JinjaMacroRegistry = JinjaMacroRegistry()

    _path: t.Optional[pathlib.Path] = None

    _query_validator = expression_validator

    @classmethod
    def load(
        cls,
        expressions: t.List[exp.Expression],
        *,
        path: pathlib.Path,
        dialect: t.Optional[str] = None,
    ) -> Audit:
        """Load an audit from a parsed SQLMesh audit file.

        Args:
            expressions: Audit, *Statements, Query
            path: An optional path of the file.
            dialect: The default dialect if no audit dialect is configured.
        """
        if len(expressions) < 2:
            _raise_config_error("Incomplete audit definition, missing AUDIT or QUERY", path)

        meta, *statements, query = expressions

        if not isinstance(meta, d.Audit):
            _raise_config_error(
                "AUDIT statement is required as the first statement in the definition",
                path,
            )
            raise

        provided_meta_fields = {p.name for p in meta.expressions}

        missing_required_fields = AuditMeta.missing_required_fields(provided_meta_fields)
        if missing_required_fields:
            _raise_config_error(
                f"Missing required fields {missing_required_fields} in the audit definition",
                path,
            )

        extra_fields = AuditMeta.extra_fields(provided_meta_fields)
        if extra_fields:
            _raise_config_error(
                f"Invalid extra fields {extra_fields} in the audit definition", path
            )

        if not isinstance(query, exp.Subqueryable):
            _raise_config_error("Missing SELECT query in the audit definition", path)
            raise

        try:
            audit = cls(
                query=query,
                expressions=statements,
                **{
                    "dialect": dialect or "",
                    **AuditMeta(
                        **{prop.name: prop.args.get("value") for prop in meta.expressions if prop},
                    ).dict(),
                },
            )
        except Exception as ex:
            _raise_config_error(str(ex), path)

        audit._path = path
        return audit

    @classmethod
    def load_multiple(
        cls,
        expressions: t.List[exp.Expression],
        *,
        path: pathlib.Path,
        dialect: t.Optional[str] = None,
    ) -> t.Generator[Audit, None, None]:
        audit_block: t.List[exp.Expression] = []
        for expression in expressions:
            if isinstance(expression, d.Audit):
                if audit_block:
                    yield Audit.load(
                        expressions=audit_block,
                        path=path,
                        dialect=dialect,
                    )
                    audit_block.clear()
            audit_block.append(expression)
        yield Audit.load(
            expressions=audit_block,
            path=path,
            dialect=dialect,
        )

    def render_query(
        self,
        snapshot_or_model: t.Union[Snapshot, Model],
        *,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        latest: t.Optional[TimeLike] = None,
        snapshots: t.Optional[t.Dict[str, Snapshot]] = None,
        is_dev: bool = False,
        **kwargs: t.Any,
    ) -> exp.Subqueryable:
        """Renders the audit's query.

        Args:
            snapshot_or_model: The snapshot or model which is being audited.
            start: The start datetime to render. Defaults to epoch start.
            end: The end datetime to render. Defaults to epoch start.
            latest: The latest datetime to use for non-incremental queries. Defaults to epoch start.
            snapshots: All snapshots (by model name) to use for mapping of physical locations.
            audit_name: The name of audit if the query to render is for an audit.
            is_dev: Indicates whether the rendering happens in the development mode and temporary
                tables / table clones should be used where applicable.
            kwargs: Additional kwargs to pass to the renderer.

        Returns:
            The rendered expression.
        """

        if isinstance(snapshot_or_model, _Model):
            model = snapshot_or_model
            this_model = snapshot_or_model.name
        else:
            model = snapshot_or_model.model
            this_model = snapshot_or_model.table_name(is_dev=is_dev, for_read=True)

        query_renderer = self._create_query_renderer(model)

        query = (
            exp.select("*")
            .from_(this_model)
            .where(
                query_renderer.time_column_filter(start or c.EPOCH, end or c.EPOCH)
                if query_renderer._time_column
                else None
            )
            .subquery()
        )

        return query_renderer.render(
            start=start,
            end=end,
            latest=latest,
            snapshots=snapshots,
            is_dev=is_dev,
            this_model=query,
            **kwargs,
        )

    @property
    def expressions(self) -> t.List[exp.Expression]:
        return self.expressions_ or []

    @property
    def macro_definitions(self) -> t.List[d.MacroDef]:
        """All macro definitions from the list of expressions."""
        return [s for s in self.expressions if isinstance(s, d.MacroDef)]

    def _create_query_renderer(self, model: Model) -> QueryRenderer:
        return QueryRenderer(
            self.query,
            self.dialect,
            self.macro_definitions,
            path=self._path or Path(),
            jinja_macro_registry=self.jinja_macros,
            python_env=model.python_env,
            time_column=model.time_column,
            time_converter=model.convert_to_time_column,
            only_latest=model.kind.only_latest,
        )


class AuditResult(PydanticModel):
    audit: Audit
    """The audit this result is for."""
    count: t.Optional[int]
    """The number of records returned by the audit query. This could be None if the audit was skipped."""
    query: t.Optional[exp.Expression]
    """The rendered query used by the audit. This could be None if the audit was skipped."""
    skipped: bool = False
    """Whether this audit was skipped or not."""


def _raise_config_error(msg: str, path: pathlib.Path) -> None:
    raise_config_error(msg, location=path, error_type=AuditConfigError)
