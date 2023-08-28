from __future__ import annotations

import pathlib
import sys
import typing as t
from pathlib import Path

from pydantic import Field
from sqlglot import exp
from sqlglot.optimizer.qualify_columns import quote_identifiers

from sqlmesh.core import constants as c
from sqlmesh.core import dialect as d
from sqlmesh.core.model.common import bool_validator, expression_validator
from sqlmesh.core.model.definition import _Model
from sqlmesh.core.node import _Node
from sqlmesh.core.renderer import QueryRenderer
from sqlmesh.utils.date import TimeLike
from sqlmesh.utils.errors import AuditConfigError, SQLMeshError, raise_config_error
from sqlmesh.utils.hashing import hash_data
from sqlmesh.utils.jinja import JinjaMacroRegistry
from sqlmesh.utils.metaprogramming import Executable
from sqlmesh.utils.pydantic import PydanticModel, field_validator

if t.TYPE_CHECKING:
    from sqlmesh.core.snapshot import Snapshot

if sys.version_info >= (3, 9):
    from typing import Literal
else:
    from typing_extensions import Literal


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
    defaults: t.Dict[str, exp.Expression] = {}
    """Default values for the audit query."""

    _bool_validator = bool_validator

    @field_validator("name", "dialect", mode="before")
    @classmethod
    def _string_validator(cls, v: t.Any) -> t.Optional[str]:
        if isinstance(v, exp.Expression):
            return v.name.lower()
        return str(v).lower() if v is not None else None

    @field_validator("defaults", mode="before")
    @classmethod
    def _map_validator(cls, v: t.Any) -> t.Dict[str, t.Any]:
        if isinstance(v, (exp.Tuple, exp.Array)):
            return dict(map(_maybe_parse_arg_pair, v.expressions))
        elif isinstance(v, dict):
            return v
        else:
            raise_config_error(
                "Defaults must be a tuple of exp.EQ or a dict", error_type=AuditConfigError
            )
        return {}


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
        snapshot_or_node: t.Union[Snapshot, _Model, StandaloneAudit],
        *,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        execution_time: t.Optional[TimeLike] = None,
        snapshots: t.Optional[t.Dict[str, Snapshot]] = None,
        is_dev: bool = False,
        **kwargs: t.Any,
    ) -> exp.Subqueryable:
        """Renders the audit's query.

        Args:
            snapshot_or_model: The snapshot or model which is being audited.
            start: The start datetime to render. Defaults to epoch start.
            end: The end datetime to render. Defaults to epoch start.
            execution_time: The date/time time reference to use for execution time.
            snapshots: All snapshots (by model name) to use for mapping of physical locations.
            audit_name: The name of audit if the query to render is for an audit.
            is_dev: Indicates whether the rendering happens in the development mode and temporary
                tables / table clones should be used where applicable.
            kwargs: Additional kwargs to pass to the renderer.

        Returns:
            The rendered expression.
        """
        from sqlmesh.core.snapshot import Snapshot

        node = snapshot_or_node if isinstance(snapshot_or_node, _Node) else snapshot_or_node.node
        query_renderer = self._create_query_renderer(node)
        extra_kwargs = {}

        if isinstance(node, _Model):
            model: _Model = node
            this_model = (
                model.name
                if isinstance(snapshot_or_node, _Model)
                else t.cast(Snapshot, snapshot_or_node).table_name(is_dev=is_dev, for_read=True)
            )

            columns_to_types: t.Optional[t.Dict[str, t.Any]] = None
            if "engine_adapter" in kwargs:
                try:
                    columns_to_types = kwargs["engine_adapter"].columns(this_model)
                except Exception:
                    pass

            if model.time_column:
                where = exp.column(model.time_column.column).between(
                    model.convert_to_time_column(start or c.EPOCH, columns_to_types),
                    model.convert_to_time_column(end or c.EPOCH, columns_to_types),
                )
            else:
                where = None

            # The model's name is already normalized, but in case of snapshots we also prepend a
            # case-sensitive physical schema name, so we quote here to ensure that we won't have
            # a broken schema reference after the resulting query is normalized in `render`.
            quoted_model_name = quote_identifiers(
                exp.to_table(this_model, dialect=self.dialect), dialect=self.dialect
            )
            extra_kwargs["this_model"] = (
                exp.select("*").from_(quoted_model_name).where(where).subquery()
            )

        rendered_query = query_renderer.render(
            start=start,
            end=end,
            execution_time=execution_time,
            snapshots=snapshots,
            is_dev=is_dev,
            **{**self.defaults, **extra_kwargs, **kwargs},  # type: ignore
        )

        if rendered_query is None:
            raise SQLMeshError(
                f"Failed to render query for audit '{self.name}', model '{model.name}'."
            )

        return rendered_query

    @property
    def expressions(self) -> t.List[exp.Expression]:
        return self.expressions_ or []

    @property
    def macro_definitions(self) -> t.List[d.MacroDef]:
        """All macro definitions from the list of expressions."""
        return [s for s in self.expressions if isinstance(s, d.MacroDef)]

    def _create_query_renderer(
        self, model_or_audit: t.Union[_Model, StandaloneAudit]
    ) -> QueryRenderer:
        kwargs: t.Dict[str, t.Any] = {}
        if isinstance(model_or_audit, _Model):
            kwargs["only_execution_time"] = model_or_audit.kind.only_execution_time

        return QueryRenderer(
            self.query,
            self.dialect or model_or_audit.dialect,
            self.macro_definitions,
            path=self._path or Path(),
            jinja_macro_registry=self.jinja_macros,
            python_env=model_or_audit.python_env,
            **kwargs,
        )


class StandaloneAudit(_Node):
    """
    Args:
        name: The name of the model, which is of the form [catalog].[db].table.
            The catalog and db are optional.
        owner: The owner of the model.
        description: The optional model description.
        stamp: An optional arbitrary string sequence used to create new model versions without making
            changes to any of the functional components of the definition.
        tags:
        hash_raw_query:
    """

    name: str
    depends_on_: t.Optional[t.Set[str]] = Field(default=None, alias="depends_on")
    audit: Audit
    tags: t.List[str] = []
    hash_raw_query: bool = False

    python_env_: t.Optional[t.Dict[str, Executable]] = Field(default=None, alias="python_env")
    source_type: Literal["audit"] = "audit"

    _path: Path = Path()
    _depends_on: t.Optional[t.Set[str]] = None

    @property
    def depends_on(self) -> t.Set[str]:
        if self._depends_on is None:
            self._depends_on = self.depends_on_ or set()

            query = exp.select("*").from_("table")  # self.render_query(optimize=False)
            if query is not None:
                self._depends_on |= d.find_tables(query, dialect=self.dialect)

            self._depends_on -= {self.name}
        return self._depends_on

    @property
    def python_env(self) -> t.Dict[str, Executable]:
        return self.python_env_ or {}

    @property
    def sorted_python_env(self) -> t.List[t.Tuple[str, Executable]]:
        """Returns the python env sorted by executable kind and then var name."""
        return sorted(self.python_env.items(), key=lambda x: (x[1].kind, x[0]))

    @property
    def dialect(self) -> str:
        return self.audit.dialect

    @property
    def data_hash(self) -> str:
        """
        Computes the data hash for the node.

        Returns:
            The data hash for the node.
        """
        # StandaloneAudits do not have a data hash
        return ""

    def metadata_hash(self, audits: t.Dict[str, Audit]) -> str:
        """
        Computes the metadata hash for the node.

        Args:
            audits: Available audits by name.

        Returns:
            The metadata hash for the node.
        """
        data = [
            self.owner,
            self.description,
            *sorted(self.tags),
            str(self.sorted_python_env),
            self.stamp,
        ]

        query = (
            self.audit.query
            if self.hash_raw_query
            else self.audit.render_query(self) or self.audit.query
        )
        data.append(query.sql(comments=False))

        return hash_data(data)

    def text_diff(self, other: StandaloneAudit) -> str:
        """Produce a text diff against another model.

        Args:
            other: The model to diff against.

        Returns:
            A unified text diff showing additions and deletions.
        """
        return d.text_diff(self.audit.query, other.audit.query, self.dialect)


def create_standalone_audit(
    name: str,
    audit: Audit,
    *,
    path: Path = Path(),
    depends_on: t.Optional[t.Set[str]] = None,
    **kwargs: t.Any,
) -> StandaloneAudit:
    try:
        standalone = StandaloneAudit(
            name=name,
            audit=audit,
            **{
                "depends_on": depends_on,
                **kwargs,
            },
        )
    except Exception as ex:
        raise_config_error(str(ex), location=path)
        raise

    standalone._path = path

    return standalone


class AuditResult(PydanticModel):
    audit: Audit
    """The audit this result is for."""
    model: t.Optional[_Model] = None
    """The model this audit is for."""
    count: t.Optional[int] = None
    """The number of records returned by the audit query. This could be None if the audit was skipped."""
    query: t.Optional[exp.Expression] = None
    """The rendered query used by the audit. This could be None if the audit was skipped."""
    skipped: bool = False
    """Whether this audit was skipped or not."""


def _raise_config_error(msg: str, path: pathlib.Path) -> None:
    raise_config_error(msg, location=path, error_type=AuditConfigError)


# mypy doesn't realize raise_config_error raises an exception
@t.no_type_check
def _maybe_parse_arg_pair(e: exp.Expression) -> t.Tuple[str, exp.Expression]:
    if isinstance(e, exp.EQ):
        return e.left.name, e.right
    raise_config_error(f"Invalid defaults expression: {e}", error_type=AuditConfigError)


def is_audit(node: _Node) -> bool:
    return isinstance(node, StandaloneAudit)
