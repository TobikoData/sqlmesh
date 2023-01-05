from __future__ import annotations

import pathlib
import typing as t

from pydantic import Field, validator
from sqlglot import exp, maybe_parse

from sqlmesh.core import dialect as d
from sqlmesh.utils.errors import AuditConfigError
from sqlmesh.utils.pydantic import PydanticModel


class AuditMeta(PydanticModel):
    """Metadata for audits which can be defined in SQL."""

    name: str
    """The name of this audit."""
    model: str
    """The model being audited."""
    dialect: str = ""
    """The dialect of the audit query."""
    skip: bool = False
    """Setting this to `true` will cause this audit to be skipped. Defaults to `false`."""
    blocking: bool = True
    """Setting this to `true` will cause the pipeline execution to stop if this audit fails. Defaults to `true`."""

    @validator("name", "dialect", pre=True)
    def _string_validator(cls, v: t.Any) -> t.Optional[str]:
        if isinstance(v, exp.Expression):
            return v.name
        return str(v) if v is not None else None

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

    query: exp.Subqueryable
    """The audit query."""
    expressions_: t.Optional[t.List[exp.Expression]] = Field(
        default=None, alias="expressions"
    )
    _path: t.Optional[pathlib.Path] = None

    @validator("query", pre=True)
    def _parse_expression(cls, v: str) -> exp.Expression:
        """Helper method to deserialize SQLGlot expressions in Pydantic models."""
        expression = maybe_parse(v)
        if not expression:
            raise ValueError(f"Could not parse {v}")
        return expression

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
            _raise_config_error(
                "Incomplete audit definition, missing AUDIT and QUERY", path
            )

        meta, *statements, query = expressions

        if not isinstance(meta, d.Audit):
            _raise_config_error(
                "AUDIT statement is required as the first statement in the definition",
                path,
            )
            raise

        provided_meta_fields = {p.name for p in meta.expressions}

        missing_required_fields = AuditMeta.missing_required_fields(
            provided_meta_fields
        )
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

        if not query.expressions:
            _raise_config_error("Query missing select statements", path)

        try:
            audit = cls(
                query=query,
                expressions=statements,
                **{
                    "dialect": dialect or "",
                    **AuditMeta(
                        **{
                            prop.name: prop.args.get("value")
                            for prop in meta.expressions
                            if prop
                        },
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


class AuditResult(PydanticModel):
    audit: Audit
    """The audit this result is for."""
    count: int
    """The number of records returned by the audit query."""
    query: exp.Expression
    """The rendered query used by the audit."""


def _raise_config_error(msg: str, path: pathlib.Path) -> None:
    raise AuditConfigError(f"{msg}: '{path}'")
