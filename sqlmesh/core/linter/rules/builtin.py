"""Contains all the standard rules included with SQLMesh"""

from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.errors import SqlglotError

from sqlmesh.core.linter.rule import Rule, RuleViolation, RuleSet
from sqlmesh.core.model import Model, SqlModel


class NoSelectStar(Rule):
    def check(self, model: Model) -> t.Optional[RuleViolation]:
        if not isinstance(model, SqlModel):
            return None

        anchor_exprs = []
        for star in model.query.find_all(exp.Star):
            parent = star.parent
            if isinstance(parent, exp.Select):
                anchor_exprs.append(parent)

        return (
            RuleViolation(rule=self, model=model, anchor_exprs=anchor_exprs)
            if anchor_exprs
            else None
        )

    @property
    def summary(self) -> str:
        return "Query should not contain any SELECT *, even if they can be expanded."

    @property
    def description(self) -> str:
        return ""


class InvalidSelectStarExpansion(Rule):
    def __init__(
        self,
        deps: t.Optional[str] = None,
        model_fqn: t.Optional[str] = None,
        name: t.Optional[str] = None,
    ) -> None:
        super().__init__(name=name)
        self._deps = deps
        self._model_fqn = model_fqn

    def check(self, model: Model) -> t.Optional[RuleViolation]:
        return None

    @property
    def summary(self) -> str:
        return (
            f"SELECT * cannot be expanded due to missing schema(s) for model(s): {self._deps}. "
            "Run `sqlmesh create_external_models` and / or make sure that the model "
            f"'{self._model_fqn}' can be rendered at parse time."
        )

    @property
    def description(self) -> str:
        return ""


class AmbiguousOrInvalidColumn(Rule):
    def __init__(
        self, error: t.Optional[SqlglotError] = None, name: t.Optional[str] = None
    ) -> None:
        super().__init__(name=name)
        self._error = error
        self._model_fqn = ""

    def check(self, model: Model) -> t.Optional[RuleViolation]:
        return None

    @property
    def summary(self) -> str:
        return f"{self._error} for model '{self._model_fqn}', the column may not exist or is ambiguous."

    @property
    def description(self) -> str:
        return ""


BUILTIN_RULES = RuleSet.from_args(
    NoSelectStar,
    InvalidSelectStarExpansion,
    AmbiguousOrInvalidColumn,
)
