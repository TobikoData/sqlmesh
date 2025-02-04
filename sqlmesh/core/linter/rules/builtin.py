"""Contains all the standard rules included with SQLMesh"""

from __future__ import annotations

import typing as t

from sqlglot import exp

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


BUILTIN_RULES = RuleSet.from_args(
    NoSelectStar,
)
