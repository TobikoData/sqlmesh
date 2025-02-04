from __future__ import annotations

import typing as t

from sqlmesh.core.linter.rule import Rule, RuleViolation
from sqlmesh.core.model import Model


class NoMissingOwner(Rule):
    """All models should have an owner specified."""

    def check_model(self, model: Model) -> t.Optional[RuleViolation]:
        return self.violation() if not model.owner else None
