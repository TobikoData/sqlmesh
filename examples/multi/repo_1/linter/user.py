"""Contains all the standard rules included with SQLMesh"""

from __future__ import annotations

import typing as t

from sqlmesh.core.linter.rule import Rule, RuleViolation
from sqlmesh.core.model import Model


class NoMissingDescription(Rule):
    """All models should be documented."""

    def check_model(self, model: Model) -> t.Optional[RuleViolation]:
        return self.violation() if not model.description else None
