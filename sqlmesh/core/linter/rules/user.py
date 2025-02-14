"""Contains all the standard rules included with SQLMesh"""

from __future__ import annotations

import typing as t


from sqlmesh.core.linter.rule import Rule, RuleViolation, RuleSet
from sqlmesh.core.model import Model


# TODO(vaggelis): Delete this rule, it's only for testing purposes!
class NoVaggelisOwner(Rule):
    def check(self, model: Model) -> t.Optional[RuleViolation]:
        return RuleViolation(rule=self, model=model) if model.owner == "vaggelis" else None

    @property
    def summary(self) -> str:
        return "Owner shouldn't be Vaggelis, he is a bad engineer!"

    @property
    def description(self) -> str:
        return "No vaggelis"


USER_RULES = RuleSet.from_args(
    NoVaggelisOwner,
)
