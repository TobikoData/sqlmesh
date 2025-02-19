from __future__ import annotations

from sqlmesh.core.linter.rule import RuleSet
from sqlmesh.core.linter.rules.builtin import (
    AmbiguousOrInvalidColumn,
    InvalidSelectStarExpansion,
    NoSelectStar,
)

BUILTIN_RULES = RuleSet.from_args(
    NoSelectStar,
    InvalidSelectStarExpansion,
    AmbiguousOrInvalidColumn,
)
