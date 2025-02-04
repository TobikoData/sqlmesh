from __future__ import annotations

from sqlmesh.core.linter.rule import RuleSet
from sqlmesh.core.linter.rules.builtin import BUILTIN_RULES
from sqlmesh.core.linter.rules.user import USER_RULES


# This set contains all the rules, even if they'll be included/excluded from the config
ALL_RULES: RuleSet = BUILTIN_RULES.union(
    USER_RULES,
)
