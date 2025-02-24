from __future__ import annotations

import typing as t


from sqlmesh.core.config.base import BaseConfig
from sqlmesh.utils.errors import raise_config_error

from sqlmesh.core.linter.rule import RuleSet

from sqlmesh.core.linter.rules.builtin import (
    InvalidSelectStarExpansion,
    AmbiguousOrInvalidColumn,
    NoSelectStar,
)
from sqlmesh.core.linter.rules import BUILTIN_RULES


class LinterConfig(BaseConfig):
    """Configuration for model linting

    Args:
        enabled: Flag indicating whether the linter should run

        rules: A list of error rules to be applied on model
        warn_rules: A list of rules to be applied on models but produce warnings instead of raising errors.
        exclude_rules: A list of rules to be excluded/ignored

    """

    enabled: bool = True

    rules: RuleSet = RuleSet()
    warn_rules: RuleSet = RuleSet()
    exclude_rules: RuleSet = RuleSet()

    ALL_RULES: RuleSet = RuleSet()
    _data: t.Dict[t.Any, t.Any] = {}

    def __init__(self, **kwargs: t.Any) -> None:
        super().__init__()
        self.enabled = kwargs.pop("enabled", True)
        self._data = kwargs.copy() if any(value for value in kwargs.values()) else {}

    def gather_rules(self, rule_names: t.Union[t.List[str], str]) -> RuleSet:
        if rule_names == "ALL":
            return self.ALL_RULES

        rs = RuleSet()

        for rule_name in rule_names:
            if rule_name not in self.ALL_RULES:
                raise_config_error(f"Rule {rule_name} could not be found")

            rs[rule_name] = self.ALL_RULES[rule_name]

        return rs

    def fill_rules(self, USER_RULES: RuleSet) -> None:
        self.ALL_RULES = BUILTIN_RULES.union(USER_RULES)

        self.rules = self.gather_rules(self._data.get("rules", []))
        self.warn_rules = self.gather_rules(self._data.get("warn_rules", []))
        self.exclude_rules = self.gather_rules(self._data.get("exclude_rules", []))

        if overlapping := self.rules.intersection(self.warn_rules):
            raise_config_error(f"Found overlapping rules {overlapping} in lint config.")

        all_defined_rules = self.rules.union(self.warn_rules, self.exclude_rules)

        builtin_warn_rules = RuleSet.from_args(
            AmbiguousOrInvalidColumn, InvalidSelectStarExpansion
        ).difference(all_defined_rules)
        builtin_exclude_rules = RuleSet.from_args(NoSelectStar).difference(all_defined_rules)

        if not self.warn_rules:
            self.warn_rules = builtin_warn_rules

        if not self.exclude_rules:
            self.exclude_rules = builtin_exclude_rules
