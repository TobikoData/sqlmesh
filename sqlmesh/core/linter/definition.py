from __future__ import annotations

import typing as t

from sqlmesh.core.config.linter import LinterConfig

from sqlmesh.core.model import Model

from sqlmesh.utils.errors import raise_config_error
from sqlmesh.core.console import get_console
from sqlmesh.core.linter.rule import RuleSet


def select_rules(all_rules: RuleSet, rule_names: t.Set[str]) -> RuleSet:
    if "all" in rule_names:
        return all_rules

    rules = set()
    for rule_name in rule_names:
        if rule_name not in all_rules:
            raise_config_error(f"Rule {rule_name} could not be found")

        rules.add(all_rules[rule_name])

    return RuleSet(rules)


class Linter:
    def __init__(
        self, enabled: bool, all_rules: RuleSet, rules: RuleSet, warn_rules: RuleSet
    ) -> None:
        self.enabled = enabled
        self.all_rules = all_rules
        self.rules = rules
        self.warn_rules = warn_rules

    @classmethod
    def from_rules(cls, all_rules: RuleSet, config: LinterConfig) -> Linter:
        ignored_rules = select_rules(all_rules, config.ignored_rules)
        included_rules = all_rules.difference(ignored_rules)

        rules = select_rules(included_rules, config.rules)
        warn_rules = select_rules(included_rules, config.warn_rules)

        if overlapping := rules.intersection(warn_rules):
            overlapping_rules = ", ".join(rule for rule in overlapping)
            raise_config_error(
                f"Rules cannot simultaneously warn and raise an error: [{overlapping_rules}]"
            )

        return Linter(config.enabled, all_rules, rules, warn_rules)

    def lint_model(self, model: Model) -> bool:
        if not self.enabled:
            return False

        ignored_rules = select_rules(self.all_rules, model.ignored_rules)

        rules = self.rules.difference(ignored_rules)
        warn_rules = self.warn_rules.difference(ignored_rules)

        error_violations = rules.check_model(model)
        warn_violations = warn_rules.check_model(model)

        if warn_violations:
            get_console().show_linter_violations(warn_violations, model)

        if error_violations:
            get_console().show_linter_violations(error_violations, model, is_error=True)
            return True

        return False
