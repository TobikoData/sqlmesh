from __future__ import annotations


import typing as t

from sqlmesh.core.config.linter import LinterConfig

from sqlmesh.core.model import Model

from sqlmesh.utils.errors import raise_config_error
from sqlmesh.core.console import get_console

from sqlmesh.core.linter.rule import RuleSet
from sqlmesh.core.linter.rules import ALL_RULES


class Linter:
    def gather_rules(
        self, rule_names: t.Optional[t.List[str] | str], defaults_to_all: bool = False
    ) -> RuleSet:
        if isinstance(rule_names, str) and rule_names == "ALL":
            return ALL_RULES
        if not rule_names:
            return ALL_RULES if defaults_to_all else RuleSet()

        return RuleSet(ALL_RULES[rule_name] for rule_name in rule_names)

    def __init__(self, config: LinterConfig) -> None:
        self.config = config

        included_rules: RuleSet = self.gather_rules(config.rules, defaults_to_all=True)
        exclude_rules: RuleSet = self.gather_rules(config.exclude_rules)
        self.warn_rules: RuleSet = self.gather_rules(config.warn_rules)

        overlapping = exclude_rules.intersection(self.warn_rules)
        if overlapping:
            raise_config_error(f"Excluded linter rules {overlapping} found on warning rules")

        exclude_plus_warn_rules = self.warn_rules.union(exclude_rules)
        if self.config.rules and isinstance(self.config.rules, list):
            overlapping = included_rules.intersection(exclude_plus_warn_rules)
            if overlapping:
                raise_config_error(
                    f"Included linter rules {overlapping} are also added in excluded & warning rules"
                )

        self.rules: RuleSet = included_rules.difference(exclude_plus_warn_rules)

    def lint(self, model: Model) -> None:
        if model.ignore_lints:
            model_ignore_rules = self.gather_rules(model.ignore_lints)
            rules = self.rules.difference(model_ignore_rules)
            warn_rules = self.warn_rules.difference(model_ignore_rules)
        else:
            rules = self.rules
            warn_rules = self.warn_rules

        error_violations = rules.check(model)
        warn_violations = warn_rules.check(model)

        if warn_violations:
            warn_msg = "\n".join(warn_violation.message for warn_violation in warn_violations)
            get_console().log_warning(f"Linter warnings for {model}:\n{warn_msg}")

        if error_violations:
            error_msg = "\n".join(error_violations.message for error_violations in error_violations)

            raise_config_error(f"Linter error for {model}:\n{error_msg}")
