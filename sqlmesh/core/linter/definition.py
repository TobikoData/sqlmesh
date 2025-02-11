from __future__ import annotations


import typing as t

from sqlmesh.core.config.linter import LinterConfig

from sqlmesh.core.model import Model, SqlModel

from sqlmesh.utils.errors import raise_config_error
from sqlmesh.core.console import get_console

from sqlmesh.core.linter.rule import RuleSet, RuleViolation
from sqlmesh.core.linter.rules import ALL_RULES


class Linter:
    def gather_rules(
        self, rule_names: t.Optional[t.List[str] | str], defaults_to_all: bool = False
    ) -> RuleSet:
        if not rule_names:
            return ALL_RULES if defaults_to_all else RuleSet()
        if rule_names == "ALL" or rule_names[0] == "ALL":
            return ALL_RULES

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

    def _parse_model_noqa_rules(self, model: Model) -> t.Optional[RuleSet]:
        noqa = []

        with open(model._path, "r", encoding="utf-8") as file:
            line = file.readline()
            exclude_rules_str = line.split("linter: noqa:")
            if len(exclude_rules_str) > 1:
                noqa = list(
                    filter(
                        lambda x: x.strip(),
                        [rule.strip() for rule in exclude_rules_str[1].split(",")],
                    )
                )

        return self.gather_rules(noqa) if noqa else None

    def lint(self, model: Model) -> None:
        model_noqa = self._parse_model_noqa_rules(model)

        if model_noqa:
            rules = self.rules.difference(model_noqa)
            warn_rules = self.warn_rules.difference(model_noqa)
        else:
            rules = self.rules
            warn_rules = self.warn_rules

        error_violations = rules.check(model)
        warn_violations = warn_rules.check(model)

        if isinstance(model, SqlModel):
            violations = model._render_violations
            for rule in violations:
                if rule.name in rules:
                    error_violations.append(RuleViolation(rule=rule, model=model))
                elif rule.name in warn_rules:
                    warn_violations.append(RuleViolation(rule=rule, model=model))

        if warn_violations:
            warn_msg = "\n".join(warn_violation.message for warn_violation in warn_violations)
            get_console().log_warning(f"Linter warnings for {model}:\n{warn_msg}")

        if error_violations:
            error_msg = "\n".join(error_violations.message for error_violations in error_violations)

            raise_config_error(f"Linter error for {model}:\n{error_msg}")
