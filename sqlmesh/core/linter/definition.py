from __future__ import annotations

import itertools
import typing as t

from sqlmesh.core.config.linter import LinterConfig

from sqlmesh.core.model import Model, SqlModel

from sqlmesh.utils.errors import raise_config_error
from sqlmesh.core.console import get_console

from sqlmesh.core.linter.rule import RuleSet
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

        self.rules: RuleSet = self.gather_rules(config.rules)
        self.exclude_rules: RuleSet = self.gather_rules(config.exclude_rules)
        self.warn_rules: RuleSet = self.gather_rules(config.warn_rules)

    def lint(self, model: Model) -> None:
        model_noqa = self.gather_rules(model.ignore_lints) if model.ignore_lints else None

        if model_noqa:
            rules = self.rules.difference(model_noqa)
            warn_rules = self.warn_rules.difference(model_noqa)
        else:
            rules = self.rules
            warn_rules = self.warn_rules

        error_violations = rules.check(model)
        warn_violations = warn_rules.check(model)

        print(f"warn {warn_violations} error {error_violations} - {warn_rules}")
        if warn_violations:
            warn_msg = "\n".join(warn_violation.message for warn_violation in warn_violations)
            get_console().log_warning(f"Linter warnings for {model}:\n{warn_msg}")

        if error_violations:
            if isinstance(model, SqlModel):
                model._query_renderer.update_cache(None, optimized=False)

            #     model._query_renderer._optimized_cache = None


            error_msg = "\n".join(error_violations.message for error_violations in error_violations)

            raise_config_error(f"Linter error for {model}:\n{error_msg}")
