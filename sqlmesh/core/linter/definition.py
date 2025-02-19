from __future__ import annotations


from sqlmesh.core.config.linter import LinterConfig

from sqlmesh.core.model import Model

from sqlmesh.utils.errors import raise_config_error
from sqlmesh.core.console import get_console


class Linter:
    def __init__(self, config: LinterConfig) -> None:
        self.config = config

    def lint(self, model: Model) -> None:
        model_noqa = self.config.gather_rules(model.ignore_lints or [])

        rules = self.config.rules.difference(model_noqa)
        warn_rules = self.config.warn_rules.difference(model_noqa)

        error_violations = rules.check(model)
        warn_violations = warn_rules.check(model)

        if warn_violations:
            warn_msg = "\n".join(warn_violation.message for warn_violation in warn_violations)
            get_console().log_warning(f"Linter warnings for {model._path}:\n{warn_msg}")

        if error_violations:
            error_msg = "\n".join(error_violations.message for error_violations in error_violations)

            raise_config_error(f"Linter error for {model._path}:\n{error_msg}")
