from __future__ import annotations

import typing as t

import itertools

from sqlmesh.core.config.base import BaseConfig
from sqlmesh.utils.errors import raise_config_error
from sqlmesh.utils.pydantic import model_validator

class LinterConfig(BaseConfig):
    """Configuration for model linting

    Args:
        enabled: Flag indicating whether the linter should run

        rules: A list of error rules to be applied on model
        warn_rules: A list of rules to be applied on models but produce warnings instead of raising errors.
        exclude_rules: A list of rules to be excluded/ignored

    """

    enabled: bool = True

    rules: t.List[str] | str = []
    warn_rules: t.List[str] | str = []
    exclude_rules: t.List[str] | str = []


    @model_validator(mode="before")
    def validate_rules(cls, data: t.Any) -> t.Any:
        if not isinstance(data, dict):
            return data

        from sqlmesh.core.linter.rules import ALL_RULES

        def gather_rules(rules: t.Optional[t.List[str] | str]):
            if isinstance(rules, str) and rules.upper() == "ALL":
                return set(ALL_RULES.keys())
            return set(rules)

        rules = gather_rules(data.get("rules", []))
        warn_rules = gather_rules(data.get("warn_rules", []))
        exclude_rules = gather_rules(data.get("exclude_rules", []))

        print(f"\n\n\nBEFORE {data}")

        if (overlapping := rules.intersection(warn_rules)):
            raise_config_error(f"Found overlapping rules {overlapping} in lint config.")

        all_rules = set(itertools.chain(rules, warn_rules, exclude_rules))

        missing_builtin_warn_rules = {
            "ambiguousorinvalidcolumn",
            "invalidselectstarexpansion",
        } - all_rules
        missing_builtin_exclude_rules = {"noselectstar"} - all_rules

        if not warn_rules:
            warn_rules = missing_builtin_warn_rules

        if not exclude_rules:
            exclude_rules = missing_builtin_exclude_rules


        print(f"FINAL rules {rules} - {warn_rules} - {exclude_rules}")

        data.update({"rules": list(rules), "warn_rules": list(warn_rules), "exclude_rules": list(exclude_rules)})
        return data