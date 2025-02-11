from __future__ import annotations

import typing as t

from sqlmesh.core.config.base import BaseConfig


class LinterConfig(BaseConfig):
    """Configuration for model linting

    Args:
        enabled: Flag indicating whether the linter should run

        rules: A list of rules to be applied on models (None => ALL are applied)
        exclude_rules: A list of rules to be excluded/ignored from the linting process
        warn_rules: A list of rules to be applied on models but produce warnings instead of raising errors.

    """

    enabled: bool = False

    rules: t.Optional[t.List[str] | str] = None
    exclude_rules: t.Optional[t.List[str] | str] = None
    warn_rules: t.Optional[t.List[str] | str] = None
