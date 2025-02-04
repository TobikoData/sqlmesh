from __future__ import annotations

import typing as t

from sqlmesh.core.config.base import BaseConfig


class LinterConfig(BaseConfig):
    """Configuration for model linting

    Args:
        enabled: Flag indicating whether the linter should run

        rules: A list of error rules to be applied on model
        warn_rules: A list of rules to be applied on models but produce warnings instead of raising errors.
        exclude_rules: A list of rules to be excluded/ignored

    """

    enabled: bool = False

    rules: t.Union[t.List[str], str] = []
    warn_rules: t.Union[t.List[str], str] = []
    exclude_rules: t.Union[t.List[str], str] = []
