from __future__ import annotations

from sqlmesh.core.config.base import BaseConfig


class UIConfig(BaseConfig):
    """The UI configuration for SQLMesh.

    Args:
        format_on_save: Whether to format the SQL code on save or not.
    """

    format_on_save: bool = True
