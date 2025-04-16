from __future__ import annotations


from sqlmesh.core.config.base import BaseConfig


class JanitorConfig(BaseConfig):
    """The configuration for the janitor.

    Args:
        warn_on_delete_failure: Whether to warn instead of erroring if the janitor fails to delete the expired environment schema / views.
    """

    warn_on_delete_failure: bool = False
