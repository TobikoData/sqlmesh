from __future__ import annotations

from sqlmesh.core.config.base import BaseConfig


class NameInferenceConfig(BaseConfig):
    """Configuration for name inference of models from directory structure.

    Args:
        name_inference: A flag indicating whether name inference is enabled.
                        Default is False.
    """

    name_inference: bool = False
