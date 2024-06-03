from __future__ import annotations

from sqlmesh.core.config.base import BaseConfig


class NameInferenceConfig(BaseConfig):
    """Configuration for name inference of models from directory structure.

    Args:
        infer_names: A flag indicating whether name inference is enabled.

    """

    infer_names: bool = False
