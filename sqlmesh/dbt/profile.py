from __future__ import annotations

import logging
import os
import typing as t
from pathlib import Path

from sqlmesh.dbt.common import PROJECT_FILENAME, load_yaml
from sqlmesh.dbt.context import DbtContext
from sqlmesh.dbt.target import TargetConfig
from sqlmesh.utils import yaml
from sqlmesh.utils.errors import ConfigError

logger = logging.getLogger(__name__)


class Profile:
    """
    A class to read DBT profiles and obtain the project's target data warehouse configuration
    """

    PROFILE_FILE = "profiles.yml"

    def __init__(
        self,
        path: Path,
        target_name: str,
        target: TargetConfig,
        project_model_config: t.Dict[str, t.Any],
    ):
        """
        Args:
            path: Path to the profile file
            target_name: Name of the target loaded
            target: TargetConfig for target_name
            project_model_config: Model configuration values from the project YAML
        """
        self.path = path
        self.target_name = target_name
        self.target = target
        self.project_model_config = project_model_config

    @classmethod
    def load(
        cls,
        context: DbtContext,
        target_name: t.Optional[str] = None,
        project_yaml: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> Profile:
        """
        Loads the profile for the specified project

        Args:
            context: DBT context for this profile

        Returns:
            The Profile for the specified project
        """
        if not project_yaml:
            project_file = Path(context.project_root, PROJECT_FILENAME)
            if not project_file.exists():
                raise ConfigError(f"Could not find {PROJECT_FILENAME} in {context.project_root}")
            project_yaml = load_yaml(project_file)

        project_model_config = project_yaml.get("models", {})

        if not context.profile_name:
            context.profile_name = context.render(
                project_yaml.get("profile", "")
            ) or context.render(project_yaml.get("name", ""))
            if not context.profile_name:
                raise ConfigError(f"{project_file.stem} must include project name.")

        profile_filepath = cls._find_profile(context.project_root)
        if not profile_filepath:
            raise ConfigError(f"{cls.PROFILE_FILE} not found.")

        target_name, target = cls._read_profile(profile_filepath, context, target_name)
        return Profile(profile_filepath, target_name, target, project_model_config)

    @classmethod
    def _find_profile(cls, project_root: Path) -> t.Optional[Path]:
        dir = os.environ.get("DBT_PROFILES_DIR", "")
        path = Path(project_root, dir, cls.PROFILE_FILE)
        if path.exists():
            return path
        elif dir:
            return None

        path = Path(Path.home(), ".dbt", cls.PROFILE_FILE)
        if path.exists():
            return path

        return None

    @classmethod
    def _read_profile(
        cls, path: Path, context: DbtContext, target_name: t.Optional[str] = None
    ) -> t.Tuple[str, TargetConfig]:
        logger.debug("Processing profile '%s'.", path)
        project_data = load_yaml(path).get(context.profile_name)
        if not project_data:
            raise ConfigError(f"Profile '{context.profile_name}' not found in profiles.")

        outputs = project_data.get("outputs")
        if not outputs:
            raise ConfigError(f"No outputs exist in profiles for '{context.profile_name}'.")

        if not target_name:
            if "target" not in project_data:
                raise ConfigError(f"No target specified for '{context.profile_name}'.")
            target_name = context.render(project_data.get("target"))

        if target_name not in outputs:
            raise ConfigError(
                f"Target '{target_name}' not specified in profiles for '{context.profile_name}'."
            )

        target_fields = load_yaml(context.render(yaml.dump(outputs[target_name])))
        target = TargetConfig.load(
            {"name": target_name, "profile_name": context.profile_name, **target_fields}
        )

        return (target_name, target)
