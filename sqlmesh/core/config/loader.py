from __future__ import annotations

import os
import typing as t
from pathlib import Path

from sqlmesh.core import constants as c
from sqlmesh.core.config.root import Config
from sqlmesh.utils import merge_dicts, sys_path
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.metaprogramming import import_python_file
from sqlmesh.utils.yaml import load as yaml_load


def load_config_from_paths(
    project_paths: t.List[Path] = [],
    personal_paths: t.List[Path] = [],
    config_name: str = "config",
    load_from_env: bool = True,
) -> Config:
    visited_folders: t.Set[Path] = set()

    def _load_configs(paths: t.List[Path]) -> t.Dict[str, t.Optional[t.Union[Config, t.List]]]:
        python_config: Config | None = None
        non_python_configs: t.List[t.Dict] = []
        for path in paths:
            if not path.exists():
                continue

            if not path.is_file():
                raise ConfigError(f"Path '{path}' must be a file.")

            parent_path = path.parent
            if parent_path in visited_folders:
                raise ConfigError(f"Multiple configuration files found in folder '{parent_path}'.")
            visited_folders.add(parent_path)

            extension = path.name.split(".")[-1].lower()
            if extension in ("yml", "yaml"):
                if config_name != "config":
                    raise ConfigError(
                        "YAML configs do not support multiple configs. Use Python instead."
                    )
                non_python_configs.append(load_config_from_yaml(path))
            elif extension == "py":
                python_config = load_config_from_python_module(path, config_name=config_name)
            else:
                raise ConfigError(
                    f"Unsupported config file extension '{extension}' in config file '{path}'."
                )

        return {"python_config": python_config, "non_python_configs": non_python_configs}

    project_config_dicts = _load_configs(project_paths)
    personal_config_dicts = _load_configs(personal_paths)

    no_dialect_err_msg = "Default model SQL dialect is a required configuration parameter - set it in the `model_defaults` key."

    if project_config_dicts.get("non_python_configs"):
        non_python_config = Config.parse_obj(
            merge_dicts(*project_config_dicts.get("non_python_configs"))
        )
        non_python_defaults = non_python_config.model_defaults
        if non_python_defaults.dialect is None:
            raise ConfigError(no_dialect_err_msg)

        non_python_config.update_with(merge_dicts(*personal_config_dicts.get("non_python_configs")))
    else:
        non_python_config = Config()

    if load_from_env:
        env_config = load_config_from_env()
        if env_config:
            non_python_config.update_with(load_config_from_env())

    python_config = project_config_dicts["python_config"]
    if python_config:
        python_defaults = python_config.model_defaults
        if python_defaults.dialect is None:
            raise ConfigError(no_dialect_err_msg)
        return python_config.update_with(non_python_config)

    return non_python_config


def load_config_from_yaml(path: Path) -> t.Dict[str, t.Any]:
    return yaml_load(path)


def load_config_from_python_module(module_path: Path, config_name: str = "config") -> Config:
    with sys_path(module_path.parent):
        try:
            config_module = import_python_file(module_path, module_path.parent)
        except ImportError:
            raise ConfigError(f"Config module '{module_path}' was not found.")

    try:
        config_obj = getattr(config_module, config_name)
    except AttributeError:
        raise ConfigError(f"Config '{config_name}' was not found.")

    if config_obj is None or not isinstance(config_obj, Config):
        raise ConfigError(
            f"Config needs to be a valid object of type sqlmesh.core.config.Config. Found `{config_obj}` instead at '{module_path}'."
        )

    return config_obj


def load_config_from_env() -> t.Dict[str, t.Any]:
    config_dict: t.Dict[str, t.Any] = {}

    for key, value in os.environ.items():
        key = key.lower()
        if key.startswith(f"{c.SQLMESH}__"):
            segments = key.split("__")[1:]
            if not segments or not segments[-1]:
                raise ConfigError(f"Invalid SQLMesh configuration variable '{key}'.")

            target_dict = config_dict
            for config_key in segments[:-1]:
                if config_key not in target_dict:
                    target_dict[config_key] = {}
                target_dict = target_dict[config_key]
            target_dict[segments[-1]] = value

    return config_dict
