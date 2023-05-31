from __future__ import annotations

import os
import typing as t
from pathlib import Path

from sqlmesh.core import constants as c
from sqlmesh.core.config.root import Config
from sqlmesh.utils import sys_path
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.metaprogramming import import_python_file
from sqlmesh.utils.yaml import load as yaml_load


def load_config_from_paths(
    *paths: Path, config_name: str = "config", load_from_env: bool = True
) -> Config:
    config: t.Optional[Config] = None

    visited_folders: t.Set[Path] = set()
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
                    f"YAML configs do not support multiple configs. Use Python instead."
                )
            next_config = load_config_from_yaml(path)
        elif extension == "py":
            next_config = load_config_from_python_module(path, config_name=config_name)
        else:
            raise ConfigError(
                f"Unsupported config file extension '{extension}' in config file '{path}'."
            )

        config = config.update_with(next_config) if config is not None else next_config

    if config is None:
        raise ConfigError(
            "SQLMesh config could not be found. Point the cli to the right path with `sqlmesh -p`. If you haven't set up SQLMesh, run `sqlmesh init`."
        )

    if load_from_env:
        config = config.update_with(load_config_from_env())

    return config


def load_config_from_yaml(path: Path) -> Config:
    config_dict = yaml_load(path)
    return Config.parse_obj(config_dict)


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


def load_config_from_env() -> Config:
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

    return Config.parse_obj(config_dict)
