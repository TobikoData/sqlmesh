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
    *paths: Path, config_name: str = "config", load_from_env: bool = True
) -> Config:
    """
    Loads a configuration from a list of paths. First YAML config is loaded and overriden by environment variables
    and then the Python config is loaded and overrides the YAML + Env config.
    Therefore the override order is: YAML < Env < Python.
    """
    visited_folders: t.Set[Path] = set()
    yaml_dict_config: t.Dict[str, t.Any] = {}
    python_config: t.Optional[Config] = None
    for path in paths:
        if not path.exists():
            continue

        if not path.is_file():
            raise ConfigError(f"Path '{path}' must be a file.")

        parent_path = path.parent
        if parent_path in visited_folders:
            raise ConfigError(f"Multiple configuration files found in folder '{parent_path}'.")
        visited_folders.add(parent_path)

        extension = path.suffix
        if extension in (".yml", ".yaml"):
            yaml_dict_config = merge_dicts(yaml_dict_config, yaml_load(path))
        elif extension == ".py":
            new_python_config = load_config_from_python_module(path, config_name=config_name)
            python_config = (
                python_config.update_with(new_python_config) if python_config else new_python_config
            )
        else:
            raise ConfigError(
                f"Unsupported config file extension '{extension}' in config file '{path}'."
            )
    config: t.Optional[Config] = None
    if yaml_dict_config:
        config = Config.parse_obj(
            merge_dicts(yaml_dict_config, load_config_dict_from_env() if load_from_env else {})
        )
    if python_config:
        config = config.update_with(python_config) if config else python_config

    if config is None:
        raise ConfigError(
            "SQLMesh config could not be found. Point the cli to the right path with `sqlmesh -p`. If you haven't set up SQLMesh, run `sqlmesh init`."
        )

    return config


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


def load_config_dict_from_env() -> t.Dict[str, t.Any]:
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


def load_config_from_env() -> Config:
    return Config.parse_obj(load_config_dict_from_env())
