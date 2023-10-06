"""
.. include:: ../README.md
"""
from __future__ import annotations

import logging
import os
import sys
import typing as t
from datetime import datetime
from enum import Enum

from sqlmesh.core.dialect import extend_sqlglot

extend_sqlglot()

from sqlmesh.core.config import Config
from sqlmesh.core.context import Context, ExecutionContext
from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.core.macros import macro
from sqlmesh.core.model import Model, model
from sqlmesh.core.snapshot import Snapshot
from sqlmesh.utils import debug_mode_enabled

try:
    from sqlmesh._version import __version__, __version_tuple__  # type: ignore
except ImportError:
    pass


class RuntimeEnv(str, Enum):
    """Enum defining what environment SQLMesh is running in."""

    TERMINAL = "terminal"
    DATABRICKS = "databricks"
    GOOGLE_COLAB = "google_colab"  # Not currently officially supported
    JUPYTER = "jupyter"

    @classmethod
    def get(cls) -> RuntimeEnv:
        """Get the console class to use based on the environment that the code is running in
        Reference implementation: https://github.com/noklam/rich/blob/d3a1ae61a77d934844563514370084971bc3e143/rich/console.py#L511-L528

        Unlike the rich implementation we try to split out by notebook type instead of treating it all as Jupyter.
        """
        try:
            shell = get_ipython()  # type: ignore
            if os.getenv("DATABRICKS_RUNTIME_VERSION"):
                return RuntimeEnv.DATABRICKS
            if "google.colab" in str(shell.__class__):  # type: ignore
                return RuntimeEnv.GOOGLE_COLAB
            if shell.__class__.__name__ == "ZMQInteractiveShell":  # type: ignore
                return RuntimeEnv.JUPYTER
        except NameError:
            pass

        return RuntimeEnv.TERMINAL

    @property
    def is_terminal(self) -> bool:
        return self == RuntimeEnv.TERMINAL

    @property
    def is_databricks(self) -> bool:
        return self == RuntimeEnv.DATABRICKS

    @property
    def is_jupyter(self) -> bool:
        return self == RuntimeEnv.JUPYTER

    @property
    def is_google_colab(self) -> bool:
        return self == RuntimeEnv.GOOGLE_COLAB

    @property
    def is_notebook(self) -> bool:
        return not self.is_terminal


runtime_env = RuntimeEnv.get()


if runtime_env.is_notebook:
    try:
        from sqlmesh.magics import register_magics

        register_magics()
    except ImportError:
        pass


LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)"


# SO: https://stackoverflow.com/questions/384076/how-can-i-color-python-logging-output
class CustomFormatter(logging.Formatter):
    """Custom logging formatter."""

    grey = "\x1b[38;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"

    FORMATS = {
        logging.DEBUG: grey + LOG_FORMAT + reset,
        logging.INFO: grey + LOG_FORMAT + reset,
        logging.WARNING: yellow + LOG_FORMAT + reset,
        logging.ERROR: red + LOG_FORMAT + reset,
        logging.CRITICAL: bold_red + LOG_FORMAT + reset,
    }

    def format(self, record: logging.LogRecord) -> str:
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


def enable_logging(level: t.Optional[int] = None, write_to_file: bool = False) -> None:
    """Enable logging to send to stdout and color different levels"""
    level = level or (logging.DEBUG if debug_mode_enabled() else logging.INFO)
    logger = logging.getLogger()
    logger.setLevel(level if not debug_mode_enabled() else logging.DEBUG)
    if not logger.hasHandlers():
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)
        handler.setFormatter(CustomFormatter())
        logger.addHandler(handler)

        if write_to_file:
            os.makedirs("logs", exist_ok=True)
            filename = f"logs/sqlmesh_{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}.log"
            file_handler = logging.FileHandler(filename, mode="w", encoding="utf-8")
            file_handler.setLevel(level)
            file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
            logger.addHandler(file_handler)
