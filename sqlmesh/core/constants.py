from __future__ import annotations

import datetime
import os
import sys
import typing as t
from pathlib import Path

SQLMESH = "sqlmesh"
SQLMESH_PATH = Path.home() / ".sqlmesh"

PROD = "prod"
"""Prod"""
DEV = "dev"
"""Dev"""

SNAPSHOTS_PATH = "snapshots"
"""Snapshots path"""
DEFAULT_SNAPSHOT_TTL = "in 1 week"
"""Default snapshot TTL"""
DEFAULT_ENVIRONMENT_TTL = "in 1 week"
"""Default environment TTL"""
IGNORE_PATTERNS = [
    ".ipynb_checkpoints/*",
]
"""Ignore patterns"""
DATA_VERSION_LIMIT = 10
"""Data version limit"""
DEFAULT_TIME_COLUMN_FORMAT = "%Y-%m-%d"
"""Default time column format"""
MAX_MODEL_DEFINITION_SIZE = 10000
"""Maximum number of characters in a model definition"""


def is_daemon_process() -> bool:
    """
    Determines whether the current process is running as a daemon (background process).

    This function checks if the standard output (stdout) is connected to a terminal.
    It does this by calling `sys.stdout.fileno()` to retrieve the file descriptor
    for the stdout stream and `os.isatty()` to check if this file descriptor is
    associated with a terminal device.

    Returns:
        bool: True if the process is running as a daemon (not attached to a terminal),
              False if the process is running in a terminal (interactive mode).
    """
    return not os.isatty(sys.stdout.fileno())


# The maximum number of fork processes, used for loading projects
# None means default to process pool, 1 means don't fork, :N is number of processes
# Factors in the number of available CPUs even if the process is bound to a subset of them
# (e.g. via taskset) to avoid oversubscribing the system and causing kill signals
if hasattr(os, "fork") and not is_daemon_process():
    try:
        MAX_FORK_WORKERS: t.Optional[int] = int(os.getenv("MAX_FORK_WORKERS"))  # type: ignore
    except TypeError:
        MAX_FORK_WORKERS = (
            len(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None  # type: ignore
        )
else:
    MAX_FORK_WORKERS = 1

EPOCH = datetime.date(1970, 1, 1)

DEFAULT_MAX_LIMIT = 1000
"""The default maximum row limit that is used when evaluating a model."""

DEFAULT_LOG_LIMIT = 20
"""The default number of logs to keep."""

DEFAULT_LOG_FILE_DIR = "logs"
"""The default directory for log files."""

AUDITS = "audits"
CACHE = ".cache"
EXTERNAL_MODELS = "external_models"
MACROS = "macros"
MATERIALIZATIONS = "materializations"
METRICS = "metrics"
MODELS = "models"
SEEDS = "seeds"
SIGNALS = "signals"
TESTS = "tests"

EXTERNAL_MODELS_YAML = "external_models.yaml"
EXTERNAL_MODELS_DEPRECATED_YAML = "schema.yaml"

DEFAULT_SCHEMA = "default"

SQLMESH_VARS = "__sqlmesh__vars__"
VAR = "var"
GATEWAY = "gateway"

SQLMESH_MACRO = "__sqlmesh__macro__"
SQLMESH_BUILTIN = "__sqlmesh__builtin__"
SQLMESH_METADATA = "__sqlmesh__metadata__"

BUILTIN = "builtin"
AIRFLOW = "airflow"
DBT = "dbt"
NATIVE = "native"
