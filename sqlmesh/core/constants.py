from __future__ import annotations

import datetime
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

EPOCH = datetime.date(1970, 1, 1)

DEFAULT_MAX_LIMIT = 1000
"""The default maximum row limit that is used when evaluating a model."""

DEFAULT_LOG_LIMIT = 20
"""The default number of logs to keep."""

DEFAULT_LOG_FILE_DIR = "logs"
"""The default directory for log files."""

AUDITS = "audits"
MACROS = "macros"
METRICS = "metrics"
MODELS = "models"
EXTERNAL_MODELS = "external_models"
SEEDS = "seeds"
TESTS = "tests"
CACHE = ".cache"
SCHEMA_YAML = "schema.yaml"


DEFAULT_SCHEMA = "default"

SQLMESH_VARS = "__sqlmesh__vars__"
VAR = "var"
GATEWAY = "gateway"
