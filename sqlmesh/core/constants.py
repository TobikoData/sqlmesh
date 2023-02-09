SQLMESH = "sqlmesh"

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
CACHE_PATH = ".cache"
"""Cache path"""
TABLE_INFO_CACHE = "table_info_cache"
"""Table info cache"""
DATA_VERSION_LIMIT = 10
"""Data version limit"""
DEFAULT_TIME_COLUMN_FORMAT = "%Y-%m-%d"
"""Default time column format"""

EPOCH_DS = "1970-01-01"

JINJA_MACROS = "JINJA_MACROS"
"""Used to store jinja macros in the execution env."""
