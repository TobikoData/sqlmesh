from __future__ import annotations

import logging
import typing as t

from sqlmesh.core.engine_adapter.base_postgres import BasePostgresEngineAdapter
from sqlmesh.core.engine_adapter.mixins import (
    LogicalReplaceQueryMixin,
    PandasNativeFetchDFSupportMixin,
)

if t.TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class PostgresEngineAdapter(
    BasePostgresEngineAdapter, LogicalReplaceQueryMixin, PandasNativeFetchDFSupportMixin
):
    DIALECT = "postgres"
    SUPPORTS_INDEXES = True
