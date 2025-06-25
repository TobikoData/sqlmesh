from __future__ import annotations

import logging
import typing as t

from sqlglot import __version__ as SQLGLOT_VERSION
from sqlglot import exp
from sqlglot.helper import seq_get

from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.core.state_sync.db.utils import (
    fetchone,
    SQLMESH_VERSION,
)
from sqlmesh.core.state_sync.base import (
    SCHEMA_VERSION,
    Versions,
)
from sqlmesh.utils.migration import index_text_type

logger = logging.getLogger(__name__)


class VersionState:
    def __init__(self, engine_adapter: EngineAdapter, schema: t.Optional[str] = None):
        self.engine_adapter = engine_adapter
        self.versions_table = exp.table_("_versions", db=schema)

        index_type = index_text_type(engine_adapter.dialect)
        self._version_columns_to_types = {
            "schema_version": exp.DataType.build("int"),
            "sqlglot_version": exp.DataType.build(index_type),
            "sqlmesh_version": exp.DataType.build(index_type),
        }

    def update_versions(
        self,
        schema_version: int = SCHEMA_VERSION,
        sqlglot_version: str = SQLGLOT_VERSION,
        sqlmesh_version: str = SQLMESH_VERSION,
    ) -> None:
        import pandas as pd

        self.engine_adapter.delete_from(self.versions_table, "TRUE")

        self.engine_adapter.insert_append(
            self.versions_table,
            pd.DataFrame(
                [
                    {
                        "schema_version": schema_version,
                        "sqlglot_version": sqlglot_version,
                        "sqlmesh_version": sqlmesh_version,
                    }
                ]
            ),
            columns_to_types=self._version_columns_to_types,
        )

    def get_versions(self) -> Versions:
        no_version = Versions()

        if not self.engine_adapter.table_exists(self.versions_table):
            return no_version

        query = exp.select("*").from_(self.versions_table)
        row = fetchone(self.engine_adapter, query)
        if not row:
            return no_version

        return Versions(
            schema_version=row[0], sqlglot_version=row[1], sqlmesh_version=seq_get(row, 2)
        )
