from __future__ import annotations

import logging
import typing as t


from sqlglot import Dialect

from sqlmesh.core.engine_adapter.postgres import PostgresEngineAdapter
from sqlmesh.core.engine_adapter.shared import (
    set_catalog,
    CatalogSupport,
    CommentCreationView,
    CommentCreationTable,
)


if t.TYPE_CHECKING:
    from sqlmesh.core._typing import SessionProperties

logger = logging.getLogger(__name__)


@set_catalog()
class RisingwaveEngineAdapter(PostgresEngineAdapter):
    DIALECT = "risingwave"
    DEFAULT_BATCH_SIZE = 400
    CATALOG_SUPPORT = CatalogSupport.SINGLE_CATALOG_ONLY
    COMMENT_CREATION_TABLE = CommentCreationTable.COMMENT_COMMAND_ONLY
    COMMENT_CREATION_VIEW = CommentCreationView.UNSUPPORTED
    # COMMENT_CREATION_VIEW = CommentCreationView.COMMENT_COMMAND_ONLY
    SUPPORTS_MATERIALIZED_VIEWS = True
    # Temporarily set this because integration test: test_transaction uses truncate table operation, which is not supported in risingwave.
    SUPPORTS_TRANSACTIONS = False

    def _set_flush(self) -> None:
        sql = "SET RW_IMPLICIT_FLUSH TO true;"
        self._execute(sql)

    def __init__(
        self,
        connection_factory: t.Callable[[], t.Any],
        dialect: str = "",
        sql_gen_kwargs: t.Optional[t.Dict[str, Dialect | bool | str]] = None,
        multithreaded: bool = False,
        cursor_kwargs: t.Optional[t.Dict[str, t.Any]] = None,
        cursor_init: t.Optional[t.Callable[[t.Any], None]] = None,
        default_catalog: t.Optional[str] = None,
        execute_log_level: int = logging.DEBUG,
        register_comments: bool = True,
        pre_ping: bool = False,
        **kwargs: t.Any,
    ):
        super().__init__(
            connection_factory,
            dialect,
            sql_gen_kwargs,
            multithreaded,
            cursor_kwargs,
            cursor_init,
            default_catalog,
            execute_log_level,
            register_comments,
            pre_ping,
            **kwargs,
        )
        if hasattr(self, "cursor"):
            self._set_flush()

    def _begin_session(self, properties: SessionProperties) -> t.Any:
        """Begin a new session."""
        self._set_flush()
