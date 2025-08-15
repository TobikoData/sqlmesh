from __future__ import annotations

import time
import typing as t
from contextlib import contextmanager
from threading import local
from dataclasses import dataclass, field


@dataclass
class QueryExecutionContext:
    """
    Container for tracking rows processed or other execution information during snapshot evaluation.

    It accumulates statistics from multiple cursor.execute() calls during a single snapshot evaluation.

    Attributes:
        id: Identifier linking this context to a specific operation
        total_rows_processed: Running sum of cursor.rowcount from all executed queries during evaluation
        query_count: Total number of SQL statements executed
        queries_executed: List of (sql_snippet, row_count, timestamp) tuples for debugging
    """

    id: str
    total_rows_processed: int = 0
    query_count: int = 0
    queries_executed: t.List[t.Tuple[str, t.Optional[int], float]] = field(default_factory=list)

    def add_execution(self, sql: str, row_count: t.Optional[int]) -> None:
        if row_count is not None and row_count >= 0:
            self.total_rows_processed += row_count
        self.query_count += 1
        # TODO: remove this
        # for debugging
        self.queries_executed.append((sql[:300], row_count, time.time()))

    def get_execution_stats(self) -> t.Dict[str, t.Any]:
        return {
            "id": self.id,
            "total_rows_processed": self.total_rows_processed,
            "query_count": self.query_count,
            "queries": self.queries_executed,
        }


class QueryExecutionTracker:
    """
    Thread-local context manager for snapshot execution statistics, such as
    rows processed.
    """

    _thread_local = local()
    _contexts: t.Dict[str, QueryExecutionContext] = {}

    @classmethod
    def get_execution_context(cls, snapshot_id_batch: str) -> t.Optional[QueryExecutionContext]:
        return cls._contexts.get(snapshot_id_batch)

    @classmethod
    def is_tracking(cls) -> bool:
        return getattr(cls._thread_local, "context", None) is not None

    @classmethod
    @contextmanager
    def track_execution(
        cls, snapshot_id_batch: str, condition: bool = True
    ) -> t.Iterator[t.Optional[QueryExecutionContext]]:
        """
        Context manager for tracking snapshot execution statistics.
        """
        if not condition:
            yield None
            return

        context = QueryExecutionContext(id=snapshot_id_batch)
        cls._thread_local.context = context
        cls._contexts[snapshot_id_batch] = context
        try:
            yield context
        finally:
            cls._thread_local.context = None

    @classmethod
    def record_execution(cls, sql: str, row_count: t.Optional[int]) -> None:
        context = getattr(cls._thread_local, "context", None)
        if context is not None:
            context.add_execution(sql, row_count)

    @classmethod
    def get_execution_stats(cls, snapshot_id_batch: str) -> t.Optional[t.Dict[str, t.Any]]:
        context = cls.get_execution_context(snapshot_id_batch)
        cls._contexts.pop(snapshot_id_batch, None)
        return context.get_execution_stats() if context else None
