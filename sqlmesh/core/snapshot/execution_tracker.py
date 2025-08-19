from __future__ import annotations

import time
import typing as t
from contextlib import contextmanager
from threading import local, Lock
from dataclasses import dataclass, field


@dataclass
class QueryExecutionStats:
    snapshot_batch_id: str
    total_rows_processed: t.Optional[int] = None
    total_bytes_processed: t.Optional[int] = None
    query_count: int = 0
    queries_executed: t.List[t.Tuple[str, t.Optional[int], t.Optional[int], float]] = field(
        default_factory=list
    )


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

    snapshot_batch_id: str
    stats: QueryExecutionStats = field(init=False)

    def __post_init__(self) -> None:
        self.stats = QueryExecutionStats(snapshot_batch_id=self.snapshot_batch_id)

    def add_execution(
        self, sql: str, row_count: t.Optional[int], bytes_processed: t.Optional[int]
    ) -> None:
        if row_count is not None and row_count >= 0:
            if self.stats.total_rows_processed is None:
                self.stats.total_rows_processed = row_count
            else:
                self.stats.total_rows_processed += row_count

            # conditional on row_count because we should only count bytes corresponding to
            # DML actions whose rows were captured
            if bytes_processed is not None:
                if self.stats.total_bytes_processed is None:
                    self.stats.total_bytes_processed = bytes_processed
                else:
                    self.stats.total_bytes_processed += bytes_processed

        self.stats.query_count += 1
        # TODO: remove this
        # for debugging
        self.stats.queries_executed.append((sql[:300], row_count, bytes_processed, time.time()))

    def get_execution_stats(self) -> QueryExecutionStats:
        return self.stats


class QueryExecutionTracker:
    """
    Thread-local context manager for snapshot execution statistics, such as
    rows processed.
    """

    _thread_local = local()
    _contexts: t.Dict[str, QueryExecutionContext] = {}
    _contexts_lock = Lock()

    def get_execution_context(self, snapshot_id_batch: str) -> t.Optional[QueryExecutionContext]:
        with self._contexts_lock:
            return self._contexts.get(snapshot_id_batch)

    @classmethod
    def is_tracking(cls) -> bool:
        return getattr(cls._thread_local, "context", None) is not None

    @contextmanager
    def track_execution(
        self, snapshot_id_batch: str
    ) -> t.Iterator[t.Optional[QueryExecutionContext]]:
        """
        Context manager for tracking snapshot execution statistics.
        """
        context = QueryExecutionContext(snapshot_batch_id=snapshot_id_batch)
        self._thread_local.context = context
        with self._contexts_lock:
            self._contexts[snapshot_id_batch] = context

        try:
            yield context
        finally:
            self._thread_local.context = None

    @classmethod
    def record_execution(
        cls, sql: str, row_count: t.Optional[int], bytes_processed: t.Optional[int]
    ) -> None:
        context = getattr(cls._thread_local, "context", None)
        if context is not None:
            context.add_execution(sql, row_count, bytes_processed)

    def get_execution_stats(self, snapshot_id_batch: str) -> t.Optional[QueryExecutionStats]:
        with self._contexts_lock:
            context = self._contexts.get(snapshot_id_batch)
            self._contexts.pop(snapshot_id_batch, None)
        return context.get_execution_stats() if context else None
