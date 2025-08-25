from __future__ import annotations

import typing as t
from contextlib import contextmanager
from threading import local, Lock
from dataclasses import dataclass, field
from sqlmesh.core.snapshot import SnapshotIdBatch


@dataclass
class QueryExecutionStats:
    snapshot_id_batch: SnapshotIdBatch
    total_rows_processed: t.Optional[int] = None
    total_bytes_processed: t.Optional[int] = None


@dataclass
class QueryExecutionContext:
    """
    Container for tracking rows processed or other execution information during snapshot evaluation.

    It accumulates statistics from multiple cursor.execute() calls during a single snapshot evaluation.

    Attributes:
        snapshot_id_batch: Identifier linking this context to a specific snapshot evaluation
        stats: Running sum of cursor.rowcount and possibly bytes processed from all executed queries during evaluation
    """

    snapshot_id_batch: SnapshotIdBatch
    stats: QueryExecutionStats = field(init=False)

    def __post_init__(self) -> None:
        self.stats = QueryExecutionStats(snapshot_id_batch=self.snapshot_id_batch)

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

    def get_execution_stats(self) -> QueryExecutionStats:
        return self.stats


class QueryExecutionTracker:
    """Thread-local context manager for snapshot execution statistics, such as rows processed."""

    _thread_local = local()
    _contexts: t.Dict[SnapshotIdBatch, QueryExecutionContext] = {}
    _contexts_lock = Lock()

    def get_execution_context(
        self, snapshot_id_batch: SnapshotIdBatch
    ) -> t.Optional[QueryExecutionContext]:
        with self._contexts_lock:
            return self._contexts.get(snapshot_id_batch)

    @classmethod
    def is_tracking(cls) -> bool:
        return getattr(cls._thread_local, "context", None) is not None

    @contextmanager
    def track_execution(
        self, snapshot_id_batch: SnapshotIdBatch
    ) -> t.Iterator[t.Optional[QueryExecutionContext]]:
        """Context manager for tracking snapshot execution statistics such as row counts and bytes processed."""
        context = QueryExecutionContext(snapshot_id_batch=snapshot_id_batch)
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

    def get_execution_stats(
        self, snapshot_id_batch: SnapshotIdBatch
    ) -> t.Optional[QueryExecutionStats]:
        with self._contexts_lock:
            context = self._contexts.get(snapshot_id_batch)
            self._contexts.pop(snapshot_id_batch, None)
        return context.get_execution_stats() if context else None
