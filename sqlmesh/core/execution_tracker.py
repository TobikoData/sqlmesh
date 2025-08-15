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
        """Record a single query execution."""
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
    Thread-local context manager for snapshot evaluation execution statistics, such as
    rows processed.
    """

    _thread_local = local()

    @classmethod
    def get_execution_context(cls) -> t.Optional[QueryExecutionContext]:
        return getattr(cls._thread_local, "context", None)

    @classmethod
    def is_tracking(cls) -> bool:
        return cls.get_execution_context() is not None

    @classmethod
    @contextmanager
    def track_execution(cls, snapshot_name_batch: str) -> t.Iterator[QueryExecutionContext]:
        """
        Context manager for tracking snapshot evaluation execution statistics.
        """
        context = QueryExecutionContext(id=snapshot_name_batch)
        cls._thread_local.context = context
        try:
            yield context
        finally:
            if hasattr(cls._thread_local, "context"):
                delattr(cls._thread_local, "context")

    @classmethod
    def record_execution(cls, sql: str, row_count: t.Optional[int]) -> None:
        context = cls.get_execution_context()
        if context is not None:
            context.add_execution(sql, row_count)

    @classmethod
    def get_execution_stats(cls) -> t.Optional[t.Dict[str, t.Any]]:
        context = cls.get_execution_context()
        return context.get_execution_stats() if context else None


class SeedExecutionTracker:
    _seed_contexts: t.Dict[str, QueryExecutionContext] = {}
    _thread_local = local()

    @classmethod
    @contextmanager
    def track_execution(cls, model_name: str) -> t.Iterator[QueryExecutionContext]:
        """
        Context manager for tracking seed creation execution statistics.
        """
        context = QueryExecutionContext(id=model_name)
        cls._seed_contexts[model_name] = context
        cls._thread_local.seed_id = model_name

        try:
            yield context
        finally:
            if hasattr(cls._thread_local, "seed_id"):
                delattr(cls._thread_local, "seed_id")

    @classmethod
    def get_and_clear_seed_stats(cls, model_name: str) -> t.Optional[t.Dict[str, t.Any]]:
        context = cls._seed_contexts.pop(model_name, None)
        return context.get_execution_stats() if context else None

    @classmethod
    def clear_all_seed_stats(cls) -> None:
        """Clear all remaining seed stats. Used for cleanup after evaluation completes."""
        cls._seed_contexts.clear()

    @classmethod
    def is_tracking(cls) -> bool:
        return hasattr(cls._thread_local, "seed_id")

    @classmethod
    def record_execution(cls, sql: str, row_count: t.Optional[int]) -> None:
        seed_id = getattr(cls._thread_local, "seed_id", None)
        if seed_id:
            context = cls._seed_contexts.get(seed_id)
            if context is not None:
                context.add_execution(sql, row_count)


def record_execution(sql: str, row_count: t.Optional[int]) -> None:
    """
    Record execution statistics for a single SQL statement.

    Automatically infers which tracker is active based on the current thread.
    """
    if SeedExecutionTracker.is_tracking():
        SeedExecutionTracker.record_execution(sql, row_count)
        return
    if QueryExecutionTracker.is_tracking():
        QueryExecutionTracker.record_execution(sql, row_count)
