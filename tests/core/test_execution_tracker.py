# Tests the sqlmesh.core.execution_tracker module
# - creates a scenario where executions will take place in multiple threads
# - generates the scenario with known numbers of rows to be processed
# - tests that the execution tracker correctly tracks the number of rows processed in both threads
# - may use mocks, an existing test project, manually created snapshots, or a duckdb database to create the scenario

from __future__ import annotations

import threading
from queue import Queue
from typing import List, Optional

from sqlmesh.core.execution_tracker import QueryExecutionTracker


def test_execution_tracker_thread_isolation_and_aggregation() -> None:
    """
    Two worker threads each track executions in their own context. Verify:
    - isolation across threads
    - correct aggregation of rows
    - query metadata is captured
    - main thread has no active tracking
    """

    assert not QueryExecutionTracker.is_tracking()
    assert QueryExecutionTracker.get_execution_stats() is None

    counts_a: List[Optional[int]] = [10, 5, None]
    counts_b: List[Optional[int]] = [3, 7]

    start_barrier = threading.Barrier(3)  # 2 workers + main
    results: "Queue[dict]" = Queue()

    def worker(batch_id: str, counts: List[Optional[int]]) -> None:
        with QueryExecutionTracker.track_execution(batch_id) as ctx:
            # tracking active in this thread
            assert QueryExecutionTracker.is_tracking()
            # synchronize start to overlap execution
            start_barrier.wait()
            for c in counts:
                QueryExecutionTracker.record_execution("SELECT 1", c)

            stats = ctx.get_execution_stats()

            assert stats["snapshot_batch"] == batch_id
            assert stats["query_count"] == len(counts)
            results.put(stats)

    t1 = threading.Thread(target=worker, args=("batch_A", counts_a))
    t2 = threading.Thread(target=worker, args=("batch_B", counts_b))

    t1.start()
    t2.start()
    # Release workers at the same time
    start_barrier.wait()
    t1.join()
    t2.join()

    # Main thread has no active tracking context
    assert not QueryExecutionTracker.is_tracking()
    QueryExecutionTracker.record_execution("q", 10)
    assert QueryExecutionTracker.get_execution_stats() is None

    collected = [results.get_nowait(), results.get_nowait()]
    # by name since order is non-deterministic
    by_batch = {s["snapshot_batch"]: s for s in collected}

    stats_a = by_batch["batch_A"]
    assert stats_a["total_rows_processed"] == 15  # 10 + 5 + 0 (None)
    assert stats_a["query_count"] == len(counts_a)

    stats_b = by_batch["batch_B"]
    assert stats_b["total_rows_processed"] == 10  # 3 + 7
    assert stats_b["query_count"] == len(counts_b)
