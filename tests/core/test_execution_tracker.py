from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from sqlmesh.core.snapshot.execution_tracker import QueryExecutionStats, QueryExecutionTracker


def test_execution_tracker_thread_isolation() -> None:
    def worker(id: str, row_counts: list[int]) -> QueryExecutionStats:
        with execution_tracker.track_execution(id) as ctx:
            assert execution_tracker.is_tracking()

            for count in row_counts:
                execution_tracker.record_execution("SELECT 1", count, None)

            assert ctx is not None
            return ctx.get_execution_stats()

    execution_tracker = QueryExecutionTracker()

    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(worker, "batch_A", [10, 5]),
            executor.submit(worker, "batch_B", [3, 7]),
        ]
        results = [f.result() for f in futures]

    # Main thread has no active tracking context
    assert not execution_tracker.is_tracking()
    execution_tracker.record_execution("q", 10, None)
    assert execution_tracker.get_execution_stats("q") is None

    # Order of results is not deterministic, so look up by id
    by_batch = {s.snapshot_batch_id: s for s in results}

    assert by_batch["batch_A"].total_rows_processed == 15
    assert by_batch["batch_B"].total_rows_processed == 10
