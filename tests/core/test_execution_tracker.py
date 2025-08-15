from __future__ import annotations

import typing as t
from concurrent.futures import ThreadPoolExecutor

from sqlmesh.core.execution_tracker import QueryExecutionTracker


def test_execution_tracker_thread_isolation() -> None:
    def worker(id: str, row_counts: list[int]) -> t.Dict[str, t.Any]:
        with QueryExecutionTracker.track_execution(id) as ctx:
            assert QueryExecutionTracker.is_tracking()

            for count in row_counts:
                QueryExecutionTracker.record_execution("SELECT 1", count)

            assert ctx is not None
            return ctx.get_execution_stats()

    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(worker, "batch_A", [10, 5]),
            executor.submit(worker, "batch_B", [3, 7]),
        ]
        results = [f.result() for f in futures]

    # Main thread has no active tracking context
    assert not QueryExecutionTracker.is_tracking()
    QueryExecutionTracker.record_execution("q", 10)
    assert QueryExecutionTracker.get_execution_stats("q") is None

    # Order of results is not deterministic, so look up by id
    by_batch = {s["id"]: s for s in results}

    assert by_batch["batch_A"]["total_rows_processed"] == 15
    assert by_batch["batch_A"]["query_count"] == 2
    assert by_batch["batch_B"]["total_rows_processed"] == 10
    assert by_batch["batch_B"]["query_count"] == 2
