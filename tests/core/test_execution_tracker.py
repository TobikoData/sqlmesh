from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from sqlmesh.core.snapshot.execution_tracker import QueryExecutionStats, QueryExecutionTracker
from sqlmesh.core.snapshot import SnapshotIdBatch, SnapshotId


def test_execution_tracker_thread_isolation() -> None:
    def worker(id: SnapshotId, row_counts: list[int]) -> QueryExecutionStats:
        with execution_tracker.track_execution(SnapshotIdBatch(snapshot_id=id, batch_id=0)) as ctx:
            assert execution_tracker.is_tracking()

            for count in row_counts:
                execution_tracker.record_execution("SELECT 1", count, None)

            assert ctx is not None
            return ctx.get_execution_stats()

    execution_tracker = QueryExecutionTracker()

    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(worker, SnapshotId(name="batch_A", identifier="batch_A"), [10, 5]),
            executor.submit(worker, SnapshotId(name="batch_B", identifier="batch_B"), [3, 7]),
        ]
        results = [f.result() for f in futures]

    # Main thread has no active tracking context
    assert not execution_tracker.is_tracking()

    # Order of results is not deterministic, so look up by id
    by_batch = {s.snapshot_id_batch: s for s in results}

    assert (
        by_batch[
            SnapshotIdBatch(
                snapshot_id=SnapshotId(name="batch_A", identifier="batch_A"), batch_id=0
            )
        ].total_rows_processed
        == 15
    )
    assert (
        by_batch[
            SnapshotIdBatch(
                snapshot_id=SnapshotId(name="batch_B", identifier="batch_B"), batch_id=0
            )
        ].total_rows_processed
        == 10
    )
