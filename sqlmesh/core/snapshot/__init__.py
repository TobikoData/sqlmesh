from sqlmesh.core.snapshot.definition import (
    Intervals,
    QualifiedViewName,
    Snapshot,
    SnapshotChangeCategory,
    SnapshotDataVersion,
    SnapshotId,
    SnapshotIdLike,
    SnapshotInfoLike,
    SnapshotTableInfo,
    fingerprint_from_model,
    merge_intervals,
    table_name,
)
from sqlmesh.core.snapshot.evaluator import SnapshotEvaluator
