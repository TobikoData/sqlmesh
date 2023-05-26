from sqlmesh.core.snapshot.categorizer import categorize_change
from sqlmesh.core.snapshot.definition import (
    Intervals,
    QualifiedViewName,
    Snapshot,
    SnapshotChangeCategory,
    SnapshotDataVersion,
    SnapshotFingerprint,
    SnapshotId,
    SnapshotIdLike,
    SnapshotInfoLike,
    SnapshotIntervals,
    SnapshotNameVersion,
    SnapshotNameVersionLike,
    SnapshotTableInfo,
    fingerprint_from_model,
    has_paused_forward_only,
    merge_intervals,
    table_name,
    to_table_mapping,
)
from sqlmesh.core.snapshot.evaluator import SnapshotEvaluator
