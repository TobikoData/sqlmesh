import typing as t
from concurrent.futures import ThreadPoolExecutor, wait
from enum import Enum, auto

from sqlmesh.core.snapshot import SnapshotId, SnapshotInfoLike
from sqlmesh.utils.errors import ConfigError


class ConcurrentApplyOrder(Enum):
    UNDEFINED = auto()
    TOPOLOGICAL = auto()
    REVERSED_TOPOLOGICAL = auto()


T = t.TypeVar("T", bound=SnapshotInfoLike)


def concurrent_apply_to_snapshots(
    snapshots: t.Iterable[T],
    fn: t.Callable[[T], None],
    tasks_num: int,
    order: ConcurrentApplyOrder = ConcurrentApplyOrder.UNDEFINED,
) -> None:
    """Applies a function to the given collection of snapshots concurrently while optionally
    preserving the topological order between snapshots.

    Args:
        snapshots: Target snapshots.
        fn: The function that will be applied concurrently to each snapshot.
        tasks_num: The number of concurrent tasks.
        order: The order in which the application should take place. Default: undefined.
    """
    if tasks_num <= 0:
        raise ConfigError(f"Invalid number of concurrent tasks {tasks_num}")

    groups: t.List[t.Iterable[T]] = (
        create_topological_groups(snapshots)
        if order != ConcurrentApplyOrder.UNDEFINED
        else [snapshots]
    )
    if order == ConcurrentApplyOrder.REVERSED_TOPOLOGICAL:
        groups.reverse()

    if tasks_num > 1:
        with ThreadPoolExecutor(max_workers=tasks_num) as pool:
            for group in groups:
                wait([pool.submit(fn, snapshot) for snapshot in group])
    else:
        for group in groups:
            for snapshot in group:
                fn(snapshot)


def create_topological_groups(
    snapshots: t.Iterable[T],
) -> t.List[t.Iterable[T]]:
    """Groups snapshots into sets of snapshots that can be processed concurrently while preserving
    the topological order between snapshots that are part of these groups.

    Args:
        snapshots: Target snapshots.

    Returns:
        The collection of groups sorted in the topological order.
    """
    snapshot_by_ids = {s.snapshot_id: s for s in snapshots}

    upstream_dependencies: t.Dict[SnapshotId, t.Set[SnapshotId]] = {}
    for snapshot in snapshots:
        dependencies = set()
        for p_sid in snapshot.parents:
            if p_sid in snapshot_by_ids:
                dependencies.add(p_sid)
        upstream_dependencies[snapshot.snapshot_id] = dependencies

    groups: t.List[t.Iterable[T]] = []
    while upstream_dependencies:
        next_group = {
            sid: snapshot_by_ids[sid]
            for sid, deps in upstream_dependencies.items()
            if not deps
        }
        next_group_sids = set(next_group)

        for sid in next_group:
            upstream_dependencies.pop(sid)

        for deps in upstream_dependencies.values():
            deps -= next_group_sids

        groups.append(list(next_group.values()))

    return groups
