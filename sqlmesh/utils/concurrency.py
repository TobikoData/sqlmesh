import typing as t
from concurrent.futures import Future, ThreadPoolExecutor, wait
from threading import Event

from sqlmesh.core.dag import DAG
from sqlmesh.core.snapshot import SnapshotId, SnapshotInfoLike
from sqlmesh.utils.errors import ConfigError

T = t.TypeVar("T", bound=SnapshotInfoLike)


def concurrent_apply_to_snapshots(
    snapshots: t.Iterable[T],
    fn: t.Callable[[T], None],
    tasks_num: int,
    reverse_order: bool = False,
) -> None:
    """Applies a function to the given collection of snapshots concurrently while
    preserving the topological order between snapshots.

    Args:
        snapshots: Target snapshots.
        fn: The function that will be applied concurrently to each snapshot.
        tasks_num: The number of concurrent tasks.
        reverse_order: Whether the order should be reversed. Default: False..
    """
    snapshots_by_id = {s.snapshot_id: s for s in snapshots}

    dag: DAG[SnapshotId] = DAG[SnapshotId]()
    for snapshot in snapshots:
        dag.add(
            snapshot.snapshot_id,
            [p_sid for p_sid in snapshot.parents if p_sid in snapshots_by_id],
        )

    concurrent_apply_to_dag(
        dag,
        lambda s_id: fn(snapshots_by_id[s_id]),
        tasks_num,
        reverse_order=reverse_order,
    )


H = t.TypeVar("H", bound=t.Hashable)


def concurrent_apply_to_dag(
    dag: DAG[H], fn: t.Callable[[H], None], tasks_num: int, reverse_order: bool = False
) -> None:
    """Applies a function to the given DAG concurrently while preserving the topological
    order between snapshots.

    Args:
        dag: The target DAG.
        fn: The function that will be applied concurrently to each snapshot.
        tasks_num: The number of concurrent tasks.
        reverse_order: Whether the order should be reversed. Default: False..
    """
    if tasks_num <= 0:
        raise ConfigError(f"Invalid number of concurrent tasks {tasks_num}")

    ordered_nodes = dag.sorted()
    if reverse_order:
        ordered_nodes.reverse()

    if tasks_num == 1:
        for node in ordered_nodes:
            fn(node)
        return

    future_map: t.Dict[H, Future] = {}
    future_map_ready = Event()

    def process_node(node: H) -> None:
        future_map_ready.wait()

        wait_for = [
            future_map[n]
            for n in (dag.upstream(node) if not reverse_order else dag.downstream(node))
        ]
        wait(wait_for)
        fn(node)

    with ThreadPoolExecutor(max_workers=tasks_num) as pool:
        for node in ordered_nodes:
            future_map[node] = pool.submit(process_node, node)
        future_map_ready.set()

    wait(future_map.values())
