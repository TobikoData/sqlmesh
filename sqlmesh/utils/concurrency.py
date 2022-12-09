import typing as t
from concurrent.futures import Executor, Future, ThreadPoolExecutor
from threading import Lock

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

    if tasks_num == 1:
        sequential_apply_to_dag(dag, fn, reverse_order)
        return

    unprocessed_nodes = dag.graph if not reverse_order else dag.reversed_graph
    unprocessed_nodes_lock = Lock()
    finished_future = Future()  # type: ignore

    def submit_next_nodes(executor: Executor) -> None:
        with unprocessed_nodes_lock:
            if not unprocessed_nodes:
                finished_future.set_result(None)
                return

            next_nodes = [node for node, deps in unprocessed_nodes.items() if not deps]
            for node in next_nodes:
                unprocessed_nodes.pop(node)
                executor.submit(process_node, node, executor)

    def process_node(node: H, executor: Executor) -> None:
        try:
            fn(node)
        except Exception as ex:
            finished_future.set_exception(ex)
            return

        with unprocessed_nodes_lock:
            for deps in unprocessed_nodes.values():
                deps -= {node}
        submit_next_nodes(executor)

    with ThreadPoolExecutor(max_workers=tasks_num) as pool:
        submit_next_nodes(pool)
        finished_future.result()


def sequential_apply_to_dag(
    dag: DAG[H], fn: t.Callable[[H], None], reverse_order: bool = False
) -> None:
    ordered_nodes = dag.sorted()
    if reverse_order:
        ordered_nodes.reverse()

    for node in ordered_nodes:
        fn(node)
