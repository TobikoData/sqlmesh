import typing as t
from concurrent.futures import Executor, Future, ThreadPoolExecutor
from threading import Lock

from sqlmesh.core.dag import DAG
from sqlmesh.core.snapshot import SnapshotId, SnapshotInfoLike
from sqlmesh.utils.errors import ConfigError, SQLMeshError

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

    Raises:
        NodeExecutionFailedError when execution fails for any snapshot.
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

    Raises:
        NodeExecutionFailedError when execution fails for any node.
    """
    if tasks_num <= 0:
        raise ConfigError(f"Invalid number of concurrent tasks {tasks_num}")

    if tasks_num == 1:
        sequential_apply_to_dag(dag, fn, reverse_order)
        return

    unprocessed_nodes = dag.graph if not reverse_order else dag.reversed_graph
    unprocessed_nodes_num = len(unprocessed_nodes)
    unprocessed_nodes_lock = Lock()
    finished_future = Future()  # type: ignore

    def submit_next_nodes(
        executor: Executor, processed_node: t.Optional[H] = None
    ) -> None:
        if not unprocessed_nodes_num:
            finished_future.set_result(None)
            return

        submitted_nodes = []
        for next_node, deps in unprocessed_nodes.items():
            if processed_node:
                deps.discard(processed_node)
            if not deps:
                executor.submit(process_node, next_node, executor)
                submitted_nodes.append(next_node)
        for submitted_node in submitted_nodes:
            unprocessed_nodes.pop(submitted_node)

    def process_node(node: H, executor: Executor) -> None:
        try:
            fn(node)

            with unprocessed_nodes_lock:
                nonlocal unprocessed_nodes_num
                unprocessed_nodes_num -= 1
                submit_next_nodes(executor, node)
        except Exception as ex:
            error = NodeExecutionFailedError(node)
            error.__cause__ = ex
            finished_future.set_exception(error)

    with ThreadPoolExecutor(max_workers=tasks_num) as pool:
        with unprocessed_nodes_lock:
            submit_next_nodes(pool)
        finished_future.result()


def sequential_apply_to_dag(
    dag: DAG[H], fn: t.Callable[[H], None], reverse_order: bool = False
) -> None:
    ordered_nodes = dag.sorted()
    if reverse_order:
        ordered_nodes.reverse()

    for node in ordered_nodes:
        try:
            fn(node)
        except Exception as ex:
            raise NodeExecutionFailedError(node) from ex


class NodeExecutionFailedError(t.Generic[H], SQLMeshError):
    def __init__(self, node: H):
        self.node = node
        super().__init__(f"Execution failed for node {node}")
