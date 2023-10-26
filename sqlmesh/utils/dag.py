"""
# DAG

A DAG, or directed acyclic graph, is a graph where the edges are directional and there are no cycles with
all the edges pointing in the same direction. SQLMesh uses a DAG to keep track of a project's models. This
allows SQLMesh to easily determine a model's lineage and to identify upstream and downstream dependencies.
"""
from __future__ import annotations

import typing as t

from sqlmesh.utils.errors import SQLMeshError

T = t.TypeVar("T", bound=t.Hashable)


class DAG(t.Generic[T]):
    def __init__(self, graph: t.Optional[t.Dict[T, t.Set[T]]] = None):
        self._dag: t.Dict[T, t.Set[T]] = {}
        self._sorted: t.Optional[t.List[T]] = None

        for node, dependencies in (graph or {}).items():
            self.add(node, dependencies)

    def add(self, node: T, dependencies: t.Optional[t.Iterable[T]] = None) -> None:
        """Add a node to the graph with an optional upstream dependency.

        Args:
            node: The node to add.
            dependencies: Optional dependencies to add to the node.
        """
        self._sorted = None
        if node not in self._dag:
            self._dag[node] = set()
        if dependencies:
            self._dag[node].update(dependencies)
            for d in dependencies:
                self.add(d)

    @property
    def reversed(self) -> DAG[T]:
        """Returns a copy of this DAG with all its edges reversed."""
        result = DAG[T]()

        for node, deps in self._dag.items():
            result.add(node)
            for dep in deps:
                result.add(dep, [node])

        return result

    def subdag(self, *nodes: T) -> DAG[T]:
        """Create a new subdag given node(s).

        Args:
            nodes: The nodes of the new subdag.

        Returns:
            A new dag consisting of the specified nodes.
        """
        queue = set(nodes)
        graph = {}

        while queue:
            node = queue.pop()
            deps = self._dag.get(node, set())
            graph[node] = deps
            queue.update(deps)

        return DAG(graph)

    def upstream(self, node: T) -> t.List[T]:
        """Returns all upstream dependencies in topologically sorted order."""
        return self.subdag(node).sorted[:-1]

    @property
    def leaves(self) -> t.Set[T]:
        """Returns all nodes in the graph without any upstream dependencies."""
        return {dep for deps in self._dag.values() for dep in deps if dep not in self._dag}

    @property
    def graph(self) -> t.Dict[T, t.Set[T]]:
        graph = {}
        for node, deps in self._dag.items():
            graph[node] = deps.copy()
        return graph

    @property
    def sorted(self) -> t.List[T]:
        """Returns a list of nodes sorted in topological order."""
        if self._sorted is None:
            self._sorted = []

            unprocessed_nodes = self.graph
            while unprocessed_nodes:
                next_nodes = {node for node, deps in unprocessed_nodes.items() if not deps}

                if not next_nodes:
                    raise SQLMeshError(
                        "Detected a cycle in the DAG. "
                        "Please make sure there are no circular references between models."
                    )

                for node in next_nodes:
                    unprocessed_nodes.pop(node)

                for deps in unprocessed_nodes.values():
                    deps -= next_nodes

                # Sort to make the order deterministic
                # TODO: Make protocol that makes the type var both hashable and sortable once we are on Python 3.8+
                self._sorted.extend(sorted(next_nodes))  # type: ignore

        return self._sorted

    def downstream(self, node: T) -> t.List[T]:
        """Get all nodes that have the input node as an upstream dependency.

        Args:
            node: The ancestor node.

        Returns:
            A list of descendant nodes sorted in topological order.
        """
        sorted_nodes = self.sorted
        try:
            node_index = sorted_nodes.index(node)
        except ValueError:
            return []

        def visit() -> t.Iterator[T]:
            """Visit topologically sorted nodes after input node and yield downstream dependants."""
            downstream = {node}
            for current_node in sorted_nodes[node_index + 1 :]:
                upstream = self._dag.get(current_node, set())
                if not upstream.isdisjoint(downstream):
                    downstream.add(current_node)
                    yield current_node

        return list(visit())

    def lineage(self, node: T) -> DAG[T]:
        """Get a dag of the node and its upstream dependencies and downstream dependents.

        Args:
            node: The node used to determine lineage.

        Returns:
            A new dag consisting of the dependent and descendant nodes.
        """
        return self.subdag(node, *self.downstream(node))

    def __contains__(self, item: T) -> bool:
        return item in self.graph

    def __iter__(self) -> t.Iterator[T]:
        for node in self.sorted:
            yield node
