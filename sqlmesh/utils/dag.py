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
        self._upstream: t.Dict[T, t.Set[T]] = {}

        for node, dependencies in (graph or {}).items():
            self.add(node, dependencies)

    def add(self, node: T, dependencies: t.Optional[t.Iterable[T]] = None) -> None:
        """Add a node to the graph with an optional upstream dependency.

        Args:
            node: The node to add.
            dependencies: Optional dependencies to add to the node.
        """
        self._sorted = None
        self._upstream.clear()
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
            A new dag consisting of the specified nodes and upstream.
        """
        queue = set(nodes)
        dag: DAG[T] = DAG()

        while queue:
            node = queue.pop()
            deps = self._dag.get(node, set())
            dag.add(node, deps)
            queue.update(deps)

        return dag

    def prune(self, *nodes: T) -> DAG[T]:
        """Create a dag keeping only the included nodes.

        Args:
            nodes: The nodes of the new pruned dag.

        Returns:
            A new dag consisting of the specified nodes.
        """
        dag: DAG[T] = DAG()

        for node, deps in self._dag.items():
            if node in nodes:
                dag.add(node, (dep for dep in deps if dep in nodes))

        return dag

    def upstream(self, node: T) -> t.Set[T]:
        """Returns all upstream dependencies."""
        if node not in self._upstream:
            deps = self._dag.get(node, set())
            self._upstream[node] = {
                upstream for dep in deps for upstream in self.upstream(dep)
            } | deps

        return self._upstream[node]

    @property
    def roots(self) -> t.Set[T]:
        """Returns all nodes in the graph without any upstream dependencies."""
        return {node for node, deps in self._dag.items() if not deps}

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

            last_processed_nodes: t.Set[T] = set()
            cycle_candidates: t.Collection = unprocessed_nodes

            while unprocessed_nodes:
                next_nodes = {node for node, deps in unprocessed_nodes.items() if not deps}

                if not next_nodes:
                    # Sort cycle candidates to make the order deterministic
                    cycle_candidates_msg = (
                        "\nPossible candidates to check for circular references: "
                        + ", ".join(str(node) for node in sorted(cycle_candidates))
                    )

                    if last_processed_nodes:
                        last_processed_msg = "\nLast nodes added to the DAG: " + ", ".join(
                            str(node) for node in last_processed_nodes
                        )
                    else:
                        last_processed_msg = ""

                    raise SQLMeshError(
                        "Detected a cycle in the DAG. "
                        "Please make sure there are no circular references between nodes."
                        f"{last_processed_msg}{cycle_candidates_msg}"
                    )

                for node in next_nodes:
                    unprocessed_nodes.pop(node)

                nodes_with_unaffected_deps: t.Set[T] = set()
                for node, deps in unprocessed_nodes.items():
                    deps_before_subtraction = deps

                    deps -= next_nodes
                    if deps_before_subtraction == deps:
                        nodes_with_unaffected_deps.add(node)

                cycle_candidates = nodes_with_unaffected_deps or unprocessed_nodes

                # Sort to make the order deterministic
                # TODO: Make protocol that makes the type var both hashable and sortable once we are on Python 3.8+
                last_processed_nodes = sorted(next_nodes)  # type: ignore
                self._sorted.extend(last_processed_nodes)

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
