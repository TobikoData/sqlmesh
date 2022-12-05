"""
# DAG

A DAG, or directed acyclic graph, is a graph where the edges are directional and there are no cycles with
all the edges pointing in the same direction. SQLMesh uses a DAG to keep track of a project's models. This
allows SQLMesh to easily determine a model's lineage and to identify upstream and downstream dependencies.
"""
from __future__ import annotations

import typing as t

T = t.TypeVar("T")


class DAG(t.Generic[T]):
    def __init__(self, graph: t.Optional[t.Dict[T, t.Set[T]]] = None):
        self.graph = graph or {}

    def add(self, node: T, dependency: t.Optional[T] = None) -> None:
        """Add a node to the graph with an optional upstream dependency.

        Args:
            node: The node to add.
            dependency: An optional dependency to add to the node.
        """
        if node not in self.graph:
            self.graph[node] = set()
        if dependency:
            self.graph[node].add(dependency)

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
            deps = self.graph.get(node, set())
            graph[node] = deps
            queue.update(deps)

        return DAG(graph)

    def upstream(self, node: T) -> t.List[T]:
        """Returns all upstream dependencies in topologically sorted order."""
        return self.subdag(node).sort()[:-1]

    @property
    def leaves(self) -> t.Set[T]:
        """Returns all nodes in the graph without any upstream dependencies."""
        return {
            dep for deps in self.graph.values() for dep in deps if dep not in self.graph
        }

    def sort(self) -> t.List[T]:
        """Topologically sort the graph.

        Returns:
            A list of the nodes sorted in topological order.

        Raises:
            ValueError: If a cycle has been detected in the graph.
        """
        result = []

        def visit(node: T, visited: t.Set[T]) -> None:
            if node in result:
                return
            if node in visited:
                raise ValueError("Cycle error")

            visited.add(node)

            for dep in self.graph.get(node, []):
                visit(dep, visited)

            visited.remove(node)
            result.append(node)

        for node in self.graph:
            visit(node, set())

        return result

    def downstream(self, node: T) -> t.List[T]:
        """Get all nodes that have the input node as an upstream dependency.

        Args:
            node: The ancestor node.

        Returns:
            A list of descendant nodes sorted in topological order.
        """
        sorted_nodes = self.sort()
        try:
            node_index = sorted_nodes.index(node)
        except ValueError:
            return []

        def visit() -> t.Iterator[T]:
            """Visit topologically sorted nodes after input node and yield downstream dependants."""
            downstream = {node}
            for current_node in sorted_nodes[node_index + 1 :]:
                upstream = self.graph.get(current_node, set())
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
