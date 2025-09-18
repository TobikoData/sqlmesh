import dagre from 'dagre'

import {
  DEFAULT_NODE_WIDTH,
  type LayoutedGraph,
  type LineageEdgeData,
  type LineageNodeData,
  type NodeId,
} from '../utils'

self.onmessage = <
  TNodeData extends LineageNodeData = LineageNodeData,
  TEdgeData extends LineageEdgeData = LineageEdgeData,
>(
  event: MessageEvent<LayoutedGraph<TNodeData, TEdgeData>>,
) => {
  try {
    const { edges, nodesMap } = event.data
    const nodes = Object.values(nodesMap)
    const nodeCount = nodes.length
    const edgeCount = edges.length

    if (nodeCount === 0)
      return self.postMessage({
        edges: [],
        nodesMap: {},
      } as LayoutedGraph<TNodeData, TEdgeData>)

    const g = new dagre.graphlib.Graph({
      compound: true,
      multigraph: true,
      directed: true,
    })

    g.setGraph({
      rankdir: 'LR',
      nodesep: 0,
      ranksep: 48,
      edgesep: 0,
      ranker: 'longest-path',
    })

    g.setDefaultEdgeLabel(() => ({}))

    // Building layout already heavy operation, so trying to optimize it a bit
    for (let i = 0; i < edgeCount; i++) {
      g.setEdge(edges[i].source, edges[i].target)
    }

    for (let i = 0; i < nodeCount; i++) {
      const node = nodes[i]
      g.setNode(node.id, {
        width: node.width || DEFAULT_NODE_WIDTH,
        height: node.height || 0,
      })
    }

    dagre.layout(g)

    // Building layout already heavy operation, so trying to optimize it a bit
    for (let i = 0; i < nodeCount; i++) {
      const node = nodes[i]
      const width = node.width || DEFAULT_NODE_WIDTH
      const height = node.height || 0
      const nodeId = node.id as NodeId
      const nodeWithPosition = g.node(nodeId)
      const halfWidth = width / 2
      const halfHeight = height / 2

      nodesMap[nodeId] = {
        ...node,
        position: {
          x: nodeWithPosition.x - halfWidth,
          y: nodeWithPosition.y - halfHeight,
        },
      }
    }

    self.postMessage({
      edges,
      nodesMap,
    } as LayoutedGraph<TNodeData, TEdgeData>)
  } catch (outerError) {
    self.postMessage({ error: outerError } as { error: ErrorEvent })
  }
}
