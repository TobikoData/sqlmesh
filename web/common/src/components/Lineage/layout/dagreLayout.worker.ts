import dagre from 'dagre'

import {
  DEFAULT_NODE_WIDTH,
  type LayoutedGraph,
  type LineageNodeData,
  type NodeId,
} from '../utils'

self.onmessage = <TNodeData extends LineageNodeData = LineageNodeData>(
  event: MessageEvent<LayoutedGraph<TNodeData>>,
) => {
  try {
    const { edges, nodesMap } = event.data
    const nodes = Object.values(nodesMap)
    const nodeCount = nodes.length
    const edgeCount = edges.length
    const maxCount = Math.max(nodeCount, edgeCount)

    if (nodeCount === 0)
      return self.postMessage({
        edges: [],
        nodesMap: {},
      })

    const g = new dagre.graphlib.Graph({
      compound: true,
      multigraph: true,
      directed: true,
    })

    g.setGraph({
      rankdir: 'LR',
      nodesep: 24,
      ranksep: DEFAULT_NODE_WIDTH / 2,
      edgesep: 0,
      ranker: 'longest-path',
    })

    g.setDefaultEdgeLabel(() => ({}))

    for (let i = 0; i < maxCount; i++) {
      if (i < edgeCount) {
        g.setEdge(edges[i].source, edges[i].target)
      }
      if (i < nodeCount) {
        const node = nodes[i]
        g.setNode(node.id, {
          width: node.width || DEFAULT_NODE_WIDTH,
          height: node.height || 0,
        })
      }
    }

    dagre.layout(g)

    for (let i = 0; i < nodeCount; i++) {
      const node = nodes[i]
      const width = node.width || DEFAULT_NODE_WIDTH
      const height = node.height || 0
      const nodeId = node.id as NodeId
      const nodeWithPosition = g.node(nodeId)
      const halfWidth = width >> 1
      const halfHeight = height >> 1

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
    })
  } catch (outerError) {
    self.postMessage({ error: outerError })
  }
}
