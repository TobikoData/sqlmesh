import {
  DEFAULT_NODE_WIDTH,
  type EdgeId,
  type LineageEdge,
  type LineageEdgeData,
  type LineageNodeData,
  type LineageNodesMap,
  type NodeId,
  type PortId,
} from '../utils'
import dagre from 'dagre'

export function buildLayout<
  TNodeData extends LineageNodeData = LineageNodeData,
  TEdgeData extends LineageEdgeData = LineageEdgeData,
  TEdgeID extends string = EdgeId,
  TSourceID extends string = NodeId,
  TTargetID extends string = NodeId,
  TSourceHandleID extends string = PortId,
  TTargetHandleID extends string = PortId,
>({
  edges,
  nodesMap,
  shouldReuseExistingPosition = true,
}: {
  edges: LineageEdge<
    TEdgeData,
    TEdgeID,
    TSourceID,
    TTargetID,
    TSourceHandleID,
    TTargetHandleID
  >[]
  nodesMap: LineageNodesMap<TNodeData>
  shouldReuseExistingPosition?: boolean
}) {
  const nodes = Object.values(nodesMap)
  const nodeCount = nodes.length
  const edgeCount = edges.length

  if (nodeCount === 0) return {}

  const g = new dagre.graphlib.Graph({
    compound: true,
    multigraph: true,
    directed: true,
  })

  g.setGraph({
    rankdir: 'LR',
    nodesep: 12,
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
    const isDefaultPosition = node.position.x === 0 && node.position.y === 0

    nodesMap[nodeId] = {
      ...node,
      position: {
        x:
          shouldReuseExistingPosition && isDefaultPosition
            ? nodeWithPosition.x - halfWidth
            : node.position.x,
        y:
          shouldReuseExistingPosition && isDefaultPosition
            ? nodeWithPosition.y - halfHeight
            : node.position.y,
      },
    }
  }

  return { ...nodesMap }
}
