import ELK from 'elkjs/lib/elk-api'
import { MarkerType } from 'reactflow'
import { isArrayNotEmpty } from '../../../utils'

interface GraphNodeData {
  label: string
  [key: string]: any
}

interface GraphNodePosition {
  x: number
  y: number
}

interface GraphNode {
  id: string
  type: string
  position: GraphNodePosition
  data: GraphNodeData
  connectable: boolean
  selectable: boolean
  deletable: boolean
  focusable: boolean
  sourcePosition?: 'left' | 'right'
  targetPosition?: 'left' | 'right'
}

interface GraphEdge {
  id: string
  source: string
  target: string
  style: {
    strokeWidth: number
    stroke: string
  }
  markerStart: {
    type: MarkerType
    color: string
    width: number
    height: number
  }
}

interface GraphOptions {
  data?: Record<string, string[]>
  nodeWidth?: number
  nodeHeight?: number
  algorithm?: string
}

const elk = new ELK({
  workerUrl: '/node_modules/elkjs/lib/elk-worker.min.js',
})

const ALGORITHM_LAYERED = 'layered'

export async function getNodesAndEdges({
  data,
  nodeWidth = 172,
  nodeHeight = 32,
}: GraphOptions): Promise<{ nodes: GraphNode[]; edges: GraphEdge[] }> {
  if (data == null) return await Promise.resolve({ nodes: [], edges: [] })

  const targets = new Set(Object.values(data).flat())
  const models = Object.keys(data)
  const nodesMap: Record<string, GraphNode> = models.reduce(
    (acc, label) => Object.assign(acc, { [label]: toGraphNode({ label }) }),
    {},
  )
  const edges = models.map(source => getNodeEdges(data[source], source)).flat()

  const graph = {
    id: 'root',
    layoutOptions: { algorithm: ALGORITHM_LAYERED },
    children: Object.values(nodesMap).map(node => ({
      id: node.id,
      width: nodeWidth,
      height: nodeHeight,
    })),
    edges: edges.map(edge => ({
      id: edge.id,
      sources: [edge.source],
      targets: [edge.target],
    })),
  }

  const layout = await elk.layout(graph)
  const nodes: GraphNode[] = []

  layout.children?.forEach((node, idx: number) => {
    const output = nodesMap[node.id]

    if (output == null) return

    if (isArrayNotEmpty(data[node.id])) {
      output.sourcePosition = 'left'
    }

    if (targets.has(node.id)) {
      output.targetPosition = 'right'
    }

    output.position = {
      x: node.x == null ? 0 : -node.x,
      y: node.y == null ? 0 : -node.y,
    }

    nodes.push(output)
  })

  return { nodes, edges }
}

function getNodeEdges(targets: string[] = [], source: string): GraphEdge[] {
  return targets.map(target => toGraphEdge(source, target))
}

function toGraphNode(
  data: GraphNodeData,
  type: string = 'model',
  position: GraphNodePosition = { x: 0, y: 0 },
): GraphNode {
  return {
    id: data.label,
    type,
    position,
    data,
    connectable: false,
    selectable: false,
    deletable: false,
    focusable: false,
  }
}

function toGraphEdge(source: string, target: string): GraphEdge {
  const id = `${source}_${target}`

  return {
    id,
    source,
    target,
    style: {
      strokeWidth: 2,
      stroke: 'var(--color-graph-edge)', //
    },
    markerStart: {
      color: '#0066FF',
      type: MarkerType.ArrowClosed,
      width: 12,
      height: 12,
    },
  }
}
