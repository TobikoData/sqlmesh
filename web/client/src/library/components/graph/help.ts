import ELK, { type ElkNode } from 'elkjs/lib/elk-api'
import { isArrayNotEmpty, isNil } from '../../../utils'
import { type Model } from '@api/client'
import { Position, type Edge, type Node, type XYPosition } from 'reactflow'

interface GraphNodeData {
  label: string
  [key: string]: any
}

interface GraphOptions {
  data: Record<string, string[]>
  models: Map<string, Model>
}

const elk = new ELK()

const NODE_WIDTH = 256
const NODE_HEIGHT = 128

export function getNodesAndEdges({ data, models }: GraphOptions): {
  nodesMap: Record<string, Node>
  edges: Edge[]
  targets: Set<string>
  columns: Record<string, { ins: string[]; outs: string[] }>
} {
  const targets = new Set(Object.values(data).flat())
  const modelNames = Object.keys(data)
  const nodesMap: Record<string, Node> = modelNames.reduce(
    (acc, label) => Object.assign(acc, { [label]: toGraphNode({ label }) }),
    {},
  )
  const edges: Edge[] = []
  const columns: Record<string, { ins: string[]; outs: string[] }> = {}

  modelNames.forEach(source => {
    const columnsSource = models.get(source)?.columns ?? []

    data[source]?.forEach(target => {
      const columnsTarget = models.get(target)?.columns ?? []

      edges.push(toGraphEdge(source, target))

      columnsSource?.forEach(columnSource => {
        const sourceId = `${source}_${columnSource.name}`

        if (columns[sourceId] == null) {
          columns[sourceId] = {
            ins: [],
            outs: [],
          }
        }

        columnsTarget?.forEach(columnTarget => {
          const targetId = `${target}_${columnTarget.name}`

          if (columns[targetId] == null) {
            columns[targetId] = {
              ins: [],
              outs: [],
            }
          }

          columns[sourceId]?.ins.push(targetId)
          columns[targetId]?.outs.push(sourceId)

          edges.push(
            toGraphEdge(
              source,
              target,
              `source_${source}_${columnSource.name}`,
              `target_${target}_${columnTarget.name}`,
              true,
              {
                target,
                source,
                columnSource,
                columnTarget,
              },
            ),
          )
        })
      })
    })
  })

  return { targets, edges, nodesMap, columns }
}

export function createGraph({
  nodesMap,
  edges = [],
}: {
  nodesMap: Record<string, Node>
  edges: Edge[]
}): ElkNode {
  return {
    id: 'root',
    layoutOptions: { algorithm: 'layered' },
    children: Object.values(nodesMap).map(node => ({
      id: node.id,
      width: NODE_WIDTH,
      height: NODE_HEIGHT,
    })),
    edges: edges.map(edge => ({
      id: edge.id,
      sources: [edge.source],
      targets: [edge.target],
    })),
  }
}

export async function createGraphLayout({
  data,
  nodesMap,
  edges = [],
  targets,
  lineage,
}: {
  data: Record<string, string[]>
  nodesMap: Record<string, Node>
  edges: Edge[]
  targets: Set<string>
  lineage: ElkNode
}): Promise<{ nodes: Node[]; edges: Edge[] }> {
  const layout = await elk.layout(lineage)
  const nodes: Node[] = []

  layout.children?.forEach(node => {
    const output = nodesMap[node.id]

    if (output == null) return

    if (isArrayNotEmpty(data[node.id])) {
      output.sourcePosition = Position.Left
    }

    if (targets.has(node.id)) {
      output.targetPosition = Position.Right
    }

    output.position = {
      x: node.x == null ? 0 : -node.x * 2,
      y: node.y == null ? 0 : -node.y * 1.5,
    }

    nodes.push(output)
  })

  return { nodes, edges }
}

function toGraphNode(
  data: GraphNodeData,
  type: string = 'model',
  position: XYPosition = { x: 0, y: 0 },
  hidden: boolean = false,
): Node {
  return {
    id: data.label,
    dragHandle: '.drag-handle',
    type,
    position,
    hidden,
    data,
    connectable: false,
    selectable: false,
    deletable: false,
    focusable: false,
  }
}

function toGraphEdge<TData = any>(
  source: string,
  target: string,
  sourceHandle?: string,
  targetHandle?: string,
  hidden: boolean = false,
  data?: TData,
): Edge<TData> {
  const id = `${source}_${target}${sourceHandle != null ? `_${sourceHandle}` : ''
    }${targetHandle != null ? `_${targetHandle}` : ''}`

  const output: Edge = {
    id,
    source,
    target,
    hidden,
    data,
    style: {
      strokeWidth: isNil(sourceHandle) && isNil(sourceHandle) ? 3 : 1,
      stroke:
        isNil(sourceHandle) && isNil(sourceHandle)
          ? 'var(--color-graph-edge-main)'
          : 'var(--color-graph-edge-secondary)',
    },
  }

  if (sourceHandle != null) {
    output.sourceHandle = sourceHandle
  }

  if (targetHandle != null) {
    output.targetHandle = targetHandle
  }

  return output
}
