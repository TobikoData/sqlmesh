import ELK, { type ElkNode } from 'elkjs/lib/elk.bundled.js'
import { isArrayNotEmpty, isFalse, isNil } from '../../../utils'
import { type Dag, type Model } from '@api/client'
import { Position, type Edge, type Node, type XYPosition } from 'reactflow'

export interface GraphNodeData {
  label: string
  isHighlighted: boolean
  isInteractive: boolean
  [key: string]: any
}

const elk = new ELK()

const NODE_WIDTH = 172
const NODE_HEIGHT = 32

export function getNodesAndEdges({
  data,
  highlightedNodes,
}: {
  data: Record<string, Dag>
  highlightedNodes: string[]
}): {
  nodesMap: Record<string, Node>
  edges: Edge[]
  targets: Set<string>
  columns: Record<string, { ins: string[]; outs: string[] }>
} {
  const targets = new Set(
    Object.values(data)
      .map(item => item.models)
      .flat(),
  )
  const modelNames = Object.keys(data)
  const nodesMap: Record<string, Node> = modelNames.reduce(
    (acc, label) =>
      Object.assign(acc, {
        [label]: toGraphNode({
          label,
          isHighlighted: highlightedNodes.includes(label),
          isInteractive:
            isArrayNotEmpty(highlightedNodes) &&
            isFalse(highlightedNodes.includes(label)),
        }),
      }),
    {},
  )
  const edges: Edge[] = []
  const columns: Record<string, { ins: string[]; outs: string[] }> = {}

  for (const modelSource of modelNames) {
    const dag = data[modelSource]

    if (dag == null) continue

    const modelsTarget = dag.models
    const columnsLineage = dag.columns

    modelsTarget.forEach(modelTarget => {
      edges.push(toGraphEdge(modelSource, modelTarget))
    })

    if (columnsLineage == null) continue

    for (const columnSource in columnsLineage) {
      const sourceId = toNodeOrEdgeId(modelSource, columnSource)

      if (columns[sourceId] == null) {
        columns[sourceId] = {
          ins: [],
          outs: [],
        }
      }

      const modelsTarget = columnsLineage[columnSource]

      if (modelsTarget == null) continue

      for (const modelTarget in modelsTarget) {
        const columnsTarget = modelsTarget[modelTarget]

        if (columnsTarget == null) continue

        for (const columnTarget of columnsTarget) {
          const targetId = toNodeOrEdgeId(modelTarget, columnTarget)

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
              modelSource,
              modelTarget,
              toNodeOrEdgeId('source', modelSource, columnSource),
              toNodeOrEdgeId('target', modelTarget, columnTarget),
              true,
              {
                target: modelTarget,
                source: modelSource,
                columnSource,
                columnTarget,
              },
            ),
          )
        }
      }
    }
  }

  return { targets, edges, nodesMap, columns }
}

export function createGraph({
  nodesMap,
  edges = [],
  models,
}: {
  nodesMap: Record<string, Node>
  edges: Edge[]
  models: Map<string, Model>
}): ElkNode {
  return {
    id: 'root',
    layoutOptions: { algorithm: 'layered' },
    children: Object.values(nodesMap).map(node => ({
      id: node.id,
      width: NODE_WIDTH,
      height: NODE_HEIGHT + 32 * (models.get(node.id)?.columns?.length ?? 0),
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
  data: Record<string, Dag>
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

    if (isArrayNotEmpty(data[node.id]?.models)) {
      output.sourcePosition = Position.Left
    }

    if (targets.has(node.id)) {
      output.targetPosition = Position.Right
    }

    output.position = {
      x: node.x == null ? 0 : -node.x * 2,
      y: node.y == null ? 0 : -node.y * 1.25,
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
  const output: Edge = {
    id: toNodeOrEdgeId(source, target, sourceHandle, targetHandle),
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

export function toNodeOrEdgeId(...args: Array<string | undefined>): string {
  return args.filter(Boolean).join('__')
}
