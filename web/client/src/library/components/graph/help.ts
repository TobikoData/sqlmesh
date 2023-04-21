import ELK, { type ElkNode } from 'elkjs/lib/elk.bundled.js'
import { isArrayNotEmpty, isFalse, isNil, isObjectEmpty } from '../../../utils'
import { type Model } from '@api/client'
import { Position, type Edge, type Node, type XYPosition } from 'reactflow'
import { type Lineage } from '@context/editor'

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
  lineage,
  highlightedNodes,
  models,
  nodes = [],
}: {
  lineage: Record<string, Lineage>
  highlightedNodes: string[]
  models: Map<string, Model>
  nodes: Node[]
}): {
  nodesMap: Record<string, Node>
  edges: Edge[]
  nodes: Node[]
  columns: Record<string, { ins: string[]; outs: string[] }>
} {
  const targets = new Set(
    Object.values(lineage)
      .map(l => l.models)
      .flat(),
  )
  const modelNames = Object.keys(lineage)
  const nodesMap = getNodeMap(
    modelNames,
    lineage,
    highlightedNodes,
    models,
    targets,
    nodes,
  )
  const edges: Edge[] = []
  const columns: Record<string, { ins: string[]; outs: string[] }> = {}

  for (const modelSource of modelNames) {
    const modelLineage = lineage[modelSource]

    if (modelLineage == null) continue

    modelLineage.models.forEach(modelTarget => {
      edges.push(createGraphEdge(modelSource, modelTarget))
    })

    if (modelLineage.columns == null || isObjectEmpty(modelLineage.columns))
      continue

    for (const columnSource in modelLineage.columns) {
      const sourceId = toNodeOrEdgeId(modelSource, columnSource)
      const modelsTarget = modelLineage.columns[columnSource]?.models

      if (columns[sourceId] == null) {
        columns[sourceId] = {
          ins: [],
          outs: [],
        }
      }

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
            createGraphEdge(
              modelSource,
              modelTarget,
              toNodeOrEdgeId('source', modelSource, columnSource),
              toNodeOrEdgeId('target', modelTarget, columnTarget),
              false,
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

  return {
    edges,
    nodes: Object.values(nodesMap),
    nodesMap,
    columns,
  }
}

export async function createGraphLayout({
  nodes = [],
  edges = [],
  nodesMap,
}: {
  nodes: Node[]
  edges: Edge[]
  nodesMap: Record<string, Node>
}): Promise<{ nodes: Node[]; edges: Edge[] }> {
  const layout = await elk.layout({
    id: 'root',
    layoutOptions: { algorithm: 'layered' },
    children: nodes.map(node => ({
      id: node.id,
      width: node.data.width,
      height: node.data.height,
    })),
    edges: edges.map(edge => ({
      id: edge.id,
      sources: [edge.source],
      targets: [edge.target],
    })),
  })

  return {
    edges,
    nodes: repositionNodes(layout.children, nodesMap),
  }
}

function getNodeMap(
  modelNames: string[],
  lineage: Record<string, Lineage>,
  highlightedNodes: string[],
  models: Map<string, Model>,
  targets: Set<string>,
  nodes: Node[],
): Record<string, Node> {
  const current = nodes.reduce(
    (acc: Record<string, Node>, node) =>
      Object.assign(acc, { [node.id]: node }),
    {},
  )

  return modelNames.reduce((acc: Record<string, Node>, label: string) => {
    const node =
      current[label] ??
      createGraphNode({
        label,
        isHighlighted: highlightedNodes.includes(label),
        isInteractive:
          isArrayNotEmpty(highlightedNodes) &&
          isFalse(highlightedNodes.includes(label)),
        width: NODE_WIDTH,
        height: NODE_HEIGHT + 32 * (models.get(label)?.columns?.length ?? 0),
      })

    if (isArrayNotEmpty(lineage[node.id]?.models)) {
      node.sourcePosition = Position.Left
    }

    if (targets.has(node.id)) {
      node.targetPosition = Position.Right
    }

    acc[label] = node

    return acc
  }, {})
}

function repositionNodes(
  elkNodes: ElkNode[] = [],
  nodesMap: Record<string, Node>,
): Node[] {
  const nodes: Node[] = []

  elkNodes.forEach(node => {
    const output = nodesMap[node.id]

    if (output == null) return

    if (output.position.x === 0 && node.x != null) {
      output.position.x = -node.x * 2
    }

    if (output.position.y === 0 && node.y != null) {
      output.position.y = -node.y * 1.5
    }

    nodes.push(output)
  })

  return nodes
}

function createGraphNode(
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
    style: {
      zIndex: 'auto',
    },
  }
}

function createGraphEdge<TData = any>(
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
