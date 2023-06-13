import ELK, { type ElkNode } from 'elkjs/lib/elk.bundled.js'
import { isArrayNotEmpty, isFalse, isNil, isObjectEmpty } from '../../../utils'
import { type LineageColumn, type Column, type Model } from '@api/client'
import { Position, type Edge, type Node, type XYPosition } from 'reactflow'
import { type Lineage } from '@context/editor'
import { type ModelSQLMeshModel } from '@models/sqlmesh-model'
import { type Connections } from './context'
import { EnumSide } from '~/types/enum'

export interface GraphNodeData {
  type: string
  label: string
  isInteractive?: boolean
  highlightedNodes?: Record<string, string[]>
  [key: string]: any
}

export {
  getNodesAndEdges,
  createGraphLayout,
  toNodeOrEdgeId,
  mergeLineageWithModels,
  mergeLineageWithColumns,
  hasNoModels,
  mergeConnections,
}

async function createGraphLayout({
  nodes = [],
  edges = [],
  nodesMap,
}: {
  nodes: Node[]
  edges: Edge[]
  nodesMap: Record<string, Node>
}): Promise<{ nodes: Node[]; edges: Edge[] }> {
  const elk = new ELK()
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

function getNodesAndEdges({
  models,
  highlightedNodes = {},
  lineage = {},
  nodes = [],
  edges = [],
  model,
  withColumns = true,
}: {
  models: Map<string, Model>
  highlightedNodes?: Record<string, string[]>
  lineage?: Record<string, Lineage>
  nodes?: Node[]
  edges?: Edge[]
  model: ModelSQLMeshModel
  withColumns?: boolean
}): {
  nodesMap: Record<string, Node>
  edges: Edge[]
  nodes: Node[]
} {
  const currentEdges = edges.reduce(
    (acc: Record<string, Edge>, edge) =>
      Object.assign(acc, { [edge.id]: edge }),
    {},
  )
  const sources = new Set(
    Object.values(lineage)
      .map(l => l.models)
      .flat(),
  )
  const modelNames = Object.keys(lineage)
  const nodesMap = getNodeMap({
    modelNames,
    lineage,
    highlightedNodes,
    models,
    sources,
    nodes,
    model,
    withColumns,
  })
  const outputEdges: Edge[] = []

  for (const targetModelName of modelNames) {
    const targetModel = lineage[targetModelName]!

    targetModel.models.forEach(sourceModelName => {
      outputEdges.push(createGraphEdge(sourceModelName, targetModelName))
    })

    if (isObjectEmpty(targetModel?.columns) || isFalse(withColumns)) continue

    for (const targetColumnName in targetModel.columns) {
      const sourceModel = targetModel.columns[targetColumnName]

      if (sourceModel == null || sourceModel.models == null) continue

      for (const sourceModelName in sourceModel.models) {
        const sourceColumns = sourceModel.models[sourceModelName]

        if (sourceColumns == null) continue

        for (const sourceColumnName of sourceColumns) {
          const sourceHandler = toNodeOrEdgeId(
            EnumSide.Right,
            sourceModelName,
            sourceColumnName,
          )
          const targetHandler = toNodeOrEdgeId(
            EnumSide.Left,
            targetModelName,
            targetColumnName,
          )
          const edgeId = toNodeOrEdgeId(sourceHandler, targetHandler)
          const edge =
            currentEdges[edgeId] ??
            createGraphEdge(
              sourceModelName,
              targetModelName,
              sourceHandler,
              targetHandler,
              true,
              {
                columnSource: sourceColumnName,
                columnTarget: targetColumnName,
              },
            )

          outputEdges.push(edge)
        }
      }
    }
  }

  return {
    edges: outputEdges,
    nodes: Object.values(nodesMap),
    nodesMap,
  }
}

function getNodeMap({
  modelNames,
  lineage,
  highlightedNodes,
  models,
  sources,
  nodes,
  model,
  withColumns,
}: {
  modelNames: string[]
  lineage: Record<string, Lineage>
  highlightedNodes: Record<string, string[]>
  models: Map<string, Model>
  sources: Set<string>
  nodes: Node[]
  model: ModelSQLMeshModel
  withColumns: boolean
}): Record<string, Node> {
  const NODE_BALANCE_SPACE = 64
  const COLUMN_LINE_HEIGHT = 24
  const CHAR_WIDTH = 8
  const MAX_VISIBLE_COLUMNS = 5

  const current = nodes.reduce((acc: Record<string, Node>, node) => {
    // Checking if any nodes have been removed from the graph
    if (models.has(node.id)) {
      acc[node.id] = node
    }

    return acc
  }, {})

  return modelNames.reduce((acc: Record<string, Node>, label: string) => {
    const node =
      current[label] ??
      createGraphNode({
        label,
        type: models.has(label) ? 'model' : 'cte',
      })
    const columnsCount = withColumns
      ? models.get(label)?.columns?.length ?? 0
      : 0

    const maxWidth = getNodeMaxWidth(label, columnsCount === 0)
    const maxHeight = getNodeMaxHeight(columnsCount)

    node.data.width = NODE_BALANCE_SPACE + maxWidth + NODE_BALANCE_SPACE
    node.data.height = NODE_BALANCE_SPACE + maxHeight + NODE_BALANCE_SPACE
    node.data.highlightedNodes = highlightedNodes
    node.data.isInteractive = model.name !== label && models.has(label)

    if (isArrayNotEmpty(lineage[node.id]?.models)) {
      node.targetPosition = Position.Left
    }

    if (sources.has(node.id)) {
      node.sourcePosition = Position.Right
    }

    acc[label] = node

    return acc
  }, {})

  function getNodeMaxWidth(label: string, hasColumns: boolean = false): number {
    const defaultWidth = label.length * CHAR_WIDTH
    const columns = models.get(label)?.columns ?? []

    return hasColumns
      ? Math.max(...columns.map(getColumnWidth), defaultWidth)
      : defaultWidth
  }

  function getNodeMaxHeight(columnsCount: number): number {
    return (
      COLUMN_LINE_HEIGHT * Math.min(columnsCount, MAX_VISIBLE_COLUMNS) +
      NODE_BALANCE_SPACE
    )
  }

  function getColumnWidth(column: Column): number {
    return (
      (column.name.length + column.type.length) * CHAR_WIDTH +
      NODE_BALANCE_SPACE
    )
  }
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
      output.position.x = node.x
    }

    if (output.position.y === 0 && node.y != null) {
      output.position.y = node.y
    }

    nodes.push(output)
  })

  return nodes
}

function createGraphNode(
  data: GraphNodeData,
  position: XYPosition = { x: 0, y: 0 },
  hidden: boolean = false,
): Node {
  return {
    id: data.label,
    dragHandle: '.drag-handle',
    type: 'model',
    position,
    hidden,
    data,
    connectable: false,
    selectable: false,
    deletable: false,
    focusable: false,
    zIndex: -1,
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
      strokeWidth: isNil(sourceHandle) || isNil(sourceHandle) ? 1 : 3,
      stroke:
        isNil(sourceHandle) || isNil(sourceHandle)
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

function toNodeOrEdgeId(...args: Array<string | undefined>): string {
  return args.filter(Boolean).join('__')
}

function mergeLineageWithModels(
  currentLineage: Record<string, Lineage> = {},
  data: Record<string, string[]> = {},
): Record<string, Lineage> {
  return Object.entries(data).reduce(
    (acc: Record<string, Lineage>, [key, models = []]) => {
      acc[key] = {
        models,
        columns: currentLineage?.[key]?.columns ?? undefined,
      }

      return acc
    },
    {},
  )
}

function mergeLineageWithColumns(
  currentLineage: Record<string, Lineage> = {},
  newLineage: Record<string, Record<string, LineageColumn>> = {},
): Record<string, Lineage> {
  for (const targetModelName in newLineage) {
    if (currentLineage[targetModelName] == null) {
      currentLineage[targetModelName] = { columns: {}, models: [] }
    }

    const currentLineageModel = currentLineage[targetModelName]!
    const newLineageModel = newLineage[targetModelName]!

    for (const targetColumnName in newLineageModel) {
      const newLineageModelColumn = newLineageModel[targetColumnName]!

      if (currentLineageModel.columns == null) {
        currentLineageModel.columns = {}
      }

      // New Column Lineage delivers fresh data, so we can just assign it
      currentLineageModel.columns[targetColumnName] = {
        expression: newLineageModelColumn.expression,
        source: newLineageModelColumn.source,
        models: {},
      }

      // If there are no models in new lineage, skip
      if (isObjectEmpty(newLineageModelColumn.models)) continue

      const currentLineageModelColumn =
        currentLineageModel.columns[targetColumnName]!
      const currentLineageModelColumnModels = currentLineageModelColumn.models!

      for (const sourceColumnName in newLineageModelColumn.models) {
        const currentLineageModelColumnModel =
          currentLineageModelColumnModels[sourceColumnName]!
        const newLineageModelColumnModel =
          newLineageModelColumn.models[sourceColumnName]!

        currentLineageModelColumnModels[sourceColumnName] =
          currentLineageModelColumnModel == null
            ? newLineageModelColumnModel
            : Array.from(
                new Set(
                  currentLineageModelColumnModel.concat(
                    newLineageModelColumnModel,
                  ),
                ),
              )
      }
    }
  }

  return currentLineage
}

function hasNoModels(
  lineage: Record<string, Record<string, LineageColumn>> = {},
): boolean {
  for (const targetModelName in lineage) {
    const model = lineage[targetModelName]

    if (model == null) continue

    for (const targetColumnName in model) {
      const column = model[targetColumnName]

      if (column == null) continue

      return Object.keys(column.models ?? {}).length === 0
    }
  }

  return false
}

function mergeConnections(
  connections: Map<string, Connections>,
  lineage: Record<string, Record<string, LineageColumn>> = {},
  addActiveEdges: (edges: string[]) => void,
): Map<string, Connections> {
  // We are getting lineage in format of target -> source
  for (const targetModelName in lineage) {
    const model = lineage[targetModelName]!

    for (const targetColumnName in model) {
      const column = model[targetColumnName]

      // We don't have any connectins so we skip
      if (column?.models == null) continue

      // At this point our Node is model -> {modelName} and column -> {columnName}
      // It is a target (left handler)
      // but it can also be a source (right handler) for other connections
      const modelColumnIdTarget = toNodeOrEdgeId(
        targetModelName,
        targetColumnName,
      )

      // We need to check if {modelColumnIdTarget} is already a source/target for other connections
      // Left and Right coresponds to node's handlers for {columnName} column
      const connectionsModelTarget = connections.get(modelColumnIdTarget) ?? {
        left: [],
        right: [],
      }

      Object.entries(column.models).forEach(([sourceModelName, columns]) => {
        columns.forEach(sourceColumnName => {
          // It is a source (right handler)
          // but it can also be a target (left handler) for other connections
          const modelColumnIdSource = toNodeOrEdgeId(
            sourceModelName,
            sourceColumnName,
          )

          // We need to check if {modelColumnIdSource} is already a source/target for other connections
          // Left and Right coresponds to node's handlers for {column} column
          const connectionsModelSource = connections.get(
            modelColumnIdSource,
          ) ?? { left: [], right: [] }

          // we need to add {modelColumnIdTarget} to {connectionsModelSource}'s right handlers
          connectionsModelSource.right = Array.from(
            new Set(connectionsModelSource.right.concat(modelColumnIdTarget)),
          )

          // We need to add {modelColumnIdSource} to {connectionsModelTarget}'s right handlers
          connectionsModelTarget.left = Array.from(
            new Set(connectionsModelTarget.left.concat(modelColumnIdSource)),
          )

          connections.set(modelColumnIdSource, connectionsModelSource)
          connections.set(modelColumnIdTarget, connectionsModelTarget)

          // Now we need to update active edges from connections
          // Left bucket contains references to all sources (right handlers)
          // And right bucket contains references to all targets (left handlers)
          addActiveEdges(
            [
              connectionsModelSource.left.map(id =>
                toNodeOrEdgeId(EnumSide.Right, id),
              ),
              connectionsModelSource.right.map(id =>
                toNodeOrEdgeId(EnumSide.Left, id),
              ),
            ].flat(),
          )
        })
      })

      // Now we need to update active edges from connections
      // Left bucket contains references to all sources (right handlers)
      // And right bucket contains references to all targets (left handlers)
      addActiveEdges(
        [
          connectionsModelTarget.left.map(id =>
            toNodeOrEdgeId(EnumSide.Right, id),
          ),
          connectionsModelTarget.right.map(id =>
            toNodeOrEdgeId(EnumSide.Left, id),
          ),
        ].flat(),
      )
    }
  }

  return new Map(connections)
}
