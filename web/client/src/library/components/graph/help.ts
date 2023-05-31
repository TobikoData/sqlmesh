import ELK, { type ElkNode } from 'elkjs/lib/elk.bundled.js'
import { isArrayNotEmpty, isFalse, isNil, isObjectEmpty } from '../../../utils'
import { type LineageColumn, type Column, type Model } from '@api/client'
import { Position, type Edge, type Node, type XYPosition } from 'reactflow'
import { type Lineage } from '@context/editor'
import { type ModelSQLMeshModel } from '@models/sqlmesh-model'
import { type Connections } from './context'

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
  const targets = new Set(
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
    targets,
    nodes,
    model,
    withColumns,
  })
  const outputEdges: Edge[] = []

  for (const modelSource of modelNames) {
    const modelLineage = lineage[modelSource]!

    modelLineage.models.forEach(modelTarget => {
      outputEdges.push(createGraphEdge(modelSource, modelTarget))
    })

    if (isObjectEmpty(modelLineage?.columns) || isFalse(withColumns)) continue

    for (const columnSource in modelLineage.columns) {
      const modelsTarget = modelLineage.columns[columnSource]?.models

      if (modelsTarget == null) continue

      for (const modelTarget in modelsTarget) {
        const columnsTarget = modelsTarget[modelTarget]

        if (columnsTarget == null) continue

        for (const columnTarget of columnsTarget) {
          const sourceHandle = toNodeOrEdgeId('left', modelSource, columnSource)
          const targetHandle = toNodeOrEdgeId(
            'right',
            modelTarget,
            columnTarget,
          )
          const edgeId = toNodeOrEdgeId(sourceHandle, targetHandle)
          const edge =
            currentEdges[edgeId] ??
            createGraphEdge(
              modelSource,
              modelTarget,
              sourceHandle,
              targetHandle,
              true,
              {
                columnSource,
                columnTarget,
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
  targets,
  nodes,
  model,
  withColumns,
}: {
  modelNames: string[]
  lineage: Record<string, Lineage>
  highlightedNodes: Record<string, string[]>
  models: Map<string, Model>
  targets: Set<string>
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
      node.sourcePosition = Position.Left
    }

    if (targets.has(node.id)) {
      node.targetPosition = Position.Right
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
      output.position.x = -node.x
    }

    if (output.position.y === 0 && node.y != null) {
      output.position.y = -node.y
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
  for (const modelName in newLineage) {
    if (currentLineage[modelName] == null) {
      currentLineage[modelName] = { columns: {}, models: [] }
    }

    const currentLineageModel = currentLineage[modelName]!
    const newLineageModel = newLineage[modelName]!

    for (const columnName in newLineageModel) {
      const newLineageModelColumn = newLineageModel[columnName]!

      if (currentLineageModel.columns == null) {
        currentLineageModel.columns = {}
      }

      // New Column Lineage delivers fresh data, so we can just assign it
      currentLineageModel.columns[columnName] = {
        expression: newLineageModelColumn.expression,
        source: newLineageModelColumn.source,
        models: {},
      }

      // If there are no models in new lineage, skip
      if (isObjectEmpty(newLineageModelColumn.models)) continue

      const currentLineageModelColumn = currentLineageModel.columns[columnName]!
      const currentLineageModelColumnModels = currentLineageModelColumn.models!

      for (const columnModelName in newLineageModelColumn.models) {
        const currentLineageModelColumnModel =
          currentLineageModelColumnModels[columnModelName]!
        const newLineageModelColumnModel =
          newLineageModelColumn.models[columnModelName]!

        currentLineageModelColumnModels[columnModelName] =
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
  models: Record<string, Record<string, LineageColumn>> = {},
): boolean {
  for (const modelName in models) {
    const model = models[modelName]

    if (models[modelName] == null) continue

    for (const columnName in model) {
      const lineage = model[columnName]

      if (lineage == null) continue

      return Object.keys(lineage.models ?? {}).length === 0
    }
  }

  return false
}

function mergeConnections(
  connections: Map<string, Connections>,
  lineage: Record<string, Record<string, LineageColumn>> = {},
  addActiveEdges: (edges: string[]) => void,
): Map<string, Connections> {
  for (const modelName in lineage) {
    const model = lineage[modelName]!

    for (const columnName in model) {
      const column = model[columnName]

      // We don't have any connectins so we skip
      if (column?.models == null) continue

      const columnId = toNodeOrEdgeId(modelName, columnName)
      const connectionSource = connections.get(columnId) ?? {
        left: [],
        right: [],
      }

      Object.entries(column.models).forEach(([id, columns]) => {
        columns.forEach(column => {
          const connectionTarget = connections.get(
            toNodeOrEdgeId(id, column),
          ) ?? { left: [], right: [] }

          connectionTarget.right = Array.from(
            new Set(connectionTarget.right.concat(columnId)),
          )
          connectionSource.left = Array.from(
            new Set(connectionSource.left.concat(toNodeOrEdgeId(id, column))),
          )

          connections.set(toNodeOrEdgeId(id, column), connectionTarget)
          connections.set(columnId, connectionSource)
        })
      })

      const modelColumnConnectionsLeft = connectionSource.left.map(id =>
        toNodeOrEdgeId('right', id),
      )
      const modelColumnConnectionsRight = connectionSource.right.map(id =>
        toNodeOrEdgeId('left', id),
      )

      addActiveEdges(
        modelColumnConnectionsLeft.concat(modelColumnConnectionsRight),
      )
    }
  }

  return new Map(connections)
}
