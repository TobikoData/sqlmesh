import ELK, { type ElkNode } from 'elkjs/lib/elk.bundled.js'
import {
  isArrayNotEmpty,
  isFalse,
  isNil,
  isNotNil,
  isObjectEmpty,
} from '../../../utils'
import { type LineageColumn, type Column, type Model } from '@api/client'
import { Position, type Edge, type Node, type XYPosition } from 'reactflow'
import { type Lineage } from '@context/editor'
import { type ActiveEdges, type Connections } from './context'
import { EnumSide } from '~/types/enum'
import { EnumLineageNodeModelType, type LineageNodeModelType } from './Graph'

export interface GraphNodeData {
  label: string
  type: LineageNodeModelType
  [key: string]: any
}

export {
  getNodeMap,
  getEdges,
  createGraphLayout,
  toNodeOrEdgeId,
  mergeLineageWithModels,
  mergeLineageWithColumns,
  mergeConnections,
  getNodesBetween,
  getLineageIndex,
  getActiveNodes,
  getUpdatedNodes,
  getUpdatedEdges,
  hasActiveEdge,
  hasActiveEdgeConnector,
  getModelNodeTypeTitle,
}

async function createGraphLayout({
  nodesMap,
  nodes = [],
  edges = [],
}: {
  nodesMap: Record<string, Node>
  nodes: Node[]
  edges: Edge[]
}): Promise<{ nodes: Node[]; edges: Edge[] }> {
  // https://eclipse.dev/elk/reference/options.html

  const elk = new ELK()
  const layout = await elk.layout({
    id: 'root',
    layoutOptions: {
      'elk.algorithm': 'layered',
      'elk.layered.layering.strategy': 'NETWORK_SIMPLEX',
      'elk.layered.crossingMinimization.strategy': 'INTERACTIVE',
      'elk.direction': 'RIGHT',
      'elk.layered.considerModelOrder.strategy': 'NODES_AND_EDGES',
    },
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

function getEdges(lineage: Record<string, Lineage> = {}): Edge[] {
  const modelNames = Object.keys(lineage)
  const outputEdges: Edge[] = []

  for (const targetModelName of modelNames) {
    const targetModel = lineage[targetModelName]!

    targetModel.models.forEach(sourceModelName => {
      outputEdges.push(createGraphEdge(sourceModelName, targetModelName))
    })

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

          outputEdges.push(
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
            ),
          )
        }
      }
    }
  }

  return outputEdges
}

function getNodeMap({
  lineage,
  models,
  withColumns,
}: {
  models: Map<string, Model>
  withColumns: boolean
  lineage?: Record<string, Lineage>
}): Record<string, Node> {
  if (isNil(lineage)) return {}

  const NODE_BALANCE_SPACE = 64
  const COLUMN_LINE_HEIGHT = 24
  const CHAR_WIDTH = 8
  const MAX_VISIBLE_COLUMNS = 5

  const currentModelNames = Array.from(models.keys())
  const lineageAllModels = Object.values(lineage)
    .map(({ models }) => models)
    .flat()
  const modelNames = Object.keys(lineage)
  const sources = new Set(
    Object.values(lineage)
      .map(l => l.models)
      .flat(),
  )

  return modelNames.reduce((acc: Record<string, Node>, modelName: string) => {
    const model = models.get(modelName)
    const node = createGraphNode({
      label: modelName,
      type: isNotNil(model)
        ? (model.type as LineageNodeModelType)
        : // If model name present in lineage but not in global models
        // it means either this is a CTE or model is UNKNOWN
        // CTEs only have connections between columns
        // where UNKNOWN model has connection only from another model
        isFalse(currentModelNames.includes(modelName)) &&
          lineageAllModels.includes(modelName)
        ? EnumLineageNodeModelType.unknown
        : EnumLineageNodeModelType.cte,
    })
    const columnsCount = withColumns
      ? models.get(modelName)?.columns?.length ?? 0
      : 0

    const maxWidth = Math.min(
      getNodeMaxWidth(modelName, columnsCount === 0),
      320,
    )
    const maxHeight = getNodeMaxHeight(columnsCount)

    node.data.width = maxWidth + NODE_BALANCE_SPACE * 3
    node.data.height = withColumns
      ? maxHeight + NODE_BALANCE_SPACE * 2
      : NODE_BALANCE_SPACE
    node.data.withColumns = withColumns

    if (isArrayNotEmpty(lineage[node.id]?.models)) {
      node.targetPosition = Position.Left
    }

    if (sources.has(node.id)) {
      node.sourcePosition = Position.Right
    }

    acc[modelName] = node

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

    if (isNil(output)) return

    if (isNotNil(node.x) && output.position.x === 0) {
      output.position.x = node.x
    }

    if (isNotNil(node.y) && output.position.y === 0) {
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
    type: 'smoothstep',
    style: {
      strokeWidth: isNil(sourceHandle) || isNil(targetHandle) ? 2 : 4,
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
    if (isNil(currentLineage[targetModelName])) {
      currentLineage[targetModelName] = { columns: {}, models: [] }
    }

    const currentLineageModel = currentLineage[targetModelName]!
    const newLineageModel = newLineage[targetModelName]!

    for (const targetColumnName in newLineageModel) {
      const newLineageModelColumn = newLineageModel[targetColumnName]!

      if (isNil(currentLineageModel.columns)) {
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

        currentLineageModelColumnModels[sourceColumnName] = isNil(
          currentLineageModelColumnModel,
        )
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

function mergeConnections(
  connections: Map<string, Connections>,
  lineage: Record<string, Record<string, LineageColumn>> = {},
): {
  connections: Map<string, Connections>
  activeEdges: Array<[string, string]>
} {
  const activeEdges: Array<[string, string]> = []

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
          connectionsModelSource.left.forEach(id => {
            activeEdges.push([
              toNodeOrEdgeId(EnumSide.Left, modelColumnIdSource),
              toNodeOrEdgeId(EnumSide.Right, id),
            ])
          })
          connectionsModelSource.right.forEach(id => {
            activeEdges.push([
              toNodeOrEdgeId(EnumSide.Left, id),
              toNodeOrEdgeId(EnumSide.Right, modelColumnIdSource),
            ])
          })
        })
      })
    }
  }

  return {
    connections,
    activeEdges,
  }
}

function getNodesBetween(
  source: string,
  target: string,
  lineage: Record<string, Lineage> = {},
): Array<{ source: string; target: string }> {
  const models = lineage[source]?.models ?? []
  const output: Array<{ source: string; target: string }> = []

  if (models.includes(target)) {
    output.push({ source: target, target: source })
  }

  models.forEach(node => {
    const found = getNodesBetween(node, target, lineage)

    if (isArrayNotEmpty(found)) {
      output.push({ source: node, target: source }, ...found)
    }
  })

  return output
}

function getLineageIndex(lineage: Record<string, Lineage> = {}): string {
  return Object.keys(lineage)
    .reduce((acc: string[], key) => {
      const { models = [], columns = {} } = lineage[key]!
      const allModels = new Set<string>()

      models.forEach(m => allModels.add(m))

      if (isNotNil(columns)) {
        Object.keys(columns).forEach(columnName => {
          const column = columns[columnName]

          if (isNotNil(column) && isNotNil(column.models)) {
            Object.keys(column.models).forEach(m => allModels.add(m))
          }
        })
      }

      return acc.concat(Array.from(allModels))
    }, [])
    .sort()
    .join('')
}

function getActiveNodes(
  edges: Edge[] = [],
  activeEdges: ActiveEdges,
  selectedEdges: Set<string>,
  nodesMap: Record<string, Node>,
): Set<string> {
  return new Set<string>(
    edges.reduce((acc: string[], edge) => {
      const sourceNode = isNil(edge.sourceHandle)
        ? undefined
        : nodesMap[edge.sourceHandle]
      const targetNode = isNil(edge.targetHandle)
        ? undefined
        : nodesMap[edge.targetHandle]

      if (
        isNotNil(sourceNode) &&
        isNotNil(edge.sourceHandle) &&
        sourceNode.data.type === EnumLineageNodeModelType.external &&
        hasActiveEdgeConnector(activeEdges, edge.sourceHandle)
      ) {
        acc.push(edge.source)
      } else if (
        isNotNil(targetNode) &&
        isNotNil(edge.targetHandle) &&
        targetNode.data.type === EnumLineageNodeModelType.external &&
        hasActiveEdgeConnector(activeEdges, edge.targetHandle)
      ) {
        acc.push(edge.target)
      } else {
        const isActiveEdge = hasActiveEdge(activeEdges, [
          edge.targetHandle,
          edge.sourceHandle,
        ])

        if (isActiveEdge || selectedEdges.has(edge.id)) {
          if (isNotNil(edge.source)) {
            acc.push(edge.source)
          }

          if (isNotNil(edge.target)) {
            acc.push(edge.target)
          }
        }
      }
      return acc
    }, []),
  )
}

function getUpdatedEdges(
  edges: Edge[] = [],
  connections: Map<string, Connections>,
  activeEdges: ActiveEdges,
  activeNodes: Set<string>,
  selectedEdges: Set<string>,
  selectedNodes: Set<string>,
  connectedNodes: Set<string>,
  withConnected: boolean = false,
  withImpacted: boolean = false,
  withSecondary: boolean = false,
): Edge[] {
  const tempEdges = edges.map(edge => {
    const isActiveEdge = hasActiveEdge(activeEdges, [
      edge.targetHandle,
      edge.sourceHandle,
    ])

    edge.hidden = true

    if (isNil(edge.sourceHandle) && isNil(edge.targetHandle)) {
      // Edge between models
      const hasSelections =
        selectedNodes.size > 0 || connections.size > 0 || activeNodes.size > 0
      const isImpactedEdge =
        connectedNodes.has(edge.source) || connectedNodes.has(edge.target)
      const isSecondaryEdge =
        isFalse(connectedNodes.has(edge.source)) ||
        isFalse(connectedNodes.has(edge.target))
      const withoutImpactedNodes =
        isFalse(withImpacted) &&
        isFalse(withConnected) &&
        isFalse(hasSelections)
      const withoutSecondaryNodes =
        isFalse(withSecondary) && isFalse(hasSelections)
      const shouldHideSecondary = isSecondaryEdge && withoutSecondaryNodes
      const shouldHideImpacted = isImpactedEdge && withoutImpactedNodes
      const isVisibleEdge =
        selectedNodes.size > 0 &&
        selectedEdges.has(edge.id) &&
        activeNodes.has(edge.source) &&
        activeNodes.has(edge.target)

      if (
        isFalse(shouldHideImpacted) &&
        isFalse(shouldHideSecondary) &&
        (isFalse(hasSelections) || isVisibleEdge)
      ) {
        edge.hidden = false
      }
    } else {
      // Edge between columns
      if (connections.size > 0 && isActiveEdge) {
        edge.hidden = false
      }
    }

    let stroke = 'var(--color-graph-edge-main)'
    let strokeWidth = 2

    const isConnectedSource = connectedNodes.has(edge.source)
    const isConnectedTarget = connectedNodes.has(edge.target)

    if (
      selectedEdges.has(edge.id) ||
      (withConnected && isConnectedSource && isConnectedTarget)
    ) {
      strokeWidth = 4
      stroke = 'var(--color-graph-edge-selected)'
      edge.zIndex = 10
    } else {
      if (isActiveEdge) {
        stroke = 'var(--color-graph-edge-secondary)'
      } else if (isConnectedSource && isConnectedTarget) {
        strokeWidth = 4
        stroke = 'var(--color-graph-edge-direct)'
        edge.zIndex = 10
      }
    }

    edge.style = {
      ...edge.style,
      stroke,
      strokeWidth,
    }

    return edge
  })

  return tempEdges
}

function getUpdatedNodes(
  nodes: Node[] = [],
  activeNodes: Set<string>,
  mainNode: string,
  connectedNodes: Set<string>,
  selectedNodes: Set<string>,
  connections: Map<string, Connections>,
  withConnected: boolean = true,
  withImpacted: boolean = true,
  withSecondary: boolean = true,
  withColumns: boolean = true,
): Node[] {
  return nodes.map(node => {
    node.hidden = true

    const hasSelections = selectedNodes.size > 0 || connections.size > 0
    const isActiveNode = activeNodes.size === 0 || activeNodes.has(node.id)
    const isImpactedNode = connectedNodes.has(node.id)
    const isSecondaryNode = isFalse(connectedNodes.has(node.id))
    const withoutImpactedNodes =
      isFalse(withImpacted) && isFalse(withConnected) && isFalse(hasSelections)
    const withoutSecondaryNodes =
      isFalse(withSecondary) && isFalse(hasSelections)
    const shouldHideSecondary = isSecondaryNode && withoutSecondaryNodes
    const shouldHideImpacted = isImpactedNode && withoutImpactedNodes

    if (isFalse(shouldHideImpacted) && isFalse(shouldHideSecondary)) {
      node.hidden = isFalse(isActiveNode)
    }

    if (node.data.type === EnumLineageNodeModelType.cte && withColumns) {
      node.hidden = isFalse(activeNodes.has(node.id))
    }

    if (mainNode === node.id) {
      node.hidden = false
    }

    return node
  })
}

function hasActiveEdge(
  activeEdges: ActiveEdges = new Map(),
  [leftConnect, rightConnect]: [Maybe<string>, Maybe<string>],
): boolean {
  if (isNil(leftConnect) && isNil(rightConnect)) return false

  const left = isNil(leftConnect) ? undefined : activeEdges.get(leftConnect)
  const right = isNil(rightConnect) ? undefined : activeEdges.get(rightConnect)

  if (isNil(left) && isNil(right)) return false

  const inLeft = Boolean(
    left?.some(([l, r]) => l === leftConnect && r === rightConnect),
  )
  const inRight = Boolean(
    right?.some(([l, r]) => l === leftConnect && r === rightConnect),
  )

  return inLeft || inRight
}

function hasActiveEdgeConnector(
  activeEdges: ActiveEdges = new Map(),
  connector: string,
): boolean {
  return (activeEdges.get(connector) ?? []).length > 0
}

function getModelNodeTypeTitle(type: LineageNodeModelType): string {
  if (type === EnumLineageNodeModelType.python) return 'PYTHON'
  if (type === EnumLineageNodeModelType.sql) return 'SQL'
  if (type === EnumLineageNodeModelType.seed) return 'SEED'
  if (type === EnumLineageNodeModelType.cte) return 'CTE'
  if (type === EnumLineageNodeModelType.external) return 'EXTERNAL'

  return 'UNKNOWN'
}
