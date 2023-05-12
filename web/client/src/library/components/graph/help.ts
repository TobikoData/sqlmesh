import ELK, { type ElkNode } from 'elkjs/lib/elk.bundled.js'
import { isArrayNotEmpty, isFalse, isNil, isObjectEmpty } from '../../../utils'
import {
  type ColumnLineageApiLineageModelNameColumnNameGet200,
  type Model,
} from '@api/client'
import { Position, type Edge, type Node, type XYPosition } from 'reactflow'
import { type Lineage } from '@context/editor'
import { type ActiveColumns } from './context'
import { type ModelSQLMeshModel } from '@models/sqlmesh-model'

export interface GraphNodeData {
  label: string
  isInteractive?: boolean
  highlightedNodes?: boolean
  [key: string]: any
}

export { getNodesAndEdges, createGraphLayout, toNodeOrEdgeId }

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
  activeColumns: ActiveColumns
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
  const activeColumns: ActiveColumns = new Map()

  for (const modelSource of modelNames) {
    const modelLineage = lineage[modelSource]

    if (modelLineage == null) continue

    modelLineage.models.forEach(modelTarget => {
      const edge = createGraphEdge(modelSource, modelTarget)

      outputEdges.push(edge)
    })

    if (
      modelLineage.columns == null ||
      isObjectEmpty(modelLineage.columns) ||
      isFalse(withColumns)
    )
      continue

    for (const columnSource in modelLineage.columns) {
      const sourceId = toNodeOrEdgeId(modelSource, columnSource)
      const modelsTarget = modelLineage.columns[columnSource]?.models

      if (isFalse(activeColumns.has(sourceId))) {
        activeColumns.set(sourceId, {
          ins: [],
          outs: [],
        })
      }

      if (modelsTarget == null) continue

      for (const modelTarget in modelsTarget) {
        const columnsTarget = modelsTarget[modelTarget]

        if (columnsTarget == null) continue

        for (const columnTarget of columnsTarget) {
          const targetId = toNodeOrEdgeId(modelTarget, columnTarget)

          if (isFalse(activeColumns.has(targetId))) {
            activeColumns.set(targetId, {
              ins: [],
              outs: [],
            })
          }

          activeColumns.get(sourceId)?.ins.push(targetId)
          activeColumns.get(targetId)?.outs.push(sourceId)

          const sourceHandle = toNodeOrEdgeId(
            'source',
            modelSource,
            columnSource,
          )
          const targetHandle = toNodeOrEdgeId(
            'target',
            modelTarget,
            columnTarget,
          )
          const edgeId = toNodeOrEdgeId(
            modelSource,
            modelTarget,
            sourceHandle,
            targetHandle,
          )
          const currentEdge = currentEdges[edgeId]
          const edge =
            currentEdge ??
            createGraphEdge(
              modelSource,
              modelTarget,
              sourceHandle,
              targetHandle,
              false,
              {
                target: modelTarget,
                source: modelSource,
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
    activeColumns,
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

  const current = nodes.reduce(
    (acc: Record<string, Node>, node) =>
      Object.assign(acc, { [node.id]: node }),
    {},
  )

  return modelNames.reduce((acc: Record<string, Node>, label: string) => {
    const node = current[label] ?? createGraphNode({ label })
    const columnsFactor = withColumns
      ? models.get(label)?.columns?.length ?? 0
      : 0

    const maxWidth =
      columnsFactor === 0
        ? label.length * CHAR_WIDTH
        : Math.max(
            ...(models
              .get(label)
              ?.columns?.map(
                column =>
                  (column.name.length + column.type.length) * CHAR_WIDTH +
                  NODE_BALANCE_SPACE,
              ) ?? []),
            label.length * CHAR_WIDTH,
          )
    const maxHeight =
      COLUMN_LINE_HEIGHT * Math.min(columnsFactor, 5) + NODE_BALANCE_SPACE

    node.data.width = NODE_BALANCE_SPACE + maxWidth
    node.data.height = NODE_BALANCE_SPACE + maxHeight
    node.data.highlightedNodes = highlightedNodes
    node.data.isInteractive = model.name !== label

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

// TODO: use better merge
export function mergeLineage(
  models: Map<string, Model>,
  lineage: Record<string, Lineage> = {},
  columns: ColumnLineageApiLineageModelNameColumnNameGet200 = {},
): Record<string, Lineage> {
  lineage = structuredClone(lineage)

  for (const model in columns) {
    const lineageModel = lineage[model]
    const columnsModel = columns[model]

    if (lineageModel == null || columnsModel == null) continue

    if (lineageModel.columns == null) {
      lineageModel.columns = {}
    }

    for (const columnName in columnsModel) {
      const columnsModelColumn = columnsModel[columnName]

      if (columnsModelColumn == null) continue

      const lineageModelColumn = lineageModel.columns[columnName] ?? {}

      lineageModelColumn.source = columnsModelColumn.source
      lineageModelColumn.models = {}

      lineageModel.columns[columnName] = lineageModelColumn

      if (isObjectEmpty(columnsModelColumn.models)) continue

      for (const columnModel in columnsModelColumn.models) {
        const columnsModelColumnModel = columnsModelColumn.models[columnModel]

        if (columnsModelColumnModel == null) continue

        const lineageModelColumnModel = lineageModelColumn.models[columnModel]

        if (lineageModelColumnModel == null) {
          lineageModelColumn.models[columnModel] = columnsModelColumnModel
        } else {
          lineageModelColumn.models[columnModel] = Array.from(
            new Set(lineageModelColumnModel.concat(columnsModelColumnModel)),
          )
        }
      }
    }
  }

  for (const modelName in lineage) {
    const model = models.get(modelName)
    const modelLineage = lineage[modelName]

    if (model == null || modelLineage == null) {
      delete lineage[modelName]

      continue
    }

    if (modelLineage.columns == null) continue

    if (model.columns == null) {
      delete modelLineage.columns

      continue
    }

    for (const columnName in modelLineage.columns) {
      const found = model.columns.find(c => c.name === columnName)

      if (found == null) {
        delete modelLineage.columns[columnName]

        continue
      }
    }
  }

  return lineage
}
