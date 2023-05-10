import {
  type Model,
  type Column,
  type ColumnLineageApiLineageModelNameColumnNameGet200,
} from '@api/client'
import { useStoreContext } from '@context/context'
import { type Lineage } from '@context/editor'
import { type ModelSQLMeshModel } from '@models/sqlmesh-model'
import { isObjectEmpty } from '@utils/index'
import { createContext, useState, useContext } from 'react'
import {
  applyNodeChanges,
  type NodeChange,
  type Edge,
  type Node,
  type OnConnect,
  type OnEdgesChange,
  type OnNodesChange,
  type EdgeChange,
  applyEdgeChanges,
  type Connection,
  addEdge,
} from 'reactflow'

export type ActiveColumns = Map<string, { ins: string[]; outs: string[] }>
export type ActiveEdges = Map<string, number>

interface ReactFlowStore {
  nodes: Node[]
  edges: Edge[]
  setNodes: (nodes: Node[]) => void
  setEdges: (edges: Edge[]) => void
  onNodesChange: OnNodesChange
  onEdgesChange: OnEdgesChange
  onConnect: OnConnect
}

interface LineageFlow extends ReactFlowStore {
  lineage?: Record<string, Lineage>
  withColumns: boolean
  models: Map<string, ModelSQLMeshModel>
  activeEdges: ActiveEdges
  activeColumns: ActiveColumns
  hasActiveEdge: (edge: string) => boolean
  addActiveEdges: (edges: string[]) => void
  removeActiveEdges: (edges: string[]) => void
  clearActiveEdges: () => void
  setActiveColumns: React.Dispatch<React.SetStateAction<ActiveColumns>>
  setActiveEdges: React.Dispatch<React.SetStateAction<ActiveEdges>>
  setLineage: React.Dispatch<
    React.SetStateAction<Record<string, Lineage> | undefined>
  >
  handleClickModel?: (modelName: string) => void
  handleError?: (error: Error) => void
  manuallySelectedColumn?: [ModelSQLMeshModel, Column]
  setManuallySelectedColumn: React.Dispatch<
    React.SetStateAction<[ModelSQLMeshModel, Column] | undefined>
  >
}

export const LineageFlowContext = createContext<LineageFlow>({
  lineage: {},
  withColumns: true,
  nodes: [],
  edges: [],
  activeEdges: new Map(),
  activeColumns: new Map(),
  setNodes: () => {},
  setEdges: () => {},
  onNodesChange: () => {},
  onEdgesChange: () => {},
  onConnect: () => {},
  hasActiveEdge: () => false,
  addActiveEdges: () => {},
  removeActiveEdges: () => {},
  clearActiveEdges: () => {},
  setActiveColumns: () => {},
  setActiveEdges: () => {},
  models: new Map(),
  handleClickModel: () => {},
  manuallySelectedColumn: undefined,
  setManuallySelectedColumn: () => {},
  handleError: () => {},
  setLineage: () => {},
})

export default function LineageFlowProvider({
  handleError,
  handleClickModel,
  children,
  withColumns = true,
}: {
  children: React.ReactNode
  handleClickModel?: (modelName: string) => void
  handleError?: (error: Error) => void
  withColumns?: boolean
}): JSX.Element {
  const models = useStoreContext(s => s.models)

  const [manuallySelectedColumn, setManuallySelectedColumn] =
    useState<[ModelSQLMeshModel, Column]>()
  const [nodes, setNodes] = useState<Node[]>([])
  const [edges, setEdges] = useState<Edge[]>([])
  const [activeEdges, setActiveEdges] = useState<ActiveEdges>(new Map())
  const [activeColumns, setActiveColumns] = useState<ActiveColumns>(new Map())
  const [lineage, setLineage] = useState<Record<string, Lineage> | undefined>()

  function onNodesChange(changes: NodeChange[]): void {
    setNodes(applyNodeChanges(changes, nodes))
  }

  function onEdgesChange(changes: EdgeChange[]): void {
    setEdges(applyEdgeChanges(changes, edges))
  }

  function onConnect(connection: Connection): void {
    setEdges(addEdge(connection, edges))
  }

  function addActiveEdges(edges: string[]): void {
    edges.forEach(edge => {
      activeEdges.set(edge, (activeEdges.get(edge) ?? 0) + 1)
    })

    setActiveEdges(new Map(activeEdges))
  }

  function hasActiveEdge(edge: string): boolean {
    return (activeEdges.get(edge) ?? 0) > 0
  }

  function removeActiveEdges(edges: string[]): void {
    edges.forEach(edge => {
      activeEdges.set(edge, Math.max((activeEdges.get(edge) ?? 0) - 1, 0))
    })

    setActiveEdges(new Map(activeEdges))
  }

  function clearActiveEdges(): void {
    setActiveEdges(new Map())
  }

  return (
    <LineageFlowContext.Provider
      value={{
        setLineage,
        lineage,
        withColumns,
        nodes,
        edges,
        setNodes,
        setEdges,
        onNodesChange,
        onEdgesChange,
        onConnect,
        activeEdges,
        activeColumns,
        setActiveEdges,
        setActiveColumns,
        addActiveEdges,
        clearActiveEdges,
        removeActiveEdges,
        hasActiveEdge,
        models,
        handleClickModel,
        manuallySelectedColumn,
        setManuallySelectedColumn,
        handleError,
      }}
    >
      {children}
    </LineageFlowContext.Provider>
  )
}

export function useLineageFlow(): LineageFlow {
  return useContext(LineageFlowContext)
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
