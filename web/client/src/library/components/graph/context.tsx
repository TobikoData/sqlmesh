import { type Column } from '@api/client'
import { useStoreContext } from '@context/context'
import { type ModelSQLMeshModel } from '@models/sqlmesh-model'
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
  models: Map<string, ModelSQLMeshModel>
  activeEdges: ActiveEdges
  activeColumns: ActiveColumns
  hasActiveEdge: (edge: string) => boolean
  addActiveEdges: (edges: string[]) => void
  removeActiveEdges: (edges: string[]) => void
  clearActiveEdges: () => void
  setActiveColumns: React.Dispatch<React.SetStateAction<ActiveColumns>>
  setActiveEdges: React.Dispatch<React.SetStateAction<ActiveEdges>>
  handleClickModel?: (modelName: string) => void
  refreshModels: () => void
  manuallySelectedColumn?: [ModelSQLMeshModel, Column]
  setManuallySelectedColumn: React.Dispatch<
    React.SetStateAction<[ModelSQLMeshModel, Column] | undefined>
  >
}

export const LineageFlowContext = createContext<LineageFlow>({
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
  refreshModels: () => {},
  handleClickModel: () => {},
  manuallySelectedColumn: undefined,
  setManuallySelectedColumn: () => {},
})

export default function LineageFlowProvider({
  handleClickModel,
  children,
}: {
  children: React.ReactNode
  handleClickModel?: (modelName: string) => void
}): JSX.Element {
  const models = useStoreContext(s => s.models)
  const refreshModels = useStoreContext(s => s.refreshModels)

  const [manuallySelectedColumn, setManuallySelectedColumn] =
    useState<[ModelSQLMeshModel, Column]>()
  const [nodes, setNodes] = useState<Node[]>([])
  const [edges, setEdges] = useState<Edge[]>([])
  const [activeEdges, setActiveEdges] = useState<ActiveEdges>(new Map())
  const [activeColumns, setActiveColumns] = useState<ActiveColumns>(new Map())

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
        refreshModels,
        handleClickModel,
        manuallySelectedColumn,
        setManuallySelectedColumn,
      }}
    >
      {children}
    </LineageFlowContext.Provider>
  )
}

export function useLineageFlow(): LineageFlow {
  return useContext(LineageFlowContext)
}
