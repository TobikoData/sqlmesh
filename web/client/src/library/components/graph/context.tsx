import { type Column } from '@api/client'
import { useStoreContext } from '@context/context'
import { type Lineage } from '@context/editor'
import { type ModelSQLMeshModel } from '@models/sqlmesh-model'
import {
  createContext,
  useState,
  useContext,
  useCallback,
  useEffect,
} from 'react'
import { getNodesBetween, toNodeOrEdgeId } from './help'
import { type ErrorIDE } from '~/library/pages/ide/context'
import { EnumSide } from '~/types/enum'
import { isNil } from '@utils/index'
import { type Edge, type Node } from 'reactflow'

export interface Connections {
  left: string[]
  right: string[]
}
export type ActiveColumns = Map<string, { ins: string[]; outs: string[] }>
export type ActiveEdges = Map<string, number>
export type ActiveNodes = Set<string>
export type SelectedNodes = Set<string>

interface LineageFlow {
  nodes: Node[]
  edges: Edge[]
  setNodes: React.Dispatch<React.SetStateAction<Node[]>>
  setEdges: React.Dispatch<React.SetStateAction<Edge[]>>
  withAdjacent: boolean
  lineage?: Record<string, Lineage>
  withColumns: boolean
  models: Map<string, ModelSQLMeshModel>
  mainNode?: string
  activeEdges: ActiveEdges
  activeNodes: ActiveNodes
  adjacentNodes: Set<string>
  selectedNodes: SelectedNodes
  selectedEdges: Set<string>
  connections: Map<string, Connections>
  setWithAdjacent: React.Dispatch<React.SetStateAction<boolean>>
  setMainNode: React.Dispatch<React.SetStateAction<string | undefined>>
  setSelectedNodes: React.Dispatch<React.SetStateAction<SelectedNodes>>
  setActiveNodes: React.Dispatch<React.SetStateAction<ActiveNodes>>
  setWithColumns: React.Dispatch<React.SetStateAction<boolean>>
  setConnections: React.Dispatch<React.SetStateAction<Map<string, Connections>>>
  hasActiveEdge: (edge?: string | null) => boolean
  addActiveEdges: (edges: string[]) => void
  removeActiveEdges: (edges: string[]) => void
  setActiveEdges: React.Dispatch<React.SetStateAction<ActiveEdges>>
  setLineage: React.Dispatch<
    React.SetStateAction<Record<string, Lineage> | undefined>
  >
  handleClickModel?: (modelName: string) => void
  handleError?: (error: ErrorIDE) => void
  manuallySelectedColumn?: [ModelSQLMeshModel, Column]
  setManuallySelectedColumn: React.Dispatch<
    React.SetStateAction<[ModelSQLMeshModel, Column] | undefined>
  >
  isActiveColumn: (modelName: string, columnName: string) => boolean
}

export const LineageFlowContext = createContext<LineageFlow>({
  selectedEdges: new Set(),
  lineage: {},
  withColumns: true,
  setWithColumns: () => {},
  mainNode: undefined,
  activeEdges: new Map(),
  activeNodes: new Set(),
  withAdjacent: false,
  setWithAdjacent: () => false,
  hasActiveEdge: () => false,
  addActiveEdges: () => {},
  removeActiveEdges: () => {},
  setActiveEdges: () => {},
  models: new Map(),
  handleClickModel: () => {},
  manuallySelectedColumn: undefined,
  setManuallySelectedColumn: () => {},
  handleError: () => {},
  setLineage: () => {},
  isActiveColumn: () => false,
  setConnections: () => {},
  connections: new Map(),
  setSelectedNodes: () => {},
  selectedNodes: new Set(),
  setActiveNodes: () => {},
  setMainNode: () => {},
  adjacentNodes: new Set(),
  nodes: [],
  edges: [],
  setNodes: () => {},
  setEdges: () => {},
})

export default function LineageFlowProvider({
  handleError,
  handleClickModel,
  children,
  withColumns = true,
}: {
  children: React.ReactNode
  handleClickModel?: (modelName: string) => void
  handleError?: (error: ErrorIDE) => void
  withColumns?: boolean
}): JSX.Element {
  const models = useStoreContext(s => s.models)

  const [nodes, setNodes] = useState<Node[]>([])
  const [edges, setEdges] = useState<Edge[]>([])
  const [manuallySelectedColumn, setManuallySelectedColumn] =
    useState<[ModelSQLMeshModel, Column]>()
  const [activeEdges, setActiveEdges] = useState<ActiveEdges>(new Map())
  const [activeNodes, setActiveNodes] = useState<ActiveNodes>(new Set())
  const [lineage, setLineage] = useState<Record<string, Lineage> | undefined>()
  const [connections, setConnections] = useState<Map<string, Connections>>(
    new Map(),
  )
  const [hasColumns, setWithColumns] = useState(withColumns)
  const [mainNode, setMainNode] = useState<string>()
  const [selectedNodes, setSelectedNodes] = useState<SelectedNodes>(new Set())
  const [adjacentNodes, setAdjacentNodes] = useState(new Set<string>())
  const [selectedEdges, setSelectedEdges] = useState(new Set<string>())
  const [withAdjacent, setWithAdjacent] = useState(false)

  const hasActiveEdge = useCallback(
    function hasActiveEdge(edge?: string | null): boolean {
      return edge == null ? false : (activeEdges.get(edge) ?? 0) > 0
    },
    [activeEdges],
  )

  const addActiveEdges = useCallback(function addActiveEdges(
    edges: string[],
  ): void {
    setActiveEdges(activeEdges => {
      edges.forEach(edge => {
        activeEdges.set(edge, 1)
      })

      return new Map(activeEdges)
    })
  }, [])

  const removeActiveEdges = useCallback(
    function removeActiveEdges(edges: string[]): void {
      setActiveEdges(activeEdges => {
        edges.forEach(edge => {
          activeEdges.set(toNodeOrEdgeId(EnumSide.Left, edge), 0)
          activeEdges.set(toNodeOrEdgeId(EnumSide.Right, edge), 0)
        })

        return new Map(activeEdges)
      })

      setConnections(connections => {
        edges.forEach(edge => {
          connections.delete(edge)
        })

        return new Map(connections)
      })
    },
    [setActiveEdges, setConnections],
  )

  const isActiveColumn = useCallback(
    function isActive(modelName: string, columnName: string): boolean {
      return (
        hasActiveEdge(toNodeOrEdgeId(EnumSide.Left, modelName, columnName)) ||
        hasActiveEdge(toNodeOrEdgeId(EnumSide.Right, modelName, columnName))
      )
    },
    [hasActiveEdge],
  )

  useEffect(() => {
    if (isNil(lineage) || isNil(mainNode) || isNil(lineage[mainNode]?.models)) {
      setAdjacentNodes(new Set())
      setSelectedEdges(new Set())
      setActiveNodes(new Set())
    } else {
      const modelsBefore = lineage[mainNode]!.models
      const modelsAfter = Object.keys(lineage).filter(
        key => lineage[key]?.models?.includes(mainNode),
      )
      const nodesConnections = Array.from(selectedNodes)
        .map(id => [
          getNodesBetween(id, mainNode, lineage),
          getNodesBetween(mainNode, id, lineage),
        ])
        .flat(10)

      setAdjacentNodes(new Set(modelsBefore.concat(modelsAfter)))
      setSelectedEdges(
        new Set(
          nodesConnections.map(({ source, target }) =>
            toNodeOrEdgeId(source, target),
          ),
        ),
      )
    }
  }, [mainNode, selectedNodes, lineage])

  useEffect(() => {
    setActiveNodes(
      new Set(
        edges
          .filter(edge => {
            return (
              hasActiveEdge(edge.sourceHandle) ||
              hasActiveEdge(edge.targetHandle) ||
              selectedEdges.has(edge.id)
            )
          })
          .map(edge => [edge.source, edge.target].filter(Boolean))
          .flat(),
      ),
    )
  }, [edges, selectedEdges, hasActiveEdge])

  return (
    <LineageFlowContext.Provider
      value={{
        nodes,
        edges,
        activeEdges,
        selectedEdges,
        activeNodes,
        selectedNodes,
        adjacentNodes,
        mainNode,
        connections,
        lineage,
        models,
        manuallySelectedColumn,
        withColumns: hasColumns,
        withAdjacent,
        setWithAdjacent,
        setNodes,
        setEdges,
        setSelectedNodes,
        setMainNode,
        setConnections,
        setLineage,
        setWithColumns,
        setActiveEdges,
        setActiveNodes,
        setManuallySelectedColumn,
        addActiveEdges,
        removeActiveEdges,
        hasActiveEdge,
        handleClickModel,
        handleError,
        isActiveColumn,
      }}
    >
      {children}
    </LineageFlowContext.Provider>
  )
}

export function useLineageFlow(): LineageFlow {
  return useContext(LineageFlowContext)
}
