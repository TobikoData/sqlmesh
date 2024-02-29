import { type Column } from '@api/client'
import { useStoreContext } from '@context/context'
import { type Lineage } from '@context/editor'
import { type ModelSQLMeshModel } from '@models/sqlmesh-model'
import {
  createContext,
  useState,
  useContext,
  useCallback,
  useMemo,
} from 'react'
import { getNodeMap, hasActiveEdge, hasActiveEdgeConnector } from './help'
import { EnumSide } from '~/types/enum'
import { isFalse, toID } from '@utils/index'
import { type ConnectedNode } from '~/workers/lineage'
import { type Node } from 'reactflow'
import { type ErrorIDE } from '~/library/pages/root/context/notificationCenter'
export interface Connections {
  left: string[]
  right: string[]
}
export type ActiveColumns = Map<string, { ins: string[]; outs: string[] }>
export type ActiveEdges = Map<string, Array<[string, string]>>
export type ActiveNodes = Set<string>
export type SelectedNodes = Set<string>
export type HighlightedNodes = Record<string, string[]>

interface LineageFlow {
  lineage: Record<string, Lineage>
  lineageCache?: Record<string, Lineage>
  mainNode?: string
  connectedNodes: Set<string>
  activeEdges: ActiveEdges
  activeNodes: ActiveNodes
  selectedNodes: SelectedNodes
  selectedEdges: ConnectedNode[]
  models: Map<string, ModelSQLMeshModel>
  unknownModels: Set<string>
  connections: Map<string, Connections>
  withConnected: boolean
  withColumns: boolean
  hasBackground: boolean
  withImpacted: boolean
  withSecondary: boolean
  showControls: boolean
  manuallySelectedColumn?: [ModelSQLMeshModel, Column]
  highlightedNodes: HighlightedNodes
  nodesMap: Record<string, Node>
  setHighlightedNodes: React.Dispatch<React.SetStateAction<HighlightedNodes>>
  setActiveNodes: React.Dispatch<React.SetStateAction<ActiveNodes>>
  setWithConnected: React.Dispatch<React.SetStateAction<boolean>>
  setMainNode: React.Dispatch<React.SetStateAction<string | undefined>>
  setSelectedNodes: React.Dispatch<React.SetStateAction<SelectedNodes>>
  setWithColumns: React.Dispatch<React.SetStateAction<boolean>>
  setHasBackground: React.Dispatch<React.SetStateAction<boolean>>
  setWithImpacted: React.Dispatch<React.SetStateAction<boolean>>
  setWithSecondary: React.Dispatch<React.SetStateAction<boolean>>
  setConnections: React.Dispatch<React.SetStateAction<Map<string, Connections>>>
  hasActiveEdge: (edge: [Maybe<string>, Maybe<string>]) => boolean
  addActiveEdges: (edges: Array<[string, string]>) => void
  removeActiveEdges: (edges: Array<[string, string]>) => void
  setActiveEdges: React.Dispatch<React.SetStateAction<ActiveEdges>>
  setUnknownModels: React.Dispatch<React.SetStateAction<Set<string>>>
  setLineage: React.Dispatch<React.SetStateAction<Record<string, Lineage>>>
  setLineageCache: React.Dispatch<
    React.SetStateAction<Optional<Record<string, Lineage>>>
  >
  handleClickModel?: (modelName: string) => void
  handleError?: (error: ErrorIDE) => void
  setManuallySelectedColumn: React.Dispatch<
    React.SetStateAction<[ModelSQLMeshModel, Column] | undefined>
  >
  setNodeConnections: React.Dispatch<Record<string, ConnectedNode>>
  isActiveColumn: (modelName: string, columnName: string) => boolean
}

export const LineageFlowContext = createContext<LineageFlow>({
  selectedEdges: [],
  lineage: {},
  lineageCache: undefined,
  withColumns: false,
  withConnected: false,
  withImpacted: true,
  withSecondary: false,
  hasBackground: true,
  mainNode: undefined,
  activeEdges: new Map(),
  activeNodes: new Set(),
  models: new Map(),
  unknownModels: new Set(),
  manuallySelectedColumn: undefined,
  connections: new Map(),
  selectedNodes: new Set(),
  connectedNodes: new Set(),
  highlightedNodes: {},
  nodesMap: {},
  showControls: true,
  setHighlightedNodes: () => {},
  setWithColumns: () => false,
  setHasBackground: () => false,
  setWithImpacted: () => false,
  setWithSecondary: () => false,
  setWithConnected: () => false,
  hasActiveEdge: () => false,
  addActiveEdges: () => {},
  removeActiveEdges: () => {},
  setActiveEdges: () => {},
  handleClickModel: () => {},
  setManuallySelectedColumn: () => {},
  handleError: () => {},
  setLineage: () => {},
  setLineageCache: () => {},
  isActiveColumn: () => false,
  setConnections: () => {},
  setSelectedNodes: () => {},
  setMainNode: () => {},
  setActiveNodes: () => {},
  setNodeConnections: () => {},
  setUnknownModels: () => {},
})

export default function LineageFlowProvider({
  handleError,
  handleClickModel,
  children,
  showColumns = false,
  showConnected = false,
  showControls = true,
}: {
  children: React.ReactNode
  handleClickModel?: (modelName: string) => void
  handleError?: (error: ErrorIDE) => void
  showColumns?: boolean
  showConnected?: boolean
  showControls?: boolean
}): JSX.Element {
  const models = useStoreContext(s => s.models)

  const [lineage, setLineage] = useState<Record<string, Lineage>>({})
  const [unknownModels, setUnknownModels] = useState(new Set<string>())
  const [lineageCache, setLineageCache] = useState<
    Record<string, Lineage> | undefined
  >(undefined)
  const [nodesConnections, setNodeConnections] = useState<
    Record<string, ConnectedNode>
  >({})
  const [withColumns, setWithColumns] = useState(showColumns)
  const [mainNode, setMainNode] = useState<string>()
  const [manuallySelectedColumn, setManuallySelectedColumn] =
    useState<[ModelSQLMeshModel, Column]>()
  const [activeEdges, setActiveEdges] = useState<ActiveEdges>(new Map())
  const [connections, setConnections] = useState<Map<string, Connections>>(
    new Map(),
  )
  const [withConnected, setWithConnected] = useState(showConnected)
  const [selectedNodes, setSelectedNodes] = useState<SelectedNodes>(new Set())
  const [activeNodes, setActiveNodes] = useState<ActiveNodes>(new Set())
  const [highlightedNodes, setHighlightedNodes] = useState<HighlightedNodes>({})
  const [hasBackground, setHasBackground] = useState(true)
  const [withImpacted, setWithImpacted] = useState(true)
  const [withSecondary, setWithSecondary] = useState(false)

  const nodesMap = useMemo(
    () =>
      getNodeMap({
        lineage,
        models,
        unknownModels,
        withColumns,
      }),
    [lineage, models, withColumns, unknownModels],
  )

  const checkActiveEdge = useCallback(
    function checkActiveEdge(edge: [Maybe<string>, Maybe<string>]): boolean {
      return hasActiveEdge(activeEdges, edge)
    },
    [activeEdges],
  )

  const addActiveEdges = useCallback(
    function addActiveEdges(edges: Array<[string, string]>): void {
      setActiveEdges(activeEdges => {
        edges.forEach(([leftConnect, rightConnect]) => {
          const left = activeEdges.get(leftConnect) ?? []
          const right = activeEdges.get(rightConnect) ?? []
          const hasDuplicateLeft = left.some(
            ([left, right]) => left === leftConnect && right === rightConnect,
          )
          const hasDuplicateRight = right.some(
            ([left, right]) => left === leftConnect && right === rightConnect,
          )

          if (isFalse(hasDuplicateLeft)) {
            left.push([leftConnect, rightConnect])
          }

          if (isFalse(hasDuplicateRight)) {
            right.push([leftConnect, rightConnect])
          }

          activeEdges.set(leftConnect, left)
          activeEdges.set(rightConnect, right)
        })

        return new Map(activeEdges)
      })
    },
    [setActiveEdges],
  )

  const removeActiveEdges = useCallback(
    function removeActiveEdges(edges: Array<[string, string]>): void {
      setActiveEdges(activeEdges => {
        edges.forEach(([left, right]) => {
          const edgesLeft = (activeEdges.get(left) ?? []).filter(
            e => e[0] !== left && e[1] !== right,
          )
          const edgesRight = (activeEdges.get(right) ?? []).filter(
            e => e[0] !== left && e[1] !== right,
          )

          activeEdges.set(left, edgesLeft)
          activeEdges.set(right, edgesRight)
        })

        return new Map(activeEdges)
      })

      setConnections(connections => {
        edges.forEach(([left, right]) => {
          connections.delete(left)
          connections.delete(right)
        })

        return new Map(connections)
      })
    },
    [setActiveEdges, setConnections],
  )

  const isActiveColumn = useCallback(
    function isActive(modelName: string, columnName: string): boolean {
      const leftConnector = toID(EnumSide.Left, modelName, columnName)
      const rightConnector = toID(EnumSide.Right, modelName, columnName)

      return (
        hasActiveEdgeConnector(activeEdges, leftConnector) ||
        hasActiveEdgeConnector(activeEdges, rightConnector)
      )
    },
    [checkActiveEdge, activeEdges],
  )

  const connectedNodes: Set<string> = useMemo(
    () => new Set(Object.keys(nodesConnections)),
    [nodesConnections],
  )

  const selectedEdges = useMemo(
    () =>
      Array.from(selectedNodes)
        .flatMap(id => nodesConnections[id])
        .filter(Boolean) as ConnectedNode[],
    [nodesConnections, selectedNodes],
  )

  return (
    <LineageFlowContext.Provider
      value={{
        highlightedNodes,
        connectedNodes,
        activeEdges,
        selectedEdges,
        activeNodes,
        selectedNodes,
        mainNode,
        connections,
        lineage,
        lineageCache,
        models,
        manuallySelectedColumn,
        withColumns,
        withConnected,
        withImpacted,
        withSecondary,
        showControls,
        hasBackground,
        nodesMap,
        unknownModels,
        setHighlightedNodes,
        setActiveNodes,
        setNodeConnections,
        setLineageCache,
        setUnknownModels,
        setWithConnected,
        setWithImpacted,
        setWithSecondary,
        setHasBackground,
        setSelectedNodes,
        setMainNode,
        setConnections,
        setLineage,
        setWithColumns,
        setActiveEdges,
        setManuallySelectedColumn,
        addActiveEdges,
        removeActiveEdges,
        hasActiveEdge: checkActiveEdge,
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
