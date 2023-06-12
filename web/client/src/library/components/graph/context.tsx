import { type Column } from '@api/client'
import { useStoreContext } from '@context/context'
import { type Lineage } from '@context/editor'
import { type ModelSQLMeshModel } from '@models/sqlmesh-model'
import { createContext, useState, useContext, useCallback } from 'react'
import { toNodeOrEdgeId } from './help'
import { type ErrorIDE } from '~/library/pages/ide/context'
import { EnumSide } from '~/types/enum'

export interface Connections {
  left: string[]
  right: string[]
}
export type ActiveColumns = Map<string, { ins: string[]; outs: string[] }>
export type ActiveEdges = Map<string, number>

interface LineageFlow {
  lineage?: Record<string, Lineage>
  withColumns: boolean
  models: Map<string, ModelSQLMeshModel>
  activeEdges: ActiveEdges
  connections: Map<string, Connections>
  shouldRecalculate: boolean
  setShouldRecalculate: React.Dispatch<React.SetStateAction<boolean>>
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
  lineage: {},
  withColumns: true,
  activeEdges: new Map(),
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
  shouldRecalculate: false,
  setShouldRecalculate: () => {},
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

  const [manuallySelectedColumn, setManuallySelectedColumn] =
    useState<[ModelSQLMeshModel, Column]>()
  const [activeEdges, setActiveEdges] = useState<ActiveEdges>(new Map())
  const [lineage, setLineage] = useState<Record<string, Lineage> | undefined>()
  const [connections, setConnections] = useState<Map<string, Connections>>(
    new Map(),
  )
  const [shouldRecalculate, setShouldRecalculate] = useState(false)

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

  return (
    <LineageFlowContext.Provider
      value={{
        connections,
        setConnections,
        setLineage,
        lineage,
        withColumns,
        activeEdges,
        setActiveEdges,
        addActiveEdges,
        removeActiveEdges,
        hasActiveEdge,
        models,
        handleClickModel,
        manuallySelectedColumn,
        setManuallySelectedColumn,
        handleError,
        isActiveColumn,
        shouldRecalculate,
        setShouldRecalculate,
      }}
    >
      {children}
    </LineageFlowContext.Provider>
  )
}

export function useLineageFlow(): LineageFlow {
  return useContext(LineageFlowContext)
}
