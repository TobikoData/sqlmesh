import { type Column } from '@api/client'
import { useStoreContext } from '@context/context'
import { type Lineage } from '@context/editor'
import { type ModelSQLMeshModel } from '@models/sqlmesh-model'
import { createContext, useState, useContext, useCallback } from 'react'
import { toNodeOrEdgeId } from './help'

export type ActiveColumns = Map<string, { ins: string[]; outs: string[] }>
export type ActiveEdges = Map<string, number>

interface LineageFlow {
  lineage?: Record<string, Lineage>
  withColumns: boolean
  models: Map<string, ModelSQLMeshModel>
  activeEdges: ActiveEdges
  activeColumns: ActiveColumns
  hasActiveEdge: (edge: string) => boolean
  toggleLineageColumn: (
    action: 'add' | 'remove',
    columnId: string,
    connections?: { ins: string[]; outs: string[] },
  ) => void
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
  isActiveColumn: (modelName: string, columnName: string) => boolean
}

export const LineageFlowContext = createContext<LineageFlow>({
  lineage: {},
  withColumns: true,
  activeEdges: new Map(),
  activeColumns: new Map(),
  hasActiveEdge: () => false,
  addActiveEdges: () => {},
  removeActiveEdges: () => {},
  clearActiveEdges: () => {},
  setActiveColumns: () => {},
  setActiveEdges: () => {},
  toggleLineageColumn: () => {},
  models: new Map(),
  handleClickModel: () => {},
  manuallySelectedColumn: undefined,
  setManuallySelectedColumn: () => {},
  handleError: () => {},
  setLineage: () => {},
  isActiveColumn: () => false,
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
  const [activeEdges, setActiveEdges] = useState<ActiveEdges>(new Map())
  const [activeColumns, setActiveColumns] = useState<ActiveColumns>(new Map())
  const [lineage, setLineage] = useState<Record<string, Lineage> | undefined>()

  const hasActiveEdge = useCallback(
    function hasActiveEdge(edge: string): boolean {
      return (activeEdges.get(edge) ?? 0) > 0
    },
    [activeEdges],
  )

  const addActiveEdges = useCallback(function addActiveEdges(
    edges: string[],
  ): void {
    setActiveEdges(activeEdges => {
      edges.forEach(edge => {
        activeEdges.set(edge, (activeEdges.get(edge) ?? 0) + 1)
      })

      return new Map(activeEdges)
    })
  }, [])

  const removeActiveEdges = useCallback(function removeActiveEdges(
    edges: string[],
  ): void {
    setActiveEdges(activeEdges => {
      edges.forEach(edge => {
        activeEdges.set(edge, Math.max((activeEdges.get(edge) ?? 0) - 1, 0))
      })

      return new Map(activeEdges)
    })
  }, [])

  const clearActiveEdges = useCallback(function clearActiveEdges(): void {
    setActiveEdges(new Map())
  }, [])

  const toggleLineageColumn = useCallback(function toggleLineageColumn(
    action: 'add' | 'remove',
    columnId: string,
    connections: { ins: string[]; outs: string[] } = { ins: [], outs: [] },
  ): void {
    const sourceId = toNodeOrEdgeId('source', columnId)
    const targetId = toNodeOrEdgeId('target', columnId)

    const edges = new Set(
      [
        connections.ins.map(id => toNodeOrEdgeId('source', id)),
        connections.outs.map(id => toNodeOrEdgeId('target', id)),
      ]
        .flat()
        .concat([sourceId, targetId]),
    )

    if (action === 'remove') {
      removeActiveEdges(Array.from(edges))
    }

    if (action === 'add') {
      addActiveEdges(Array.from(edges))
    }
  }, [])

  const isActiveColumn = useCallback(
    function isActive(modelName: string, columnName: string): boolean {
      return (
        hasActiveEdge(toNodeOrEdgeId('source', modelName, columnName)) ||
        hasActiveEdge(toNodeOrEdgeId('target', modelName, columnName))
      )
    },
    [activeEdges, activeColumns],
  )

  return (
    <LineageFlowContext.Provider
      value={{
        setLineage,
        lineage,
        withColumns,
        activeEdges,
        activeColumns,
        setActiveEdges,
        setActiveColumns,
        addActiveEdges,
        clearActiveEdges,
        removeActiveEdges,
        hasActiveEdge,
        toggleLineageColumn,
        models,
        handleClickModel,
        manuallySelectedColumn,
        setManuallySelectedColumn,
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
