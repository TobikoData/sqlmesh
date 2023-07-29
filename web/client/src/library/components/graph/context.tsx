import { type Column } from '@api/client'
import { useStoreContext } from '@context/context'
import { type Lineage } from '@context/editor'
import { type ModelSQLMeshModel } from '@models/sqlmesh-model'
import { createContext, useState, useContext, useCallback } from 'react'
import { getAllModelsForNode, toNodeOrEdgeId } from './help'
import { type ErrorIDE } from '~/library/pages/ide/context'
import { EnumSide } from '~/types/enum'
import { isArrayNotEmpty } from '@utils/index'

export interface Connections {
  left: string[]
  right: string[]
}
export type ActiveColumns = Map<string, { ins: string[]; outs: string[] }>
export type ActiveEdges = Map<string, number>
export type ActiveNodes = Set<string>

interface LineageFlow {
  lineage?: Record<string, Lineage>
  withColumns: boolean
  models: Map<string, ModelSQLMeshModel>
  activeEdges: ActiveEdges
  activeNodes: ActiveNodes
  connections: Map<string, Connections>
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
  getNodesBetween: (source: string, target: string) => string[]
}

export const LineageFlowContext = createContext<LineageFlow>({
  lineage: {},
  withColumns: true,
  setWithColumns: () => {},
  activeEdges: new Map(),
  activeNodes: new Set(),
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
  setActiveNodes: () => {},
  getNodesBetween: () => [],
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
  const [activeNodes, setActiveNodes] = useState<ActiveNodes>(new Set())
  const [lineage, setLineage] = useState<Record<string, Lineage> | undefined>()
  const [connections, setConnections] = useState<Map<string, Connections>>(
    new Map(),
  )
  const [hasColumns, setWithColumns] = useState(withColumns)

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

  const map = Object.keys(lineage ?? {}).reduce(
    (acc: Record<string, string[]>, it) => {
      acc[it] = getAllModelsForNode(it, lineage)

      return acc
    },
    {},
  )

  // TODO: this is a mess, refactor
  const getNodesBetween = useCallback(
    function getNodesBetween(
      source: string,
      target: string,
      visited: Set<string> = new Set(),
    ): string[] {
      const upstream = map[source]?.concat() ?? []
      const downstream = Object.keys(map ?? {}).filter(
        key => map[key]?.includes(source),
      )
      const output: string[] = []

      visited.add(source)

      if (upstream.includes(target)) {
        output.push(toNodeOrEdgeId(target, source))
      } else {
        upstream.forEach(node => {
          if (visited.has(node)) return

          const found = getNodesBetween(node, target, visited)

          if (isArrayNotEmpty(found)) {
            output.push(toNodeOrEdgeId(node, source), ...found)
          }
        })
      }

      if (downstream.includes(target)) {
        output.push(toNodeOrEdgeId(source, target))
      } else {
        downstream.forEach(node => {
          if (visited.has(node)) return

          const found = getNodesBetween(node, target, visited)

          if (isArrayNotEmpty(found)) {
            output.push(toNodeOrEdgeId(node, source), ...found)
          }
        })
      }

      return output
    },
    [lineage, map],
  )

  return (
    <LineageFlowContext.Provider
      value={{
        getNodesBetween,
        connections,
        setConnections,
        setLineage,
        lineage,
        withColumns: hasColumns,
        setWithColumns,
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
        activeNodes,
        setActiveNodes,
      }}
    >
      {children}
    </LineageFlowContext.Provider>
  )
}

export function useLineageFlow(): LineageFlow {
  return useContext(LineageFlowContext)
}
