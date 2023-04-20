import { create } from 'zustand'

interface LineageStore {
  activeEdges: Map<string, number>
  columns?: Record<string, { ins: string[]; outs: string[] }>
  hasActiveEdge: (edge: string) => boolean
  addActiveEdges: (edges: string[]) => void
  removeActiveEdges: (edges: string[]) => void
  setColumns: (
    columns?: Record<string, { ins: string[]; outs: string[] }>,
  ) => void
}
export const useStoreLineage = create<LineageStore>((set, get) => ({
  columns: undefined,
  dag: undefined,
  activeEdges: new Map(),
  addActiveEdges(edges) {
    const { activeEdges } = get()

    edges.forEach(edge => {
      activeEdges.set(edge, (activeEdges.get(edge) ?? 0) + 1)
    })

    set({ activeEdges: new Map(activeEdges) })
  },
  hasActiveEdge(edge) {
    const { activeEdges } = get()

    return Boolean((activeEdges.get(edge) ?? 0) > 0)
  },
  removeActiveEdges(edges) {
    const { activeEdges } = get()

    edges.forEach(edge => {
      activeEdges.set(edge, Math.max((activeEdges.get(edge) ?? 0) - 1, 0))
    })

    set({ activeEdges: new Map(activeEdges) })
  },
  setColumns(columns) {
    set({ columns })
  },
}))
