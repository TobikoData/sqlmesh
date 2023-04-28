import { create } from 'zustand'
import {
  type Connection,
  type Edge,
  type EdgeChange,
  type Node,
  type NodeChange,
  addEdge,
  type OnNodesChange,
  type OnEdgesChange,
  type OnConnect,
  applyNodeChanges,
  applyEdgeChanges,
} from 'reactflow'

interface LineageStore {
  activeEdges: Map<string, number>
  columns?: Record<string, { ins: string[]; outs: string[] }>
  hasActiveEdge: (edge: string) => boolean
  addActiveEdges: (edges: string[]) => void
  removeActiveEdges: (edges: string[]) => void
  clearActiveEdges: () => void
  setColumns: (
    columns?: Record<string, { ins: string[]; outs: string[] }>,
  ) => void
}

interface ReactFlowStore {
  nodes: Node[]
  edges: Edge[]
  setNodes: (nodes: Node[]) => void
  setEdges: (edges: Edge[]) => void
  onNodesChange: OnNodesChange
  onEdgesChange: OnEdgesChange
  onConnect: OnConnect
}

export const useStoreReactFlow = create<ReactFlowStore>((set, get) => ({
  nodes: [],
  edges: [],
  setNodes(nodes) {
    set({ nodes })
  },
  setEdges(edges) {
    set({ edges })
  },
  onNodesChange: (changes: NodeChange[]) => {
    set({
      nodes: applyNodeChanges(changes, get().nodes),
    })
  },
  onEdgesChange: (changes: EdgeChange[]) => {
    set({
      edges: applyEdgeChanges(changes, get().edges),
    })
  },
  onConnect: (connection: Connection) => {
    set({
      edges: addEdge(connection, get().edges),
    })
  },
}))

export const useStoreLineage = create<LineageStore>((set, get) => ({
  columns: undefined,
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

    return (activeEdges.get(edge) ?? 0) > 0
  },
  removeActiveEdges(edges) {
    const { activeEdges } = get()

    edges.forEach(edge => {
      activeEdges.set(edge, Math.max((activeEdges.get(edge) ?? 0) - 1, 0))
    })

    set({ activeEdges: new Map(activeEdges) })
  },
  clearActiveEdges() {
    set({ activeEdges: new Map() })
  },
  setColumns(columns) {
    set({ columns })
  },
}))
