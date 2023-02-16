import { MouseEvent, useEffect, useMemo } from 'react'

import ReactFlow, {
  Controls,
  Background,
  useNodesState,
  useEdgesState,
  Panel,
  Handle,
  Position,
  BackgroundVariant,
} from 'reactflow'
import { Button } from '../button/Button'
import { useApiDag } from '../../../api'
import 'reactflow/dist/base.css'
import { isArrayNotEmpty } from '../../../utils'
import dagre from 'dagre'

const dagreGraph = new dagre.graphlib.Graph()

dagreGraph.setDefaultEdgeLabel(() => ({}))

const nodeWidth = 172
const nodeHeight = 32

export default function Graph({ closeGraph }: any): JSX.Element {
  const { data } = useApiDag()

  const { nodes: initialNodes, edges: initialEdges } = useMemo(() => {
    if (data == null) return []

    const nodes = data.sorted.reduce((acc: any, label: string, idx: number) => {
      const node = {
        id: label,
        type: 'model',
        position: { x: 0, y: 0 },
        data: { label },
        connectable: false,
        selectable: false,
        deletable: false,
        focusable: false,
      }

      acc[label] = node

      return acc
    }, {})

    const edges = Object.keys(data.graph).reduce((acc: any, source: string) => {
      if (isArrayNotEmpty(data.graph[source])) {
        data.graph[source].forEach((target: string) => {
          const id = `${source}_${target}`
          const edge = {
            id,
            source,
            target,
            style: {
              strokeWidth: 2,
              stroke: 'hsl(260, 100%, 80%)',
            },
          }
          acc[id] = edge
        })
      }

      return acc
    }, {})

    return getLayoutedElements(
      Object.values(nodes),
      Object.values(edges),
      data.graph,
    )
  }, [data])
  const nodeTypes = useMemo(() => ({ model: ModelNode }), [])

  const [nodes, setNodes, onNodesChange] = useNodesState([])
  const [edges, setEdges, onEdgesChange] = useEdgesState([])

  useEffect(() => {
    setNodes(initialNodes)
    setEdges(initialEdges)
  }, [initialNodes, initialEdges])

  return (
    <div className="px-2 py-1 w-full h-[90vh]">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        nodeOrigin={[0.5, 0.5]}
        nodeTypes={nodeTypes}
        fitView
      >
        <Panel position="top-right">
          <Button
            size="sm"
            variant="alternative"
            className="mx-0"
            onClick={(e: MouseEvent) => {
              e.stopPropagation()

              closeGraph()
            }}
          >
            Close
          </Button>
        </Panel>
        <Controls className="bg-secondary-100" />
        <Background
          variant={BackgroundVariant.Dots}
          gap={16}
          size={2}
        />
      </ReactFlow>
    </div>
  )
}

function getLayoutedElements(nodes: any, edges: any, graph: any): any {
  const targets = new Set(Object.values(graph).flat())

  dagreGraph.setGraph({ rankdir: 'LR' })

  nodes.forEach((node: any) => {
    dagreGraph.setNode(node.id, { width: nodeWidth, height: nodeHeight })
  })

  edges.forEach((edge: any) => {
    dagreGraph.setEdge(edge.source, edge.target)
  })

  dagre.layout(dagreGraph)

  nodes.forEach((node: any) => {
    const nodeWithPosition = dagreGraph.node(node.id)

    if (isArrayNotEmpty(graph[node.id])) {
      node.sourcePosition = 'right'
    }

    if (targets.has(node.id)) {
      node.targetPosition = 'left'
    }

    node.position = {
      x: nodeWithPosition.x - nodeWidth / 2,
      y: nodeWithPosition.y - nodeHeight / 2,
    }

    return node
  })

  return { nodes, edges }
}

function ModelNode({ data, sourcePosition, targetPosition }: any): JSX.Element {
  return (
    <div className="bg-secondary-100 border-2 border-secondary-500 px-3 py-1 rounded-full text-xs font-semibold text-secondary-500">
      {targetPosition === Position.Left && (
        <Handle
          type="target"
          position={Position.Left}
          isConnectable={false}
          className="bg-secondary-500 w-2 h-2 rounded-full"
        />
      )}
      <div>{data.label}</div>
      {sourcePosition === Position.Right && (
        <Handle
          type="source"
          position={Position.Right}
          className="bg-secondary-500 w-2 h-2 rounded-full"
          isConnectable={false}
        />
      )}
    </div>
  )
}
