import { MouseEvent, useEffect, useMemo, useState } from 'react'
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
import { getNodesAndEdges } from './help'
import { isFalse } from '../../../utils'

export default function Graph({ closeGraph }: any): JSX.Element {
  const { data } = useApiDag()
  const [graph, setGraph] = useState<{ nodes: any[]; edges: any[] }>()
  const [algorithm, setAlgorithm] = useState('layered')
  const nodeTypes = useMemo(() => ({ model: ModelNode }), [])
  const [nodes, setNodes, onNodesChange] = useNodesState([])
  const [edges, setEdges, onEdgesChange] = useEdgesState([])

  useEffect(() => {
    let active = true

    void load()

    return () => {
      active = false
    }

    async function load(): Promise<void> {
      setGraph(undefined)

      const graph = await getNodesAndEdges({ data, algorithm })

      if (isFalse(active)) return

      setGraph(graph)
    }
  }, [data, algorithm])

  useEffect(() => {
    if (graph == null) return

    setNodes(graph.nodes)
    setEdges(graph.edges)
  }, [graph])

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
        <Panel
          position="top-right"
          className="flex"
        >
          <select
            onChange={(e: React.ChangeEvent<HTMLSelectElement>) => {
              e.stopPropagation()

              setAlgorithm(e.target.value)
            }}
            value={algorithm}
          >
            <option value="layered">Layered</option>
            <option value="stress">Stress</option>
            <option value="mrtree">Mr. Tree</option>
            <option value="radial">Radial</option>
            <option value="force">Force</option>
          </select>
          <Button
            size="sm"
            variant="alternative"
            className="mx-0 ml-4"
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
