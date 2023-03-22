import { type MouseEvent, useEffect, useMemo, useState } from 'react'
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
import { isFalse, isNil } from '../../../utils'

export default function Graph({ closeGraph }: any): JSX.Element {
  const { data } = useApiDag()
  const [graph, setGraph] = useState<{ nodes: any[]; edges: any[] }>()
  const nodeTypes = useMemo(() => ({ model: ModelNode }), [])
  const [nodes, setNodes, onNodesChange] = useNodesState([])
  const [edges, setEdges, onEdgesChange] = useEdgesState([])

  useEffect(() => {
    if (isNil(data)) return

    let active = true

    void load()

    return () => {
      active = false
    }

    async function load(): Promise<void> {
      setGraph(undefined)

      const graph = await getNodesAndEdges({ data })

      if (isFalse(active)) return

      setGraph(graph)
    }
  }, [data])

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
    <div className="bg-theme-lighter border-2 border-secondary-500 px-3 py-1 rounded-full text-xs font-semibold text-secondary-500 dark:text-secondary-100 dark:border-primary-500 dark:text-primary-500 ">
      {targetPosition === Position.Right && (
        <Handle
          type="target"
          position={Position.Right}
          isConnectable={false}
          className="!bg-secondary-500 dark:!bg-primary-500 w-2 h-2 rounded-full mr-[0.05rem]"
        />
      )}
      <div>{data.label}</div>
      {sourcePosition === Position.Left && (
        <Handle
          type="source"
          position={Position.Left}
          className="!bg-transparent w-0 h-0 border-none ml-[0.3rem] rounded-full"
          isConnectable={false}
        />
      )}
    </div>
  )
}
