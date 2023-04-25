import React, {
  type MouseEvent,
  useEffect,
  useMemo,
  useState,
  useCallback,
  Fragment,
} from 'react'
import ReactFlow, {
  Controls,
  Background,
  Panel,
  Handle,
  Position,
  BackgroundVariant,
  type NodeProps,
  type Edge,
} from 'reactflow'
import { Button } from '../button/Button'
import 'reactflow/dist/base.css'
import {
  getNodesAndEdges,
  createGraphLayout,
  toNodeOrEdgeId,
  type GraphNodeData,
} from './help'
import { debounceAsync, isArrayNotEmpty, isFalse, isTrue } from '../../../utils'
import { EnumSize, EnumVariant } from '~/types/enum'
import { useStoreContext } from '@context/context'
import {
  ArrowRightCircleIcon,
  InformationCircleIcon,
} from '@heroicons/react/24/solid'
import { useStoreLineage, useStoreReactFlow } from '@context/lineage'
import clsx from 'clsx'
import { type Column } from '@api/client'
import { useStoreFileTree } from '@context/fileTree'
import { useApiColumnLineage } from '@api/index'
import { Popover, Transition } from '@headlessui/react'
import { useStoreEditor, type Lineage } from '@context/editor'
import Loading from '@components/loading/Loading'
import Spinner from '@components/logo/Spinner'

export default function Flow({
  lineage,
  closeGraph,
  highlightedNodes = [],
}: {
  lineage: Record<string, Lineage>
  closeGraph?: () => void
  highlightedNodes?: string[]
}): JSX.Element {
  const models = useStoreContext(s => s.models)

  const nodes = useStoreReactFlow(s => s.nodes)
  const edges = useStoreReactFlow(s => s.edges)
  const setNodes = useStoreReactFlow(s => s.setNodes)
  const setEdges = useStoreReactFlow(s => s.setEdges)
  const onNodesChange = useStoreReactFlow(s => s.onNodesChange)
  const onEdgesChange = useStoreReactFlow(s => s.onEdgesChange)
  const onConnect = useStoreReactFlow(s => s.onConnect)

  const setColumns = useStoreLineage(s => s.setColumns)
  const activeEdges = useStoreLineage(s => s.activeEdges)
  const hasActiveEdge = useStoreLineage(s => s.hasActiveEdge)

  const nodeTypes = useMemo(() => ({ model: ModelNode }), [ModelNode])
  const nodesAndEdges = useMemo(
    () =>
      getNodesAndEdges({
        lineage,
        highlightedNodes,
        models,
        nodes,
        edges,
      }),
    [lineage, highlightedNodes, models],
  )

  useEffect(() => {
    let active = true

    void load()

    return () => {
      active = false
    }

    async function load(): Promise<void> {
      const layout = await createGraphLayout({
        nodes: nodesAndEdges.nodes,
        edges: nodesAndEdges.edges,
        nodesMap: nodesAndEdges.nodesMap,
      })

      if (isFalse(active)) return

      setNodes(layout.nodes)
      setEdges(layout.edges)
      setColumns(nodesAndEdges.columns)
    }
  }, [nodesAndEdges])

  useEffect(() => {
    setEdges(toggleEdge(nodesAndEdges.edges))
  }, [activeEdges])

  useEffect(() => {
    setNodes(nodesAndEdges.nodes)
    setEdges(nodesAndEdges.edges)
    setColumns(nodesAndEdges.columns)
  }, [lineage, highlightedNodes, models])

  function toggleEdge(edges: Edge[] = []): Edge[] {
    return edges.map(edge => {
      if (edge.sourceHandle != null && edge.targetHandle != null) {
        edge.hidden =
          isFalse(hasActiveEdge(edge.sourceHandle)) &&
          isFalse(hasActiveEdge(edge.targetHandle))
      } else {
        edge.hidden = false
      }

      return edge
    })
  }

  return (
    <div className="px-2 py-1 w-full h-full">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        onConnect={onConnect}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        fitView
      >
        {closeGraph != null && (
          <Panel
            position="top-right"
            className="flex"
          >
            <Button
              size={EnumSize.sm}
              variant={EnumVariant.Neutral}
              className="mx-0 ml-4"
              onClick={(e: MouseEvent) => {
                e.stopPropagation()

                closeGraph()
              }}
            >
              Close
            </Button>
          </Panel>
        )}
        <Controls className="bg-light p-1 rounded-md !border-none !shadow-lg" />
        <Background
          variant={BackgroundVariant.Dots}
          gap={16}
          size={2}
        />
      </ReactFlow>
    </div>
  )
}

function ModelNode({
  id,
  data,
  sourcePosition,
  targetPosition,
}: NodeProps & { data: GraphNodeData }): JSX.Element {
  const COLUMS_LIMIT_DEFAULT = 5
  const COLUMS_LIMIT_COLLAPSED = 2

  const models = useStoreContext(s => s.models)

  const files = useStoreFileTree(s => s.files)
  const selectFile = useStoreFileTree(s => s.selectFile)

  const activeEdges = useStoreLineage(s => s.activeEdges)
  const addActiveEdges = useStoreLineage(s => s.addActiveEdges)
  const removeActiveEdges = useStoreLineage(s => s.removeActiveEdges)

  const model = models.get(data.label)
  const file = model != null ? files.get(model.path) : undefined
  const columns = model != null ? model.columns : []

  const [showColumns, setShowColumns] = useState(
    columns.length <= COLUMS_LIMIT_DEFAULT,
  )
  const toggleEdgeById = useCallback(
    function toggleEdgeById(
      action: 'add' | 'remove',
      edgeIds: [string, string],
      connections: { ins: string[]; outs: string[] } = { ins: [], outs: [] },
    ): void {
      const edges = [
        connections.ins.map(id => toNodeOrEdgeId('source', id)),
        connections.outs.map(id => toNodeOrEdgeId('target', id)),
      ]
        .flat()
        .concat(edgeIds)

      if (action === 'remove') {
        removeActiveEdges(edges)
      }

      if (action === 'add') {
        addActiveEdges(edges)
      }
    },
    [removeActiveEdges, addActiveEdges, activeEdges],
  )
  const [columnsVisible = [], columnHidden = []] = useMemo(() => {
    const visible: Column[] = []
    const hidden: Column[] = []

    if (showColumns) return [columns, []]

    columns.forEach(column => {
      if (visible.length < COLUMS_LIMIT_COLLAPSED) {
        visible.push(column)
      } else {
        hidden.push(column)
      }
    })

    return [visible, hidden]
  }, [columns, showColumns, activeEdges])

  return (
    <div
      className={clsx(
        'text-xs font-semibold text-secondary-500 dark:text-primary-100 rounded-xl shadow-lg relative z-1',
        isTrue(data.isHighlighted) && 'border-4 border-brand-500',
      )}
    >
      <div className="drag-handle">
        <ModelNodeHandles
          key={id}
          id={id}
          className="rounded-t-lg bg-secondary-100 dark:bg-primary-900 py-2"
          sourcePosition={sourcePosition}
          targetPosition={targetPosition}
          isLeading={true}
        >
          {file != null && (
            <span
              title={
                file.isSQLMeshModelPython
                  ? 'Column lineage disabled for python models'
                  : 'SQL Model'
              }
              className="inline-block mr-2 bg-primary-30 px-2 rounded-[0.25rem] text-[0.5rem]"
            >
              {file.isSQLMeshModelPython && 'Python'}
              {file.isSQLMeshModelSQL && 'SQL'}
            </span>
          )}
          <span
            className={clsx(
              'inline-block',
              isTrue(data.isInteractive) && 'cursor-pointer hover:underline',
            )}
            onClick={(e: MouseEvent) => {
              e.stopPropagation()

              const model = models.get(id)

              if (isFalse(data.isInteractive) || model == null) return

              const file = files.get(model.path)

              if (file == null) return

              selectFile(file)
            }}
          >
            <span>{data.label}</span>
          </span>
        </ModelNodeHandles>
      </div>
      {file != null && (
        <div
          className={clsx(
            'w-full py-2 bg-theme-lighter opacity-90 cursor-default',
            columns.length <= COLUMS_LIMIT_DEFAULT && 'rounded-b-lg',
          )}
        >
          {columnsVisible.map(column => (
            <ModelColumn
              key={column.name}
              id={id}
              column={column}
              sourcePosition={sourcePosition}
              targetPosition={targetPosition}
              toggleEdgeById={toggleEdgeById}
              disabled={file.isSQLMeshModelPython}
            />
          ))}
          {columnHidden.map(column => (
            <ModelColumn
              className={clsx('invisible h-0')}
              key={column.name}
              id={id}
              column={column}
              sourcePosition={sourcePosition}
              targetPosition={targetPosition}
              toggleEdgeById={toggleEdgeById}
              disabled={file.isSQLMeshModelPython}
            />
          ))}
        </div>
      )}
      {columns.length > COLUMS_LIMIT_DEFAULT && (
        <div className="flex px-3 py-2 bg-theme-lighter rounded-b-lg cursor-default">
          <Button
            className="w-full"
            size={EnumSize.xs}
            variant={EnumVariant.Neutral}
            onClick={(e: MouseEvent) => {
              e.stopPropagation()

              setShowColumns(prev => !prev)
            }}
          >
            {showColumns ? 'Hide' : `Show ${columnHidden.length} More`}
          </Button>
        </div>
      )}
    </div>
  )
}

function ModelColumn({
  id,
  column,
  sourcePosition,
  targetPosition,
  toggleEdgeById,
  className,
  disabled = false,
}: {
  className?: string
  id: string
  column: Column
  sourcePosition?: Position
  targetPosition?: Position
  disabled?: boolean
  toggleEdgeById: (
    type: 'add' | 'remove',
    edgeIds: [string, string],
    connections?: { ins: string[]; outs: string[] },
  ) => void
}): JSX.Element {
  const { refetch: getColumnLineage, isFetching } = useApiColumnLineage(
    id,
    column.name,
  )

  const debouncedGetColumnLineage = useCallback(
    debounceAsync(getColumnLineage, 1000, true),
    [getColumnLineage],
  )

  const activeEdges = useStoreLineage(s => s.activeEdges)
  const hasActiveEdge = useStoreLineage(s => s.hasActiveEdge)
  const columns = useStoreLineage(s => s.columns)

  const columnId = toNodeOrEdgeId(id, column.name)
  const sourceId = toNodeOrEdgeId('source', columnId)
  const targetId = toNodeOrEdgeId('target', columnId)

  const previewLineage = useStoreEditor(s => s.previewLineage)
  const setPreviewLineage = useStoreEditor(s => s.setPreviewLineage)

  const [isShowing, setIsShowing] = useState(false)
  const [isActive, setIsActive] = useState(
    hasActiveEdge(sourceId) || hasActiveEdge(targetId),
  )
  const [shouldUpdate, setShouldUpdate] = useState(false)

  useEffect(() => {
    setIsActive(hasActiveEdge(sourceId) || hasActiveEdge(targetId))
  }, [activeEdges])

  useEffect(() => {
    if (shouldUpdate) {
      toggleEdgeById('add', [sourceId, targetId], columns?.[columnId])
      setShouldUpdate(false)
    }
  }, [columns, shouldUpdate])

  const lineage = previewLineage?.[id]?.columns?.[column.name]
  const hasTarget = isArrayNotEmpty(columns?.[columnId]?.outs)
  const hasSource = isArrayNotEmpty(columns?.[columnId]?.ins)

  return (
    <div
      key={column.name}
      className={clsx(
        isActive && 'bg-secondary-10 dark:bg-primary-900',
        disabled && 'opacity-50 cursor-not-allowed',
        className,
      )}
      onClick={() => {
        if (disabled) return

        if (isFalse(isActive)) {
          void debouncedGetColumnLineage().then(({ data: columnLineage }) => {
            if (columnLineage?.[id]?.[column.name] != null) {
              setPreviewLineage(previewLineage, columnLineage)
              setShouldUpdate(true)
            }
          })
        } else {
          toggleEdgeById('remove', [sourceId, targetId], columns?.[columnId])
        }
      }}
    >
      <ModelNodeHandles
        id={columnId}
        targetPosition={targetPosition}
        sourcePosition={sourcePosition}
        hasTarget={hasTarget}
        hasSource={hasSource}
        className={clsx(disabled && 'pointer-events-none')}
      >
        <div className="flex w-full justify-between">
          <div className={clsx('mr-3 flex')}>
            {isFetching && (
              <Loading className="inline-block mr-3 w-4 h-4">
                <Spinner className="w-4 h-4 mr-2" />
              </Loading>
            )}
            {lineage?.source != null && isFalse(disabled) && (
              <Popover
                onMouseEnter={() => {
                  setIsShowing(true)
                }}
                onMouseLeave={() => {
                  setIsShowing(false)
                }}
                className="relative flex"
              >
                {() => (
                  <>
                    <InformationCircleIcon className="text-secondary-500 dark:text-primary-500 inline-block mr-3 w-4 h-4" />
                    <Transition
                      show={isShowing}
                      as={Fragment}
                      enter="transition ease-out duration-200"
                      enterFrom="opacity-0 translate-y-1"
                      enterTo="opacity-100 translate-y-0"
                      leave="transition ease-in duration-150"
                      leaveFrom="opacity-100 translate-y-0"
                      leaveTo="opacity-0 translate-y-1"
                    >
                      <Popover.Panel className="absolute bottom-2 z-10 transform">
                        <div
                          className="overflow-auto scrollbar scrollbar--vertical scrollbar--horizontal max-h-[25vh] max-w-[50vw] rounded-lg bg-theme p-4 border-4 border-primary-20"
                          dangerouslySetInnerHTML={{
                            __html: `<pre class='inline-block w-full h-full'>${
                              lineage.source ?? ''
                            }</pre>`,
                          }}
                        ></div>
                      </Popover.Panel>
                    </Transition>
                  </>
                )}
              </Popover>
            )}
            {column.name}
          </div>
          <div className="text-neutral-400 dark:text-neutral-300">
            {column.type}
          </div>
        </div>
      </ModelNodeHandles>
    </div>
  )
}

function ModelNodeHandles({
  id,
  sourcePosition,
  targetPosition,
  children,
  className,
  isLeading = false,
  hasTarget = true,
  hasSource = true,
}: {
  id: string
  sourcePosition?: Position
  targetPosition?: Position
  children: React.ReactNode
  isLeading?: boolean
  className?: string
  hasTarget?: boolean
  hasSource?: boolean
}): JSX.Element {
  return (
    <div
      className={clsx(
        'flex w-full !relative px-3 py-1 items-center',
        isFalse(isLeading) && 'hover:bg-secondary-10 dark:hover:bg-primary-10',
        className,
      )}
    >
      {targetPosition === Position.Right && (
        <Handle
          type="target"
          id={toNodeOrEdgeId('target', id)}
          position={Position.Right}
          isConnectable={false}
          className={clsx(
            'w-2 h-2 rounded-full !bg-secondary-500 dark:!bg-primary-500',
            hasTarget ? 'visible' : 'invisible',
          )}
        />
      )}
      {children}
      {sourcePosition === Position.Left && (
        <Handle
          type="source"
          id={toNodeOrEdgeId('source', id)}
          position={Position.Left}
          isConnectable={false}
          className={clsx(
            isLeading
              ? '!bg-transparent -ml-2 dark:text-primary-500'
              : 'w-2 h-2 rounded-full !bg-secondary-500 dark:!bg-primary-500',
            hasSource ? 'visible' : 'invisible',
          )}
        >
          {isLeading && (
            <ArrowRightCircleIcon className="w-5 bg-theme rounded-full" />
          )}
        </Handle>
      )}
    </div>
  )
}
