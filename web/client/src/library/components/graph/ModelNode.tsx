import { isNil, isArrayNotEmpty, isNotNil, toID, isFalse } from '@utils/index'
import clsx from 'clsx'
import { useMemo, useCallback, useState } from 'react'
import {
  EnumLineageNodeModelType,
  ModelColumns,
  ModelNodeHeaderHandles,
} from './Graph'
import { useLineageFlow } from './context'
import { type GraphNodeData } from './help'
import { Position, type NodeProps } from 'reactflow'

export const EnumColumnType = {
  UNKNOWN: 'UNKNOWN',
  STRUCT: 'STRUCT',
} as const

export type ColumnType = KeyOf<typeof EnumColumnType>

export default function ModelNode({
  id,
  data,
  sourcePosition,
  targetPosition,
}: NodeProps): JSX.Element {
  const nodeData: GraphNodeData = data ?? {}
  const {
    connections,
    models,
    handleClickModel,
    lineage = {},
    selectedNodes,
    setSelectedNodes,
    mainNode,
    activeNodes,
    withConnected,
    connectedNodes,
    highlightedNodes,
  } = useLineageFlow()

  const { columns } = useMemo(() => {
    const model = models.get(id)
    const columns = model?.columns ?? []

    Object.keys(lineage[id]?.columns ?? {}).forEach((column: string) => {
      const found = columns.find(({ name }) => name === column)

      if (isNil(found)) {
        columns.push({ name: column, type: EnumColumnType.UNKNOWN })
      }
    })

    columns.forEach(column => {
      let columnType = column.type ?? EnumColumnType.UNKNOWN

      if (columnType.startsWith(EnumColumnType.STRUCT)) {
        columnType = EnumColumnType.STRUCT
      }

      column.type = columnType
    })

    return {
      columns,
    }
  }, [id, models, lineage])

  const highlightedNodeModels = useMemo(
    () => Object.values(highlightedNodes).flat(),
    [highlightedNodes],
  )

  const [isMouseOver, setIsMouseOver] = useState(false)

  const handleClick = useCallback(
    (e: React.MouseEvent) => {
      e.stopPropagation()

      handleClickModel?.(id)
    },
    [handleClickModel, id, data.isInteractive],
  )

  const handleSelect = useCallback(
    (e: React.MouseEvent) => {
      e.stopPropagation()

      if (highlightedNodeModels.includes(id) || mainNode === id) return

      setSelectedNodes(current => {
        if (current.has(id)) {
          current.delete(id)
        } else {
          current.add(id)
        }

        return new Set(current)
      })
    },
    [setSelectedNodes, highlightedNodeModels],
  )

  const hasSelectedColumns = columns.some(({ name }) =>
    connections.get(toID(id, name)),
  )
  const hasHighlightedNodes = Object.keys(highlightedNodes).length > 0
  const highlighted = Object.keys(highlightedNodes ?? {}).find(key =>
    highlightedNodes[key]!.includes(id),
  )
  const splat = highlightedNodes?.['*']
  const isMainNode = mainNode === id
  const isHighlightedNode = highlightedNodeModels.includes(id)
  const isSelected = selectedNodes.has(id)
  const isInteractive = mainNode !== id && isNotNil(handleClickModel)
  const isCTE = nodeData.type === EnumLineageNodeModelType.cte
  const isModelExternal = nodeData.type === EnumLineageNodeModelType.external
  const isModelSeed = nodeData.type === EnumLineageNodeModelType.seed
  const isModelPython = nodeData.type === EnumLineageNodeModelType.python
  const showColumns =
    (hasSelectedColumns ||
      nodeData.withColumns ||
      isMouseOver ||
      isSelected ||
      isMainNode) &&
    isArrayNotEmpty(columns) &&
    isFalse(hasHighlightedNodes)
  const isActiveNode =
    selectedNodes.size > 0 || activeNodes.size > 0 || withConnected
      ? isSelected ||
        activeNodes.has(id) ||
        (withConnected && connectedNodes.has(id))
      : connectedNodes.has(id)

  return (
    <div
      onMouseEnter={() => setIsMouseOver(true)}
      onMouseLeave={() => setIsMouseOver(false)}
      className={clsx(
        'text-xs font-semibold rounded-lg shadow-lg relative z-1',
        isCTE ? 'text-neutral-100' : 'text-secondary-500 dark:text-primary-100',
        (isModelExternal || isModelSeed) &&
          'border-4 border-accent-500 ring-8 ring-accent-200',
        isSelected && 'border-4 border-secondary-500 ring-8 ring-secondary-200',
        isNil(highlighted)
          ? isMainNode
            ? 'ring-8 ring-brand-200'
            : splat
          : highlighted,
        (hasHighlightedNodes ? isHighlightedNode : isActiveNode) || isMainNode
          ? 'opacity-100'
          : 'opacity-40 hover:opacity-100',
      )}
      style={{
        maxWidth: isNil(nodeData.width)
          ? 'auto'
          : `${nodeData.width as number}px`,
      }}
    >
      <ModelNodeHeaderHandles
        id={id}
        type={nodeData.type}
        label={nodeData.label}
        isSelected={isSelected}
        isDraggable={true}
        className={clsx(
          'rounded-md',
          isCTE
            ? 'bg-accent-500'
            : isMainNode
            ? 'bg-brand-500 text-brand-100 font-black'
            : 'bg-secondary-100 dark:bg-primary-900',
        )}
        hasLeft={targetPosition === Position.Left}
        hasRight={sourcePosition === Position.Right}
        handleClick={isInteractive ? handleClick : undefined}
        handleSelect={
          mainNode === id || isCTE || highlightedNodeModels.includes(id)
            ? undefined
            : handleSelect
        }
        count={columns.length}
      />
      {showColumns && (
        <>
          <ModelColumns
            className="max-h-[15rem]"
            nodeId={id}
            columns={columns}
            disabled={isModelPython}
            withHandles={true}
            withSource={true}
            withDescription={false}
          />
          <div
            className={clsx(
              'rounded-b-md py-1',
              isCTE
                ? 'bg-accent-500'
                : isMainNode
                ? 'bg-brand-500'
                : 'bg-secondary-100 dark:bg-primary-900',
            )}
          ></div>
        </>
      )}
    </div>
  )
}
