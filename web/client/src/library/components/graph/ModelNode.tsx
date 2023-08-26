import { isNil, isArrayNotEmpty, isNotNil } from '@utils/index'
import clsx from 'clsx'
import { useMemo, useCallback } from 'react'
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
}: NodeProps & { data: GraphNodeData }): JSX.Element {
  const {
    models,
    withColumns,
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

  const { model, columns } = useMemo(() => {
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
      model,
      columns,
    }
  }, [id, models, lineage])

  const highlightedNodeModels = useMemo(
    () => Object.values(highlightedNodes ?? {}).flat(),
    [highlightedNodes],
  )

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

  const highlighted = Object.keys(highlightedNodes ?? {}).find(key =>
    highlightedNodes[key]!.includes(id),
  )
  const splat = highlightedNodes?.['*']
  const isInteractive = mainNode !== id && isNotNil(handleClickModel)
  const isCTE = data.type === EnumLineageNodeModelType.cte
  const isModelExternal = model?.type === EnumLineageNodeModelType.external
  const isModelSeed = model?.type === EnumLineageNodeModelType.seed
  const showColumns = withColumns && isArrayNotEmpty(columns)
  const isMainNode = mainNode === id || highlightedNodeModels.includes(id)
  const isActiveNode =
    selectedNodes.size > 0 || activeNodes.size > 0 || withConnected
      ? selectedNodes.has(id) ||
        activeNodes.has(id) ||
        (withConnected && connectedNodes.has(id))
      : connectedNodes.has(id)

  return (
    <div
      className={clsx(
        'text-xs font-semibold rounded-lg shadow-lg relative z-1',
        isCTE ? 'text-neutral-100' : 'text-secondary-500 dark:text-primary-100',
        (isModelExternal || isModelSeed) &&
          'border-4 border-accent-500 ring-8 ring-accent-200',
        selectedNodes.has(id) &&
          'border-4 border-secondary-500 ring-8 ring-secondary-200',
        isMainNode && 'ring-8 ring-brand-200',
        isNil(highlighted) ? splat : highlighted,
        isActiveNode || isMainNode
          ? 'opacity-100'
          : 'opacity-40 hover:opacity-100',
      )}
      style={{
        maxWidth: isNil(data.width) ? 'auto' : `${data.width as number}px`,
      }}
    >
      <ModelNodeHeaderHandles
        id={id}
        type={data.type}
        label={data.label}
        isSelected={selectedNodes.has(id)}
        isDraggable={true}
        className={clsx(
          showColumns ? 'rounded-t-md' : 'rounded-lg',
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
      {showColumns && isArrayNotEmpty(columns) && (
        <>
          <ModelColumns
            className="max-h-[15rem]"
            nodeId={id}
            columns={columns}
            disabled={model?.type !== EnumLineageNodeModelType.sql}
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
