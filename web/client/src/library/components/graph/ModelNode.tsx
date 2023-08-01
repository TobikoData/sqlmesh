import { isNil, isTrue, isArrayNotEmpty } from '@utils/index'
import clsx from 'clsx'
import { useMemo, useCallback } from 'react'
import { ModelColumns, ModelNodeHeaderHandles } from './Graph'
import { useLineageFlow } from './context'
import { type GraphNodeData } from './help'
import { Position, type NodeProps } from 'reactflow'

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
    adjacentNodes,
    mainNode,
    activeNodes,
    withAdjacent,
  } = useLineageFlow()

  const { model, columns } = useMemo(() => {
    const model = models.get(id)
    const columns = model?.columns ?? []

    Object.keys(lineage[id]?.columns ?? {}).forEach((column: string) => {
      const found = columns.find(({ name }) => name === column)

      if (isNil(found)) {
        columns.push({ name: column, type: 'UNKNOWN' })
      }
    })

    columns.forEach(column => {
      column.type = isNil(column.type)
        ? 'UNKNOWN'
        : column.type.startsWith('STRUCT')
        ? 'STRUCT'
        : column.type
    })

    return {
      model,
      columns,
    }
  }, [id, models, lineage])

  const highlightedNodes = useMemo(
    () => Object.values(data.highlightedNodes ?? {}).flat(),
    [data.highlightedNodes],
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

      if (highlightedNodes.includes(id) || mainNode === id) return

      setSelectedNodes(current => {
        if (current.has(id)) {
          current.delete(id)
        } else {
          current.add(id)
        }

        return new Set(current)
      })
    },
    [setSelectedNodes, highlightedNodes],
  )

  const highlighted = Object.keys(data.highlightedNodes ?? {}).find(key =>
    data.highlightedNodes[key].includes(id),
  )
  const splat = data.highlightedNodes?.['*']
  const isInteractive = isTrue(data.isInteractive) && handleClickModel != null
  const isCTE = data.type === 'cte'
  const isModelExternal = model?.type === 'external'
  const isModelSeed = model?.type === 'seed'
  const showColumns = withColumns && isArrayNotEmpty(columns)
  const type = isCTE ? 'cte' : model?.type
  const isMainNode = mainNode === id || highlightedNodes.includes(id)
  const isActiveNode =
    selectedNodes.size > 0 || activeNodes.size > 0 || withAdjacent
      ? selectedNodes.has(id) ||
        activeNodes.has(id) ||
        (withAdjacent && adjacentNodes.has(id))
      : adjacentNodes.has(id)

  return (
    <div
      className={clsx(
        'text-xs font-semibold rounded-lg shadow-lg relative z-1',
        isCTE ? 'text-neutral-100' : 'text-secondary-500 dark:text-primary-100',
        (isModelExternal || isModelSeed) && 'border-4 border-accent-500',
        mainNode === id && 'border-4 border-brand-500',
        selectedNodes.has(id) && 'ring-8 ring-success-300',
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
        type={type}
        label={data.label}
        isSelected={selectedNodes.has(id)}
        isDraggable={true}
        className={clsx(
          showColumns ? 'rounded-t-md' : 'rounded-lg',
          isCTE ? 'bg-accent-500' : 'bg-secondary-100 dark:bg-primary-900',
        )}
        hasLeft={targetPosition === Position.Left}
        hasRight={sourcePosition === Position.Right}
        handleClick={isInteractive ? handleClick : undefined}
        handleSelect={
          mainNode === id || isCTE || highlightedNodes.includes(id)
            ? undefined
            : handleSelect
        }
      />
      {showColumns && isArrayNotEmpty(columns) && (
        <>
          <ModelColumns
            className="max-h-[15rem]"
            nodeId={id}
            columns={columns}
            disabled={model?.type === 'python' || data.type !== 'model'}
            withHandles={true}
            withSource={true}
          />
          <div
            className={clsx(
              'rounded-b-md py-1',
              isCTE ? 'bg-accent-500' : 'bg-secondary-100 dark:bg-primary-900',
            )}
          ></div>
        </>
      )}
    </div>
  )
}
