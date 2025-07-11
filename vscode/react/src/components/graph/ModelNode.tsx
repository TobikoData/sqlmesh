import { isNil, isArrayNotEmpty, isNotNil, isFalse } from '@/utils/index'
import clsx from 'clsx'
import { useMemo, useCallback, useState } from 'react'
import { ModelType, type Model } from '@/api/client'
import { useLineageFlow } from './context'
import { type GraphNodeData } from './help'
import { Position, type NodeProps } from 'reactflow'
import { ModelNodeHeaderHandles } from './ModelNodeHeaderHandles'
import { ModelColumns } from './ModelColumns'
import { fromAPIColumn, type Column } from '@/domain/column'
import { decode, type ModelEncodedFQN } from '@/domain/models'
import { toKeys } from './types'
import { MAX_VISIBLE_COLUMNS } from './constants'

export const EnumLineageNodeModelType = {
  ...ModelType,
  cte: 'cte',
  unknown: 'unknown',
} as const

export const EnumColumnType = {
  UNKNOWN: 'UNKNOWN',
  STRUCT: 'STRUCT',
} as const

export type LineageNodeModelType = keyof typeof EnumLineageNodeModelType
export type ColumnType = keyof typeof EnumColumnType

export default function ModelNode({
  id: idProp,
  data,
  sourcePosition,
  targetPosition,
}: NodeProps<GraphNodeData>): JSX.Element {
  const id = idProp as ModelEncodedFQN
  const nodeData: GraphNodeData = data ?? {
    label: '',
    type: EnumLineageNodeModelType.unknown,
    withColumns: false,
  }
  const {
    // connections,
    models,
    handleClickModel,
    lineage,
    lineageCache,
    selectedNodes,
    setSelectedNodes,
    mainNode,
    withConnected,
    connectedNodes,
    highlightedNodes,
    activeNodes,
  } = useLineageFlow()

  const columns: Column[] = useMemo(() => {
    const modelsArray = Object.values(models)
    const decodedId = decode(id)
    const model = modelsArray.find((m: Model) => m.fqn === decodedId)
    const modelColumns = model?.columns?.map(fromAPIColumn) ?? []

    toKeys(lineage[decodedId]?.columns ?? {}).forEach(column => {
      const found = modelColumns.find(({ name }) => name === column)
      if (isNil(found)) {
        modelColumns.push(
          fromAPIColumn({ name: column, type: EnumColumnType.UNKNOWN }),
        )
      }
    })
    return modelColumns.map(column => {
      let columnType = column.type ?? EnumColumnType.UNKNOWN
      if (columnType.startsWith(EnumColumnType.STRUCT)) {
        columnType = EnumColumnType.STRUCT
      }
      return {
        ...column,
        type: columnType,
      }
    })
  }, [id, models, lineage])

  const highlightedNodeModels = useMemo(
    () => Object.values(highlightedNodes).flat(),
    [highlightedNodes],
  )

  const [isMouseOver, setIsMouseOver] = useState(false)

  const handleClick = useCallback(
    (e: React.MouseEvent) => {
      e.stopPropagation()
      if (handleClickModel) {
        handleClickModel(id)
      }
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

  const splat = highlightedNodes['*']
  // const hasSelectedColumns = columns.some(({ name }) =>
  //   connections.get(toID(id, name)),
  // )
  const hasHighlightedNodes = Object.keys(highlightedNodes).length > 0
  const highlighted = Object.keys(highlightedNodes).find(key =>
    highlightedNodes[key]!.includes(id),
  )
  const isMainNode = mainNode === id
  const isHighlightedNode = highlightedNodeModels.includes(id)
  const isSelected = selectedNodes.has(id)
  // Ensure nodeData.type is a valid LineageNodeModelType
  const nodeType: LineageNodeModelType = Object.values(
    EnumLineageNodeModelType,
  ).includes(nodeData.type)
    ? (nodeData.type as LineageNodeModelType)
    : EnumLineageNodeModelType.unknown

  const isModelSQL = nodeType === EnumLineageNodeModelType.sql
  const isCTE = nodeType === EnumLineageNodeModelType.cte
  const isModelExternal = nodeType === EnumLineageNodeModelType.external
  const isModelSeed = nodeType === EnumLineageNodeModelType.seed
  const isModelUnknown = nodeType === EnumLineageNodeModelType.unknown
  const showColumns =
    nodeData.withColumns &&
    isArrayNotEmpty(columns) &&
    isFalse(hasHighlightedNodes)
  const isActiveNode =
    selectedNodes.size > 0 || activeNodes.size > 0 || withConnected
      ? isSelected ||
        activeNodes.has(id as ModelEncodedFQN) ||
        (withConnected && connectedNodes.has(id))
      : connectedNodes.has(id)
  const isInteractive = true
  // mainNode !== id &&
  // isNotNil(handleClickModel) &&
  // isFalse(isCTE) &&
  // isFalse(isModelUnknown)
  const shouldDisableColumns = isFalse(isModelSQL)

  return (
    <div
      onMouseEnter={() => setIsMouseOver(true)}
      onMouseLeave={() => setIsMouseOver(false)}
      className={clsx(
        'text-xs font-semibold border-4',
        isMouseOver ? 'z-50' : 'z-1',
        showColumns ? 'rounded-xl' : 'rounded-2xl',
        (hasHighlightedNodes ? isHighlightedNode : isActiveNode) || isMainNode
          ? 'opacity-100'
          : 'opacity-40 hover:opacity-100',
        isNil(highlighted)
          ? hasHighlightedNodes
            ? splat
            : [
                isCTE
                  ? 'border-accent-500 bg-accent-500 text-accent-500 dark:border-accent-300 dark:bg-accent-300 dark:text-accent-300'
                  : isModelUnknown
                    ? 'border-neutral-500 bg-neutral-500 text-neutral-500 dark:border-neutral-300 dark:bg-neutral-300 dark:text-neutral-300'
                    : 'border-secondary-500 bg-secondary-500 text-secondary-500 dark:bg-primary-500  dark:border-primary-500 dark:text-primary-500',
                isMainNode
                  ? 'ring-8 ring-brand-50'
                  : isModelExternal || isModelSeed
                    ? 'ring-8 ring-accent-50'
                    : '',
              ]
          : highlighted,
        isSelected && isCTE
          ? 'ring-8 ring-accent-50'
          : isSelected && isModelUnknown
            ? 'ring-8 ring-neutral-50'
            : isSelected && 'ring-8 ring-secondary-50 dark:ring-primary-50',
      )}
      style={{
        maxWidth: isNil(nodeData.width)
          ? 'auto'
          : `${nodeData.width as number}px`,
      }}
    >
      <ModelNodeHeaderHandles
        id={id}
        type={nodeType}
        label={nodeData.label}
        isSelected={isSelected}
        isDraggable={true}
        className={clsx(
          'bg-theme-lighter',
          showColumns ? 'rounded-t-[8px]' : 'rounded-xl',
        )}
        hasLeft={targetPosition === Position.Left && isNil(lineageCache)}
        hasRight={sourcePosition === Position.Right && isNil(lineageCache)}
        handleClick={isInteractive ? handleClick : undefined}
        handleSelect={
          mainNode === id ||
          isCTE ||
          hasHighlightedNodes ||
          isNotNil(lineageCache)
            ? undefined
            : handleSelect
        }
        numberOfColumns={columns.length}
      />
      {showColumns && (
        <ModelColumns
          className="nowheel rounded-b-lg bg-theme-lighter text-xs"
          nodeId={id}
          limit={MAX_VISIBLE_COLUMNS}
          columns={columns}
          disabled={shouldDisableColumns}
          withHandles={true}
          withDescription={false}
          maxHeight="10rem"
        />
      )}
    </div>
  )
}
