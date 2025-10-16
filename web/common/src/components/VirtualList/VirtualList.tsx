import {
  useVirtualizer,
  Virtualizer,
  type VirtualItem,
} from '@tanstack/react-virtual'
import React from 'react'
import { HorizontalContainer } from '../HorizontalContainer/HorizontalContainer'
import { cn } from '@sqlmesh-common/utils'
import { Button } from '../Button/Button'
import { ScrollContainer } from '../ScrollContainer/ScrollContainer'
import { VerticalContainer } from '../VerticalContainer/VerticalContainer'

export interface VirtualListProps<TItem> {
  items: TItem[]
  estimatedListItemHeight: number
  renderListItem: (
    item: TItem,
    virtualItem?: VirtualItem,
    virtualizer?: Virtualizer<HTMLDivElement, Element>,
  ) => React.ReactNode
  isSelected?: (item: TItem) => boolean
  className?: string
}

export function VirtualList<TItem>({
  items,
  estimatedListItemHeight,
  renderListItem,
  isSelected,
  className,
}: VirtualListProps<TItem>) {
  const scrollableAreaRef = React.useRef<HTMLDivElement>(null)

  const [activeItemIndex] = React.useMemo(() => {
    let activeIndex = -1
    const itemsLength = items.length

    for (let i = 0; i < itemsLength; i++) {
      if (isSelected?.(items[i])) {
        activeIndex = i
        break
      }
    }

    return [activeIndex]
  }, [items, isSelected])

  const rowVirtualizer = useVirtualizer({
    count: items.length,
    getScrollElement: () => scrollableAreaRef.current,
    estimateSize: () => estimatedListItemHeight,
  })

  const scrollToItem = React.useCallback(
    ({
      itemIndex,
      isSmoothScroll = true,
    }: {
      itemIndex: number
      isSmoothScroll?: boolean
    }): void => {
      rowVirtualizer.scrollToIndex(itemIndex, {
        align: 'center',
        behavior: isSmoothScroll ? 'smooth' : 'auto',
      })
    },
    [rowVirtualizer],
  )

  const isOutsideVisibleRange = React.useCallback(
    (itemIndex: number): boolean => {
      const range = rowVirtualizer.range
      return (
        range !== null &&
        (range.startIndex > itemIndex || range?.endIndex < itemIndex)
      )
    },
    [rowVirtualizer],
  )

  /**
   * The return button should appear when the
   * active item is available in the list (not
   * filtered out) and it is not in the visible
   * range of the virtualized list
   */
  const shouldShowReturnButton =
    activeItemIndex > -1 && isOutsideVisibleRange(activeItemIndex)
  const rows = rowVirtualizer.getVirtualItems()
  const totalSize = rowVirtualizer.getTotalSize()

  return (
    <HorizontalContainer
      data-component="VirtualList"
      className={cn('p-1', className)}
    >
      {shouldShowReturnButton && (
        <Button
          className="absolute left-[50%] translate-x-[-50%] z-10 shadow-md top-1"
          onClick={() => scrollToItem({ itemIndex: activeItemIndex })}
          size="2xs"
          variant="alternative"
        >
          Scroll to selected
        </Button>
      )}
      <ScrollContainer
        ref={scrollableAreaRef}
        className="h-auto overflow-auto"
      >
        <div
          style={{
            height: totalSize > 0 ? `${totalSize}px` : '100%',
            contain: 'strict',
          }}
        >
          <VerticalContainer
            className="absolute top-0 left-0 px-1"
            style={{ transform: `translateY(${rows[0]?.start ?? 0}px)` }}
            role="list"
          >
            {rows.map(row => renderListItem(items[row.index]))}
          </VerticalContainer>
        </div>
      </ScrollContainer>
    </HorizontalContainer>
  )
}
