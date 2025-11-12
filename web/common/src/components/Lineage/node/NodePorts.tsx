import { cn } from '@sqlmesh-common/utils'
import { VirtualList } from '@sqlmesh-common/components/VirtualList/VirtualList'
import { FilterableList } from '@sqlmesh-common/components/VirtualList/FilterableList'
import type { IFuseOptions } from 'fuse.js'

export function NodePorts<TPort>({
  ports,
  estimatedListItemHeight,
  renderPort,
  className,
  isFilterable = true,
  filterOptions,
}: {
  ports: TPort[]
  estimatedListItemHeight: number
  renderPort: (port: TPort) => React.ReactNode
  className?: string
  isFilterable?: boolean
  filterOptions?: IFuseOptions<TPort>
}) {
  function renderVirtualList(items: TPort[]) {
    return (
      <VirtualList
        items={items}
        estimatedListItemHeight={estimatedListItemHeight}
        renderListItem={item => renderPort(item)}
        className={cn(!isFilterable && className)}
      />
    )
  }
  return isFilterable ? (
    <FilterableList
      data-component="NodePorts"
      items={ports}
      placeholder="Filter by name or description..."
      filterOptions={filterOptions}
      className={cn('nowheel', className)}
    >
      {renderVirtualList}
    </FilterableList>
  ) : (
    renderVirtualList(ports)
  )
}
