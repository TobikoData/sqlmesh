import { ArrowRight } from 'lucide-react'

import { cn } from '@sqlmesh-common/utils'

export function NodeHandleIcon({
  className,
  iconSize = 20,
}: {
  className?: string
  iconSize?: number
}) {
  return (
    <ArrowRight
      data-component="NodeHandleIcon"
      size={iconSize}
      className={cn(
        'flex-shrink-0 p-0.5 border-2 rounded-full bg-lineage-node-handle-icon-background',
        className,
      )}
      strokeWidth={3}
    />
  )
}
