import { Metadata, cn } from '@sqlmesh-common/index'

import { NodeDivider } from './NodeDivider'

export function NodeDetail({
  label,
  value,
  hasDivider = true,
  className,
}: {
  label: string
  value: string
  hasDivider?: boolean
  className?: string
}) {
  return (
    <>
      {hasDivider && <NodeDivider />}
      <Metadata
        data-component="NodeDetail"
        label={label}
        value={value}
        className={cn('px-2 text-xs shrink-0 h-6', className)}
      />
    </>
  )
}
