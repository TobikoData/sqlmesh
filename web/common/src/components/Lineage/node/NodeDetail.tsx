import { Metadata, cn } from '@tobikodata/sqlmesh-common'

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
        label={label}
        value={value}
        className={cn('px-2 text-xs shrink-0 h-6', className)}
      />
    </>
  )
}
