import { ControlButton } from '@xyflow/react'

import { cn } from '@/utils'
import { Tooltip } from '../Tooltip/Tooltip'

export function LineageControlButton({
  text,
  onClick,
  disabled = false,
  className,
  children,
}: {
  text: string
  children: React.ReactNode
  onClick?: () => void
  disabled?: boolean
  className?: string
}) {
  return (
    <Tooltip
      side="left"
      sideOffset={8}
      delayDuration={0}
      className="px-2 py-1 text-xs rounded-sm font-semibold"
      trigger={
        <div>
          <ControlButton
            onClick={onClick}
            className={cn(
              'p-0 !bg-lineage-control-background hover:!bg-lineage-control-background-hover',
              className,
            )}
            disabled={disabled}
          >
            {children}
          </ControlButton>
        </div>
      }
    >
      {text}
    </Tooltip>
  )
}
