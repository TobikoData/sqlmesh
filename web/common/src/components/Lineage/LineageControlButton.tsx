import { ControlButton } from '@xyflow/react'

import { cn } from '@sqlmesh-common/utils'
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
  onClick?: (e: React.MouseEvent<HTMLButtonElement>) => void
  disabled?: boolean
  className?: string
}) {
  return (
    <Tooltip
      side="left"
      sideOffset={8}
      delayDuration={0}
      className="px-2 py-1 text-xs rounded-sm font-semibold bg-lineage-control-button-tooltip-background text-lineage-control-button-tooltip-foreground border-2 border-lineage-control-button-tooltip-border"
      trigger={
        <div data-component="LineageControlButton">
          <ControlButton
            onClick={onClick}
            className={cn(
              'p-0 !bg-lineage-control-background hover:!bg-lineage-control-background-hover',
              className,
            )}
            disabled={disabled}
            title={text}
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
