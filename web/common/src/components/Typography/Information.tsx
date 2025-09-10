import { Info } from 'lucide-react'
import React from 'react'

import { cn } from '@/utils'
import { getTextSize } from './help'
import type { Size } from '@/types'
import { Tooltip } from '../Tooltip/Tooltip'

export function Information({
  children,
  className,
  classNameTooltip,
  side = 'right',
  size = 's',
  sideOffset = 4,
  delayDuration = 200,
  info,
  infoIcon = (
    <Info
      aria-label="Info Icon"
      size={16}
    />
  ),
  ...props
}: {
  children?: React.ReactNode
  className?: string
  classNameTooltip?: string
  side?: 'right' | 'left'
  size?: Size
  sideOffset?: number
  delayDuration?: number
  info?: React.ReactNode
  infoIcon?: React.ReactNode
}) {
  return (
    <div
      data-component="Information"
      className={cn('flex items-center gap-2 text-typography-info', className)}
      {...props}
    >
      {children}
      <Tooltip
        delayDuration={delayDuration}
        sideOffset={sideOffset}
        side={side}
        className={cn(
          'z-50 select-none max-w-md whitespace-wrap rounded-md bg-dark text-light px-4 py-2 shadow-[hsl(206_22%_7%_/_35%)_0px_10px_38px_-10px,_hsl(206_22%_7%_/_20%)_0px_10px_20px_-15px] will-change-[transform,opacity] data-[state=delayed-open]:data-[side=bottom]:animate-slideUpAndFade data-[state=delayed-open]:data-[side=left]:animate-slideRightAndFade data-[state=delayed-open]:data-[side=right]:animate-slideLeftAndFade data-[state=delayed-open]:data-[side=top]:animate-slideDownAndFade',
          getTextSize(size),
          classNameTooltip,
        )}
        trigger={infoIcon}
      >
        {info}
      </Tooltip>
    </div>
  )
}
