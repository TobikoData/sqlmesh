import { Info } from 'lucide-react'
import React from 'react'

import { cn } from '@sqlmesh-common/utils'
import { getTextSize } from './help'
import type { Size } from '@sqlmesh-common/types'
import { Tooltip } from '../Tooltip/Tooltip'

export interface InformationProps {
  children?: React.ReactNode
  className?: string
  classNameTooltip?: string
  side?: 'right' | 'left'
  size?: Size
  sideOffset?: number
  delayDuration?: number
  info?: React.ReactNode
  infoIcon?: React.ReactNode
}

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
}: InformationProps) {
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
          'z-50 select-none whitespace-wrap rounded-md',
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
