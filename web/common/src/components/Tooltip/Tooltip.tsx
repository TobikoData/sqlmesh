import {
  TooltipProvider,
  Tooltip as TooltipRoot,
  TooltipTrigger,
  TooltipPortal,
  TooltipContent,
} from '@radix-ui/react-tooltip'
import React from 'react'

import { cn } from '@/utils'
import type { Position } from '@/types'

import './Tooltip.css'

export type TooltipSide = Extract<Position, 'top' | 'bottom' | 'left' | 'right'>
export type TooltipAlign = Extract<Position, 'center' | 'start' | 'end'>

export function Tooltip({
  delayDuration = 200,
  sideOffset = 0,
  alignOffset = 0,
  side = 'right',
  align = 'center',
  trigger,
  children,
  className,
}: {
  trigger: React.ReactNode
  side?: TooltipSide
  align?: TooltipAlign
  delayDuration?: number
  sideOffset?: number
  alignOffset?: number
  children: React.ReactNode
  className?: string
}) {
  return (
    <TooltipProvider delayDuration={delayDuration}>
      <TooltipRoot>
        <TooltipTrigger asChild>{trigger}</TooltipTrigger>
        <TooltipPortal>
          <TooltipContent
            className={cn(
              'max-w-md break-words rounded-lg bg-tooltip-background text-tooltip-foreground px-4 py-2 shadow-[hsl(206_22%_7%_/_35%)_0px_10px_38px_-10px,_hsl(206_22%_7%_/_20%)_0px_10px_20px_-15px] will-change-[transform,opacity] data-[state=delayed-open]:data-[side=bottom]:animate-slideUpAndFade data-[state=delayed-open]:data-[side=left]:animate-slideRightAndFade data-[state=delayed-open]:data-[side=right]:animate-slideLeftAndFade data-[state=delayed-open]:data-[side=top]:animate-slideDownAndFade z-10',
              className,
            )}
            align={align}
            alignOffset={alignOffset}
            sideOffset={sideOffset}
            side={side}
          >
            {children}
          </TooltipContent>
        </TooltipPortal>
      </TooltipRoot>
    </TooltipProvider>
  )
}
