import { type ReactNode } from 'react'
import { ChevronDownIcon, ChevronRightIcon } from '@heroicons/react/24/outline'
import { Card, CardHeader, CardContent } from './Card'
import { twColors, twMerge } from './tailwind-utils'

interface Props {
  id: string
  title: string
  children: ReactNode
  expanded: boolean
  onToggle: () => void
  badge?: { text: string; color?: string }
}

export function SectionCard({
  title,
  children,
  expanded,
  onToggle,
  badge,
}: Props) {
  return (
    <Card className="mb-4">
      <CardHeader className="p-0">
        <button
          onClick={onToggle}
          className={twMerge(
            'w-full flex items-center justify-between px-3 py-1 text-left transition-colors',
            twColors.bgHover,
            twColors.textForeground,
          )}
        >
          <div className="flex items-center gap-1.5">
            <span className="font-medium text-xs">{title}</span>
            {badge && (
              <span
                className={twMerge(
                  'inline-block px-2 py-0.5 text-xs font-medium rounded-full',
                  badge.color || twColors.bgNeutral10,
                )}
              >
                {badge.text}
              </span>
            )}
          </div>
          {expanded ? (
            <ChevronDownIcon className="w-3 h-3" />
          ) : (
            <ChevronRightIcon className="w-3 h-3" />
          )}
        </button>
      </CardHeader>
      <div
        className={twMerge(
          'transition-all duration-200 ease-in-out overflow-hidden',
          expanded ? 'max-h-[2000px]' : 'max-h-0',
        )}
      >
        <CardContent>{children}</CardContent>
      </div>
    </Card>
  )
}
