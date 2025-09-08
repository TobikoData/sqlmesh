import type { HeadlineLevel, Size } from '@/types'

export function getHeadlineTextSize(level: HeadlineLevel) {
  const defaultSize = 'text-4xl font-bold'
  return (
    {
      1: defaultSize,
      2: 'text-3xl font-semibold',
      3: 'text-2xl font-medium',
      4: 'text-l font-medium',
      5: 'text-m font-medium',
      6: 'text-s font-medium',
    }[level] ?? defaultSize
  )
}

export function getTextSize(size: Size) {
  const defaultSize = 'text-sm'
  return (
    {
      '2xs': 'text-xs',
      xs: 'text-xs',
      s: defaultSize,
      m: 'text-base',
      l: 'text-lg',
      xl: 'text-xl',
      '2xl': 'text-2xl',
    }[size] ?? defaultSize
  )
}
