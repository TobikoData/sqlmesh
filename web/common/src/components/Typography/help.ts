import type { HeadlineLevel, Size } from '@sqlmesh-common/types'

export function getHeadlineTextSize(level: HeadlineLevel) {
  return {
    1: 'text-4xl font-bold',
    2: 'text-3xl font-bold',
    3: 'text-2xl font-bold',
    4: 'text-l font-semibold',
    5: 'text-s font-semibold',
    6: 'text-xs font-semibold',
  }[level]
}

export function getTextSize(size: Size) {
  return {
    '2xs': 'text-xs',
    xs: 'text-xs',
    s: 'text-sm',
    m: 'text-base',
    l: 'text-lg',
    xl: 'text-xl',
    '2xl': 'text-2xl',
  }[size]
}
