import { describe, expect, test } from 'vitest'

import { getHeadlineTextSize, getTextSize } from './help'

describe('Typography Utils', () => {
  test('getHeadlineTextSize', () => {
    expect(getHeadlineTextSize(1)).toBe('text-4xl font-bold')
    expect(getHeadlineTextSize(2)).toBe('text-3xl font-bold')
    expect(getHeadlineTextSize(3)).toBe('text-2xl font-bold')
    expect(getHeadlineTextSize(4)).toBe('text-l font-semibold')
    expect(getHeadlineTextSize(5)).toBe('text-s font-semibold')
    expect(getHeadlineTextSize(6)).toBe('text-xs font-semibold')
  })
  test('getTextSize', () => {
    expect(getTextSize('s')).toBe('text-sm')
    expect(getTextSize('m')).toBe('text-base')
    expect(getTextSize('l')).toBe('text-lg')
    expect(getTextSize('xl')).toBe('text-xl')
    expect(getTextSize('2xl')).toBe('text-2xl')
    expect(getTextSize('2xs')).toBe('text-xs')
    expect(getTextSize('xs')).toBe('text-xs')
  })
})
