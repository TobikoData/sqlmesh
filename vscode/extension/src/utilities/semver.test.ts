import { describe, it, expect } from 'vitest'
import { isSemVerGreaterThanOrEqual } from './semver'

describe('isSemVerGreaterThanOrEqual', () => {
  it('should return true when major version is greater', () => {
    expect(isSemVerGreaterThanOrEqual([2, 0, 0], [1, 0, 0])).toBe(true)
    expect(isSemVerGreaterThanOrEqual([3, 0, 0], [2, 5, 10])).toBe(true)
  })

  it('should return false when major version is less', () => {
    expect(isSemVerGreaterThanOrEqual([1, 0, 0], [2, 0, 0])).toBe(false)
    expect(isSemVerGreaterThanOrEqual([0, 10, 20], [1, 0, 0])).toBe(false)
  })

  it('should compare minor version when major versions are equal', () => {
    expect(isSemVerGreaterThanOrEqual([1, 2, 0], [1, 1, 0])).toBe(true)
    expect(isSemVerGreaterThanOrEqual([1, 1, 0], [1, 2, 0])).toBe(false)
    expect(isSemVerGreaterThanOrEqual([2, 5, 0], [2, 3, 10])).toBe(true)
  })

  it('should compare patch version when major and minor versions are equal', () => {
    expect(isSemVerGreaterThanOrEqual([1, 1, 2], [1, 1, 1])).toBe(true)
    expect(isSemVerGreaterThanOrEqual([1, 1, 1], [1, 1, 2])).toBe(false)
    expect(isSemVerGreaterThanOrEqual([2, 3, 10], [2, 3, 5])).toBe(true)
  })

  it('should return true when versions are equal', () => {
    expect(isSemVerGreaterThanOrEqual([1, 0, 0], [1, 0, 0])).toBe(true)
    expect(isSemVerGreaterThanOrEqual([2, 5, 10], [2, 5, 10])).toBe(true)
  })

  it('should handle zero versions correctly', () => {
    expect(isSemVerGreaterThanOrEqual([0, 0, 1], [0, 0, 0])).toBe(true)
    expect(isSemVerGreaterThanOrEqual([0, 1, 0], [0, 0, 10])).toBe(true)
    expect(isSemVerGreaterThanOrEqual([0, 0, 0], [0, 0, 0])).toBe(true)
  })
})
