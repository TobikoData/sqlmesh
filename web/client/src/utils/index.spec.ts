import { describe, it, test, expect } from 'vitest'
import {
  isArrayNotEmpty,
  isArrayEmpty,
  isObjectEmpty,
  isObject,
  isNil,
  isNotNil,
  isDate,
  toDate,
  toDateFormat,
} from './index'

describe('isArrayNotEmpty', () => {
  test('returns true for non-empty arrays', () => {
    expect(isArrayNotEmpty([1, 2, 3])).toBe(true)
  })

  test('returns false for empty arrays', () => {
    expect(isArrayNotEmpty([])).toBe(false)
  })

  test('returns false for non-array values', () => {
    expect(isArrayNotEmpty(123)).toBe(false)
    expect(isArrayNotEmpty('abc')).toBe(false)
    expect(isArrayNotEmpty({})).toBe(false)
    expect(isArrayNotEmpty(null)).toBe(false)
    expect(isArrayNotEmpty(undefined)).toBe(false)
  })
})

describe('isArrayEmpty', () => {
  test('returns false for non-empty arrays', () => {
    expect(isArrayEmpty([1, 2, 3])).toBe(false)
  })

  test('returns true for empty arrays', () => {
    expect(isArrayEmpty([])).toBe(true)
  })

  test('returns false for non-array values', () => {
    expect(isArrayEmpty(123)).toBe(false)
    expect(isArrayEmpty('abc')).toBe(false)
    expect(isArrayEmpty({})).toBe(false)
    expect(isArrayEmpty(null)).toBe(false)
    expect(isArrayEmpty(undefined)).toBe(false)
  })
})

describe('isObjectEmpty', () => {
  test('returns true for empty objects', () => {
    expect(isObjectEmpty({})).toBe(true)
  })

  test('returns false for non-empty objects', () => {
    expect(isObjectEmpty({ a: 1, b: 2 })).toBe(false)
  })

  test('returns false for non-object values', () => {
    expect(isObjectEmpty(123)).toBe(false)
    expect(isObjectEmpty('abc')).toBe(false)
    expect(isObjectEmpty([])).toBe(false)
    expect(isObjectEmpty(null)).toBe(false)
    expect(isObjectEmpty(undefined)).toBe(false)
  })
})

describe('isObject', () => {
  test('returns true for objects', () => {
    expect(isObject({})).toBe(true)
    expect(isObject({ a: 1, b: 2 })).toBe(true)
  })

  test('returns false for non-object values', () => {
    expect(isObject(123)).toBe(false)
    expect(isObject('abc')).toBe(false)
    expect(isObject([])).toBe(false)
    expect(isObject(null)).toBe(false)
    expect(isObject(undefined)).toBe(false)
  })
})

describe('isNil', () => {
  test('returns true for null and undefined', () => {
    expect(isNil(null)).toBe(true)
    expect(isNil(undefined)).toBe(true)
  })

  test('returns false for other values', () => {
    expect(isNil(123)).toBe(false)
    expect(isNil('abc')).toBe(false)
    expect(isNil({})).toBe(false)
    expect(isNil([])).toBe(false)
  })
})

describe('isNotNil', () => {
  it('should return true for a non-nil value', () => {
    expect(isNotNil('foo')).toBe(true)
  })

  it('should return false for a nil value', () => {
    expect(isNotNil(null)).toBe(false)
  })
})

describe('isDate', () => {
  it('returns true for a Date object', () => {
    expect(isDate(new Date())).toBe(true)
  })

  it('returns false for a string', () => {
    expect(isDate('2023-02-07')).toBe(false)
  })

  it('returns false for a number', () => {
    expect(isDate(123456789)).toBe(false)
  })
})

describe('toDate', () => {
  it('returns a Date object for a valid string date', () => {
    const date = toDate('2023-02-07')
    expect(isDate(date)).toBe(true)
  })

  it('returns a Date object for a valid numeric timestamp', () => {
    const date = toDate(1612738400000)
    expect(isDate(date)).toBe(true)
  })

  it('returns undefined for an invalid date string', () => {
    expect(toDate('not a date')).toBe(undefined)
  })

  it('returns undefined for an invalid numeric value', () => {
    expect(toDate('not a number')).toBe(undefined)
  })
})

describe('toDateFormat', () => {
  it('returns an empty string for a null date', () => {
    expect(toDateFormat(undefined)).toBe('')
  })

  it('returns a formatted date string for a valid date and default format', () => {
    expect(toDateFormat(new Date('2023-02-07 00:00:00'))).toBe('2023-02-07')
  })

  it('returns a default formatted date string for a unsupported custom format', () => {
    expect(toDateFormat(new Date('2023-02-07 00:00:00'), 'dd/mm/yyyy')).toBe(
      'Tue Feb 07 2023',
    )
  })
})
