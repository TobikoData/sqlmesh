export function isTrue(value: unknown): boolean {
  return value === true
}

export function isFalse(value: unknown): boolean {
  return value === false
}

export function isFalseOrNil(value: unknown): boolean {
  return isNil(value) || isFalse(value)
}

export function isString(value: unknown): value is string {
  return typeof value === 'string'
}

export function isNumber(value: unknown): value is number {
  return typeof value === 'number'
}

export function isPrimitive(value: unknown): value is Primitive {
  return isString(value) || isNumber(value) || typeof value === 'boolean'
}

export function isStringEmptyOrNil(
  value: unknown,
): value is undefined | null | '' {
  return isNil(value) || value === ''
}

export function isStringNotEmpty(value: unknown): boolean {
  return isString(value) && value.trim() !== ''
}

export function isArrayNotEmpty<T = any>(value: unknown): value is T[] {
  return Array.isArray(value) && value.length > 0
}

export function isArrayEmpty(value: unknown): boolean {
  return Array.isArray(value) && value.length === 0
}

export function isObjectEmpty(value: unknown): boolean {
  return isObject(value) && isArrayEmpty(Object.keys(value as object))
}

export function isObjectNotEmpty(value: unknown): boolean {
  return isObject(value) && isArrayNotEmpty(Object.keys(value as object))
}

export function isObject(value: unknown): boolean {
  return (
    typeof value === 'object' && value !== null && value.constructor === Object
  )
}

export function isNil(value: unknown): value is undefined | null {
  return value == null
}

export function isNotNil<T>(value: T | null | undefined): value is T {
  return value != null
}

export function isDate(value: unknown): boolean {
  return value instanceof Date
}

export function toDate(value?: string | number): Date | undefined {
  if (isNil(value)) return undefined

  try {
    const date = new Date(value)

    return isNaN(date.getTime()) ? undefined : date
  } catch {
    return undefined
  }
}

export function toDateFormat(
  date?: Date,
  format: string = 'yyyy-mm-dd',
  isUTC: boolean = true,
): string {
  if (isNil(date)) return ''

  const year = isUTC ? date.getUTCFullYear() : date.getFullYear()
  const month = toFormatted(
    isUTC ? date.getUTCMonth() + 1 : date.getMonth() + 1,
  )
  const day = toFormatted(isUTC ? date.getUTCDate() : date.getDate())
  const hour = toFormatted(isUTC ? date.getUTCHours() : date.getHours())
  const minute = toFormatted(isUTC ? date.getUTCMinutes() : date.getMinutes())
  const second = toFormatted(isUTC ? date.getUTCSeconds() : date.getSeconds())

  const formats: Record<string, string> = {
    'mm/dd/yyyy': `${month}/${day}/${year}`,
    'yyyy-mm-dd': `${year}-${month}-${day}`,
    'yyyy-mm-dd hh-mm-ss': `${year}-${month}-${day} ${hour}:${minute}:${second}`,
  }

  return formats[format] ?? date.toDateString()

  function toFormatted(n: number): string {
    return n.toString().padStart(2, '0')
  }
}

export function includes<T>(array: T[], value: T): boolean {
  return array.includes(value)
}

export function toRatio(
  top?: number,
  bottom?: number,
  multiplier = 100,
): number {
  if (isNil(top) || isNil(bottom) || bottom === 0) return 0
  if (isNaN(top) || isNaN(bottom) || isNaN(multiplier)) return 0

  return (top / bottom) * multiplier
}

export function parseJSON<T>(value: string | null): Optional<T> {
  if (isNil(value)) return undefined

  try {
    return value === 'undefined' ? undefined : JSON.parse(value ?? '')
  } catch {
    console.log('parsing error on', { value })
    return undefined
  }
}

export function debounceSync(
  fn: (...args: any) => void,
  delay: number = 500,
  immediate: boolean = false,
): (...args: any) => void {
  let timeoutID: ReturnType<typeof setTimeout> | undefined

  return function callback(...args: any) {
    const callNow = immediate && timeoutID == null

    clearTimeout(timeoutID)

    timeoutID = setTimeout(() => {
      timeoutID = undefined

      if (isFalse(immediate)) {
        fn(...args)
      }
    }, delay)

    if (callNow) {
      fn(...args)
    }
  }
}

export function uid(): string {
  const time = new Date().getTime().toString(36)
  const random = Math.random().toString(36).substring(2, 8)

  return time + random
}

export function toUniqueName(prefix?: string, suffix?: string): string {
  // Should be enough for now
  const hex = (Date.now() % 100000).toString(16)

  return `${prefix == null ? '' : `${prefix}_`}${hex}${
    suffix ?? ''
  }`.toLowerCase()
}
