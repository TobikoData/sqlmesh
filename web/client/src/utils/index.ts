export function isArrayNotEmpty(value: unknown): boolean {
  return Array.isArray(value) && value.length > 0
}

export function isArrayEmpty(value: unknown): boolean {
  return Array.isArray(value) && value.length === 0
}

export function isObjectEmpty(value: unknown): boolean {
  return isObject(value) && isArrayEmpty(Object.keys(value as object))
}

export function isObject(value: unknown): boolean {
  return typeof value === 'object' && value !== null && value.constructor === Object
}

export function isNil(value: unknown): boolean {
  return value == null
}

export function isNotNil(value: unknown): boolean {
  return value != null
}

export async function delay(time: number = 1000): Promise<void> {
  await new Promise(resolve => {
    setTimeout(resolve, time)
  })
}

export function isDate(value: unknown): boolean {
  return value instanceof Date
}

export function toDate(value?: string | number): Date | undefined {
  if (value == null) return undefined

  try {
    const date = new Date(value)

    return isNaN(date.getTime()) ? undefined : date
  } catch {
    return undefined
  }
}

export function toDateFormat(date?: Date, format: string = 'yyyy-mm-dd'): string {
  if (date == null) return ''

  const year = date.getFullYear()
  const month = date.getMonth() + 1
  const day = date.getDate()
  const hour = date.getHours()
  const minute = date.getMinutes()
  const second = date.getSeconds()

  if (format === 'yyyy-mm-dd') return `${year}-${toFormatted(month)}-${toFormatted(day)}`
  if (format === 'yyyy-mm-dd hh-mm-ss')
    return `${year}-${toFormatted(month)}-${toFormatted(day)} ${toFormatted(hour)}:${toFormatted(
      minute
    )}:${toFormatted(second)}`

  return date.toDateString()

  function toFormatted(n: number): string {
    return n.toString().padStart(2, '0')
  }
}

export function includes<T>(array: T[], value: T): boolean {
  return array.includes(value)
}

export function toRatio(top?: number, bottom?: number, multiplier = 100): number {
  if (top == null || bottom == null || bottom === 0) return 0
  if (isNaN(top) || isNaN(bottom) || isNaN(multiplier)) return 0

  return (top / bottom) * multiplier
}
