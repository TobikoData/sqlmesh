import type { EmptyString, Maybe, Nil } from '@/types'
import { clsx, type ClassValue } from 'clsx'
import { twMerge } from 'tailwind-merge'

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

export function isNil(value: unknown): value is Nil {
  return value == null
}
export function notNil(value: unknown): value is NonNullable<unknown> {
  return value != null
}
export function isString(value: unknown): value is string {
  return typeof value === 'string'
}
export function notString(value: unknown): value is typeof value {
  return !isString(value)
}
export function isEmptyString(value: unknown): value is EmptyString {
  return isString(value) && value.length === 0
}
export function isNilOrEmptyString(
  value: unknown,
): value is Maybe<EmptyString> {
  return isNil(value) || isEmptyString(value)
}
export function nonEmptyString(value: unknown): value is string {
  return isString(value) && value.length > 0
}
export function truncate(
  text: string,
  maxChars = 0,
  limitBefore = 5,
  delimiter = '...',
  limitAfter?: number,
): string {
  const textLength = text.length
  limitBefore = Math.abs(limitBefore)
  limitAfter = isNil(limitAfter) ? limitBefore : Math.abs(limitAfter)

  if (maxChars > textLength || limitBefore + limitAfter >= textLength) {
    return text
  }

  if (limitAfter === 0) {
    return text.substring(0, limitBefore) + delimiter
  }

  return (
    text.substring(0, limitBefore) +
    delimiter +
    text.substring(textLength - limitAfter)
  )
}
