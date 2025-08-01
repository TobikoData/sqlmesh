import type { Nil } from '@/types'

export function isNil(value: unknown): value is Nil {
  return value == null
}
export function notNil(value: unknown): value is NonNullable<unknown> {
  return value != null
}
