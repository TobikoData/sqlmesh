import type { Nil } from '@/types'
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
