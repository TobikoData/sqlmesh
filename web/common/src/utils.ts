import { clsx, type ClassValue } from 'clsx'
import { twMerge } from 'tailwind-merge'

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
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
  limitAfter = limitAfter == null ? limitBefore : Math.abs(limitAfter)

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
