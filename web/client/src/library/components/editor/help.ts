import { isFalse } from '~/utils'

export function debounce(
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

export function getLanguageByExtension(extension?: string): string {
  switch (extension) {
    case '.sql':
      return 'SQL'
    case '.py':
      return 'Python'
    case '.yaml':
    case '.yml':
      return 'YAML'
    default:
      return 'Plain Text'
  }
}
