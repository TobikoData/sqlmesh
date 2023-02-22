export function debounce(
  fn: (...args: any) => void,
  before: () => void,
  after: () => void,
  delay: number = 500,
): (...args: any) => void {
  let timeoutID: ReturnType<typeof setTimeout>

  return function callback(...args: any) {
    clearTimeout(timeoutID)

    if (before != null) {
      before()
    }

    timeoutID = setTimeout(() => {
      fn(...args)

      if (after != null) {
        after()
      }
    }, delay)
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
