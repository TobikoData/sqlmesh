export { filterListBy, highlightMatch }

export const EMPTY_STRING = ''
const MATCHED_LENGTH_TO_DISPLAY = 40

function highlightMatch(source: string, match: string): string {
  return source.replaceAll(
    match,
    `<b class="inline-block text-brand-500 bg-brand-10">${match}</b>`,
  )
}

function filterListBy<T extends Record<string, any> = Record<string, any>>(
  indices: Array<[T, string]> = [],
  search: string,
): Array<[T, string]> {
  return indices.reduce((acc: Array<[T, string]>, [model, index]) => {
    const idx = index.indexOf(search.toLocaleLowerCase())

    if (idx > -1) {
      const min = Math.max(0, idx - MATCHED_LENGTH_TO_DISPLAY)
      const max = Math.min(
        index.length - 1,
        idx + search.length + MATCHED_LENGTH_TO_DISPLAY,
      )

      acc.push([
        model,
        `${min > 0 ? '... ' : EMPTY_STRING}${index.slice(min, max)}${
          max < index.length ? ' ...' : EMPTY_STRING
        }`,
      ])
    }

    return acc
  }, [])
}
