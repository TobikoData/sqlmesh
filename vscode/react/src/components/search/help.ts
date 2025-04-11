export { filterListBy, highlightMatch }

export const EMPTY_STRING = ''
const MATCHED_LENGTH_TO_DISPLAY = 40

function highlightMatch(source: string, match: string): string {
  return source.replaceAll(
    match,
    `<b class="inline-block text-brand-500 bg-brand-10">${match}</b>`,
  )
}

// NOTE: This function is used to filter the list of models in the search bar
function filterListBy<T extends Record<string, any> = Record<string, any>>(
  indices: Array<[T, string]> = [],
  search: string,
): Array<[T, string]> {
  search = search.toLocaleLowerCase()

  return indices.reduce((acc: Array<[T, string]>, [model, index]) => {
    const idx = index.toLocaleLowerCase().indexOf(search)

    if (idx < 0) return acc

    if (index.length < MATCHED_LENGTH_TO_DISPLAY) {
      acc.push([model, index])
    } else {
      const min = Math.max(0, idx - MATCHED_LENGTH_TO_DISPLAY)
      const max = Math.min(
        index.length - 1,
        idx + search.length + MATCHED_LENGTH_TO_DISPLAY,
      )

      acc.push([
        model,
        `${min > 0 ? '...' : EMPTY_STRING}${index.slice(min, max)}${
          max < index.length ? '...' : EMPTY_STRING
        }`,
      ])
    }

    return acc
  }, [])
}
