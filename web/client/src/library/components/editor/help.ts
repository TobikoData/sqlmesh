import { type Table } from 'apache-arrow'
import { type Dialect, type EditorTab } from '~/context/editor'
import { isArrayNotEmpty } from '~/utils'

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

export function showIndicatorDialects(
  tab: EditorTab,
  dialects: Dialect[],
): boolean {
  return (
    tab.file.extension === '.sql' &&
    tab.file.isLocal &&
    isArrayNotEmpty(dialects)
  )
}

type TableCellValue = number | string | null
type TableRows = Array<Record<string, TableCellValue>>
type TableColumns = string[]
type ResponseTableColumns = Array<Array<[string, TableCellValue]>>

export function toTableRow(
  row: Array<[string, TableCellValue]> = [],
): Record<string, TableCellValue> {
  // using Array.from to convert the Proxies to real objects
  return Array.from(row).reduce(
    (acc, [key, value]) => Object.assign(acc, { [key]: value }),
    {},
  )
}

export function getTableDataFromArrowStreamResult(
  result: Table<any>,
): [TableColumns?, TableRows?] {
  if (result == null) return []

  const data: ResponseTableColumns = result.toArray() // result.toArray() returns an array of Proxies
  const rows = Array.from(data).map(toTableRow) // using Array.from to convert the Proxies to real objects
  const columns = result.schema.fields.map(field => field.name)

  return [columns, rows]
}
