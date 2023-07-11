import { EnumFileExtensions, type FileExtensions } from '@models/file'
import { type Table } from 'apache-arrow'
import { type Dialect, type EditorTab } from '~/context/editor'
import { isArrayNotEmpty } from '~/utils'

export function getLanguageByExtension(extension?: FileExtensions): string {
  switch (extension) {
    case EnumFileExtensions.SQL:
      return 'SQL'
    case EnumFileExtensions.PY:
      return 'Python'
    case EnumFileExtensions.YAML:
    case EnumFileExtensions.YML:
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
    tab.file.extension === EnumFileExtensions.SQL &&
    tab.file.isLocal &&
    isArrayNotEmpty(dialects)
  )
}

export function toTableRow(
  row: Array<[string, TableCellValue]> = [],
): Record<string, TableCellValue> {
  // using Array.from to convert the Proxies to real objects
  return Array.from(row).reduce(
    (acc, [key, value]) => Object.assign(acc, { [key]: value }),
    {},
  )
}

type TableCellValue = number | string | null
type TableRows = Array<Record<string, TableCellValue>>
type TableColumns = string[]
type ResponseTableColumns = Array<Array<[string, TableCellValue]>>

export function getTableDataFromArrowStreamResult(
  result: Table<any>,
): [TableColumns?, TableRows?] {
  if (result == null) return []

  const data: ResponseTableColumns = result.toArray() // result.toArray() returns an array of Proxies
  const rows = Array.from(data).map(toTableRow) // using Array.from to convert the Proxies to real objects
  const columns = result.schema.fields.map(field => field.name)

  return [columns, rows]
}
