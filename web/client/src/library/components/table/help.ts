import { type Table } from 'apache-arrow'
import { isNil } from '~/utils'

type TableCellValue = number | string | null
export type TableRow = Record<string, TableCellValue>
export interface TableColumn {
  name: string
  type: string
}
type ResponseTableColumns = Array<Array<[string, TableCellValue]>>

export function getTableDataFromArrowStreamResult(
  result: Table<any>,
): [TableColumn[], TableRow[]] {
  if (isNil(result)) return [[], []]

  const data: ResponseTableColumns = result.toArray() // result.toArray() returns an array of Proxies
  const rows = Array.from(data).map(toTableRow) // using Array.from to convert the Proxies to real objects
  const columns = result.schema.fields.map(field => ({
    name: field.name,
    type: getColumnType(field.type.toString()),
  }))

  return [columns, rows]
}

function toTableRow(
  row: Array<[string, TableCellValue]> = [],
): Record<string, TableCellValue> {
  // using Array.from to convert the Proxies to real objects
  return Array.from(row).reduce(
    (acc, [key, value]) =>
      Object.assign(acc, { [key]: isNil(value) ? undefined : String(value) }),
    {},
  )
}

function getColumnType(type: string): string {
  if (type === 'Int32' || type === 'Int64') return 'int'
  if (type === 'Float64') return 'float'

  return 'text'
}
