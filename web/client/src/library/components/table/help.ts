import { isFalse, isNotNil, isNil } from '@utils/index'
import { type Filters } from './TableDiff'

export {
  getHeaders,
  getRows,
  hasModified,
  isAddedRow,
  isDeletedRow,
  isModified,
  getCellContent,
  getCellContentSource,
  getCellContentTarget,
}

const SOURCE_PREFIX = 's__'
const TARGET_PREFIX = 't__'
const EMTPY_TABLE_CELL = 'NULL'
export const TEST_GRAIN = ['item_id']

function getHeaders(
  {
    source_schema,
    target_schema,
  }: {
    source_schema: Record<string, string>
    target_schema: Record<string, string>
  },
  filters: Filters,
): string[] {
  const source = Object.keys(source_schema)
  const target = Object.keys(target_schema)
  const union = Array.from(new Set(source.concat(target)))
  const intersection = union.filter(
    s => source.includes(s) && target.includes(s),
  )
  const differenceSource = source.filter(s => !target.includes(s))
  const differenceTarget = target.filter(s => !source.includes(s))

  return Array.from(
    new Set(
      [
        TEST_GRAIN,
        filters.modifiedColumns && intersection,
        filters.addedColumns && differenceTarget,
        filters.removedColumns && differenceSource,
      ]
        .filter(Boolean)
        .flat(),
    ),
  ) as string[]
}

function getRows(diff: any, filters: Filters): string[] {
  const rows = Object.values(diff.row_diff.sample)[0]
  const deleted: string[] = []
  const added: string[] = []
  const rest: string[] = []

  Object.entries(rows ?? {}).forEach(([key, value]) => {
    if (isAddedRow(diff, key)) added.push(key)
    else if (isDeletedRow(diff, key)) deleted.push(key)
    else rest.push(key)
  })

  return [
    filters.modifiedRows && rest,
    filters.addedRows && added,
    filters.removedRows && deleted,
  ]
    .filter(Boolean)
    .flat() as string[]
}

function isModified(diff: any, header: string, key: string): boolean {
  const source_sample = diff.row_diff.sample[`${SOURCE_PREFIX}${header}`]?.[key]
  const target_sample = diff.row_diff.sample[`${TARGET_PREFIX}${header}`]?.[key]

  return (
    (isNotNil(source_sample) || isNotNil(target_sample)) &&
    source_sample !== target_sample
  )
}

function isDeletedRow(diff: any, key: string): boolean {
  return TEST_GRAIN.every(header => {
    const source_sample =
      diff.row_diff.sample[`${SOURCE_PREFIX}${header}`]?.[key]
    const target_sample =
      diff.row_diff.sample[`${TARGET_PREFIX}${header}`]?.[key]

    return isNotNil(source_sample) && isNil(target_sample)
  })
}

function isAddedRow(diff: any, key: string): boolean {
  return TEST_GRAIN.every(header => {
    const source_sample =
      diff.row_diff.sample[`${SOURCE_PREFIX}${header}`]?.[key]
    const target_sample =
      diff.row_diff.sample[`${TARGET_PREFIX}${header}`]?.[key]

    return isNil(source_sample) && isNotNil(target_sample)
  })
}

function hasModified(diff: any, rows: string[], header: string): boolean {
  if (header in diff.schema_diff.added || header in diff.schema_diff.removed)
    return false

  return rows.some(
    key =>
      isFalse(isAddedRow(diff, key)) &&
      isFalse(isDeletedRow(diff, key)) &&
      isModified(diff, header, key),
  )
}

function getCellContent(diff: any, header: string, key: string): string {
  if (header in diff.schema_diff.removed && isAddedRow(diff, key))
    return EMTPY_TABLE_CELL
  if (header in diff.schema_diff.added && isDeletedRow(diff, key))
    return EMTPY_TABLE_CELL

  return (
    diff.row_diff.sample[`${TARGET_PREFIX}${header}`]?.[key] ??
    diff.row_diff.sample[`${SOURCE_PREFIX}${header}`]?.[key] ??
    EMTPY_TABLE_CELL
  )
}

function getCellContentSource(diff: any, header: string, key: string): string {
  return (
    diff.row_diff.sample[`${SOURCE_PREFIX}${header}`]?.[key] ?? EMTPY_TABLE_CELL
  )
}

function getCellContentTarget(diff: any, header: string, key: string): string {
  return (
    diff.row_diff.sample[`${TARGET_PREFIX}${header}`]?.[key] ?? EMTPY_TABLE_CELL
  )
}
