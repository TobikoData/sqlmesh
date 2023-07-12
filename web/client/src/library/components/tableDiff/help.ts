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

function getHeaders(
  {
    source_schema,
    target_schema,
  }: {
    source_schema: Record<string, string>
    target_schema: Record<string, string>
  },
  filters: Filters,
  on: Array<[string, string]>,
): {
  all: string[]
  modified: number
  deleted: number
  added: number
} {
  const grain: string[] = Array.from(new Set(on.flat()))
  const source = Object.keys(source_schema)
  const target = Object.keys(target_schema)
  const union = Array.from(new Set(source.concat(target)))
  const intersection = union.filter(
    s => source.includes(s) && target.includes(s),
  )
  const differenceSource = source.filter(s => !target.includes(s))
  const differenceTarget = target.filter(s => !source.includes(s))

  return {
    all: Array.from(
      new Set(
        [
          grain,
          filters.modifiedColumns && intersection,
          filters.addedColumns && differenceTarget,
          filters.removedColumns && differenceSource,
        ]
          .filter(Boolean)
          .flat(),
      ),
    ) as string[],
    added: differenceTarget.length,
    deleted: differenceSource.length,
    modified: intersection.length - grain.length,
  }
}

function getRows(
  diff: any,
  filters: Filters,
  on: Array<[string, string]>,
): {
  all: string[]
  modified: number
  deleted: number
  added: number
} {
  const rows = Object.values(diff.row_diff.sample)[0]
  const deleted: string[] = []
  const added: string[] = []
  const rest: string[] = []

  Object.entries(rows ?? {}).forEach(([key, value]) => {
    if (isAddedRow(diff, key, on)) added.push(key)
    else if (isDeletedRow(diff, key, on)) deleted.push(key)
    else rest.push(key)
  })

  return {
    all: [
      filters.modifiedRows && rest,
      filters.addedRows && added,
      filters.removedRows && deleted,
    ]
      .filter(Boolean)
      .flat() as string[],
    added: added.length,
    deleted: deleted.length,
    modified: rest.length,
  }
}

function isModified(diff: any, header: string, key: string): boolean {
  const source_sample = diff.row_diff.sample[`${SOURCE_PREFIX}${header}`]?.[key]
  const target_sample = diff.row_diff.sample[`${TARGET_PREFIX}${header}`]?.[key]

  return (
    (isNotNil(source_sample) || isNotNil(target_sample)) &&
    source_sample !== target_sample
  )
}

function isDeletedRow(
  diff: any,
  key: string,
  on: Array<[string, string]>,
): boolean {
  return on.every(([source, target]) => {
    const source_sample =
      diff.row_diff.sample[`${SOURCE_PREFIX}${source}`]?.[key]
    const target_sample =
      diff.row_diff.sample[`${TARGET_PREFIX}${target}`]?.[key]

    return isNotNil(source_sample) && isNil(target_sample)
  })
}

function isAddedRow(
  diff: any,
  key: string,
  on: Array<[string, string]>,
): boolean {
  return on.every(([source, target]) => {
    const source_sample =
      diff.row_diff.sample[`${SOURCE_PREFIX}${source}`]?.[key]
    const target_sample =
      diff.row_diff.sample[`${TARGET_PREFIX}${target}`]?.[key]

    return isNil(source_sample) && isNotNil(target_sample)
  })
}

function hasModified(
  diff: any,
  rows: string[],
  header: string,
  on: Array<[string, string]>,
): boolean {
  if (header in diff.schema_diff.added || header in diff.schema_diff.removed)
    return false

  return rows.some(
    key =>
      isFalse(isAddedRow(diff, key, on)) &&
      isFalse(isDeletedRow(diff, key, on)) &&
      isModified(diff, header, key),
  )
}

function getCellContent(
  diff: any,
  header: string,
  key: string,
  on: Array<[string, string]>,
): string {
  if (header in diff.schema_diff.removed && isAddedRow(diff, key, on))
    return EMTPY_TABLE_CELL
  if (header in diff.schema_diff.added && isDeletedRow(diff, key, on))
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
