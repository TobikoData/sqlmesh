// Type for data values in samples - can be strings, numbers, booleans, or null
export type SampleValue = string | number | boolean | null

// Type for row data in samples
export type SampleRow = Record<string, SampleValue>

// Type for column statistics
export type ColumnStats = Record<string, number | string | null>

export interface TableDiffData {
  schema_diff: {
    source: string
    target: string
    source_schema: Record<string, string>
    target_schema: Record<string, string>
    added: Record<string, string>
    removed: Record<string, string>
    modified: Record<string, string>
  }
  row_diff: {
    source: string
    target: string
    stats: Record<string, number>
    sample: Record<string, SampleValue[]>
    joined_sample: Record<string, SampleValue[]>
    s_sample: Record<string, SampleValue[]>
    t_sample: Record<string, SampleValue[]>
    column_stats: ColumnStats
    source_count: number
    target_count: number
    count_pct_change: number
    decimals: number
    processed_sample_data?: {
      column_differences: SampleRow[]
      source_only: SampleRow[]
      target_only: SampleRow[]
    }
  }
  on: string[][]
  limit?: number
  where?: string
}

export interface TableDiffParams {
  source: string
  target: string
  model_or_snapshot: string
  on?: string
  where?: string
  temp_schema?: string
  limit?: number
}

export interface ExpandedSections {
  schema: boolean
  rows: boolean
  columnStats: boolean
  sampleData: boolean
}

export const themeColors = {
  success: 'var(--vscode-testing-iconPassed, #22c55e)',
  warning: 'var(--vscode-testing-iconQueued, #f59e0b)',
  error: 'var(--vscode-testing-iconFailed, #ef4444)',
  info: 'var(--vscode-testing-iconUnset, #3b82f6)',
  addedText: 'var(--vscode-diffEditor-insertedTextForeground, #22c55e)',
  removedText: 'var(--vscode-diffEditor-removedTextForeground, #ef4444)',
  modifiedText: 'var(--vscode-diffEditor-modifiedTextForeground, #f59e0b)',
  muted: 'var(--vscode-descriptionForeground)',
  accent: 'var(--vscode-textLink-foreground)',
  border: 'var(--vscode-panel-border)',
}

// Helper utilities
export function cn(...classes: (string | false | undefined)[]) {
  return classes.filter(Boolean).join(' ')
}

export const formatCellValue = (cell: SampleValue, decimals = 3): string => {
  if (cell == null) return 'null'
  if (typeof cell === 'number')
    return cell % 1 === 0 ? cell.toString() : cell.toFixed(decimals)
  return String(cell)
}
