import { SectionCard } from './SectionCard'
import { SchemaDiffSection } from './SchemaDiffSection'
import { RowStatsSection } from './RowStatsSection'
import { ColumnStatsSection } from './ColumnStatsSection'
import { SampleDataSection } from './SampleDataSection'
import { usePersistedState } from './hooks'
import type { TableDiffData, ExpandedSections } from './types'

interface ContentSectionsProps {
  data: TableDiffData
}

export function ContentSections({ data }: ContentSectionsProps) {
  const [expanded, setExpanded] = usePersistedState<ExpandedSections>(
    'tableDiffExpanded',
    {
      schema: true,
      rows: true,
      columnStats: false,
      sampleData: false,
    },
  )

  const toggle = (section: keyof ExpandedSections) => {
    setExpanded(prev => ({
      ...prev,
      [section]: !prev[section],
    }))
  }

  const { schema_diff, row_diff } = data

  return (
    <div className="overflow-y-auto h-[calc(100%-200px)]">
      {/* Schema Changes */}
      <SectionCard
        id="schema"
        title="Schema Changes"
        expanded={expanded.schema}
        onToggle={() => toggle('schema')}
      >
        <SchemaDiffSection schemaDiff={schema_diff} />
      </SectionCard>

      {/* Row Statistics */}
      <SectionCard
        id="rows"
        title="Row Statistics"
        expanded={expanded.rows}
        onToggle={() => toggle('rows')}
      >
        <RowStatsSection rowDiff={row_diff} />
      </SectionCard>

      {/* Column Statistics */}
      <SectionCard
        id="columnStats"
        title="Column Statistics"
        expanded={expanded.columnStats}
        onToggle={() => toggle('columnStats')}
      >
        <ColumnStatsSection columnStats={row_diff.column_stats} />
      </SectionCard>

      {/* Sample Data */}
      {row_diff.processed_sample_data && (
        <SectionCard
          id="sampleData"
          title="Data Differences"
          expanded={expanded.sampleData}
          onToggle={() => toggle('sampleData')}
        >
          <SampleDataSection rowDiff={row_diff} />
        </SectionCard>
      )}
    </div>
  )
}
