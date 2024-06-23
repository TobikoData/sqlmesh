import { getLanguageByExtension, showIndicatorDialects } from './help'
import EditorIndicator from './EditorIndicator'
import { type EditorTab, useStoreEditor } from '~/context/editor'
import { useEffect } from 'react'
import { isFalse, isNil, isNotNil, isStringEmptyOrNil } from '~/utils'
import Input from '@components/input/Input'
import { EnumSize } from '~/types/enum'
import { useStoreContext } from '@context/context'

const EnumFileType = {
  Model: 'model',
  Test: 'test',
  Audit: 'audit',
  Macro: 'macro',
  Hook: 'hook',
  Log: 'log',
  Config: 'config',
  Seed: 'seed',
  Metric: 'metric',
  Schema: 'schema',
  Unknown: 'unknown',
} as const

export type FileType = (typeof EnumFileType)[keyof typeof EnumFileType]

export default function EditorFooter({ tab }: { tab: EditorTab }): JSX.Element {
  const isModel = useStoreContext(s => s.isModel)

  const engine = useStoreEditor(s => s.engine)
  const dialects = useStoreEditor(s => s.dialects)
  const refreshTab = useStoreEditor(s => s.refreshTab)

  useEffect(() => {
    const dialectName = dialects[0]?.dialect_name

    if (isNil(tab.dialect) && isNotNil(dialectName)) {
      updateTabDialect(dialectName)
    }
  }, [dialects, tab])

  function updateTabDialect(dialect: string): void {
    tab.dialect = dialect

    refreshTab(tab)

    engine.postMessage({
      topic: 'dialect',
      payload: tab.dialect,
    })
  }

  return (
    <div className="flex w-full items-center px-2 min-h-[2rem] overflow-hidden">
      {tab.file.isRemote && (
        <EditorIndicator
          className="mr-2"
          text="Saved"
        >
          <EditorIndicator.Light ok={isFalse(tab.file.isChanged)} />
        </EditorIndicator>
      )}
      {tab.file.isRemote && tab.file.isSQL && (
        <EditorIndicator
          className="mr-2"
          text="Formatted"
        >
          <EditorIndicator.Light ok={tab.file.isFormatted} />
        </EditorIndicator>
      )}
      <EditorIndicator
        className="mr-2"
        text="Language"
      >
        <EditorIndicator.Text
          text={getLanguageByExtension(tab.file.extension)}
        />
      </EditorIndicator>
      {showIndicatorDialects(tab, dialects) && (
        <EditorIndicator
          className="mr-2"
          text="Dialect"
        >
          <Input size={EnumSize.sm}>
            {({ className, size }) => (
              <Input.Selector
                className={className}
                size={size}
                list={dialects.map(d => ({
                  text: d.dialect_title,
                  value: isStringEmptyOrNil(d.dialect_name)
                    ? 'sqlglot'
                    : d.dialect_name,
                }))}
                onChange={dialect => {
                  updateTabDialect(dialect)
                }}
                value={tab.dialect}
              />
            )}
          </Input>
        </EditorIndicator>
      )}
      {isModel(tab.file.path) && !isStringEmptyOrNil(tab.dialect) && (
        <EditorIndicator
          className="mr-2"
          text="Dialect"
        >
          <EditorIndicator.Text text={tab.dialect} />
        </EditorIndicator>
      )}
      <EditorIndicator
        className="mr-2"
        text="SQLMesh Type"
      >
        <EditorIndicator.Text text={getFileType(tab.file.path)} />
      </EditorIndicator>
    </div>
  )
}

function getFileType(path?: string): FileType {
  if (isStringEmptyOrNil(path)) return EnumFileType.Unknown

  if (path.startsWith('models')) return EnumFileType.Model
  if (path.startsWith('tests')) return EnumFileType.Test
  if (path.startsWith('logs')) return EnumFileType.Log
  if (path.startsWith('macros')) return EnumFileType.Macro
  if (path.startsWith('hooks')) return EnumFileType.Hook
  if (path.startsWith('seeds')) return EnumFileType.Seed
  if (path.startsWith('metrics')) return EnumFileType.Metric
  if (['config.yaml', 'config.yml', 'config.py'].includes(path))
    return EnumFileType.Config
  if (['external_models.yaml', 'schema.yaml'].includes(path))
    return EnumFileType.Schema

  return EnumFileType.Unknown
}
