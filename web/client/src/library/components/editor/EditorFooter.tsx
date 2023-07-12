import { getLanguageByExtension, showIndicatorDialects } from './help'
import EditorIndicator from './EditorIndicator'
import { type EditorTab, useStoreEditor } from '~/context/editor'
import { useEffect } from 'react'
import { isFalse, isNil, isNotNil } from '~/utils'
import { EnumFileExtensions } from '@models/file'

export default function EditorFooter({ tab }: { tab: EditorTab }): JSX.Element {
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

    refreshTab()

    engine.postMessage({
      topic: 'dialect',
      payload: tab.dialect,
    })
  }

  return (
    <div className="flex w-full mr-4 overflow-hidden items-center">
      {tab.file.isSQLMeshModelSQL && (
        <EditorIndicator
          className="mr-2"
          text="Valid"
        >
          <EditorIndicator.Light ok={tab.isValid} />
        </EditorIndicator>
      )}
      {tab.file.extension === EnumFileExtensions.SQL &&
        isFalse(tab.file.isSQLMeshModelSQL) && (
          <EditorIndicator
            className="mr-2"
            text="Valid SQL"
          >
            <EditorIndicator.Light ok={tab.isValid} />
          </EditorIndicator>
        )}
      {tab.file.isRemote && (
        <EditorIndicator
          className="mr-2"
          text="Saved"
        >
          <EditorIndicator.Light ok={tab.isSaved} />
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
          <EditorIndicator.Selector
            value={tab.dialect}
            options={dialects}
            onChange={(e: React.ChangeEvent<HTMLSelectElement>) => {
              updateTabDialect(e.target.value)
            }}
          >
            {dialect => (
              <EditorIndicator.SelectorOption
                key={dialect.dialect_title}
                value={dialect.dialect_name}
              >
                {dialect.dialect_title}
              </EditorIndicator.SelectorOption>
            )}
          </EditorIndicator.Selector>
        </EditorIndicator>
      )}
      {tab.file.isSQLMeshModel && tab.dialect != null && tab.dialect !== '' && (
        <EditorIndicator
          className="mr-2"
          text="Dialect"
        >
          <EditorIndicator.Text text={tab.dialect} />
        </EditorIndicator>
      )}
      {tab.file.type != null && (
        <EditorIndicator
          className="mr-2"
          text="SQLMesh Type"
        >
          <EditorIndicator.Text
            text={tab.file.isSQLMeshModel ? 'Model' : 'Unsupported'}
          />
        </EditorIndicator>
      )}
    </div>
  )
}
