import { getLanguageByExtension, showIndicatorDialects } from './help'
import EditorIndicator from './EditorIndicator'
import { useStoreEditor } from '~/context/editor'

export default function EditorFooter(): JSX.Element {
  const tab = useStoreEditor(s => s.tab)
  const engine = useStoreEditor(s => s.engine)
  const dialects = useStoreEditor(s => s.dialects)
  const refreshTab = useStoreEditor(s => s.refreshTab)

  return (
    <div className="mr-4">
      <EditorIndicator
        className="mr-2"
        text="Valid"
      >
        <EditorIndicator.Light ok={tab.isValid} />
      </EditorIndicator>
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
            value={tab.dialect ?? dialects[0]?.dialect_name}
            options={dialects}
            onChange={(e: React.ChangeEvent<HTMLSelectElement>) => {
              tab.dialect = e.target.value

              refreshTab()

              engine.postMessage({
                topic: 'dialect',
                payload: tab.dialect,
              })
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
      {tab.file.isSQLMeshModel && tab.dialect != null && (
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
            text={tab.file.isSQLMeshModel ? 'Model' : 'Plain'}
          />
        </EditorIndicator>
      )}
    </div>
  )
}
