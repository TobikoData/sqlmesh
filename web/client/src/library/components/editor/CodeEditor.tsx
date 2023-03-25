import { useCallback, useEffect, useMemo, useState } from 'react'
import CodeMirror from '@uiw/react-codemirror'
import { python } from '@codemirror/lang-python'
import { StreamLanguage } from '@codemirror/language'
import { keymap } from "@codemirror/view"
import { yaml } from '@codemirror/legacy-modes/mode/yaml'
import { type Extension } from '@codemirror/state'
import { type Model } from '~/api/client'
import { useStoreFileTree } from '~/context/fileTree'
import { type ModelFile } from '~/models'
import {
  events,
  SqlMeshModel,
  HoverTooltip,
  useSqlMeshExtension,
} from './extensions'
import { sqlglotWorker } from '~/library/components/editor/workers'
import { dracula, tomorrow } from 'thememirror'
import { useColorScheme, EnumColorScheme } from '~/context/theme'

export default function CodeEditor({
  file,
  models,
  dialect,
  dialects,
  onChange,
  saveChange
}: {
  file: ModelFile
  models: Map<string, Model>
  dialect?: string
  dialects?: string[]
  onChange: (value: string) => void
  saveChange: (value: string) => void
}): JSX.Element {
  const { mode } = useColorScheme()
  const [SqlMeshDialect, SqlMeshDialectCleanUp] = useSqlMeshExtension(dialects)
  
  const files = useStoreFileTree(s => s.files)
  const selectFile = useStoreFileTree(s => s.selectFile)
  
  const [sqlDialectOptions, setSqlDialectOptions] = useState()
  
  const extensions = useMemo(() => {
    const showSqlMeshDialect = file.extension === '.sql' && sqlDialectOptions != null

    return [
      mode === EnumColorScheme.Dark ? dracula : tomorrow,
      HoverTooltip(models),
      events(models, files, selectFile),
      SqlMeshModel(models),
      showSqlMeshDialect && SqlMeshDialect(models, file, sqlDialectOptions),
      file.extension === '.py' && python(),
      file.extension === '.yaml' && StreamLanguage.define(yaml),
      keymap.of([{
        mac: 'Cmd-s',
        win: 'Ctrl-s',
        linux: 'Cmd-s',
        preventDefault: true,
        run() {
          saveChange(file.content)

          return true;
        }
      }])
    ].filter(Boolean) as Extension[]
  }, [file, models, sqlDialectOptions, mode])

  const handleSqlGlotWorkerMessage = useCallback((e: MessageEvent): void => {
    if (e.data.topic === 'dialect') {
      setSqlDialectOptions(e.data.payload)
    }
  }, [])

  useEffect(() => {
    sqlglotWorker.addEventListener('message', handleSqlGlotWorkerMessage)

    return () => {
      sqlglotWorker.removeEventListener('message', handleSqlGlotWorkerMessage)
      SqlMeshDialectCleanUp()
    }
  }, [])

  useEffect(() => {
    sqlglotWorker.postMessage({
      topic: 'parse',
      payload: file.content,
    })
  }, [file.content, sqlglotWorker])

  useEffect(() => {
    sqlglotWorker.postMessage({
      topic: 'dialect',
      payload: dialect,
    })
  }, [dialect, sqlglotWorker])

  return (
    <CodeMirror
      value={file.content}
      height="100%"
      width="100%"
      className="w-full h-full overflow-auto text-sm font-mono"
      extensions={extensions}
      onChange={onChange}
    />
  )
}
