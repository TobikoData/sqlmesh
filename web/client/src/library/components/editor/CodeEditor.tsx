import { useCallback, useEffect, useMemo, useState } from 'react'
import CodeMirror from '@uiw/react-codemirror'
import { python } from '@codemirror/lang-python'
import { StreamLanguage } from '@codemirror/language'

import { yaml } from '@codemirror/legacy-modes/mode/yaml'
import { type Extension } from '@codemirror/state'
import { type ModelsModels } from '~/api/client'
import { useStoreFileTree } from '~/context/fileTree'
import { type ModelFile } from '~/models'
import {
  events,
  SqlMeshModel,
  HoverTooltip,
  useSqlMeshExtention,
} from './extensions'
import { sqlglotWorker } from '~/library/components/editor/workers'

export default function CodeEditor({
  file,
  models,
  dialect,
  onChange,
}: {
  file: ModelFile
  models?: ModelsModels
  dialect?: string
  onChange: (value: string) => void
}): JSX.Element {
  const files = useStoreFileTree(s => s.files)
  const selectFile = useStoreFileTree(s => s.selectFile)

  const [sqlDialectOptions, setSqlDialectOptions] = useState()

  const [SqlMeshDialectExtension, SqlMeshDialectCleanUp] = useSqlMeshExtention()

  const extensions = useMemo(() => {
    const showSqlSqlMeshDialect =
      file.extension === '.sql' && models != null && sqlDialectOptions != null
    return [
      models != null && HoverTooltip(models),
      models != null && events(models, files, selectFile),
      models != null && SqlMeshModel(models),
      showSqlSqlMeshDialect &&
        SqlMeshDialectExtension(models, file, sqlDialectOptions),
      file.extension === '.py' && python(),
      file.extension === '.yaml' && StreamLanguage.define(yaml),
    ].filter(Boolean) as Extension[]
  }, [file, models, sqlDialectOptions])

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
      className="w-full h-full overflow-auto"
      extensions={extensions}
      onChange={onChange}
    />
  )
}
