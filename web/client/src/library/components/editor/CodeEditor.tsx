import { useMemo } from 'react'
import CodeMirror from '@uiw/react-codemirror'
import { python } from '@codemirror/lang-python'
import { StreamLanguage } from '@codemirror/language'

import { yaml } from '@codemirror/legacy-modes/mode/yaml'
import { Extension } from '@codemirror/state'
import { ModelsModels } from '~/api/client'
import { useStoreFileTree } from '~/context/fileTree'
import { ModelFile } from '~/models'
import {
  events,
  SqlMeshModel,
  SqlMeshDialect,
  HoverTooltip,
} from './extensions'

export default function CodeEditor({
  file,
  models,
  onChange,
}: {
  file: ModelFile
  models?: ModelsModels
  onChange: (value: string) => void
}): JSX.Element {
  const files = useStoreFileTree(s => s.files)
  const selectFile = useStoreFileTree(s => s.selectFile)

  const extensions = useMemo(() => {
    return [
      models != null && HoverTooltip(models),
      models != null && events(models, files, selectFile),
      models != null && SqlMeshModel(models),
      file.extension === '.sql' &&
        models != null &&
        SqlMeshDialect(models, file),
      file.extension === '.py' && python(),
      file.extension === '.yaml' && StreamLanguage.define(yaml),
    ].filter(Boolean) as Extension[]
  }, [file, models])

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
