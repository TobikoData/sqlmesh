import CodeMirror from '@uiw/react-codemirror'
import { sql } from '@codemirror/lang-sql'
import { python } from '@codemirror/lang-python'
import { StreamLanguage } from '@codemirror/language'
import { yaml } from '@codemirror/legacy-modes/mode/yaml'
import clsx from 'clsx'
import { Extension } from '@codemirror/state'

export function Editor({ className, value, onChange, extension }: any) {
  const extensions = [
    extension === '.sql' && sql(),
    extension === '.py' && python(),
    extension === '.yaml' && StreamLanguage.define(yaml),
  ].filter(Boolean) as Extension[]

  return (
    <CodeMirror
      value={value}
      height="100%"
      width="100%"
      className={clsx('w-full h-full overflow-auto', className)}
      extensions={extensions}
      onChange={onChange}
    />
  )
}
