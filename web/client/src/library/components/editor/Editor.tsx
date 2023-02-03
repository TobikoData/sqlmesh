import { useCallback, useContext, useEffect, useMemo, useState } from 'react';
import CodeMirror from '@uiw/react-codemirror';
import { sql } from '@codemirror/lang-sql';
import { python } from '@codemirror/lang-python';
import { StreamLanguage } from '@codemirror/language';
import { yaml } from '@codemirror/legacy-modes/mode/yaml';
import clsx from 'clsx';
import { Extension } from '@codemirror/state';
import {
  useMutationApiSaveFile,
  useApiFileByPath,
} from '../../../api'
import { useQueryClient } from '@tanstack/react-query';
import ContextIDE from '../../../context/Ide';
import { XCircleIcon } from '@heroicons/react/24/solid';
import { Divider } from '../divider/Divider';
import { Button } from '../button/Button';
import { EnumSize } from '../../../types/enum';
import { ModelFile } from '../../../models';


export function Editor() {
  const client = useQueryClient()

  const { setActiveFile, activeFile, openedFiles, setOpenedFiles } = useContext(ContextIDE);
  const [status, setStatus] = useState('edit')
  const { data: fileData } = useApiFileByPath(activeFile?.path)
  const mutationSaveFile = useMutationApiSaveFile(client)

  useEffect(() => {
    if (fileData && activeFile && !activeFile?.isLocal) {
      activeFile.content = fileData.content ?? ''

      setStatus('updated')
    }
  }, [fileData])


  function closeEditorTab(f: ModelFile) {
    if (!f) return

    openedFiles.delete(f)

    if (openedFiles.size === 0) {
      setActiveFile(null)
    } else if (!activeFile || !openedFiles.has(activeFile)) {
      setActiveFile([...openedFiles][0])
    }

    setOpenedFiles(new Set([...openedFiles]))
  }

  function addNewFileAndSelect() {
    const file = new ModelFile()

    setOpenedFiles(new Set([...openedFiles, file]))
    setActiveFile(file)
  }

  function onChange(value: string) {
    setStatus('edit')

    if (value === activeFile?.content) return

    if (activeFile?.isLocal) {
      activeFile.content = value

      setStatus('saved')
    } else {
      setStatus('saving...')

      mutationSaveFile.mutate({
        path: activeFile?.path,
        body: value,
      })

      setStatus('saved')
    }
  }

  function sendQuery() {
    console.log('Sending query', activeFile?.content)
  }

  const debouncedChange = useMemo(() => debounce(
    onChange,
    () => {
      setStatus('editing...')
    },
    () => {
      setStatus('edit')
    },
    500
  ), [])

  return (
    <div className={clsx('h-full w-full flex flex-col overflow-hidden')}>
      <div className='flex items-center'>
        <Button className='m-0 ml-1 mr-3' size='sm' onClick={e => {
          e.stopPropagation()

          addNewFileAndSelect()
        }}>+</Button>
        <ul className="w-full whitespace-nowrap min-h-[2rem] max-h-[2rem] overflow-hidden overflow-x-auto">
          {openedFiles.size > 0 &&
            [...openedFiles].map((file, idx) => (
              <li
                key={file.path || file.id}
                className={clsx(
                  'inline-block py-1 pr-2 last-child:pr-0 overflow-hidden text-center overflow-ellipsis cursor-pointer',

                )}
                onClick={() => {
                  if (activeFile?.id !== file.id) {
                    setActiveFile(file)
                  }
                }}
              >
                <span className={clsx(
                  "flex justify-between items-center pl-2 pr-1 py-[0.25rem] min-w-[8rem] rounded-md",
                  file === activeFile
                    ? 'bg-secondary-100'
                    : 'bg-transparent  hover:shadow-border hover:shadow-secondary-300'
                )}>
                  <small className="text-xs">
                    {file.isUntitled ? `Untitled-${idx + 1}` : file.name}
                  </small>
                  {openedFiles.size > 1 && (
                    <XCircleIcon
                      onClick={(e) => {
                        e.stopPropagation()

                        closeEditorTab(file)
                      }}
                      className={`inline-block text-gray-200 w-4 h-4 ml-2 cursor-pointer hover:text-gray-700 `}
                    />
                  )}
                </span>
              </li>
            ))}
        </ul>
      </div>

      <Divider />
      <div className="w-full h-full flex flex-col overflow-hidden">
        <div className="w-full h-full overflow-hidden ">
          <CodeEditor
            className="h-full w-full"
            extension={activeFile?.extension}
            value={activeFile?.content}
            onChange={debouncedChange}
          />
        </div>
      </div>
      <Divider />
      <div className="px-2 flex justify-between items-center min-h-[2rem]">
        <small>validation: ok</small>
        <small>File Status: {status}</small>
        <small>{getLanguageByExtension(activeFile?.extension)}</small>
        <div className="flex">
          {activeFile?.extension === '.sql' && activeFile.content && (
            <>
              <Button size={EnumSize.sm} variant="alternative" onClick={e => {
                e.stopPropagation()

                sendQuery()
              }}>
                Run Query
              </Button>
              <Button size={EnumSize.sm} variant="alternative" onClick={e => {
                e.stopPropagation()

                onChange('')
              }}>
                Clear
              </Button>
            </>
          )}

          {!activeFile?.isLocal && (
            <>
              <Button size={EnumSize.sm} variant="alternative">
                Validate
              </Button>
              <Button size={EnumSize.sm} variant="alternative">
                Format
              </Button>
              <Button size={EnumSize.sm} variant="success">
                Save
              </Button>
            </>
          )}
        </div>
      </div>
    </div >
  )
}

function CodeEditor({ className, value, onChange, extension }: any) {
  const extensions = [
    extension === '.sql' && sql(),
    extension === '.py' && python(),
    extension === '.yaml' && StreamLanguage.define(yaml),
  ].filter(Boolean) as Extension[]

  return (
    <CodeMirror
      value={value}
      height='100%'
      width='100%'
      className={clsx('w-full h-full overflow-auto', className)}
      extensions={extensions}
      onChange={onChange}
    />
  );
}

function debounce(
  fn: (...args: any) => void,
  before: () => void,
  after: () => void,
  delay: number = 500
) {
  console.log('fsdfdsfsd')
  let timer: any
  return function (...args: any) {
    clearTimeout(timer)

    before && before()

    timer = setTimeout(() => {
      fn(...args)

      after && after()
    }, delay)
  }
}

function getLanguageByExtension(extension?: string) {
  switch (extension) {
    case '.sql':
      return 'SQL'
    case '.py':
      return 'Python'
    case '.yaml':
      return 'YAML'
    default:
      return 'Plain Text'
  }
}