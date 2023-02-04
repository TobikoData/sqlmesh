import { useContext, useEffect, useMemo, useState } from 'react';
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
  const [content, setContent] = useState<string>()
  const [status, setStatus] = useState('edit')

  const { data: fileData } = useApiFileByPath(activeFile?.path)
  const mutationSaveFile = useMutationApiSaveFile(client)

  useEffect(() => {
    if (fileData) {
      setContent(fileData.content)
    }
  }, [fileData])

  useEffect(() => {
    if (activeFile) {
      setContent(activeFile.content)
    }
  }, [activeFile])

  useEffect(() => {
    if (activeFile) {
      updateFileContent(content, activeFile?.id)
    }
  }, [content])


  function closeEditorTab(file: ModelFile) {
    if (!file) return

    openedFiles.delete(file)

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

    if (activeFile?.isLocal) return setContent(value)

    setStatus('saving...')

    if (value !== content) {
      mutationSaveFile.mutate({
        path: activeFile?.path,
        body: value,
      })
    }

    setStatus('saved')
  }

  function sendQuery() {
    console.log('Sending query', activeFile?.content)
  }

  function cleanUp() {
    setContent('')
    setStatus('edit')
  }

  function updateFileContent(text?: string, id?: string | number) {
    openedFiles.forEach(file => {
      if (id && file.id === id) {
        file.content = text ?? ''
      }
    })
  }

  const debouncedChange = useMemo(() => debounce(
    onChange,
    () => {
      setStatus('editing...')
    },
    () => {
      setStatus('edit')
    },
    200
  ), [])

  return (
    <div className={clsx('h-full w-full flex flex-col overflow-hidden')}>
      <div className='flex items-center'>
        <Button className='m-0 ml-1 mr-3' size='sm' onClick={e => {
          e.stopPropagation()

          // debouncedChange.cancel()

          updateFileContent(content, activeFile?.id)

          setTimeout(() => {
            addNewFileAndSelect()
          }, 200)
        }}>+</Button>
        <ul className="w-full whitespace-nowrap min-h-[2rem] max-h-[2rem] overflow-hidden overflow-x-auto">
          {openedFiles.size > 0 &&
            [...openedFiles].map((file, idx) => (
              <li
                key={file.id}
                className={clsx(
                  'inline-block py-1 pr-2 last-child:pr-0 overflow-hidden text-center overflow-ellipsis cursor-pointer',

                )}
                onClick={() => {
                  if (activeFile?.id !== file.id) {
                    updateFileContent(content, activeFile?.id)

                    setTimeout(() => {
                      setActiveFile(file)
                    }, 200)
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

                        cleanUp()
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
            value={content}
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
          {activeFile?.extension === '.sql' && content && (
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
  let timer: any

  function callback(...args: any) {
    clearTimeout(timer)

    before && before()

    timer = setTimeout(() => {
      fn(...args)

      after && after()
    }, delay)
  }

  callback.cancel = () => {
    timer = clearTimeout(timer)
  }

  return callback
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