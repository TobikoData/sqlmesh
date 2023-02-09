import { useEffect, useMemo, useState, MouseEvent } from 'react';
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
import { XCircleIcon } from '@heroicons/react/24/solid';
import { Divider } from '../divider/Divider';
import { Button } from '../button/Button';
import { EnumSize } from '../../../types/enum';
import { ModelFile } from '../../../models';
import { useStoreFileTree } from '../../../context/fileTree';


export const EnumEditorFileStatus = {
  Edit: 'edit',
  Editing: 'editing',
  Saving: 'saving',
  Saved: 'saved',
} as const;


export type EditorFileStatus = typeof EnumEditorFileStatus[keyof typeof EnumEditorFileStatus]

export function Editor() {
  const client = useQueryClient()

  const activeFileId = useStoreFileTree(s => s.activeFileId)
  const openedFiles = useStoreFileTree(s => s.openedFiles)
  const setActiveFileId = useStoreFileTree(s => s.setActiveFileId)
  const setOpenedFiles = useStoreFileTree(s => s.setOpenedFiles)
  const selectFile = useStoreFileTree(s => s.selectFile)

  const [fileStatus, setEditorFileStatus] = useState<EditorFileStatus>(EnumEditorFileStatus.Edit)
  const [activeFile, setActiveFile] = useState<ModelFile>()

  const { data: fileData } = useApiFileByPath(activeFile?.path)
  const mutationSaveFile = useMutationApiSaveFile(client)

  useEffect(() => {
    if (fileData == null || activeFile == null) return

    activeFile.content = fileData.content ?? ''

    setOpenedFiles(openedFiles)
  }, [fileData])

  useEffect(() => {
    setActiveFile(openedFiles.get(activeFileId))
  }, [activeFileId])

  useEffect(() => {
    if (openedFiles.size < 1) {
      const file = new ModelFile()

      selectFile(file)
    } else {
      openedFiles.has(activeFileId) === false && setActiveFileId([...openedFiles.values()][0].id)
    }
  }, [openedFiles])


  function closeEditorTab(file: ModelFile) {
    if (!file) return

    openedFiles.delete(file.id)

    if (openedFiles.size === 0) {
      setActiveFile(undefined)
    } else if (activeFile == null || openedFiles.has(activeFile.id) === false) {
      setActiveFile([...openedFiles.values()][0])
    }

    setOpenedFiles(new Map(openedFiles))
  }

  function addNewFileAndSelect() {
    const file = new ModelFile()

    openedFiles.set(file.id, file)

    setActiveFileId(file.id)
  }

  function onChange(value: string) {
    setEditorFileStatus(EnumEditorFileStatus.Edit)

    if (activeFile == null) return

    setEditorFileStatus(EnumEditorFileStatus.Saving)

    activeFile.content = value

    setOpenedFiles(openedFiles)

    if (activeFile?.isLocal || activeFile.content === value) return

    mutationSaveFile.mutate({
      path: activeFile?.path,
      body: { content: value },
    })

    setEditorFileStatus(EnumEditorFileStatus.Saved)
  }

  function sendQuery() {
    console.log('Sending query', activeFile?.content)
  }

  function cleanUp() {
    setEditorFileStatus(EnumEditorFileStatus.Edit)
  }

  const debouncedChange = useMemo(() => debounce(
    onChange,
    () => {
      setEditorFileStatus(EnumEditorFileStatus.Editing)
    },
    () => {
      setEditorFileStatus(EnumEditorFileStatus.Edit)
    },
    200
  ), [activeFile])

  return (
    <div className={clsx('h-full w-full flex flex-col overflow-hidden')}>
      <div className='flex items-center'>
        <Button
          className='m-0 ml-1 mr-3'
          size='sm'
          onClick={(e: MouseEvent) => {
            e.stopPropagation()

            addNewFileAndSelect()
          }}>+</Button>
        <ul className="w-full whitespace-nowrap min-h-[2rem] max-h-[2rem] overflow-hidden overflow-x-auto scrollbar">
          {openedFiles.size > 0 &&
            [...openedFiles.values()].map((file, idx) => (
              <li
                key={file.id}
                className={clsx(
                  'inline-block py-1 pr-2 last-child:pr-0 overflow-hidden text-center overflow-ellipsis cursor-pointer',
                )}
                onClick={(e: MouseEvent) => {
                  e.stopPropagation()

                  setActiveFileId(file.id)
                }}
              >
                <span className={clsx(
                  "flex justify-between items-center pl-2 pr-1 py-[0.25rem] min-w-[8rem] rounded-md",
                  file.id === activeFileId
                    ? 'bg-secondary-100'
                    : 'bg-transparent  hover:shadow-border hover:shadow-secondary-300'
                )}>
                  <small className="text-xs">
                    {file.isUntitled ? `SQL-${idx + 1}` : file.name}
                  </small>
                  {openedFiles.size > 1 && (
                    <XCircleIcon
                      onClick={(e: MouseEvent) => {
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
            key={activeFile?.id}
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
        <small>File Status: {fileStatus}</small>
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
  let timeoutID: ReturnType<typeof setTimeout>

  return function callback(...args: any) {
    clearTimeout(timeoutID)

    before && before()

    timeoutID = setTimeout(() => {
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