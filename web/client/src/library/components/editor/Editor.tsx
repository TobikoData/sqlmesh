import { useEffect, useMemo, useState, MouseEvent } from 'react'
import CodeMirror from '@uiw/react-codemirror'
import { sql } from '@codemirror/lang-sql'
import { python } from '@codemirror/lang-python'
import { StreamLanguage } from '@codemirror/language'
import { yaml } from '@codemirror/legacy-modes/mode/yaml'
import clsx from 'clsx'
import { Extension } from '@codemirror/state'
import { useMutationApiSaveFile, useApiFileByPath } from '../../../api'
import { useQueryClient } from '@tanstack/react-query'
import { XCircleIcon, PlusIcon } from '@heroicons/react/24/solid'
import { Divider } from '../divider/Divider'
import { Button } from '../button/Button'
import { EnumSize } from '../../../types/enum'
import { ModelFile } from '../../../models'
import { useStoreFileTree } from '../../../context/fileTree'
import { useStoreEditor } from '../../../context/editor'
import { fetchdfApiFetchdfPost } from '../../../api/client'
import Tabs from '../tabs/Tabs'
import { isString } from '../../../utils'
import SplitPane from '../splitPane/SplitPane'

export const EnumEditorFileStatus = {
  Edit: 'edit',
  Editing: 'editing',
  Saving: 'saving',
  Saved: 'saved',
} as const

export type EditorFileStatus =
  typeof EnumEditorFileStatus[keyof typeof EnumEditorFileStatus]

interface PropsEditor extends React.HTMLAttributes<HTMLElement> {}

export function Editor({ className }: PropsEditor): JSX.Element {
  const client = useQueryClient()

  const activeFileId = useStoreFileTree(s => s.activeFileId)
  const openedFiles = useStoreFileTree(s => s.openedFiles)
  const setActiveFileId = useStoreFileTree(s => s.setActiveFileId)
  const setOpenedFiles = useStoreFileTree(s => s.setOpenedFiles)
  const selectFile = useStoreFileTree(s => s.selectFile)
  const getNextOpenedFile = useStoreFileTree(s => s.getNextOpenedFile)
  const setTabTableContent = useStoreEditor(s => s.setTabTableContent)
  const setTabQueryPreviewContent = useStoreEditor(
    s => s.setTabQueryPreviewContent,
  )
  const setTabTerminalContent = useStoreEditor(s => s.setTabTerminalContent)
  const tabTableContent = useStoreEditor(s => s.tabTableContent)
  const tabTerminalContent = useStoreEditor(s => s.tabTerminalContent)

  const [fileStatus, setEditorFileStatus] = useState<EditorFileStatus>(
    EnumEditorFileStatus.Edit,
  )
  const [activeFile, setActiveFile] = useState<ModelFile>(getNextOpenedFile())
  const [isSaved, setIsSaved] = useState(true)

  const { data: fileData } = useApiFileByPath(activeFile.path)
  const mutationSaveFile = useMutationApiSaveFile<{ content: string }>(client, {
    onSuccess() {
      setIsSaved(true)
      setEditorFileStatus(EnumEditorFileStatus.Edit)
    },
    onMutate() {
      setIsSaved(false)
      setEditorFileStatus(EnumEditorFileStatus.Saving)
    },
  })

  const debouncedChange = useMemo(
    () =>
      debounce(
        onChange,
        () => {
          setEditorFileStatus(EnumEditorFileStatus.Editing)
        },
        () => {
          setEditorFileStatus(EnumEditorFileStatus.Edit)
        },
        200,
      ),
    [activeFile],
  )

  useEffect(() => {
    if (fileData == null) return

    activeFile.content = fileData.content ?? ''

    setOpenedFiles(openedFiles)
  }, [fileData])

  useEffect(() => {
    const activeOpenedFile = openedFiles.get(activeFileId)

    if (activeOpenedFile == null) {
      setActiveFileId(getNextOpenedFile().id)
      return
    }

    setActiveFile(activeOpenedFile)
  }, [activeFileId])

  useEffect(() => {
    if (openedFiles.size < 1) {
      const file = new ModelFile()

      selectFile(file)
    } else {
      !openedFiles.has(activeFileId) && setActiveFileId(getNextOpenedFile().id)
    }
  }, [openedFiles])

  function closeEditorTab(file: ModelFile): void {
    openedFiles.delete(file.id)

    if (activeFileId === file.id) {
      setActiveFileId(getNextOpenedFile().id)
    } else {
      setOpenedFiles(openedFiles)
    }
  }

  function addNewFileAndSelect(): void {
    const file = new ModelFile()

    openedFiles.set(file.id, file)

    setActiveFileId(file.id)
  }

  function onChange(value: string): void {
    if (activeFile.content === value) return

    activeFile.content = value

    if (activeFile.isLocal) {
      setOpenedFiles(openedFiles)
    } else {
      mutationSaveFile.mutate({
        path: activeFile.path,
        body: { content: value },
      })
    }
  }

  function sendQuery(): void {
    if (activeFile.isLocal) {
      setTabQueryPreviewContent(activeFile.content)

      fetchdfApiFetchdfPost({
        sql: activeFile.content,
      })
        .then((result: any) => {
          if (isString(result)) {
            setTabTableContent(JSON.parse(result))
            setTabTerminalContent(undefined)
          } else {
            setTabTerminalContent(result.detail)
          }
        })
        .catch(console.log)
    }
  }

  function cleanUp(): void {
    setEditorFileStatus(EnumEditorFileStatus.Edit)
  }

  const sizes = [tabTableContent, tabTerminalContent].some(Boolean)
    ? [75, 25]
    : [100, 0]

  return (
    <SplitPane
      sizes={sizes}
      direction="vertical"
      minSize={0}
      className={className}
    >
      <div className="w-full h-full flex flex-col overflow-hidden">
        <div className="flex items-center">
          <Button
            className="m-0 ml-1 mr-3"
            variant="primary"
            size="sm"
            onClick={(e: MouseEvent) => {
              e.stopPropagation()

              addNewFileAndSelect()
            }}
          >
            <PlusIcon className="inline-block text-secondary-500 font-black w-3 h-4 cursor-pointer " />
          </Button>
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
                  <span
                    className={clsx(
                      'flex justify-between items-center pl-2 pr-1 py-[0.25rem] min-w-[8rem] rounded-md',
                      file.id === activeFileId
                        ? 'bg-secondary-100'
                        : 'bg-transparent  hover:shadow-border hover:shadow-secondary-300',
                    )}
                  >
                    <small className="text-xs">
                      {file.isUntitled ? `SQL-${idx + 1}` : file.name}
                    </small>
                    {openedFiles.size > 1 && (
                      <XCircleIcon
                        className="inline-block text-gray-200 w-4 h-4 ml-2 cursor-pointer hover:text-gray-700"
                        onClick={(e: MouseEvent) => {
                          e.stopPropagation()

                          cleanUp()
                          closeEditorTab(file)
                        }}
                      />
                    )}
                  </span>
                </li>
              ))}
          </ul>
        </div>

        <Divider />
        <div className="w-full h-full flex flex-col overflow-hidden">
          <div className="w-full h-full overflow-hidden">
            <CodeEditor
              className="h-full w-full"
              extension={activeFile.extension}
              value={activeFile.content}
              onChange={debouncedChange}
            />
          </div>
        </div>

        <Divider />
        <div className="px-2 flex justify-between items-center min-h-[2rem]">
          <div className="flex align-center mr-4">
            <Indicator
              text="Valid"
              ok={true}
            />
            {!activeFile.isLocal && (
              <>
                <Divider
                  orientation="vertical"
                  className="h-[12px] mx-3"
                />
                <Indicator
                  text="Saved"
                  ok={isSaved}
                />
              </>
            )}
            <Divider
              orientation="vertical"
              className="h-[12px] mx-3"
            />
            <Indicator
              text="Status"
              value={fileStatus}
            />
            <Divider
              orientation="vertical"
              className="h-[12px] mx-3"
            />
            <Indicator
              text="Language"
              value={getLanguageByExtension(activeFile.extension)}
            />
          </div>
          <div className="flex">
            {activeFile != null &&
              activeFile.extension === '.sql' &&
              activeFile.content !== '' && (
                <>
                  <Button
                    size={EnumSize.sm}
                    variant="alternative"
                    onClick={e => {
                      e.stopPropagation()

                      sendQuery()
                    }}
                  >
                    Run Query
                  </Button>
                  <Button
                    size={EnumSize.sm}
                    variant="alternative"
                    onClick={e => {
                      e.stopPropagation()

                      onChange('')
                    }}
                  >
                    Clear
                  </Button>
                </>
              )}

            {!activeFile.isLocal && (
              <>
                <Button
                  size={EnumSize.sm}
                  variant="alternative"
                >
                  Validate
                </Button>
                <Button
                  size={EnumSize.sm}
                  variant="alternative"
                >
                  Format
                </Button>
              </>
            )}
          </div>
        </div>
      </div>
      <Tabs className="h-full w-full overflow-auto" />
    </SplitPane>
  )
}

function Indicator({
  text,
  value,
  ok = true,
}: {
  text: string
  value?: string
  ok?: boolean
}): JSX.Element {
  return (
    <small className="font-bold text-xs whitespace-nowrap">
      {text}:{' '}
      {value == null ? (
        <span
          className={clsx(
            `bg-${ok ? 'success' : 'warning'}-500`,
            'inline-block w-2 h-2 rounded-full',
          )}
        ></span>
      ) : (
        <span className="font-normal text-gray-600">{value}</span>
      )}
    </small>
  )
}

function CodeEditor({
  className,
  value,
  onChange,
  extension,
}: any): JSX.Element {
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

function debounce(
  fn: (...args: any) => void,
  before: () => void,
  after: () => void,
  delay: number = 500,
): (...args: any) => void {
  let timeoutID: ReturnType<typeof setTimeout>

  return function callback(...args: any) {
    clearTimeout(timeoutID)

    if (before != null) {
      before()
    }

    timeoutID = setTimeout(() => {
      fn(...args)

      if (after != null) {
        after()
      }
    }, delay)
  }
}

function getLanguageByExtension(extension?: string): string {
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
