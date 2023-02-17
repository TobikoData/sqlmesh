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
import {
  evaluateApiEvaluatePost,
  fetchdfApiFetchdfPost,
} from '../../../api/client'
import Tabs from '../tabs/Tabs'
import SplitPane from '../splitPane/SplitPane'
import { isFalse, isNil, isString } from '../../../utils'
import { debounce, getLanguageByExtension } from './help'
import './Editor.css'

export const EnumEditorFileStatus = {
  Edit: 'edit',
  Editing: 'editing',
  Saving: 'saving',
  Saved: 'saved',
} as const

export type EditorFileStatus =
  typeof EnumEditorFileStatus[keyof typeof EnumEditorFileStatus]

interface PropsEditor extends React.HTMLAttributes<HTMLElement> {}

const cache: Record<string, Map<string, any>> = {}

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

  useEffect(() => {
    if (isNil(cache[activeFile.id])) {
      cache[activeFile.id] = new Map()
    }

    const bucket = cache[activeFile.id]

    setTabQueryPreviewContent(bucket?.get('queryPreview'))
    setTabTableContent(bucket?.get('table'))
    setTabTerminalContent(bucket?.get('terminal'))
  }, [activeFile])

  function closeEditorTab(file: ModelFile): void {
    delete cache[file.id]

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
    const bucket = cache[activeFile.id]

    if (activeFile.isLocal) {
      bucket?.set('terminal', undefined)
      setTabTerminalContent(bucket?.get('terminal'))

      bucket?.set('queryPreview', activeFile.content)
      setTabQueryPreviewContent(bucket?.get('queryPreview'))

      fetchdfApiFetchdfPost({
        sql: activeFile.content,
      })
        .then(updateTabs)
        .catch(console.log)
    }
  }

  function evaluateModel(): void {
    const bucket = cache[activeFile.id]
    const WEEK = 1000 * 60 * 60 * 24 * 7

    bucket?.set('terminal', undefined)
    setTabTerminalContent(bucket?.get('terminal'))

    evaluateApiEvaluatePost({
      model: `sushi.${activeFile.name.replace(activeFile.extension, '')}`,
      start: Date.now() - WEEK,
      end: Date.now(),
      latest: Date.now() - WEEK,
    })
      .then(updateTabs)
      .catch(console.log)
  }

  function updateTabs(result: string | { detail: string }): void {
    const bucket = cache[activeFile.id]

    if (isString(result)) {
      bucket?.set('table', JSON.parse(result as string))
      setTabTableContent(bucket?.get('table'))
    } else {
      bucket?.set('terminal', (result as { detail: string }).detail)
      setTabTerminalContent(bucket?.get('terminal'))
    }
  }

  function cleanUp(): void {
    setEditorFileStatus(EnumEditorFileStatus.Edit)
  }

  const sizes = [tabTableContent, tabTerminalContent].some(Boolean)
    ? [75, 25]
    : [100, 0]

  // TODO: remove once we have a better way to determine if a file is a model
  const isModel = activeFile.path.includes('models/')

  return (
    <SplitPane
      sizes={sizes}
      direction="vertical"
      minSize={0}
      className={className}
    >
      <div className="flex flex-col overflow-hidden">
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
            {isFalse(activeFile.isLocal) && isModel && (
              <>
                <Button
                  size={EnumSize.sm}
                  variant="alternative"
                  onClick={e => {
                    e.stopPropagation()

                    evaluateModel()
                  }}
                >
                  Evaluate
                </Button>
              </>
            )}
            {isFalse(isModel) &&
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
                </>
              )}
            {activeFile.content !== '' && (
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
            )}
          </div>
        </div>
      </div>
      <Tabs className="overflow-auto" />
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
