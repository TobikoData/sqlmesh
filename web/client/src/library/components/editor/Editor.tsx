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
import { isFalse, isNil, isString, isTrue } from '../../../utils'
import { debounce, getLanguageByExtension } from './help'
import './Editor.css'
import Input from '../input/Input'

export const EnumEditorFileStatus = {
  Edit: 'edit',
  Editing: 'editing',
  Saving: 'saving',
  Saved: 'saved',
} as const

export const EnumEditorTabs = {
  QueryPreview: 'queryPreview',
  Table: 'table',
  Terminal: 'terminal',
} as const

export type EditorTabs = KeyOf<typeof EnumEditorTabs>
export type EditorFileStatus = KeyOf<typeof EnumEditorFileStatus>

interface PropsEditor extends React.HTMLAttributes<HTMLElement> {}

const cache: Record<string, Map<EditorTabs, any>> = {}

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
  const [formEvaluate, setFormEvaluate] = useState({
    model: `sushi.${activeFile.name.replace(activeFile.extension, '')}`,
    start: '',
    end: '',
    latest: '',
    limit: 1000,
  })

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

    if (bucket == null) return

    setTabQueryPreviewContent(bucket.get(EnumEditorTabs.QueryPreview))
    setTabTableContent(bucket.get(EnumEditorTabs.Table))
    setTabTerminalContent(bucket.get(EnumEditorTabs.Terminal))

    setFormEvaluate({
      ...formEvaluate,
      model: `sushi.${activeFile.name.replace(activeFile.extension, '')}`,
    })
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

    if (activeFile.isLocal && bucket != null) {
      bucket.set(EnumEditorTabs.Terminal, undefined)
      setTabTerminalContent(bucket.get(EnumEditorTabs.Terminal))

      bucket.set(EnumEditorTabs.QueryPreview, activeFile.content)
      setTabQueryPreviewContent(bucket.get(EnumEditorTabs.QueryPreview))

      fetchdfApiFetchdfPost({
        sql: activeFile.content,
      })
        .then(updateTabs)
        .catch(console.log)
    }
  }

  function evaluateModel(): void {
    const bucket = cache[activeFile.id]

    if (bucket == null) return

    bucket.set(EnumEditorTabs.Terminal, undefined)
    setTabTerminalContent(bucket.get(EnumEditorTabs.Terminal))

    evaluateApiEvaluatePost(formEvaluate).then(updateTabs).catch(console.log)
  }

  function updateTabs(result: string | { detail: string }): void {
    const bucket = cache[activeFile.id]

    if (bucket == null) return

    if (isString(result)) {
      bucket.set(EnumEditorTabs.Table, JSON.parse(result as string))
      setTabTableContent(bucket.get(EnumEditorTabs.Table))
    } else {
      bucket.set(EnumEditorTabs.Terminal, (result as { detail: string }).detail)
      setTabTerminalContent(bucket.get(EnumEditorTabs.Terminal))
    }
  }

  function cleanUp(): void {
    setEditorFileStatus(EnumEditorFileStatus.Edit)
  }

  // TODO: remove once we have a better way to determine if a file is a model
  const isModel = activeFile.path.includes('models/')
  const shouldEvaluate = isModel && Object.values(formEvaluate).every(Boolean)
  const sizesMain = [tabTableContent, tabTerminalContent].some(Boolean)
    ? [75, 25]
    : [100, 0]

  const sizesActions = activeFile.content === '' ? [100, 0] : [70, 30]

  return (
    <SplitPane
      sizes={sizesMain}
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
        <div className="flex h-full flex-col overflow-hidden">
          <SplitPane
            className="flex h-full"
            sizes={sizesActions}
            minSize={0}
          >
            <div className="flex h-full">
              <CodeEditor
                extension={activeFile.extension}
                value={activeFile.content}
                onChange={debouncedChange}
              />
            </div>

            <div className="flex flex-col h-full overflow-hidden">
              <div className="px-4 py-2">
                <p className="inline-block font-bold text-xs text-secondary-500 border-b-2 border-secondary-500 mr-3">
                  Actions
                </p>
                <p className="inline-block font-bold text-xs text-gray-500 border-b-2 border-white mr-3 opacity-50 cursor-not-allowed">
                  Docs
                </p>
              </div>
              <Divider />
              <div className="w-full h-full flex flex-col items-center overflow-hidden">
                <div className="w-full max-w-md h-full py-1 px-3 justify-center overflow-hidden  overflow-y-auto">
                  {isTrue(isModel) && (
                    <form className="my-3 w-full">
                      <fieldset>
                        <div className="flex items-center mb-1 px-3 text-sm font-bold">
                          <h3 className="whitespace-nowrap ml-2">Model Name</h3>
                          <p className="ml-2 px-2 py-1 bg-gray-100 text-alternative-500 text-sm rounded">
                            {formEvaluate.model}
                          </p>
                        </div>
                      </fieldset>
                      <fieldset className="my-3">
                        <div className="p-4 bg-warning-100 text-warning-700 rounded-xl">
                          <p className="text-sm">
                            Please, fill out all fileds to{' '}
                            <b>
                              {isModel ? 'evaluate the model' : 'run query'}
                            </b>
                            .
                          </p>
                        </div>
                      </fieldset>
                      <fieldset className="mb-4">
                        <Input
                          label="Start Date"
                          placeholder="02/11/2023"
                          value={formEvaluate.start}
                          onInput={(e: React.ChangeEvent<HTMLInputElement>) => {
                            e.stopPropagation()

                            const el = e.target as HTMLInputElement

                            setFormEvaluate({
                              ...formEvaluate,
                              start: el?.value ?? '',
                            })
                          }}
                        />
                        <Input
                          label="End Date"
                          placeholder="02/13/2023"
                          value={formEvaluate.end}
                          onInput={(e: React.ChangeEvent<HTMLInputElement>) => {
                            e.stopPropagation()

                            const el = e.target as HTMLInputElement

                            setFormEvaluate({
                              ...formEvaluate,
                              end: el?.value ?? '',
                            })
                          }}
                        />
                        <Input
                          label="Latest Date"
                          placeholder="02/13/2023"
                          value={formEvaluate.latest}
                          onInput={(e: React.ChangeEvent<HTMLInputElement>) => {
                            e.stopPropagation()

                            const el = e.target as HTMLInputElement

                            setFormEvaluate({
                              ...formEvaluate,
                              latest: el?.value ?? '',
                            })
                          }}
                        />
                        <Input
                          type="number"
                          label="Limit"
                          placeholder="1000"
                          value={formEvaluate.limit}
                          onInput={(e: React.ChangeEvent<HTMLInputElement>) => {
                            e.stopPropagation()

                            const el = e.target as HTMLInputElement

                            setFormEvaluate({
                              ...formEvaluate,
                              limit: el?.valueAsNumber ?? formEvaluate.limit,
                            })
                          }}
                        />
                      </fieldset>
                    </form>
                  )}
                  {isFalse(isModel) && activeFile.isLocal && (
                    <form className="my-3 w-full ">
                      <fieldset className="mb-4">
                        <Input
                          label="Environment Name (Optional)"
                          placeholder="prod"
                          value="prod"
                        />
                      </fieldset>
                    </form>
                  )}
                </div>

                <Divider />
                <div className="w-full flex overflow-hidden py-1 px-2 justify-end">
                  {isFalse(activeFile.isLocal) && isModel && (
                    <div className="flex w-full justify-between">
                      <div className="flex">
                        <Button
                          size={EnumSize.sm}
                          variant="alternative"
                          disabled={true}
                        >
                          Validate
                        </Button>
                        <Button
                          size={EnumSize.sm}
                          variant="alternative"
                          disabled={true}
                        >
                          Format
                        </Button>
                      </div>
                      {isTrue(isModel) && (
                        <Button
                          size={EnumSize.sm}
                          variant="secondary"
                          disabled={isFalse(shouldEvaluate)}
                          onClick={e => {
                            e.stopPropagation()

                            evaluateModel()
                          }}
                        >
                          Evaluate
                        </Button>
                      )}
                    </div>
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
                </div>
              </div>
            </div>
          </SplitPane>
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
  value,
  onChange,
  extension,
}: {
  value: string
  extension: string
  onChange: (value: string) => void
}): JSX.Element {
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
      className="w-full h-full overflow-auto"
      extensions={extensions}
      onChange={onChange}
    />
  )
}
