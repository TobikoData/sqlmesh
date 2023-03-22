import {
  useCallback,
  useEffect,
  useMemo,
  useState,
  type MouseEvent,
} from 'react'
import clsx from 'clsx'
import {
  useMutationApiSaveFile,
  useApiFileByPath,
  useApiPlanRun,
  apiCancelPlanRun,
} from '../../../api'
import { useQueryClient } from '@tanstack/react-query'
import { XCircleIcon, PlusIcon } from '@heroicons/react/24/solid'
import { Divider } from '../divider/Divider'
import { Button } from '../button/Button'
import { EnumSize, EnumVariant } from '../../../types/enum'
import { ModelFile } from '../../../models'
import { useStoreFileTree } from '../../../context/fileTree'
import { useStoreEditor } from '../../../context/editor'
import {
  evaluateApiCommandsEvaluatePost,
  fetchdfApiCommandsFetchdfPost,
  type File,
  renderApiCommandsRenderPost,
  type Model,
  type EvaluateInputEnd,
  type EvaluateInputLatest,
  type EvaluateInputStart,
  type RenderInputEnd,
  type RenderInputLatest,
  type RenderInputStart,
} from '../../../api/client'
import Tabs from '../tabs/Tabs'
import SplitPane from '../splitPane/SplitPane'
import {
  debounceAsync,
  isArrayNotEmpty,
  isFalse,
  isNil,
  isObjectLike,
  isStringEmptyOrNil,
  isTrue,
  toDate,
  toDateFormat,
} from '../../../utils'
import { debounce, getLanguageByExtension } from './help'
import './Editor.css'
import Input from '../input/Input'
import { type Table } from 'apache-arrow'
import { type ResponseWithDetail } from '~/api/instance'
import { type ModelEnvironment } from '~/models/environment'
import { dracula, tomorrow } from 'thememirror'
import { EnumColorScheme, useColorScheme } from '~/context/theme'
import CodeEditor from './CodeEditor'
import { sqlglotWorker } from './workers'
import { type PropsComponent } from '~/main'

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

interface PropsEditor extends React.HTMLAttributes<HTMLElement> {
  environment: ModelEnvironment
  models?: Map<string, Model>
}

const cache: Record<string, Map<EditorTabs, any>> = {}

const DAY = 24 * 60 * 60 * 1000

interface FormEvaluate {
  model?: string
  start: EvaluateInputStart | RenderInputStart
  end: EvaluateInputEnd | RenderInputEnd
  latest: EvaluateInputLatest | RenderInputLatest
  limit: number
}

export default function Editor({
  className,
  environment,
  models,
}: PropsEditor): JSX.Element {
  const client = useQueryClient()

  const activeFile = useStoreFileTree(s => s.activeFile)
  const openedFiles = useStoreFileTree(s => s.openedFiles)
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
  const [isSaved, setIsSaved] = useState(true)
  const [formEvaluate, setFormEvaluate] = useState<FormEvaluate>({
    start: toDateFormat(toDate(Date.now() - DAY)),
    end: toDateFormat(new Date()),
    latest: toDateFormat(toDate(Date.now() - DAY)),
    limit: 1000,
  })
  const [dialects, setDialects] = useState<
    Array<{ dialect_title: string; dialect_name: string }>
  >([])
  const [dialect, setDialect] = useState<string>()
  const [isValid, setIsValid] = useState(true)

  const { refetch: planRun } = useApiPlanRun(environment.name, {
    planOptions: {
      skip_tests: true,
    },
  })

  const { data: fileData, refetch: getFileContent } = useApiFileByPath(
    activeFile.path,
  )

  const mutationSaveFile = useMutationApiSaveFile(client, {
    onSuccess(file: File) {
      setIsSaved(true)
      setEditorFileStatus(EnumEditorFileStatus.Edit)

      if (file == null) return

      activeFile.content = file.content ?? ''

      setOpenedFiles(openedFiles)
    },
    onMutate() {
      setIsSaved(false)
      setEditorFileStatus(EnumEditorFileStatus.Saving)
    },
  })
  const debouncedPlanRun = useCallback(debounceAsync(planRun, 1000, true), [
    planRun,
  ])

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

  const handleSqlGlotWorkerMessage = useCallback(
    (e: MessageEvent): void => {
      if (e.data.topic === 'parse') {
        setIsValid(
          e.data.payload?.type !== 'error' || activeFile.content === '',
        )
      }

      if (e.data.topic === 'dialects') {
        setDialects(e.data.payload.dialects ?? [])
        setDialect(e.data.payload.dialect)
      }
    },
    [activeFile],
  )

  useEffect(() => {
    sqlglotWorker.postMessage({
      topic: 'dialects',
    })

    sqlglotWorker.addEventListener('message', handleSqlGlotWorkerMessage)

    return () => {
      sqlglotWorker.removeEventListener('message', handleSqlGlotWorkerMessage)
    }
  }, [handleSqlGlotWorkerMessage])

  useEffect(() => {
    if (activeFile.isSQLMeshModel || activeFile.isSQLMeshSeed) {
      void debouncedPlanRun()
    }

    return () => {
      apiCancelPlanRun(client)
    }
  }, [activeFile.content])

  useEffect(() => {
    if (fileData == null) return

    activeFile.content = fileData.content ?? ''

    setOpenedFiles(openedFiles)
  }, [fileData])

  useEffect(() => {
    if (isFalse(isStringEmptyOrNil(activeFile.path))) {
      void getFileContent()
    }

    if (isNil(cache[activeFile.id])) {
      cache[activeFile.id] = new Map()
    }

    const bucket = cache[activeFile.id]

    if (bucket == null) return

    setTabQueryPreviewContent(bucket.get(EnumEditorTabs.QueryPreview))
    setTabTableContent(bucket.get(EnumEditorTabs.Table))
    setTabTerminalContent(bucket.get(EnumEditorTabs.Terminal))

    const model = models?.get(activeFile.path)?.name

    if (model != null) {
      setFormEvaluate(s => ({
        ...s,
        model,
      }))
    }
  }, [activeFile])

  function closeEditorTab(file: ModelFile): void {
    delete cache[file.id]

    openedFiles.delete(file)

    if (activeFile === file) {
      selectFile(getNextOpenedFile())
    } else {
      setOpenedFiles(openedFiles)
    }
  }

  function addNewFileAndSelect(): void {
    selectFile(new ModelFile())
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

      fetchdfApiCommandsFetchdfPost({
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

    if (formEvaluate?.model != null) {
      renderApiCommandsRenderPost({
        ...formEvaluate,
        model: formEvaluate.model,
      })
        .then(({ sql }) => {
          bucket.set(EnumEditorTabs.QueryPreview, sql)
          setTabQueryPreviewContent(bucket.get(EnumEditorTabs.QueryPreview))
        })
        .catch(console.log)

      evaluateApiCommandsEvaluatePost({
        ...formEvaluate,
        model: formEvaluate.model,
      })
        .then(updateTabs)
        .catch(console.log)
    }
  }

  function updateTabs<T = ResponseWithDetail | Table<any>>(result: T): void {
    const bucket = cache[activeFile.id]

    if (bucket == null || isFalse(isObjectLike(result))) return

    // Potentially a response with details is error
    // and we need to show it in terminal
    const isResponseWithDetail = 'detail' in (result as ResponseWithDetail)

    if (isResponseWithDetail) {
      bucket.set(EnumEditorTabs.Terminal, (result as ResponseWithDetail).detail)
      setTabTerminalContent(bucket.get(EnumEditorTabs.Terminal))

      bucket.set(EnumEditorTabs.Table, undefined)
      setTabTableContent(bucket.get(EnumEditorTabs.Table))
    } else {
      bucket.set(EnumEditorTabs.Terminal, undefined)
      setTabTerminalContent(bucket.get(EnumEditorTabs.Terminal))

      bucket.set(
        EnumEditorTabs.Table,
        getTableDataFromArrowStreamResult(result as Table<any>),
      )
      setTabTableContent(bucket.get(EnumEditorTabs.Table))
    }
  }

  function cleanUp(): void {
    setEditorFileStatus(EnumEditorFileStatus.Edit)
  }

  // TODO: remove once we have a better way to determine if a file is a model
  const isModel = activeFile.path.includes('models/')
  const hasContentActiveFile = isFalse(isStringEmptyOrNil(activeFile.content))
  const shouldEvaluate =
    isModel && Object.values({ ...formEvaluate }).every(Boolean)
  const sizesActions = isStringEmptyOrNil(activeFile.content)
    ? [100, 0]
    : [80, 20]
  const sizesMain = [tabTableContent, tabTerminalContent].some(Boolean)
    ? [75, 25]
    : [100, 0]

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
            className="m-0 ml-1 mr-3 bg-primary-10  hover:bg-secondary-10 active:bg-secondary-10 border-none"
            variant={EnumVariant.Alternative}
            size={EnumSize.sm}
            onClick={(e: MouseEvent) => {
              e.stopPropagation()

              addNewFileAndSelect()
            }}
          >
            <PlusIcon className="inline-block w-3 h-4 text-secondary-500 dark:text-primary-500" />
          </Button>
          <ul className="w-full whitespace-nowrap min-h-[2rem] max-h-[2rem] overflow-hidden overflow-x-auto scrollbar scrollbar--horizontal">
            {openedFiles.size > 0 &&
              [...openedFiles.values()].map((file, idx) => (
                <li
                  key={file.id}
                  className={clsx(
                    'inline-block py-1 pr-2 last-child:pr-0 overflow-hidden text-center overflow-ellipsis cursor-pointer',
                  )}
                  onClick={(e: MouseEvent) => {
                    e.stopPropagation()

                    selectFile(file)
                  }}
                >
                  <span
                    className={clsx(
                      'flex border-2 justify-between items-center pl-2 pr-1 py-[0.125rem] min-w-[8rem] rounded-md group border-transparent border-r border-r-theme-darker dark:border-r-theme-lighter',
                      file === activeFile
                        ? 'bg-neutral-200 border-neutral-200 text-neutral-900 dark:bg-dark-lighter dark:border-dark-lighter dark:text-primary-500'
                        : 'bg-trasparent hover:bg-theme-darker dark:hover:bg-theme-lighter',
                    )}
                  >
                    <small className="text-xs">
                      {file.isUntitled ? `SQL-${idx + 1}` : file.name}
                    </small>
                    {openedFiles.size > 1 && (
                      <XCircleIcon
                        className="inline-block opacity-0 group-hover:opacity-100 text-neutral-600 dark:text-neutral-100 w-4 h-4 ml-2 cursor-pointer"
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
        <div className="flex flex-col h-full overflow-hidden">
          <SplitPane
            className="flex h-full"
            sizes={sizesActions}
            minSize={[320, activeFile.content === '' ? 0 : 240]}
            maxSize={[Infinity, 320]}
            snapOffset={0}
            expandToMin={true}
          >
            <div className="flex flex-col h-full">
              {models != null && (
                <CodeEditor
                  onChange={debouncedChange}
                  models={models}
                  file={activeFile}
                  dialect={dialect}
                  dialects={dialects.map(d => d.dialect_name)}
                />
              )}
            </div>
            <div className="flex flex-col h-full">
              <div className="flex flex-col w-full h-full items-center overflow-hidden">
                <div className="flex w-full h-full py-1 px-3 overflow-hidden overflow-y-auto scrollbar scrollbar--vertical">
                  {isTrue(isModel) && formEvaluate.model != null && (
                    <form className="my-3">
                      <fieldset className="flex flex-wrap items-center my-3 px-3 text-sm font-bold">
                        <h3 className="whitespace-nowrap ml-2">Model Name</h3>
                        <p className="ml-2 px-2 py-1 bg-secondary-10 text-secondary-500 dark:text-primary-500 dark:bg-primary-10  text-xs rounded">
                          {formEvaluate.model}
                        </p>
                      </fieldset>
                      <fieldset className="flex my-3 px-3">
                        <div className="p-4 bg-warning-10 text-warning-600 rounded-xl">
                          <p className="text-sm">
                            Please, fill out all fileds to{' '}
                            <b>
                              {isModel ? 'evaluate the model' : 'run query'}
                            </b>
                            .
                          </p>
                        </div>
                      </fieldset>
                      <fieldset className="my-3 px-3">
                        <Input
                          className="w-full mx-0"
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
                          className="w-full mx-0"
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
                          className="w-full mx-0"
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
                          className="w-full mx-0"
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
                    <form className="my-3 w-full">
                      <fieldset className="mb-4">
                        <Input
                          className="w-full mx-0"
                          label="Environment Name (Optional)"
                          placeholder="prod"
                          value="prod"
                          onInput={(e: React.ChangeEvent<HTMLInputElement>) => {
                            console.log(e.target.value)
                          }}
                        />
                      </fieldset>
                    </form>
                  )}
                </div>
                <Divider />
                {hasContentActiveFile && (
                  <div className="w-full flex overflow-hidden py-1 px-2 justify-end">
                    {isFalse(activeFile.isLocal) && isModel && (
                      <div className="flex w-full justify-between">
                        <div className="flex">
                          <Button
                            size={EnumSize.sm}
                            variant={EnumVariant.Alternative}
                            disabled={true}
                          >
                            Validate
                          </Button>
                        </div>
                        {isTrue(isModel) && (
                          <Button
                            size={EnumSize.sm}
                            variant={EnumVariant.Secondary}
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
                )}
              </div>
            </div>
          </SplitPane>
        </div>
        <Divider />
        <div className="px-2 flex justify-between items-center min-h-[2rem]">
          <EditorFooter
            activeFile={activeFile}
            isSaved={isSaved}
            fileStatus={fileStatus}
            dialects={dialects}
            dialect={dialect}
            setDialect={setDialect}
            isValid={isValid}
          />
        </div>
      </div>
      <Tabs className="overflow-auto scrollbar scrollbar--vertical" />
    </SplitPane>
  )
}

interface PropsIndicator extends PropsComponent {
  text: string
  value?: string | number
  ok?: boolean
}

function Indicator({
  text,
  value,
  ok = true,
  className,
}: PropsIndicator): JSX.Element {
  return (
    <small className={clsx('font-bold whitespace-nowrap text-xs', className)}>
      <span>{text}:&nbsp;</span>
      {value == null ? (
        <span
          className={clsx(
            'inline-block w-2 h-2 rounded-full',
            ok ? 'bg-success-500' : 'bg-danger-500',
          )}
        ></span>
      ) : (
        <span className="font-normal">{value}</span>
      )}
    </small>
  )
}

function EditorFooter({
  dialects = [],
  dialect,
  setDialect,
  activeFile,
  isSaved,
  isValid,
  fileStatus,
}: {
  activeFile: ModelFile
  isSaved: boolean
  isValid: boolean
  fileStatus: string
  dialects: Array<{ dialect_title: string; dialect_name: string }>
  dialect?: string
  setDialect: (dialect?: string) => void
}): JSX.Element {
  return (
    <div className="mr-4">
      <Indicator
        className="mr-2"
        text="Valid"
        ok={isValid}
      />
      {!activeFile.isLocal && (
        <Indicator
          className="mr-2"
          text="Saved"
          ok={isSaved}
        />
      )}
      <Indicator
        className="mr-2"
        text="Status"
        value={fileStatus}
      />
      <Indicator
        className="mr-2"
        text="Language"
        value={getLanguageByExtension(activeFile.extension)}
      />
      {activeFile.extension === '.sql' && isArrayNotEmpty(dialects) && (
        <span className="inline-block mr-2">
          <small className="font-bold text-xs mr-1">Dialect</small>
          <select
            className="text-xs m-0 px-1 py-[0.125rem] bg-secondary-100 rounded"
            value={dialect}
            onChange={(e: React.ChangeEvent<HTMLSelectElement>) => {
              setDialect(e.target.value)
            }}
          >
            {dialects.map(dialect => (
              <option
                key={dialect.dialect_title}
                value={dialect.dialect_name}
              >
                {dialect.dialect_title}
              </option>
            ))}
          </select>
        </span>
      )}
      {activeFile.isSQLMeshModel && (
        <Indicator
          className="mr-2"
          text="SQLMesh Type"
          value={activeFile.isSQLMeshModel ? 'Model' : 'Plain'}
        />
      )}
    </div>
  )
}

type TableCellValue = number | string | null
type TableRows = Array<Record<string, TableCellValue>>
type TableColumns = string[]
type ResponseTableColumns = Array<Array<[string, TableCellValue]>>

function getTableDataFromArrowStreamResult(
  result: Table<any>,
): [TableColumns?, TableRows?] {
  if (result == null) return []

  const data: ResponseTableColumns = result.toArray() // result.toArray() returns an array of Proxies
  const rows = Array.from(data).map(toTableRow) // using Array.from to convert the Proxies to real objects
  const columns = result.schema.fields.map(field => field.name)

  return [columns, rows]
}

function toTableRow(
  row: Array<[string, TableCellValue]> = [],
): Record<string, TableCellValue> {
  // using Array.from to convert the Proxies to real objects
  return Array.from(row).reduce(
    (acc, [key, value]) => Object.assign(acc, { [key]: value }),
    {},
  )
}
