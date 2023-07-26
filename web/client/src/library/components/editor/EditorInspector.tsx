import { isCancelledError } from '@tanstack/react-query'
import { type Table } from 'apache-arrow'
import clsx from 'clsx'
import React, { useEffect, useMemo, useState } from 'react'
import {
  type EvaluateInputStart,
  type RenderInputStart,
  type EvaluateInputEnd,
  type RenderInputEnd,
  type EvaluateInputExecutionTime,
  type RenderInputExecutionTime,
} from '~/api/client'
import { useStoreContext } from '~/context/context'
import { EnumSize, EnumVariant } from '~/types/enum'
import { debounceAsync, isFalse, toDate, toDateFormat } from '~/utils'
import { Button } from '../button/Button'
import { Divider } from '../divider/Divider'
import Input from '../input/Input'
import { type EditorTab, useStoreEditor } from '~/context/editor'
import { Tab } from '@headlessui/react'
import Banner from '@components/banner/Banner'
import Documentation from '@components/documentation/Documentation'
import { type ModelSQLMeshModel } from '@models/sqlmesh-model'
import {
  useApiEvaluate,
  useApiFetchdf,
  useApiRender,
  useApiTableDiff,
} from '@api/index'
import { EnumErrorKey } from '~/library/pages/ide/context'
import TabList from '@components/tab/Tab'
import { getTableDataFromArrowStreamResult } from '@components/table/help'

interface FormModel {
  model?: string
  start: EvaluateInputStart | RenderInputStart
  end: EvaluateInputEnd | RenderInputEnd
  execution_time: EvaluateInputExecutionTime | RenderInputExecutionTime
  limit: number
}

interface FormArbitrarySql {
  limit: number
}

const DAY = 24 * 60 * 60 * 1000
const LIMIT = 1000
const LIMIT_DIFF = 50

export default function EditorInspector({
  tab,
}: {
  tab: EditorTab
}): JSX.Element {
  const models = useStoreContext(s => s.models)
  const model = useMemo(() => models.get(tab.file.path), [tab, models])

  return (
    <div
      className={clsx(
        'flex flex-col w-full h-full items-center overflow-hidden',
      )}
    >
      {tab.file.isSQLMeshModel ? (
        model != null && (
          <InspectorModel
            tab={tab}
            model={model}
          />
        )
      ) : (
        <InspectorSql tab={tab} />
      )}
    </div>
  )
}

function InspectorModel({
  tab,
  model,
}: {
  tab: EditorTab
  model: ModelSQLMeshModel
}): JSX.Element {
  const environment = useStoreContext(s => s.environment)
  const environments = useStoreContext(s => s.environments)
  const list = Array.from(environments)
    .filter(({ isSynchronized }) => isSynchronized)
    .map(({ name }) => ({ text: name, value: name }))

  return (
    <Tab.Group>
      <TabList
        list={
          [
            'Actions',
            'Docs',
            list.length > 1 && environment.isSynchronized && 'Diff',
          ].filter(Boolean) as string[]
        }
      />
      <Tab.Panels className="h-full w-full overflow-hidden">
        <Tab.Panel
          unmount={false}
          className={clsx(
            'flex flex-col w-full h-full relative overflow-hidden',
            'ring-white ring-opacity-60 ring-offset-2 ring-offset-blue-400 focus:outline-none focus:ring-2',
          )}
        >
          <FormActionsModel
            tab={tab}
            model={model}
          />
        </Tab.Panel>
        <Tab.Panel
          unmount={false}
          className={clsx(
            'text-xs w-full h-full ring-white ring-opacity-60 ring-offset-2 ring-offset-blue-400 focus:outline-none focus:ring-2 py-2',
          )}
        >
          <Documentation
            model={model}
            withCode={false}
            withModel={false}
            withQuery={tab.file.isSQLMeshModelSQL}
            withDescription={false}
          />
        </Tab.Panel>
        {list.length > 1 && environment.isSynchronized && (
          <Tab.Panel
            unmount={false}
            className={clsx(
              'flex flex-col w-full h-full relative overflow-hidden',
              'ring-white ring-opacity-60 ring-offset-2 ring-offset-blue-400 focus:outline-none focus:ring-2',
            )}
          >
            <FormDiffModel
              tab={tab}
              model={model}
              list={list.filter(({ value }) => environment.name !== value)}
              target={{ text: environment.name, value: environment.name }}
            />
          </Tab.Panel>
        )}
      </Tab.Panels>
    </Tab.Group>
  )
}

function InspectorSql({ tab }: { tab: EditorTab }): JSX.Element {
  return (
    <Tab.Group>
      <TabList list={['Actions', 'Diff']} />
      <Tab.Panels className="h-full w-full overflow-hidden">
        <Tab.Panel
          unmount={false}
          className={clsx(
            'flex flex-col w-full h-full relative overflow-hidden',
            'ring-white ring-opacity-60 ring-offset-2 ring-offset-blue-400 focus:outline-none focus:ring-2',
          )}
        >
          <FormActionsCustomSQL tab={tab} />
        </Tab.Panel>
        <Tab.Panel
          unmount={false}
          className={clsx(
            'flex flex-col w-full h-full relative overflow-hidden',
            'ring-white ring-opacity-60 ring-offset-2 ring-offset-blue-400 focus:outline-none focus:ring-2',
          )}
        >
          <FormDiff />
        </Tab.Panel>
      </Tab.Panels>
    </Tab.Group>
  )
}

function FormFieldset({
  children,
}: {
  children: React.ReactNode
}): JSX.Element {
  return <fieldset className="flex my-3">{children}</fieldset>
}

function InspectorForm({
  children,
}: {
  children: React.ReactNode
}): JSX.Element {
  return (
    <div className="flex w-full h-full py-1 overflow-hidden overflow-y-auto hover:scrollbar scrollbar--vertical">
      {children}
    </div>
  )
}

function InspectorActions({
  children,
}: {
  children: React.ReactNode
}): JSX.Element {
  return <div className="flex w-full py-1 px-1 justify-end">{children}</div>
}

function FormActionsCustomSQL({ tab }: { tab: EditorTab }): JSX.Element {
  const setPreviewQuery = useStoreEditor(s => s.setPreviewQuery)
  const setPreviewConsole = useStoreEditor(s => s.setPreviewConsole)
  const setPreviewTable = useStoreEditor(s => s.setPreviewTable)
  const engine = useStoreEditor(s => s.engine)

  const [form, setForm] = useState<FormArbitrarySql>({
    limit: LIMIT,
  })

  const { refetch: getFetchdf, isFetching } = useApiFetchdf({
    sql: tab.file.content,
    limit: form.limit,
  })
  const debouncedGetFetchdf = debounceAsync(getFetchdf, 1000, true)

  const shouldSendQuery = Object.values(form).every(Boolean)

  function sendQuery(): void {
    setPreviewQuery(tab.file.content)
    setPreviewConsole(undefined)
    setPreviewTable(undefined)

    debouncedGetFetchdf({
      throwOnError: true,
    })
      .then(({ data }) => {
        setPreviewTable(getTableDataFromArrowStreamResult(data as Table<any>))
      })
      .catch(error => {
        if (isCancelledError(error)) {
          console.log(
            'fetchdfApiCommandsFetchdfPost',
            'Request aborted by React Query',
          )
        } else {
          setPreviewConsole([EnumErrorKey.Fetchdf, error])
        }
      })
  }

  return (
    <>
      <InspectorForm>
        <form className="my-3 w-full px-2">
          {isFalse(shouldSendQuery) && (
            <FormFieldset>
              <Banner variant={EnumVariant.Warning}>
                <Banner.Description className="w-full mr-2 text-sm">
                  Please fill out all fields to <b>run the query</b>.
                </Banner.Description>
              </Banner>
            </FormFieldset>
          )}
          <fieldset className="mb-4 w-full">
            <Input
              className="w-full mx-0"
              label="Limit"
            >
              {({ className }) => (
                <Input.Textfield
                  className={clsx(className, 'w-full')}
                  type="number"
                  placeholder={String(LIMIT)}
                  value={form.limit}
                  onInput={(e: React.ChangeEvent<HTMLInputElement>) => {
                    e.stopPropagation()

                    setForm({
                      ...form,
                      limit: e.target.valueAsNumber ?? LIMIT,
                    })
                  }}
                />
              )}
            </Input>
          </fieldset>
        </form>
      </InspectorForm>
      <Divider />
      <InspectorActions>
        <Button
          size={EnumSize.sm}
          variant={EnumVariant.Alternative}
          onClick={e => {
            e.stopPropagation()

            engine.postMessage({
              topic: 'format',
              payload: {
                sql: tab.file.content,
              },
            })
          }}
        >
          Format
        </Button>
        <Button
          size={EnumSize.sm}
          variant={EnumVariant.Alternative}
          disabled={isFalse(shouldSendQuery) || isFetching}
          onClick={e => {
            e.stopPropagation()

            sendQuery()
          }}
        >
          Run Query
        </Button>
      </InspectorActions>
    </>
  )
}

function FormActionsModel({
  tab,
  model,
}: {
  tab: EditorTab
  model: ModelSQLMeshModel
}): JSX.Element {
  const setPreviewQuery = useStoreEditor(s => s.setPreviewQuery)
  const setPreviewConsole = useStoreEditor(s => s.setPreviewConsole)
  const setPreviewTable = useStoreEditor(s => s.setPreviewTable)

  const [form, setForm] = useState<FormModel>({
    start: toDateFormat(toDate(Date.now() - DAY)),
    end: toDateFormat(new Date()),
    execution_time: toDateFormat(toDate(Date.now() - DAY)),
    limit: 1000,
  })

  const { refetch: getRender } = useApiRender(
    Object.assign(form, { model: model.name }),
  )
  const debouncedGetRender = debounceAsync(getRender, 1000, true)

  const { refetch: getEvaluate } = useApiEvaluate(
    Object.assign(form, { model: model.name }),
  )
  const debouncedGetEvaluate = debounceAsync(getEvaluate, 1000, true)

  const shouldEvaluate =
    tab.file.isSQLMeshModel && Object.values(form).every(Boolean)

  function evaluateModel(): void {
    setPreviewQuery(undefined)
    setPreviewConsole(undefined)
    setPreviewTable(undefined)

    debouncedGetRender({
      throwOnError: true,
    })
      .then(({ data }) => {
        setPreviewQuery(data?.sql)
      })
      .catch(error => {
        if (isCancelledError(error)) {
          console.log(
            'renderApiCommandsRenderPost',
            'Request aborted by React Query',
          )
        } else {
          setPreviewConsole([EnumErrorKey.RenderModel, error])
        }
      })

    debouncedGetEvaluate({
      throwOnError: true,
    })
      .then(({ data }) => {
        setPreviewTable(getTableDataFromArrowStreamResult(data as Table<any>))
      })
      .catch(error => {
        if (isCancelledError(error)) {
          console.log(
            'fetchdfApiCommandsFetchdfPost',
            'Request aborted by React Query',
          )
        } else {
          setPreviewConsole([EnumErrorKey.EvaluateModel, error])
        }
      })
  }

  return (
    <>
      <InspectorForm>
        <form className="w-full">
          {isFalse(shouldEvaluate) && (
            <FormFieldset>
              <Banner variant={EnumVariant.Warning}>
                <Banner.Description className="w-full mr-2 text-sm">
                  Please fill out all fields to <b>evaluate the model</b>.
                </Banner.Description>
              </Banner>
            </FormFieldset>
          )}
          <fieldset className="my-3 px-3 w-full">
            <Input
              className="w-full mx-0"
              label="Start Date"
            >
              {({ className }) => (
                <Input.Textfield
                  className={clsx(className, 'w-full')}
                  placeholder="02/11/2023"
                  value={form.start}
                  onInput={(e: React.ChangeEvent<HTMLInputElement>) => {
                    e.stopPropagation()

                    setForm({
                      ...form,
                      start: e.target.value ?? '',
                    })
                  }}
                />
              )}
            </Input>
            <Input
              className="w-full mx-0"
              label="End Date"
            >
              {({ className }) => (
                <Input.Textfield
                  className={clsx(className, 'w-full')}
                  placeholder="02/13/2023"
                  value={form.end}
                  onInput={(e: React.ChangeEvent<HTMLInputElement>) => {
                    e.stopPropagation()

                    setForm({
                      ...form,
                      end: e.target.value ?? '',
                    })
                  }}
                />
              )}
            </Input>
            <Input
              className="w-full mx-0"
              label="Execution Time"
            >
              {({ className }) => (
                <Input.Textfield
                  className={clsx(className, 'w-full')}
                  placeholder="02/13/2023"
                  value={form.execution_time}
                  onInput={(e: React.ChangeEvent<HTMLInputElement>) => {
                    e.stopPropagation()

                    setForm({
                      ...form,
                      execution_time: e.target.value ?? '',
                    })
                  }}
                />
              )}
            </Input>
            <Input
              className="w-full mx-0"
              label="Limit"
            >
              {({ className }) => (
                <Input.Textfield
                  className={clsx(className, 'w-full')}
                  type="number"
                  placeholder="1000"
                  value={form.limit}
                  onInput={(e: React.ChangeEvent<HTMLInputElement>) => {
                    e.stopPropagation()

                    setForm({
                      ...form,
                      limit: e.target.valueAsNumber ?? LIMIT,
                    })
                  }}
                />
              )}
            </Input>
          </fieldset>
        </form>
      </InspectorForm>
      <Divider />
      <InspectorActions>
        <div className="flex w-full justify-end">
          {tab.file.isSQLMeshModel && (
            <Button
              size={EnumSize.sm}
              variant={EnumVariant.Alternative}
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
      </InspectorActions>
    </>
  )
}

function FormDiffModel({
  tab,
  model,
  list,
  target,
}: {
  tab: EditorTab
  model: ModelSQLMeshModel
  list: Array<{ text: string; value: string }>
  target: { text: string; value: string }
}): JSX.Element {
  const setPreviewConsole = useStoreEditor(s => s.setPreviewConsole)
  const setPreviewDiff = useStoreEditor(s => s.setPreviewDiff)

  const [selectedSource, setSelectedSource] = useState(list[0]!.value)
  const [limit, setLimit] = useState(LIMIT_DIFF)
  const [on, setOn] = useState('')
  const [where, setWhere] = useState('')

  const { refetch: getDiff } = useApiTableDiff({
    source: selectedSource,
    target: target.value,
    model_or_snapshot: model.name,
    limit,
    on,
    where,
  })
  const debouncedGetDiff = debounceAsync(getDiff, 1000, true)

  useEffect(() => {
    setSelectedSource(list[0]!.value)
  }, [list])

  function getTableDiff(): void {
    setPreviewConsole(undefined)
    setPreviewDiff(undefined)

    debouncedGetDiff({
      throwOnError: true,
    })
      .then(({ data }) => {
        setPreviewDiff(data)
      })
      .catch(error => {
        if (isCancelledError(error)) {
          console.log(
            'renderApiCommandsRenderPost',
            'Request aborted by React Query',
          )
        } else {
          setPreviewConsole([EnumErrorKey.RenderModel, error])
        }
      })
  }

  const shouldEnableAction =
    tab.file.isSQLMeshModel && [selectedSource, target, limit].every(Boolean)

  return (
    <>
      <InspectorForm>
        <form className="w-full">
          <fieldset className="my-3 px-3 w-full">
            <Input
              className="w-full mx-0"
              label="Source"
              disabled={list.length < 2}
            >
              {({ disabled, className }) => (
                <Input.Selector
                  className={clsx(className, 'w-full')}
                  list={list}
                  value={selectedSource}
                  disabled={disabled}
                  onChange={setSelectedSource}
                />
              )}
            </Input>
            <Input
              className="w-full mx-0"
              label="Limit"
            >
              {({ className }) => (
                <Input.Textfield
                  className={clsx(className, 'w-full')}
                  type="number"
                  placeholder="1000"
                  value={limit}
                  onInput={(e: React.ChangeEvent<HTMLInputElement>) => {
                    e.stopPropagation()

                    setLimit(e.target.valueAsNumber ?? LIMIT_DIFF)
                  }}
                />
              )}
            </Input>
            <Input
              className="w-full mx-0"
              label="ON"
            >
              {({ className }) => (
                <Input.Textfield
                  className={clsx(className, 'w-full')}
                  placeholder="s.id = t.id"
                  value={on}
                  onInput={(e: React.ChangeEvent<HTMLInputElement>) => {
                    e.stopPropagation()

                    setOn(e.target.value)
                  }}
                />
              )}
            </Input>
            <Input
              className="w-full mx-0"
              label="WHERE"
            >
              {({ className }) => (
                <Input.Textfield
                  className={clsx(className, 'w-full')}
                  placeholder="id > 10"
                  value={where}
                  onInput={(e: React.ChangeEvent<HTMLInputElement>) => {
                    e.stopPropagation()

                    setWhere(e.target.value)
                  }}
                />
              )}
            </Input>
          </fieldset>
        </form>
      </InspectorForm>
      <Divider />
      <InspectorActions>
        <div className="flex w-full justify-between items-center px-2">
          <span className="text-xs text-neutral-400 font-medium">
            Compare current model using
            <span className="inline-block px-2 bg-brand-10 mx-1 text-brand-600 rounded-md">
              {target.value}
            </span>{' '}
            as <b>Target</b> and{' '}
            <span className="inline-block px-2 bg-brand-10 mx-1 text-brand-600 rounded-md">
              {selectedSource}
            </span>{' '}
            as <b>Source</b>
          </span>
          {tab.file.isSQLMeshModel && (
            <Button
              className="ml-2"
              size={EnumSize.sm}
              variant={EnumVariant.Alternative}
              disabled={isFalse(shouldEnableAction)}
              onClick={e => {
                e.stopPropagation()

                getTableDiff()
              }}
            >
              Get Diff
            </Button>
          )}
        </div>
      </InspectorActions>
    </>
  )
}

function FormDiff(): JSX.Element {
  const setPreviewConsole = useStoreEditor(s => s.setPreviewConsole)
  const setPreviewDiff = useStoreEditor(s => s.setPreviewDiff)

  const [source, setSource] = useState('')
  const [target, setTarget] = useState('')
  const [limit, setLimit] = useState(LIMIT_DIFF)
  const [on, setOn] = useState('')
  const [where, setWhere] = useState('')

  const { refetch: getDiff } = useApiTableDiff({
    source,
    target,
    limit,
    on,
    where,
  })
  const debouncedGetDiff = debounceAsync(getDiff, 1000, true)

  function getTableDiff(): void {
    setPreviewConsole(undefined)
    setPreviewDiff(undefined)

    debouncedGetDiff({
      throwOnError: true,
    })
      .then(({ data }) => {
        setPreviewDiff(data)
      })
      .catch(error => {
        if (isCancelledError(error)) {
          console.log(
            'renderApiCommandsRenderPost',
            'Request aborted by React Query',
          )
        } else {
          setPreviewConsole([EnumErrorKey.RenderModel, error])
        }
      })
  }

  const shouldEnableAction = [source, target, limit, on].every(Boolean)

  return (
    <>
      <InspectorForm>
        <form className="w-full">
          <fieldset className="my-3 px-3 w-full">
            <Input
              className="w-full mx-0"
              label="Source"
            >
              {({ className }) => (
                <Input.Textfield
                  className={clsx(className, 'w-full')}
                  placeholder="exp.tst_model__dev"
                  value={source}
                  onInput={(e: React.ChangeEvent<HTMLInputElement>) => {
                    e.stopPropagation()

                    setSource(e.target.value)
                  }}
                />
              )}
            </Input>
            <Input
              className="w-full mx-0"
              label="Target"
            >
              {({ className }) => (
                <Input.Textfield
                  className={clsx(className, 'w-full')}
                  placeholder="exp.tst_snapshot__1353336088"
                  value={target}
                  onInput={(e: React.ChangeEvent<HTMLInputElement>) => {
                    e.stopPropagation()

                    setTarget(e.target.value)
                  }}
                />
              )}
            </Input>
            <Input
              className="w-full mx-0"
              label="Limit"
            >
              {({ className }) => (
                <Input.Textfield
                  className={clsx(className, 'w-full')}
                  type="number"
                  placeholder="1000"
                  value={limit}
                  onInput={(e: React.ChangeEvent<HTMLInputElement>) => {
                    e.stopPropagation()

                    setLimit(e.target.valueAsNumber ?? LIMIT_DIFF)
                  }}
                />
              )}
            </Input>
            <Input
              className="w-full mx-0"
              label="ON"
            >
              {({ className }) => (
                <Input.Textfield
                  className={clsx(className, 'w-full')}
                  placeholder="s.id = t.id"
                  value={on}
                  onInput={(e: React.ChangeEvent<HTMLInputElement>) => {
                    e.stopPropagation()

                    setOn(e.target.value)
                  }}
                />
              )}
            </Input>
            <Input
              className="w-full mx-0"
              label="WHERE"
            >
              {({ className }) => (
                <Input.Textfield
                  className={clsx(className, 'w-full')}
                  placeholder="id > 10"
                  value={where}
                  onInput={(e: React.ChangeEvent<HTMLInputElement>) => {
                    e.stopPropagation()

                    setWhere(e.target.value)
                  }}
                />
              )}
            </Input>
          </fieldset>
        </form>
      </InspectorForm>
      <Divider />
      <InspectorActions>
        <Button
          className="ml-2"
          size={EnumSize.sm}
          variant={EnumVariant.Alternative}
          disabled={isFalse(shouldEnableAction)}
          onClick={e => {
            e.stopPropagation()

            getTableDiff()
          }}
        >
          Get Diff
        </Button>
      </InspectorActions>
    </>
  )
}
