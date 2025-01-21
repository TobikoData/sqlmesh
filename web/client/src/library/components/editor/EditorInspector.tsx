import { type Table } from 'apache-arrow'
import clsx from 'clsx'
import React, {
  useCallback,
  useEffect,
  useMemo,
  useState,
  type MouseEvent,
} from 'react'
import { type RenderInput, type EvaluateInput } from '~/api/client'
import { useStoreContext } from '~/context/context'
import { EnumSize, EnumVariant } from '~/types/enum'
import { isFalse, isNotNil, toDate, toDateFormat } from '~/utils'
import { Button } from '../button/Button'
import { Divider } from '../divider/Divider'
import Input from '../input/Input'
import { Bars3Icon } from '@heroicons/react/24/solid'
import { type EditorTab, useStoreEditor } from '~/context/editor'
import { Tab } from '@headlessui/react'
import Banner from '@components/banner/Banner'
import { type ModelSQLMeshModel } from '@models/sqlmesh-model'
import {
  useApiEvaluate,
  useApiFetchdf,
  useApiRender,
  useApiTableDiff,
} from '@api/index'
import TabList from '@components/tab/Tab'
import { getTableDataFromArrowStreamResult } from '@components/table/help'
import Spinner from '@components/logo/Spinner'
import ModelColumns from '@components/graph/ModelColumns'

const DAY = 24 * 60 * 60 * 1000
const LIMIT = 1000
const LIMIT_DIFF = 50

export default function EditorInspector({
  tab,
  isOpen = true,
  toggle,
}: {
  tab: EditorTab
  isOpen?: boolean
  toggle?: () => void
}): JSX.Element {
  const models = useStoreContext(s => s.models)
  const isModel = useStoreContext(s => s.isModel)

  const model = useMemo(() => models.get(tab.file.path), [tab, models])

  return (
    <div
      className={clsx(
        'flex flex-col w-full h-full items-center overflow-hidden',
      )}
    >
      {isModel(tab.file.path) ? (
        isNotNil(model) && (
          <InspectorModel
            tab={tab}
            model={model}
            isOpen={isOpen}
            toggle={toggle}
          />
        )
      ) : (
        <InspectorSql
          tab={tab}
          isOpen={isOpen}
          toggle={toggle}
        />
      )}
    </div>
  )
}

function InspectorModel({
  tab,
  model,
  isOpen = true,
  toggle,
}: {
  tab: EditorTab
  model: ModelSQLMeshModel
  isOpen?: boolean
  toggle?: () => void
}): JSX.Element {
  const environment = useStoreContext(s => s.environment)
  const environments = useStoreContext(s => s.environments)
  const list = Array.from(environments)
    .filter(({ isRemote }) => isRemote)
    .map(({ name }) => ({ text: name, value: name }))

  return (
    <Tab.Group>
      <div className="flex w-full items-center">
        <Button
          className={clsx(
            'h-6 w-6 !px-0 border-none bg-neutral-10 dark:bg-neutral-20',
            isOpen
              ? 'text-secondary-500 dark:text-secondary-300'
              : 'text-neutral-500 dark:text-neutral-300',
          )}
          variant={EnumVariant.Info}
          size={EnumSize.sm}
          onClick={(e: MouseEvent) => {
            e.stopPropagation()

            toggle?.()
          }}
        >
          <Bars3Icon className="w-4 h-4" />
        </Button>
        {isOpen && (
          <TabList
            className="flex justify-center items-center"
            list={
              [
                'Evaluate',
                'Columns',
                list.length > 1 && environment.isRemote && 'Diff',
              ].filter(Boolean) as string[]
            }
          />
        )}
      </div>
      {isOpen && (
        <Tab.Panels className="h-full w-full overflow-hidden">
          <Tab.Panel
            unmount={false}
            className={clsx(
              'flex flex-col w-full h-full relative overflow-hidden',
              'ring-white ring-opacity-60 ring-offset-2 ring-offset-blue-400 focus:outline-none focus:ring-2',
            )}
          >
            <FormActionsModel model={model} />
          </Tab.Panel>
          <Tab.Panel
            unmount={false}
            className="text-xs w-full h-full ring-white ring-opacity-60 ring-offset-2 ring-offset-blue-400 focus:outline-none focus:ring-2 px-2"
          >
            <ModelColumns
              nodeId={model.fqn}
              columns={model.columns}
              disabled={isFalse(model.isModelSQL)}
              withHandles={false}
              withSource={false}
              withDescription={true}
              limit={10}
            />
          </Tab.Panel>
          {list.length > 1 && environment.isRemote && (
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
      )}
    </Tab.Group>
  )
}

function InspectorSql({
  tab,
  isOpen = true,
  toggle,
}: {
  tab: EditorTab
  isOpen?: boolean
  toggle?: () => void
}): JSX.Element {
  return (
    <Tab.Group>
      <div className="flex w-full items-center">
        <Button
          className={clsx(
            'h-6 w-6 !px-0 border-none bg-neutral-10 dark:bg-neutral-20',
            isOpen
              ? 'text-secondary-500 dark:text-secondary-300'
              : 'text-neutral-500 dark:text-neutral-300',
          )}
          variant={EnumVariant.Info}
          size={EnumSize.sm}
          onClick={(e: MouseEvent) => {
            e.stopPropagation()

            toggle?.()
          }}
        >
          <Bars3Icon className="w-4 h-4" />
        </Button>
        {isOpen && (
          <TabList
            className="flex justify-center items-center"
            list={['Run Query', 'Diff']}
          />
        )}
      </div>
      {isOpen && (
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
      )}
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
  children?: React.ReactNode
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
  const setPreviewTable = useStoreEditor(s => s.setPreviewTable)
  const engine = useStoreEditor(s => s.engine)

  const {
    refetch: getFetchdf,
    isFetching,
    cancel: cancelRunQuery,
  } = useApiFetchdf({
    sql: tab.file.content,
  })

  useEffect(() => {
    return () => {
      cancelRunQuery()
    }
  }, [])

  function sendQuery(): void {
    setPreviewTable(undefined)
    setPreviewQuery(tab.file.content)

    void getFetchdf().then(({ data }) => {
      setPreviewTable(getTableDataFromArrowStreamResult(data as Table<any>))
    })
  }

  return (
    <>
      <InspectorForm />
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
                dialect: tab.dialect,
              },
            })
          }}
        >
          Format
        </Button>
        {isFetching ? (
          <div className="flex items-center">
            <Spinner className="w-3" />
            <small className="text-xs text-neutral-400 block mx-2">
              Running Query...
            </small>
            <Button
              size={EnumSize.sm}
              variant={EnumVariant.Danger}
              onClick={e => {
                e.stopPropagation()

                cancelRunQuery()
              }}
            >
              Cancel
            </Button>
          </div>
        ) : (
          <Button
            size={EnumSize.sm}
            variant={EnumVariant.Alternative}
            disabled={isFetching}
            onClick={e => {
              e.stopPropagation()

              sendQuery()
            }}
          >
            Run Query
          </Button>
        )}
      </InspectorActions>
    </>
  )
}

function FormActionsModel({
  model,
}: {
  model: ModelSQLMeshModel
}): JSX.Element {
  const environment = useStoreContext(s => s.environment)
  const isModel = useStoreContext(s => s.isModel)

  const setPreviewQuery = useStoreEditor(s => s.setPreviewQuery)
  const setPreviewTable = useStoreEditor(s => s.setPreviewTable)

  const [form, setForm] = useState<EvaluateInput | RenderInput>({
    model: model.displayName,
    start: toDateFormat(toDate(Date.now() - DAY)),
    end: toDateFormat(new Date()),
    execution_time: toDateFormat(toDate(Date.now() - DAY)),
    limit: 1000,
  })

  const { refetch: getRender } = useApiRender({
    model: form.model,
    start: form.start,
    end: form.end,
    execution_time: form.execution_time,
    dialect: model.dialect,
    pretty: true,
  })
  const {
    refetch: getEvaluate,
    isFetching,
    cancel: cancelEvaluate,
  } = useApiEvaluate(form as EvaluateInput)

  const shouldEvaluate =
    isModel(model.path) && Object.values(form).every(Boolean)

  useEffect(() => {
    return () => {
      cancelEvaluate()
    }
  }, [])

  function evaluateModel(): void {
    setPreviewQuery(undefined)
    setPreviewTable(undefined)

    void getRender().then(({ data }) => {
      setPreviewQuery(data?.sql)
    })

    void getEvaluate().then(({ data }) => {
      setPreviewTable(getTableDataFromArrowStreamResult(data as Table<any>))
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
          <fieldset className="px-2 w-full text-neutral-500">
            <Input
              className="w-full mx-0"
              label="Start Date"
              size={EnumSize.sm}
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
              size={EnumSize.sm}
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
              size={EnumSize.sm}
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
            {isNotNil((form as EvaluateInput).limit) && (
              <Input
                className="w-full mx-0"
                label="Limit"
                size={EnumSize.sm}
              >
                {({ className }) => (
                  <Input.Textfield
                    className={clsx(className, 'w-full')}
                    type="number"
                    placeholder="1000"
                    value={(form as EvaluateInput).limit}
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
            )}
          </fieldset>
        </form>
      </InspectorForm>
      <Divider />
      <InspectorActions>
        <div className="flex w-full justify-end">
          {isModel(model.path) && isFetching ? (
            <div className="flex items-center">
              <Spinner className="w-3" />
              <small className="text-xs text-neutral-400 block mx-2">
                Evaluating...
              </small>
              <Button
                size={EnumSize.sm}
                variant={EnumVariant.Danger}
                onClick={e => {
                  e.stopPropagation()

                  cancelEvaluate()
                }}
              >
                Cancel
              </Button>
            </div>
          ) : (
            <Button
              size={EnumSize.sm}
              variant={EnumVariant.Alternative}
              disabled={
                isFalse(shouldEvaluate) ||
                isFetching ||
                environment.isInitialProd
              }
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
  const isModel = useStoreContext(s => s.isModel)

  const setPreviewDiff = useStoreEditor(s => s.setPreviewDiff)

  const [selectedSource, setSelectedSource] = useState(list[0]!.value)
  const [limit, setLimit] = useState(LIMIT_DIFF)
  const [on, setOn] = useState('')
  const [where, setWhere] = useState('')

  const {
    refetch: getDiff,
    isFetching,
    cancel: cancelGetDiff,
  } = useApiTableDiff({
    source: selectedSource,
    target: target.value,
    model_or_snapshot: model.name,
    limit,
    on,
    where,
  })

  const getTableDiff = useCallback(() => {
    setPreviewDiff(undefined)

    void getDiff().then(({ data }) => {
      setPreviewDiff(data)
    })
  }, [model.name, model.hash])

  useEffect(() => {
    return () => {
      cancelGetDiff()
    }
  }, [])

  useEffect(() => {
    setSelectedSource(list[0]!.value)
  }, [list])

  const shouldEnableAction =
    isModel(tab.file.path) && [selectedSource, target, limit].every(Boolean)

  return (
    <>
      <InspectorForm>
        <form className="w-full">
          <fieldset className="px-2 w-full text-neutral-500">
            <Input
              className="w-full mx-0"
              label="Source"
              disabled={list.length < 2}
              size={EnumSize.sm}
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
              label="Target"
              disabled={true}
            >
              {({ disabled, className }) => (
                <Input.Textfield
                  className={clsx(className, 'w-full')}
                  disabled={disabled}
                  value={target.value}
                />
              )}
            </Input>
            <Input
              className="w-full mx-0"
              label="Limit"
              size={EnumSize.sm}
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
              size={EnumSize.sm}
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
              size={EnumSize.sm}
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
        <div className="flex w-full justify-end items-center px-2">
          {isFetching ? (
            <div className="flex items-center">
              <Spinner className="w-3" />
              <small className="text-xs text-neutral-400 block mx-2">
                Getting Diff...
              </small>
              <Button
                size={EnumSize.sm}
                variant={EnumVariant.Danger}
                onClick={e => {
                  e.stopPropagation()

                  cancelGetDiff()
                }}
              >
                Cancel
              </Button>
            </div>
          ) : (
            <Button
              className="ml-2"
              size={EnumSize.sm}
              variant={EnumVariant.Alternative}
              disabled={isFalse(shouldEnableAction) || isFetching}
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
  const setPreviewDiff = useStoreEditor(s => s.setPreviewDiff)

  const [source, setSource] = useState('')
  const [target, setTarget] = useState('')
  const [limit, setLimit] = useState(LIMIT_DIFF)
  const [on, setOn] = useState('')
  const [where, setWhere] = useState('')

  const {
    refetch: getDiff,
    isFetching,
    cancel: cancelGetDiff,
  } = useApiTableDiff({
    source,
    target,
    limit,
    on,
    where,
  })

  useEffect(() => {
    return () => {
      cancelGetDiff()
    }
  }, [])

  function getTableDiff(): void {
    setPreviewDiff(undefined)

    void getDiff().then(({ data }) => {
      setPreviewDiff(data)
    })
  }

  const shouldEnableAction = [source, target, limit, on].every(Boolean)

  return (
    <>
      <InspectorForm>
        <form className="w-full">
          <fieldset className="px-2 w-full text-neutral-500">
            <Input
              className="w-full mx-0"
              label="Source"
              size={EnumSize.sm}
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
              size={EnumSize.sm}
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
              size={EnumSize.sm}
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
              size={EnumSize.sm}
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
              size={EnumSize.sm}
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
        {isFetching ? (
          <div className="flex items-center">
            <Spinner className="w-3" />
            <small className="text-xs text-neutral-400 block mx-2">
              Getting Diff...
            </small>
            <Button
              size={EnumSize.sm}
              variant={EnumVariant.Danger}
              onClick={e => {
                e.stopPropagation()

                cancelGetDiff()
              }}
            >
              Cancel
            </Button>
          </div>
        ) : (
          <Button
            className="ml-2"
            size={EnumSize.sm}
            variant={EnumVariant.Alternative}
            disabled={isFalse(shouldEnableAction) || isFetching}
            onClick={e => {
              e.stopPropagation()

              getTableDiff()
            }}
          >
            Get Diff
          </Button>
        )}
      </InspectorActions>
    </>
  )
}
