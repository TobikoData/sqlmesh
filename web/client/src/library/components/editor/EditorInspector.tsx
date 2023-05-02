import { isCancelledError } from '@tanstack/react-query'
import { type Table } from 'apache-arrow'
import clsx from 'clsx'
import React, { useMemo, useState } from 'react'
import {
  type EvaluateInputStart,
  type RenderInputStart,
  type EvaluateInputEnd,
  type RenderInputEnd,
  type EvaluateInputLatest,
  type RenderInputLatest,
  fetchdfApiCommandsFetchdfPost,
  renderApiCommandsRenderPost,
  evaluateApiCommandsEvaluatePost,
} from '~/api/client'
import { type ResponseWithDetail } from '~/api/instance'
import { useStoreContext } from '~/context/context'
import { EnumSize, EnumVariant } from '~/types/enum'
import { isFalse, toDate, toDateFormat } from '~/utils'
import { Button } from '../button/Button'
import { Divider } from '../divider/Divider'
import Input from '../input/Input'
import { type EditorTab, useStoreEditor } from '~/context/editor'
import { getTableDataFromArrowStreamResult } from './help'
import { Tab } from '@headlessui/react'
import Documantation from '@components/documentation/Documantation'
import Banner from '@components/banner/Banner'

interface FormModel {
  model?: string
  start: EvaluateInputStart | RenderInputStart
  end: EvaluateInputEnd | RenderInputEnd
  latest: EvaluateInputLatest | RenderInputLatest
  limit: number
}

interface FormArbitrarySql {
  limit: number
}

const DAY = 24 * 60 * 60 * 1000
const LIMIT = 1000

export default function EditorInspector({
  tab,
}: {
  tab: EditorTab
}): JSX.Element {
  return (
    <div
      className={clsx(
        'flex flex-col w-full h-full items-center overflow-hidden',
      )}
    >
      {tab.file.isSQLMeshModel ? (
        <InspectorModel tab={tab} />
      ) : (
        <InspectorSql tab={tab} />
      )}
    </div>
  )
}

function InspectorModel({ tab }: { tab: EditorTab }): JSX.Element {
  const models = useStoreContext(s => s.models)

  const setPreviewQuery = useStoreEditor(s => s.setPreviewQuery)
  const setPreviewConsole = useStoreEditor(s => s.setPreviewConsole)
  const setPreviewTable = useStoreEditor(s => s.setPreviewTable)

  const model = useMemo(() => models.get(tab.file.path), [tab, models])

  const [form, setForm] = useState<FormModel>({
    start: toDateFormat(toDate(Date.now() - DAY)),
    end: toDateFormat(new Date()),
    latest: toDateFormat(toDate(Date.now() - DAY)),
    limit: 1000,
  })

  const shouldEvaluate =
    tab.file.isSQLMeshModel && Object.values(form).every(Boolean)

  function evaluateModel(): void {
    setPreviewQuery(tab.file.content)
    setPreviewConsole(undefined)
    setPreviewTable(undefined)

    if (model?.name != null) {
      renderApiCommandsRenderPost({
        ...form,
        model: model.name,
      })
        .then(({ sql }) => {
          setPreviewQuery(sql)
        })
        .catch(error => {
          if (isCancelledError(error)) {
            console.log(
              'renderApiCommandsRenderPost',
              'Request aborted by React Query',
            )
          } else {
            setPreviewConsole(error.message)
          }
        })

      evaluateApiCommandsEvaluatePost({
        ...form,
        model: model.name,
      })
        .then((result: ResponseWithDetail | Table<any>) => {
          setPreviewTable(
            getTableDataFromArrowStreamResult(result as Table<any>),
          )
        })
        .catch(error => {
          if (isCancelledError(error)) {
            console.log(
              'evaluateApiCommandsEvaluatePost',
              'Request aborted by React Query',
            )
          } else {
            setPreviewConsole(error.message)
          }
        })
    }
  }

  return (
    <Tab.Group>
      <Tab.List className="w-full whitespace-nowrap px-2 pt-3 flex justigy-between items-center">
        <Tab
          className={({ selected }) =>
            clsx(
              'inline-block text-sm font-medium px-3 py-1 mr-2 last-child:mr-0 rounded-md relative',
              selected
                ? 'bg-secondary-500 text-secondary-100 cursor-default'
                : 'bg-secondary-10 cursor-pointer',
            )
          }
        >
          Actions
        </Tab>
        <Tab
          className={({ selected }) =>
            clsx(
              'inline-block text-sm font-medium px-3 py-1 mr-2 last-child:mr-0 rounded-md relative',
              selected
                ? 'bg-secondary-500 text-secondary-100 cursor-default'
                : 'bg-secondary-10 cursor-pointer',
            )
          }
        >
          Docs
        </Tab>
      </Tab.List>
      <Tab.Panels className="h-full w-full overflow-hidden">
        <Tab.Panel
          className={clsx(
            'flex flex-col w-full h-full pt-4 relative px-2 overflow-hidden',
            'ring-white ring-opacity-60 ring-offset-2 ring-offset-blue-400 focus:outline-none focus:ring-2',
          )}
        >
          <InspectorForm>
            <form className="my-3">
              {isFalse(shouldEvaluate) && (
                <FormFieldset>
                  <Banner variant={EnumVariant.Warning}>
                    <Banner.Description className="w-full mr-2 text-sm">
                      Please fill out all fields to <b>evaluate the model</b>.
                    </Banner.Description>
                  </Banner>
                </FormFieldset>
              )}
              <fieldset className="my-3 px-3">
                <Input
                  className="w-full mx-0"
                  label="Start Date"
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
                <Input
                  className="w-full mx-0"
                  label="End Date"
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
                <Input
                  className="w-full mx-0"
                  label="Latest Date"
                  placeholder="02/13/2023"
                  value={form.latest}
                  onInput={(e: React.ChangeEvent<HTMLInputElement>) => {
                    e.stopPropagation()

                    setForm({
                      ...form,
                      latest: e.target.value ?? '',
                    })
                  }}
                />
                <Input
                  className="w-full mx-0"
                  type="number"
                  label="Limit"
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
              </fieldset>
            </form>
          </InspectorForm>
          <Divider />
          <InspectorActions>
            <div className="flex w-full justify-between">
              <div className="flex">
                <Button
                  size={EnumSize.sm}
                  variant={EnumVariant.Alternative}
                  disabled={isFalse(shouldEvaluate)}
                >
                  Validate
                </Button>
              </div>
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
        </Tab.Panel>
        <Tab.Panel
          className={clsx(
            'w-full h-full ring-white ring-opacity-60 ring-offset-2 ring-offset-blue-400 focus:outline-none focus:ring-2 p-2',
          )}
        >
          {model != null ? (
            <Documantation
              model={model}
              withCode={false}
            />
          ) : (
            <Documantation.NotFound />
          )}
        </Tab.Panel>
      </Tab.Panels>
    </Tab.Group>
  )
}

function InspectorSql({ tab }: { tab: EditorTab }): JSX.Element {
  const setPreviewQuery = useStoreEditor(s => s.setPreviewQuery)
  const setPreviewConsole = useStoreEditor(s => s.setPreviewConsole)
  const setPreviewTable = useStoreEditor(s => s.setPreviewTable)

  const [form, setForm] = useState<FormArbitrarySql>({
    limit: LIMIT,
  })

  const shouldSendQuery = Object.values(form).every(Boolean)

  function sendQuery(): void {
    setPreviewQuery(tab.file.content)
    setPreviewConsole(undefined)
    setPreviewTable(undefined)

    fetchdfApiCommandsFetchdfPost({
      sql: tab.file.content,
    })
      .then((result: ResponseWithDetail | Table<any>) => {
        setPreviewTable(getTableDataFromArrowStreamResult(result as Table<any>))
      })
      .catch(error => {
        if (isCancelledError(error)) {
          console.log(
            'fetchdfApiCommandsFetchdfPost',
            'Request aborted by React Query',
          )
        } else {
          setPreviewConsole(error.message)
        }
      })
  }

  return (
    <>
      <InspectorForm>
        <form className="my-3 w-full">
          {isFalse(shouldSendQuery) && (
            <FormFieldset>
              <Banner variant={EnumVariant.Warning}>
                <Banner.Description className="w-full mr-2 text-sm">
                  Please fill out all fields to <b>run the query</b>.
                </Banner.Description>
              </Banner>
            </FormFieldset>
          )}
          <fieldset className="mb-4">
            <Input
              className="w-full mx-0"
              type="number"
              label="Limit"
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
          </fieldset>
        </form>
      </InspectorForm>
      <Divider />
      <InspectorActions>
        <Button
          size={EnumSize.sm}
          variant={EnumVariant.Alternative}
          disabled={isFalse(shouldSendQuery)}
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

function FormFieldset({
  children,
}: {
  children: React.ReactNode
}): JSX.Element {
  return <fieldset className="flex my-3 px-3">{children}</fieldset>
}

function InspectorForm({
  children,
}: {
  children: React.ReactNode
}): JSX.Element {
  return (
    <div className="flex w-full h-full py-1 px-3 overflow-hidden overflow-y-auto scrollbar scrollbar--vertical">
      {children}
    </div>
  )
}

function InspectorActions({
  children,
}: {
  children: React.ReactNode
}): JSX.Element {
  return <div className="flex w-full py-1 px-2 justify-end">{children}</div>
}
