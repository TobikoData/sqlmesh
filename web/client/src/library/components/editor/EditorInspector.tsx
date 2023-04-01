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
import { useStoreEditor } from '~/context/editor'
import { getTableDataFromArrowStreamResult } from './help'

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

export default function EditorInspector(): JSX.Element {
  const tab = useStoreEditor(s => s.tab)

  return (
    <div
      className={clsx(
        'flex flex-col w-full h-full items-center overflow-hidden',
      )}
    >
      {tab.file.isSQLMeshModel ? <InspectorModel /> : <InspectorSql />}
    </div>
  )
}

function InspectorModel(): JSX.Element {
  const models = useStoreContext(s => s.models)

  const tab = useStoreEditor(s => s.tab)
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
    <>
      <InspectorForm>
        <form className="my-3">
          {model != null && (
            <FormFieldset>
              <ModelName modelName={model.name} />
            </FormFieldset>
          )}
          {isFalse(shouldEvaluate) && (
            <FormFieldset>
              <Banner action="evaluate the model" />
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
    </>
  )
}

function InspectorSql(): JSX.Element {
  const tab = useStoreEditor(s => s.tab)
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
              <Banner action="run the query" />
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

function Banner({ action }: { action: string }): JSX.Element {
  return (
    <div className="p-4 bg-warning-10 text-warning-600 rounded-lg">
      <p className="text-sm">
        Please fill out all fields to <b>{action}</b>.
      </p>
    </div>
  )
}

function ModelName({ modelName }: { modelName: string }): JSX.Element {
  return (
    <div className="text-sm font-bold whitespace-nowrap">
      <h3 className="ml-2">Model Name</h3>
      <p className="mt-1 px-2 py-1 bg-secondary-10 text-secondary-500 dark:text-primary-500 dark:bg-primary-10 text-xs rounded">
        {modelName}
      </p>
    </div>
  )
}
