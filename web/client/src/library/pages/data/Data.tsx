import { useStoreContext } from '@context/context'
import { useParams } from 'react-router-dom'
import { isNil, toDate, toDateFormat } from '@utils/index'
import { useEffect } from 'react'
import Table from '@components/table/Table'
import { useStoreEditor } from '@context/editor'
import { useApiEvaluate, useApiRender } from '@api/index'
import { getTableDataFromArrowStreamResult } from '@components/table/help'
import { type Table as ApacheArrowTable } from 'apache-arrow'

const DAY = 24 * 60 * 60 * 1000

export default function PageData(): JSX.Element {
  const { modelName } = useParams()

  const models = useStoreContext(s => s.models)
  const lastSelectedModel = useStoreContext(s => s.lastSelectedModel)

  const previewTable = useStoreEditor(s => s.previewTable)

  const setPreviewQuery = useStoreEditor(s => s.setPreviewQuery)
  const setPreviewTable = useStoreEditor(s => s.setPreviewTable)

  const model =
    isNil(modelName) || modelName === lastSelectedModel?.name
      ? lastSelectedModel
      : models.get(encodeURI(modelName))

  const form = {
    model: model!.displayName,
    start: toDateFormat(toDate(Date.now() - DAY)),
    end: toDateFormat(new Date()),
    execution_time: toDateFormat(toDate(Date.now() - DAY)),
    limit: 1000,
  }

  const { refetch: getRender } = useApiRender(form)
  const { refetch: getEvaluate } = useApiEvaluate(form)

  useEffect(() => {
    setPreviewQuery(undefined)
    setPreviewTable(undefined)

    void getRender().then(({ data }) => {
      setPreviewQuery(data?.sql)
    })

    void getEvaluate().then(({ data }) => {
      setPreviewTable(
        getTableDataFromArrowStreamResult(data as ApacheArrowTable),
      )
    })
  }, [model])

  return (
    <div className="flex overflow-hidden w-full h-full px-2">
      <Table
        headline={model?.displayName}
        data={previewTable ?? [[], []]}
      />
    </div>
  )
}
