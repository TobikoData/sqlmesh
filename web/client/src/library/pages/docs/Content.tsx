import { useApiModelLineage } from '@api/index'
import Documantation from '@components/documentation/Documantation'
import Graph from '@components/graph/Graph'
import Loading from '@components/loading/Loading'
import SplitPane from '@components/splitPane/SplitPane'
import { useStoreContext } from '@context/context'
import { type Lineage, useStoreEditor } from '@context/editor'
import { debounceAsync } from '@utils/index'
import { useCallback, useMemo, useEffect } from 'react'
import { useLocation } from 'react-router-dom'

export default function Content(): JSX.Element {
  const models = useStoreContext(s => s.models)
  const location = useLocation()
  const searchParams = new URLSearchParams(location.search)
  const modelName = searchParams.get('model')
  const model = modelName != null ? models.get(modelName) : undefined

  return model == null ? (
    <div>
      <Documantation.NotFound />
    </div>
  ) : (
    <div className="flex overflow-auto w-full h-full">
      <SplitPane
        className="flex h-full w-full"
        sizes={[50, 50]}
        minSize={0}
        snapOffset={0}
      >
        <div className="flex flex-col h-full bg-theme-darker dark:bg-theme-lighter round">
          <Documantation
            key={model.name}
            model={model}
            withQuery={model.details.type !== 'python'}
          />
        </div>
        <div className="flex flex-col h-full px-2">
          <ModelLineage model={model.name} />
        </div>
      </SplitPane>
    </div>
  )
}

function ModelLineage({ model }: { model: string }): JSX.Element {
  const { data: lineage, refetch: getModelLineage } = useApiModelLineage(model)

  const models = useStoreContext(s => s.models)

  const previewLineage = useStoreEditor(s => s.previewLineage)
  const setPreviewLineage = useStoreEditor(s => s.setPreviewLineage)

  const debouncedGetModelLineage = useCallback(
    debounceAsync(getModelLineage, 1000, true),
    [model],
  )

  const highlightedNodes = useMemo(() => [model], [model])

  useEffect(() => {
    void debouncedGetModelLineage()
  }, [debouncedGetModelLineage])

  useEffect(() => {
    if (lineage == null) {
      setPreviewLineage(models)
    } else {
      setPreviewLineage(
        models,
        Object.keys(lineage).reduce((acc: Record<string, Lineage>, key) => {
          acc[key] = {
            models: lineage[key] ?? [],
            columns: previewLineage?.[key]?.columns ?? undefined,
          }

          return acc
        }, {}),
      )
    }
  }, [lineage])

  return previewLineage == null ? (
    <div className="w-full h-full flex items-center justify-center bg-primary-10">
      <Loading hasSpinner>Loading Lineage...</Loading>
    </div>
  ) : (
    <Graph
      lineage={previewLineage}
      highlightedNodes={highlightedNodes}
      models={models}
    />
  )
}
