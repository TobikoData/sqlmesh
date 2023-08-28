import { useApiModelLineage } from '@api/index'
import { useEffect } from 'react'
import { ModelColumnLineage } from './Graph'
import { type ModelSQLMeshModel } from '@models/sqlmesh-model'
import { type HighlightedNodes, useLineageFlow } from './context'
import { mergeLineageWithModels } from './help'
import { ReactFlowProvider } from 'reactflow'
import { isNil, isStringEmptyOrNil } from '@utils/index'
import Loading from '@components/loading/Loading'
import Spinner from '@components/logo/Spinner'

export default function ModelLineage({
  model,
  fingerprint,
  highlightedNodes = {},
  className,
}: {
  model: ModelSQLMeshModel
  fingerprint: string | ID
  highlightedNodes?: HighlightedNodes
  className?: string
}): JSX.Element {
  const {
    setActiveEdges,
    setConnections,
    setLineage,
    handleError,
    setSelectedNodes,
    setMainNode,
    setHighlightedNodes,
  } = useLineageFlow()

  const {
    refetch: getModelLineage,
    isFetching,
    cancel,
  } = useApiModelLineage(model.name)

  useEffect(() => {
    if (isStringEmptyOrNil(fingerprint)) return

    void getModelLineage()
      .then(({ data }) => {
        setLineage(() =>
          isNil(data) ? undefined : mergeLineageWithModels({}, data),
        )
      })
      .catch(error => {
        handleError?.(error)
      })
      .finally(() => {
        setActiveEdges(new Map())
        setConnections(new Map())
        setSelectedNodes(new Set())
        setMainNode(model.name)
        setHighlightedNodes(highlightedNodes)
      })

    return () => {
      void cancel?.()
    }
  }, [fingerprint])

  return (
    <ReactFlowProvider>
      {isFetching && (
        <div className="w-full h-full bg-theme flex justify-center items-center">
          <Loading className="inline-block">
            <Spinner className="w-3 h-3 border border-neutral-10 mr-4" />
            <h3 className="text-md">Loading Model&#39;s Lineage...</h3>
          </Loading>
        </div>
      )}
      <ModelColumnLineage className={className} />
    </ReactFlowProvider>
  )
}
