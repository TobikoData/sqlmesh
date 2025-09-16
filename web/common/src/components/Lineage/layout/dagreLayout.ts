import {
  type LayoutedGraph,
  type LineageEdge,
  type LineageEdgeData,
  type LineageNodeData,
  type LineageNodesMap,
} from '../utils'

const DEFAULT_TIMEOUT = 1000 * 60 // 1 minute

let workerInstance: Worker | null = null

function getWorker(): Worker {
  if (workerInstance) return workerInstance

  workerInstance = new Worker(
    new URL('./dagreLayout.worker.ts', import.meta.url),
    { type: 'module' },
  )

  return workerInstance
}

export async function getLayoutedGraph<
  TNodeData extends LineageNodeData = LineageNodeData,
  TEdgeData extends LineageEdgeData = LineageEdgeData,
>(
  edges: LineageEdge<TEdgeData>[],
  nodesMap: LineageNodesMap<TNodeData>,
): Promise<LayoutedGraph<TNodeData, TEdgeData>> {
  let timeoutId: NodeJS.Timeout | null = null

  return new Promise((resolve, reject) => {
    const nodes = Object.values(nodesMap)

    if (nodes.length === 0) return resolve({ edges: [], nodesMap: {} })

    const worker = getWorker()

    if (worker == null)
      return errorHandler(new ErrorEvent('Failed to create worker'))

    timeoutId = setTimeout(
      () => errorHandler(new ErrorEvent('Layout calculation timed out')),
      DEFAULT_TIMEOUT,
    )

    worker.addEventListener('message', handler)
    worker.addEventListener('error', errorHandler)

    try {
      worker.postMessage({ edges, nodesMap } as LayoutedGraph<
        TNodeData,
        TEdgeData
      >)
    } catch (postError) {
      errorHandler(postError as ErrorEvent)
    }

    function handler(
      event: MessageEvent<
        LayoutedGraph<TNodeData, TEdgeData> & { error: ErrorEvent }
      >,
    ) {
      cleanup()

      if (event.data.error) return errorHandler(event.data.error)

      resolve(event.data)
    }

    function errorHandler(error: ErrorEvent) {
      cleanup()
      reject(error)
    }

    function cleanup() {
      if (timeoutId) {
        clearTimeout(timeoutId)
        timeoutId = null
      }
      worker?.removeEventListener('message', handler)
      worker?.removeEventListener('error', errorHandler)
    }
  })
}

export function cleanupLayoutWorker(): void {
  workerInstance?.terminate()
  workerInstance = null
}
