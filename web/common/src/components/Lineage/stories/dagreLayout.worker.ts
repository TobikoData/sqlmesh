import {
  type LayoutedGraph,
  type LineageEdgeData,
  type LineageNodeData,
} from '../utils'
import { buildLayout } from '../layout/dagreLayout'

self.onmessage = <
  TNodeData extends LineageNodeData = LineageNodeData,
  TEdgeData extends LineageEdgeData = LineageEdgeData,
>(
  event: MessageEvent<LayoutedGraph<TNodeData, TEdgeData>>,
) => {
  try {
    const { edges, nodesMap } = buildLayout(event.data)

    self.postMessage({
      edges,
      nodesMap,
    } as LayoutedGraph<TNodeData, TEdgeData>)
  } catch (outerError) {
    self.postMessage({ error: outerError } as { error: ErrorEvent })
  }
}
