import {
  type LayoutedGraph,
  type LineageEdgeData,
  type LineageNodeData,
  type EdgeId,
  type NodeId,
  type PortId,
} from '../utils'
import { buildLayout } from '../layout/dagreLayout'

self.onmessage = <
  TNodeData extends LineageNodeData = LineageNodeData,
  TEdgeData extends LineageEdgeData = LineageEdgeData,
  TNodeID extends string = NodeId,
  TEdgeID extends string = EdgeId,
  TPortID extends string = PortId,
>(
  event: MessageEvent<
    LayoutedGraph<TNodeData, TEdgeData, TNodeID, TEdgeID, TPortID>
  >,
) => {
  try {
    const nodesMap = buildLayout<
      TNodeData,
      TEdgeData,
      TNodeID,
      TEdgeID,
      TPortID
    >(event.data)

    self.postMessage({
      edges: event.data.edges,
      nodesMap,
    } satisfies LayoutedGraph<TNodeData, TEdgeData, TNodeID, TEdgeID, TPortID>)
  } catch (error) {
    self.postMessage({ error })
  }
}
