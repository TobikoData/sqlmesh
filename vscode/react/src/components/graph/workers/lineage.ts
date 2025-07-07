import { isFalse, isNil } from '@/utils/index'
import { type Lineage } from '@/domain/lineage'
import type { ModelEncodedFQN } from '@/domain/models'
import {
  toID,
  type NodeId,
  type LineageWorkerMessage,
  type LineageWorkerRequestMessage,
  type LineageWorkerResponseMessage,
  type LineageWorkerErrorMessage,
  type ConnectedNode,
} from '@/components/graph/types'
import type { Direction } from '../types'

interface WorkerScope {
  onmessage: ((e: MessageEvent<LineageWorkerMessage>) => void) | null
  postMessage: (message: LineageWorkerMessage) => void
}

const scope = self as unknown as WorkerScope

scope.onmessage = async (e: MessageEvent<LineageWorkerMessage>) => {
  if (e.data.topic === 'lineage') {
    try {
      const message = e.data as LineageWorkerRequestMessage
      const { currentLineage, newLineage, mainNode } = message.payload
      const lineage = await mergeLineageWithModels(currentLineage, newLineage)
      const nodesConnections = await getNodesConnections(mainNode, lineage)

      const responseMessage: LineageWorkerResponseMessage = {
        topic: 'lineage',
        payload: {
          lineage,
          nodesConnections,
        },
      }
      scope.postMessage(responseMessage)
    } catch (error) {
      const errorMessage: LineageWorkerErrorMessage = {
        topic: 'error',
        error: error as Error,
      }
      scope.postMessage(errorMessage)
    }
  }
}

async function mergeLineageWithModels(
  currentLineage: Record<string, Lineage> = {},
  data: Record<string, string[]> = {},
): Promise<Record<string, Lineage>> {
  return Object.entries(data).reduce(
    (acc: Record<string, Lineage>, [key, models = []]) => {
      key = encodeURI(key)

      acc[key] = {
        models: models.map(encodeURI) as ModelEncodedFQN[],
        columns: currentLineage?.[key]?.columns ?? undefined,
      }

      return acc
    },
    {},
  )
}

async function getNodesConnections(
  mainNode: string,
  lineage: Record<string, Lineage> = {},
): Promise<Record<string, ConnectedNode>> {
  return new Promise((resolve, reject) => {
    if (isNil(lineage) || isNil(mainNode)) return {}

    const distances: Record<string, ConnectedNode> = {}

    try {
      getConnectedNodes('upstream', mainNode, lineage, distances)
      getConnectedNodes('downstream', mainNode, lineage, distances)
    } catch (error) {
      reject(error)
    }

    resolve(distances)
  })
}

function getConnectedNodes(
  direction: Direction = 'downstream',
  node: string,
  lineage: Record<string, Lineage> = {},
  result: Record<string, ConnectedNode> = {},
): void {
  const isDownstream = direction === 'downstream'
  let models: string[] = []

  if (isDownstream) {
    models = Object.keys(lineage).filter(key =>
      lineage[key]!.models.includes(node as ModelEncodedFQN),
    )
  } else {
    models = lineage[node]?.models ?? []
  }

  if (isFalse(node in result)) {
    result[node] = { edges: [] }
  }

  for (const model of models) {
    const connectedNode = isDownstream
      ? createConnectedNode(node, model, [result[node]!])
      : createConnectedNode(model, node, [result[node]!])

    if (model in result) {
      result[model]!.edges.push(connectedNode)
    } else {
      result[model] = connectedNode
      getConnectedNodes(direction, model, lineage, result)
    }
  }
}

function createConnectedNode(
  source: NodeId,
  target: NodeId,
  edges: ConnectedNode[] = [],
): ConnectedNode {
  const id = toID(source, target)
  return { id, edges }
}
