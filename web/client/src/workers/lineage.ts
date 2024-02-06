import { type Lineage } from '@context/editor'
import { isFalse, isNil, isStringEmptyOrNil, toID } from '@utils/index'

export interface ConnectedNode {
  id?: Optional<string>
  edges: ConnectedNode[]
}

const EnumDirection = {
  Upstream: 'upstream',
  Downstream: 'downstream',
} as const

type Direction = (typeof EnumDirection)[keyof typeof EnumDirection]

const scope = self as any

scope.onmessage = async (e: MessageEvent) => {
  if (e.data.topic === 'lineage') {
    try {
      const { currentLineage, newLineage, mainNode } = e.data.payload
      const lineage = await mergeLineageWithModels(currentLineage, newLineage)
      const nodesConnections = await getNodesConnections(mainNode, lineage)

      scope.postMessage({
        topic: 'lineage',
        payload: {
          lineage,
          nodesConnections,
        },
      })
    } catch (error) {
      scope.postMessage({
        topic: 'error',
        error,
      })
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
        models: models.map(encodeURI),
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
      getConnectedNodes(EnumDirection.Upstream, mainNode, lineage, distances)
      getConnectedNodes(EnumDirection.Downstream, mainNode, lineage, distances)
    } catch (error) {
      reject(error)
    }

    resolve(distances)
  })
}

function getConnectedNodes(
  direction: Direction = EnumDirection.Downstream,
  node: string,
  lineage: Record<string, Lineage> = {},
  result: Record<string, ConnectedNode> = {},
): void {
  const isDownstream = direction === EnumDirection.Downstream
  let models: string[] = []

  if (isDownstream) {
    models = Object.keys(lineage).filter(key =>
      lineage[key]!.models.includes(node),
    )
  } else {
    models = lineage[node]?.models ?? []
  }

  if (isFalse(node in result)) {
    result[node] = createConnectedNode()
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
  source?: string,
  target?: string,
  edges: ConnectedNode[] = [],
): ConnectedNode {
  const id = toID(source, target)

  return {
    id: isStringEmptyOrNil(id) ? undefined : id,
    edges,
  }
}
