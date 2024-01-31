import { isFalse, isNil, isNotNil, truncate } from '@utils/index'
import { useLineageFlow } from './context'
import { useReactFlow, type Node } from 'reactflow'
import { EnumLineageNodeModelType } from './Graph'
import { Button, EnumButtonFormat } from '@components/button/Button'
import { EnumSize, EnumVariant } from '~/types/enum'

export default function ModelLineageDetails({
  nodes = [],
}: {
  nodes: Node[]
}): JSX.Element {
  const { setCenter } = useReactFlow()
  const {
    activeNodes,
    models,
    mainNode,
    nodesMap,
    selectedNodes,
    setSelectedNodes,
    withImpacted,
    connectedNodes,
  } = useLineageFlow()

  const model = isNil(mainNode) ? undefined : models.get(mainNode)
  const countActive =
    activeNodes.size > 0 ? activeNodes.size : connectedNodes.size
  const countSelected = selectedNodes.size
  const countUpstreamDownstream = connectedNodes.size - 1
  const countHidden = nodes.filter(n => n.hidden).length
  const countSources = nodes.filter(
    n =>
      isFalse(n.hidden) &&
      (n.data.type === EnumLineageNodeModelType.external ||
        n.data.type === EnumLineageNodeModelType.seed),
  ).length
  const countCTEs = nodes.filter(
    n => isFalse(n.hidden) && n.data.type === EnumLineageNodeModelType.cte,
  ).length
  const showActive =
    countActive > 0 && countActive !== countUpstreamDownstream + 1

  function handleCenter(): void {
    if (isNil(mainNode)) return

    const node = nodesMap[mainNode]

    if (isNil(node)) return

    setTimeout(() => {
      setCenter(node.position.x, node.position.y, {
        zoom: 0.5,
        duration: 0,
      })
    }, 200)
  }

  return (
    <>
      {isNotNil(model) && (
        <a
          className="mr-2 w-full whitespace-nowrap text-ellipsis overflow-hidden @lg:block font-bold text-neutral-600 dark:text-neutral-400 cursor-pointer hover:underline"
          onClick={handleCenter}
        >
          {truncate(model.displayName, 50, 25)}
        </a>
      )}
      <span className="bg-neutral-5 px-2 py-0.5 flex rounded-full mr-2">
        <span className="mr-2 whitespace-nowrap block">
          <b>All:</b> {nodes.length}
        </span>
        {showActive && (
          <span className="mr-2 whitespace-nowrap block">
            <b>Active:</b> {countActive}
          </span>
        )}
        {countHidden > 0 && (
          <span className="whitespace-nowrap block">
            <b>Hidden:</b> {countHidden}
          </span>
        )}
      </span>
      {countSources > 0 && (
        <span className="mr-2 whitespace-nowrap block">
          <b>Sources</b>: {countSources}
        </span>
      )}
      {isFalse(showActive) &&
        withImpacted &&
        countSelected === 0 &&
        countUpstreamDownstream > 0 && (
          <span className="mr-2 whitespace-nowrap block">
            <b>Upstream/Downstream:</b> {countUpstreamDownstream}
          </span>
        )}
      {countCTEs > 0 && (
        <span className="mr-2 whitespace-nowrap block">
          <b>CTEs:</b> {countCTEs}
        </span>
      )}
      {countSelected > 0 && (
        <span className="bg-neutral-5 px-2 py-0.5 flex rounded-full items-center">
          <span className="mr-2 whitespace-nowrap block">
            <b>Selected:</b> {countSelected}
          </span>
          <Button
            size={EnumSize.xs}
            variant={EnumVariant.Neutral}
            format={EnumButtonFormat.Ghost}
            className="!m-0 px-1"
            onClick={() => setSelectedNodes(new Set())}
          >
            Clear
          </Button>
        </span>
      )}
    </>
  )
}
