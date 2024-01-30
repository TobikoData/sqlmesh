import { isFalse, isNil, isNotNil, truncate } from '@utils/index'
import { useLineageFlow } from './context'
import { type Node } from 'reactflow'
import { EnumLineageNodeModelType } from './Graph'

export default function ModelLineageDetails({
  nodes = [],
}: {
  nodes: Node[]
}): JSX.Element {
  const { models, mainNode, selectedNodes, withImpacted, connectedNodes } =
    useLineageFlow()

  const model = isNil(mainNode) ? undefined : models.get(mainNode)

  const countSelected = selectedNodes.size
  const countImpact = connectedNodes.size - 1
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
  return (
    <>
      {isNotNil(model) && (
        <span
          title={model.displayName}
          className="mr-2 w-full whitespace-nowrap text-ellipsis overflow-hidden @lg:block font-bold text-neutral-600 dark:text-neutral-400"
        >
          {truncate(model.displayName, 50, 25)}
        </span>
      )}
      <span className="mr-2 whitespace-nowrap block">
        <b>All:</b> {nodes.length}
      </span>
      {countSources > 0 && (
        <span className="mr-2 whitespace-nowrap block">
          <b>Sources</b>: {countSources}
        </span>
      )}
      {withImpacted && countSelected === 0 && countImpact > 0 && (
        <span className="mr-2 whitespace-nowrap block">
          <b>Upstream/Downstream:</b> {countImpact}
        </span>
      )}
      {countCTEs > 0 && (
        <span className="mr-2 whitespace-nowrap block">
          <b>CTEs:</b> {countCTEs}
        </span>
      )}
      {(countSelected > 0 || countHidden > 0) && (
        <span className="bg-neutral-5 px-2 py-0.5 flex rounded-full">
          {countHidden > 0 && (
            <span className="mr-2 whitespace-nowrap block">
              <b>Hidden:</b> {countHidden}
            </span>
          )}
          {countSelected > 0 && (
            <span className="whitespace-nowrap block">
              <b>Selected:</b> {countSelected}
            </span>
          )}
        </span>
      )}
    </>
  )
}
