import { isFalse, isNil, isNotNil, truncate } from '@utils/index'
import { useLineageFlow } from './context'
import { type Node } from 'reactflow'
import { EnumLineageNodeModelType } from './Graph'

export default function ModelLineageDetails({
  nodes = [],
}: {
  nodes: Node[]
}): JSX.Element {
  const {
    models,
    mainNode,
    selectedNodes,
    withImpacted,
    withSecondary,
    activeNodes,
    connectedNodes,
    highlightedNodes,
  } = useLineageFlow()

  const model = isNil(mainNode) ? undefined : models.get(mainNode)

  const countSelected = selectedNodes.size
  const countImpact = connectedNodes.size - 1
  const countSecondary = nodes.filter(n =>
    isFalse(connectedNodes.has(n.id)),
  ).length
  const countActive =
    activeNodes.size > 0 ? activeNodes.size : connectedNodes.size
  const countHidden = nodes.filter(n => n.hidden).length
  const countVisible = nodes.filter(n => isFalse(n.hidden)).length
  const countDataSources = nodes.filter(
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
          className="mr-2 w-full whitespace-nowrap text-ellipsis overflow-hidden @lg:block"
        >
          <b>Model:</b> {truncate(model.displayName, 50, 25)}
        </span>
      )}
      {isNotNil(highlightedNodes) ?? (
        <span className="mr-2 whitespace-nowrap block">
          <b>Highlighted:</b> {Object.keys(highlightedNodes ?? {}).length}
        </span>
      )}
      {countSelected > 0 && (
        <span className="mr-2 whitespace-nowrap block">
          <b>Selected:</b> {countSelected}
        </span>
      )}
      {withImpacted && countSelected === 0 && countImpact > 0 && (
        <span className="mr-2 whitespace-nowrap block">
          <b>Impact:</b> {countImpact}
        </span>
      )}
      {withSecondary && countSelected === 0 && countSecondary > 0 && (
        <span className="mr-2 whitespace-nowrap block">
          <b>Secondary:</b> {countSecondary}
        </span>
      )}
      <span className="mr-2 whitespace-nowrap block">
        <b>Active:</b> {countActive}
      </span>
      {countVisible > 0 && countVisible !== countActive && (
        <span className="mr-2 whitespace-nowrap block">
          <b>Visible:</b> {countVisible}
        </span>
      )}
      {countHidden > 0 && (
        <span className="mr-2 whitespace-nowrap block">
          <b>Hidden:</b> {countHidden}
        </span>
      )}
      {countDataSources > 0 && (
        <span className="mr-2 whitespace-nowrap block">
          <b>Data Sources</b>: {countDataSources}
        </span>
      )}
      {countCTEs > 0 && (
        <span className="mr-2 whitespace-nowrap block">
          <b>CTEs:</b> {countCTEs}
        </span>
      )}
    </>
  )
}
