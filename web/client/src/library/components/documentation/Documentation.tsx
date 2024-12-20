import Markdown from 'react-markdown'
import {
  isArrayEmpty,
  isArrayNotEmpty,
  isFalse,
  isFalseOrNil,
  isNotNil,
  isString,
  isTrue,
  toDateFormat,
} from '@utils/index'
import { type ModelSQLMeshModel } from '@models/sqlmesh-model'
import ModelColumns from '@components/graph/ModelColumns'
import {
  TBKMetadataSection,
  TBKMetadata,
  TBKMetadataItem,
  TBKDetails,
  TBKModelName,
  TBKScroll,
  TBKBadge,
  TBKDatetime,
  TBKResizeObserver,
} from '@utils/additional-components'

const Documentation = function Documentation({
  model,
}: {
  model: ModelSQLMeshModel
}): JSX.Element {
  const topLevelDetails = new Set(
    ['path', 'displayName', 'owner', 'cron', 'cron_prev', 'cron_next'].filter(
      k => (model as any)[k] ?? (model.details as any)[k],
    ),
  )

  return (
    <TBKScroll className="w-full py-3">
      <div className="px-3 flex flex-col gap-2 ">
        <TBKDetails
          summary="Model Details"
          open
        >
          <TBKResizeObserver update-selector="tbk-model-name">
            <TBKMetadata>
              <TBKMetadataSection limit={topLevelDetails.size}>
                {isNotNil(model.path) && (
                  <TBKMetadataItem
                    label="Path"
                    value={model.path}
                  ></TBKMetadataItem>
                )}
                <TBKMetadataItem label="Name">
                  <TBKModelName
                    slot="value"
                    text={model.displayName}
                  ></TBKModelName>
                </TBKMetadataItem>
                {isNotNil(model.details.owner) && (
                  <TBKMetadataItem
                    label="Owner"
                    value={model.details.owner}
                  ></TBKMetadataItem>
                )}
                {isNotNil(model.details.cron) && (
                  <TBKMetadataItem label="Cron">
                    <TBKBadge
                      slot="value"
                      size="s"
                      variant="info"
                    >
                      {model.details.cron}
                    </TBKBadge>
                  </TBKMetadataItem>
                )}
                {isNotNil(model.details.cron_prev) && (
                  <TBKMetadataItem label="Prev Cron">
                    <TBKDatetime
                      slot="value"
                      date={model.details.cron_prev}
                    ></TBKDatetime>
                  </TBKMetadataItem>
                )}
                {isNotNil(model.details.cron_next) && (
                  <TBKMetadataItem label="Next Cron">
                    <TBKDatetime
                      slot="value"
                      date={model.details.cron_next}
                    ></TBKDatetime>
                  </TBKMetadataItem>
                )}
                {Object.entries(model.details ?? {})
                  .filter(([key, _]) => isFalse(topLevelDetails.has(key)))
                  .map(([key, value]) => (
                    <TBKMetadataItem
                      className="capitalize"
                      key={key}
                      label={key.replaceAll('_', ' ')}
                      value={getValue(value)}
                    >
                      {isArrayNotEmpty(value) ? (
                        <TBKMetadataSection limit={5}>
                          {value.map((v, idx) => (
                            <TBKMetadataItem
                              key={idx}
                              className="capitalize"
                              label={v.name.replaceAll('_', ' ')}
                              value={`Unique: ${
                                isFalseOrNil(v.unique) ? 'No' : 'Yes'
                              }`}
                            ></TBKMetadataItem>
                          ))}
                        </TBKMetadataSection>
                      ) : (
                        <></>
                      )}
                    </TBKMetadataItem>
                  ))}
              </TBKMetadataSection>
            </TBKMetadata>
          </TBKResizeObserver>
        </TBKDetails>
        <TBKDetails
          summary="Columns"
          open
        >
          <ModelColumns
            nodeId={model.fqn}
            columns={model.columns}
            disabled={isFalse(model.isModelSQL)}
            withHandles={false}
            withSource={false}
            withDescription={true}
            limit={10}
          />
        </TBKDetails>
        <TBKDetails summary="Description">
          <Markdown>{model.description ?? 'No description'}</Markdown>
        </TBKDetails>
      </div>
    </TBKScroll>
  )
}

export default Documentation

function getValue(value: Primitive): Optional<string> {
  if (isArrayNotEmpty(value)) return
  if (isArrayEmpty(value)) return 'None'

  const maybeDate = new Date(value as string)
  const isDate = isString(value) && !isNaN(maybeDate.getTime())
  const isBoolean = typeof value === 'boolean'

  if (isBoolean && isTrue(value)) return 'True'
  if (isBoolean && isFalse(value)) return 'False'
  if (isDate) return toDateFormat(maybeDate)

  return String(value)
}
