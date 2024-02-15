import { Disclosure, RadioGroup } from '@headlessui/react'
import {
  MinusCircleIcon,
  PlusCircleIcon,
  PlusIcon,
  MinusIcon,
  ArrowPathRoundedSquareIcon,
} from '@heroicons/react/24/solid'
import clsx from 'clsx'
import { Divider } from '../divider/Divider'
import {
  EnumPlanActions,
  EnumPlanChangeType,
  usePlan,
  usePlanDispatch,
  type PlanChangeType,
} from './context'
import { isArrayNotEmpty, isNil, isNotNil, truncate } from '@utils/index'
import LineageFlowProvider from '@components/graph/context'
import { useStoreContext } from '@context/context'
import ModelLineage from '@components/graph/ModelLineage'
import { type ModelSQLMeshChangeDisplay } from '@models/sqlmesh-change-display'
import { useEffect } from 'react'
import { type SnapshotChangeCategory } from '@api/client'

interface PropsPlanChangePreview extends React.HTMLAttributes<HTMLElement> {
  headline?: string
  type: PlanChangeType
}

function PlanChangePreview({
  children,
  headline,
  type,
  className,
}: PropsPlanChangePreview): JSX.Element {
  return (
    <div
      className={clsx(
        'flex flex-col rounded-md p-4',
        type === EnumPlanChangeType.Add && 'bg-success-5',
        type === EnumPlanChangeType.Remove && 'bg-danger-5',
        type === EnumPlanChangeType.Direct && 'bg-secondary-5',
        type === EnumPlanChangeType.Indirect && 'bg-warning-5',
        type === EnumPlanChangeType.Default && 'bg-neutral-5',
        className,
      )}
    >
      {isNotNil(headline) && (
        <h4
          className={clsx(
            `mb-2 font-bold whitespace-nowrap`,
            type === EnumPlanChangeType.Add &&
              'text-success-600 dark:text-success-300',
            type === EnumPlanChangeType.Remove &&
              'text-danger-600 dark:text-danger-300',
            type === EnumPlanChangeType.Direct &&
              'text-secondary-600 dark:text-secondary-300',
            type === EnumPlanChangeType.Indirect &&
              'text-warning-600 dark:text-warning-300',
            type === EnumPlanChangeType.Default &&
              'text-neutral-600 dark:text-neutral-300',
          )}
        >
          {headline}
        </h4>
      )}
      {children}
    </div>
  )
}

function PlanChangePreviewDefault({
  changes = [],
  type,
}: {
  type: PlanChangeType
  changes: ModelSQLMeshChangeDisplay[]
}): JSX.Element {
  return (
    <ul>
      {changes.map(change => (
        <li
          key={change.name}
          className={clsx(
            'flex items-center px-1 leading-5 mb-1',
            type === EnumPlanChangeType.Add &&
              'text-success-600 dark:text-success-300',
            type === EnumPlanChangeType.Remove &&
              'text-danger-600 dark:text-danger-300',
            type === EnumPlanChangeType.Direct &&
              'text-secondary-600 dark:text-secondary-300',
            type === EnumPlanChangeType.Indirect &&
              'text-warning-600 dark:text-warning-300',
            type === EnumPlanChangeType.Default &&
              'text-neutral-600 dark:text-neutral-300',
          )}
        >
          {type === EnumPlanChangeType.Add ? (
            <PlusIcon className="h-3 mr-2" />
          ) : type === EnumPlanChangeType.Remove ? (
            <MinusIcon className="h-3 mr-2" />
          ) : (
            <ArrowPathRoundedSquareIcon className="h-4 mr-2" />
          )}
          <small
            title={change.displayViewName}
            className="w-full text-xs whitespace-nowrap text-ellipsis overflow-hidden"
          >
            {truncate(change.displayViewName, 50, 25)}
          </small>
        </li>
      ))}
    </ul>
  )
}

function PlanChangePreviewDirect({
  changes = [],
}: {
  changes: ModelSQLMeshChangeDisplay[]
}): JSX.Element {
  const dispatch = usePlanDispatch()
  const { categories } = usePlan()

  const models = useStoreContext(s => s.models)

  useEffect(() => {
    dispatch(
      changes.map(change => ({
        type: EnumPlanActions.Category,
        category: categories.find(
          ({ value }) => value === change.change_category,
        ),
        change,
      })),
    )
  }, [changes])

  return (
    <ul>
      {changes.map(change => (
        <li
          key={change.name}
          className="text-secondary-500 dark:text-primary-500 mt-1"
        >
          <Disclosure>
            {({ open }) => (
              <>
                <Disclosure.Button className="flex items-center w-full justify-between rounded-lg text-left">
                  <PlanChangePreviewTitle
                    className="w-full"
                    change={change}
                  />
                  {(() => {
                    const Tag = open ? MinusCircleIcon : PlusCircleIcon

                    return (
                      <Tag className="max-h-[1rem] min-w-[1rem] dark:text-primary-500" />
                    )
                  })()}
                </Disclosure.Button>
                <Disclosure.Panel className="text-sm px-4 mb-4 overflow-hidden">
                  {isArrayNotEmpty(change.indirect) && (
                    <PlanChangePreviewRelations
                      type="indirect"
                      models={change.indirect}
                    />
                  )}
                  <Divider className="border-neutral-200 mt-2" />
                  <ChangeCategories change={change} />
                  <Divider className="border-neutral-200 mt-2" />
                  <div className="flex flex-col w-full h-full overflow-hidden overflow-y-auto hover:scrollbar scrollbar--vertical scrollbar--horizontal">
                    {isNotNil(change?.diff) && (
                      <PlanChangePreviewDiff diff={change?.diff} />
                    )}
                    {(() => {
                      const model = models.get(change.name)

                      if (isNil(model)) return <></>

                      return (
                        <div className="h-[16rem] bg-theme-lighter rounded-xl py-2">
                          <LineageFlowProvider
                            showColumns={false}
                            showConnected={true}
                          >
                            <ModelLineage
                              model={model}
                              highlightedNodes={{
                                'border-4 border-warning-500':
                                  change.indirect?.map(c => c.name) ?? [],
                                'border-4 border-secondary-500': [change.name],
                                '*': [],
                              }}
                            />
                          </LineageFlowProvider>
                        </div>
                      )
                    })()}
                  </div>
                </Disclosure.Panel>
              </>
            )}
          </Disclosure>
        </li>
      ))}
    </ul>
  )
}

function ChangeCategories({
  change,
}: {
  change: ModelSQLMeshChangeDisplay
}): JSX.Element {
  const dispatch = usePlanDispatch()

  const { change_categorization, categories } = usePlan()

  return (
    <RadioGroup
      className="flex flex-col mt-2"
      value={
        change_categorization.get(change.name)?.category?.value ??
        change.change_category
      }
      onChange={(category: SnapshotChangeCategory) => {
        dispatch({
          type: EnumPlanActions.Category,
          category: categories.find(({ value }) => value === category),
          change,
        })
      }}
    >
      {categories.map(category => (
        <RadioGroup.Option
          key={category.name}
          value={category.value}
          className={() => clsx('relative flex rounded-md')}
        >
          {({ checked }) => (
            <div
              className={clsx(
                'text-sm flex items-center px-2 py-1 w-full rounded-lg',
                checked
                  ? 'text-secondary-500 dark:text-primary-300'
                  : 'text-prose',
              )}
            >
              <div className="mt-[0.125rem] mr-2 border-2 border-neutral-400 min-w-[1rem] h-4 rounded-full flex justify-center items-center">
                {checked && (
                  <span className="inline-block w-2 h-2 bg-secondary-500 dark:bg-primary-300 rounded-full"></span>
                )}
              </div>
              <div>
                <RadioGroup.Label as="p">{category.name}</RadioGroup.Label>
                <RadioGroup.Description
                  as="span"
                  className="text-xs text-neutral-500"
                >
                  {category.description}
                </RadioGroup.Description>
              </div>
            </div>
          )}
        </RadioGroup.Option>
      ))}
    </RadioGroup>
  )
}

function PlanChangePreviewIndirect({
  changes = [],
}: {
  changes: ModelSQLMeshChangeDisplay[]
}): JSX.Element {
  return (
    <ul>
      {changes.map(change => (
        <li
          key={change.name}
          className="text-warning-700 dark:text-warning-500"
        >
          {isArrayNotEmpty(change.direct) ? (
            <Disclosure>
              {({ open }) => (
                <>
                  <Disclosure.Button className="flex items-center w-full justify-between rounded-lg text-left">
                    <PlanChangePreviewTitle change={change} />
                    {(() => {
                      const Tag = open ? MinusCircleIcon : PlusCircleIcon

                      return (
                        <Tag className="max-h-[1rem] min-w-[1rem] dark:text-primary-500 mt-0.5" />
                      )
                    })()}
                  </Disclosure.Button>
                  <Disclosure.Panel className="text-sm px-4 mb-4">
                    <PlanChangePreviewRelations
                      type="direct"
                      models={change.direct ?? []}
                    />
                  </Disclosure.Panel>
                </>
              )}
            </Disclosure>
          ) : (
            <PlanChangePreviewTitle change={change} />
          )}
        </li>
      ))}
    </ul>
  )
}

function PlanChangePreviewTitle({
  change,
  className,
}: {
  change: ModelSQLMeshChangeDisplay
  className?: string
}): JSX.Element {
  const { change_categorization } = usePlan()
  const category = change_categorization.get(change.name)?.category

  return (
    <div className={clsx('flex items-center font-bold', className)}>
      <ArrowPathRoundedSquareIcon className="h-4 mr-2" />
      <small className="w-full text-xs whitespace-nowrap text-ellipsis overflow-hidden">
        {change.displayViewName}
      </small>
      {isNotNil(category) && (
        <span className="ml-2 text-xs px-1 bg-neutral-400 text-neutral-100  dark:bg-neutral-400 dark:text-neutral-800 rounded whitespace-nowrap mr-2">
          {category.name}
        </span>
      )}
    </div>
  )
}

function PlanChangePreviewRelations({
  type,
  models,
}: {
  type: 'direct' | 'indirect'
  models: ModelSQLMeshChangeDisplay[]
}): JSX.Element {
  return (
    <ul
      className={clsx(
        'mt-2 ml-4',
        type === 'indirect' && 'text-warning-700 dark:text-warning-500',
        type === 'direct' && 'text-secondary-500 dark:text-primary-500',
      )}
    >
      {models.map(model => (
        <li
          key={model.name}
          className="flex"
        >
          <span className="h-3 w-3 border-l-2 border-b-2 inline-block mr-2"></span>
          {model.displayViewName}
        </li>
      ))}
    </ul>
  )
}

function PlanChangePreviewDiff({ diff }: { diff: string }): JSX.Element {
  return (
    <div className="my-4 bg-dark-lighter rounded-lg overflow-hidden">
      <pre className="p-4 text-primary-100 max-h-[30vh] text-xs overflow-auto hover:scrollbar scrollbar--vertical scrollbar--horizontal">
        {diff.split('\n').map((line: string, idx: number) => (
          <p
            key={`${line}-${idx}`}
            className={clsx(
              line.startsWith('+') && 'text-success-500 bg-success-500/10 px-2',
              line.startsWith('-') && 'text-danger-500 bg-danger-500/10 px-2',
              line.startsWith('@@') && 'text-primary-300 my-5 px-2',
            )}
          >
            {line}
          </p>
        ))}
      </pre>
    </div>
  )
}

PlanChangePreview.Default = PlanChangePreviewDefault
PlanChangePreview.Direct = PlanChangePreviewDirect
PlanChangePreview.Indirect = PlanChangePreviewIndirect
PlanChangePreview.Diff = PlanChangePreviewDiff
PlanChangePreview.Title = PlanChangePreviewTitle

export default PlanChangePreview
