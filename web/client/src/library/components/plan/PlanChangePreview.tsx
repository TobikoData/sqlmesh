import { Disclosure, RadioGroup } from '@headlessui/react'
import {
  MinusCircleIcon,
  PlusCircleIcon,
  PlusIcon,
  MinusIcon,
  ArrowPathRoundedSquareIcon,
} from '@heroicons/react/24/solid'
import clsx from 'clsx'
import { type ChangeDirect, type ChangeIndirect } from '~/api/client'
import { EnumPlanState, useStorePlan } from '~/context/plan'
import { Divider } from '../divider/Divider'
import {
  type Category,
  EnumPlanActions,
  EnumPlanChangeType,
  usePlan,
  usePlanDispatch,
  type PlanChangeType,
} from './context'
import { isArrayNotEmpty } from '@utils/index'
import LineageFlowProvider from '@components/graph/context'
import { useStoreContext } from '@context/context'
import ModelLineage from '@components/graph/ModelLineage'

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
        'flex flex-col rounded-md p-4 overflow-hidden overflow-y-auto hover:scrollbar scrollbar--vertical scrollbar--horizontal',
        type === EnumPlanChangeType.Add && 'bg-success-10',
        type === EnumPlanChangeType.Remove && 'bg-danger-10',
        type === EnumPlanChangeType.Direct && 'bg-secondary-10',
        type === EnumPlanChangeType.Indirect && 'bg-warning-10',
        type === 'metadata' && 'bg-neutral-10',
        className,
      )}
    >
      {headline != null && (
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
            type === EnumPlanChangeType.Metadata &&
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
  changes: string[]
}): JSX.Element {
  return (
    <ul>
      {changes.map(change => (
        <li
          key={change}
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
            type === EnumPlanChangeType.Metadata &&
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
          <small className="w-full text-xs whitespace-nowrap text-ellipsis overflow-hidden">
            {change}
          </small>
        </li>
      ))}
    </ul>
  )
}

function PlanChangePreviewDirect({
  changes = [],
}: {
  changes: ChangeDirect[]
}): JSX.Element {
  const models = useStoreContext(s => s.models)

  return (
    <ul>
      {changes.map(change => (
        <li
          key={change.model_name}
          className="text-secondary-500 dark:text-primary-500"
        >
          <Disclosure>
            {({ open }) => (
              <>
                <Disclosure.Button className="flex items-start w-full justify-between rounded-lg text-left">
                  <div className="w-full">
                    <PlanChangePreviewTitle model_name={change.model_name} />
                  </div>
                  {(() => {
                    const Tag = open ? MinusCircleIcon : PlusCircleIcon

                    return (
                      <Tag className="max-h-[1rem] min-w-[1rem] dark:text-primary-500 mt-0.5" />
                    )
                  })()}
                </Disclosure.Button>
                <Disclosure.Panel className="text-sm px-4 mb-4">
                  <PlanChangePreviewRelations
                    type="indirect"
                    models={change.indirect ?? []}
                  />
                  <Divider className="border-neutral-200 mt-2" />
                  <ChangeCategories change={change} />
                  <Divider className="border-neutral-200 mt-2" />
                  {change?.diff != null && (
                    <PlanChangePreviewDiff diff={change?.diff} />
                  )}
                  {(() => {
                    const model = models.get(change.model_name)

                    if (model == null) return <></>

                    return (
                      <div className="h-[16rem] bg-theme-lighter rounded-xl p-2">
                        <LineageFlowProvider withColumns={false}>
                          <ModelLineage
                            key={model.id}
                            fingerprint={model.id}
                            model={model}
                            highlightedNodes={{
                              'border-4 border-secondary-500': [model.name],
                              'border-4 border-warning-500':
                                change.indirect ?? [],
                              '*': ['opacity-50 hover:opacity-100'],
                            }}
                          />
                        </LineageFlowProvider>
                      </div>
                    )
                  })()}
                </Disclosure.Panel>
              </>
            )}
          </Disclosure>
        </li>
      ))}
    </ul>
  )
}

function ChangeCategories({ change }: { change: ChangeDirect }): JSX.Element {
  const dispatch = usePlanDispatch()

  const { change_categorization, categories } = usePlan()

  const planState = useStorePlan(s => s.state)

  return (
    <RadioGroup
      className={clsx(
        'flex flex-col mt-2',
        planState === EnumPlanState.Finished
          ? 'opacity-50 cursor-not-allowed'
          : 'cursor-pointer',
      )}
      value={change_categorization.get(change.model_name)?.category}
      onChange={(category: Category) => {
        dispatch({
          type: EnumPlanActions.Category,
          category,
          change,
        })
      }}
      disabled={planState === EnumPlanState.Finished}
    >
      {categories.map(category => (
        <RadioGroup.Option
          key={category.name}
          value={category}
          className={() => clsx('relative flex rounded-md')}
        >
          {({ checked }) => (
            <div
              className={clsx(
                'text-sm flex px-2 py-1 w-full rounded-lg',
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
  changes: ChangeIndirect[]
}): JSX.Element {
  return (
    <ul>
      {changes.map(change => (
        <li
          key={change.model_name}
          className="text-warning-700 dark:text-warning-500"
        >
          {isArrayNotEmpty(change.direct) ? (
            <Disclosure>
              {({ open }) => (
                <>
                  <Disclosure.Button className="flex items-center w-full justify-between rounded-lg text-left">
                    <PlanChangePreviewTitle model_name={change.model_name} />
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
            <PlanChangePreviewTitle model_name={change.model_name} />
          )}
        </li>
      ))}
    </ul>
  )
}

function PlanChangePreviewTitle({
  model_name,
}: {
  model_name: string
}): JSX.Element {
  const { change_categorization } = usePlan()
  const category = change_categorization.get(model_name)?.category

  return (
    <div className="flex items-center font-bold">
      <ArrowPathRoundedSquareIcon className="h-4 mr-2" />
      <small className="w-full text-xs whitespace-nowrap text-ellipsis overflow-hidden">
        {model_name}
      </small>
      {category != null && (
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
  models: string[]
}): JSX.Element {
  return (
    <ul
      className={clsx(
        'mt-2 ml-4',
        type === 'indirect' && 'text-warning-700 dark:text-warning-500',
        type === 'direct' && 'text-secondary-500 dark:text-primary-500',
      )}
    >
      {models.map(model_name => (
        <li
          key={model_name}
          className="flex"
        >
          <span className="h-3 w-3 border-l-2 border-b-2 inline-block mr-2"></span>
          {model_name}
        </li>
      ))}
    </ul>
  )
}

function PlanChangePreviewDiff({ diff }: { diff: string }): JSX.Element {
  return (
    <div className="my-4 bg-dark-lighter rounded-lg overflow-hidden">
      <pre className="p-4 text-primary-100 max-h-[30vh] overflow-auto hover:scrollbar scrollbar--vertical scrollbar--horizontal">
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
