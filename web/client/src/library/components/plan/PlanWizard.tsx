import { Disclosure, RadioGroup } from '@headlessui/react'
import {
  CheckCircleIcon,
  MinusCircleIcon,
  PlusCircleIcon,
} from '@heroicons/react/24/solid'
import clsx from 'clsx'
import { Suspense, useCallback, useMemo } from 'react'
import { ContextEnvironmentBackfill, ModelsDiffDirectItem } from '~/api/client'
import { EnvironmentName } from '~/context/context'
import {
  EnumPlanState,
  EnumPlanAction,
  useStorePlan,
  PlanTasks,
  PlanTaskStatus,
} from '../../../context/plan'
import {
  includes,
  isArrayNotEmpty,
  isFalse,
  isObjectNotEmpty,
} from '../../../utils'
import { Divider } from '../divider/Divider'
import Spinner from '../logo/Spinner'
import TasksProgress from '../tasksProgress/TasksProgress'
import {
  Category,
  EnumCategoryType,
  EnumPlanActions,
  EnumPlanChangeType,
  PlanChangeType,
  usePlan,
  usePlanDispatch,
} from './context'
import { getBackfillStepHeadline, isModified } from './help'
import Plan from './Plan'

export default function PlanWizard({
  environment,
}: {
  environment: EnvironmentName
}): JSX.Element {
  const dispatch = usePlanDispatch()
  const {
    backfills,
    hasChanges,
    hasBackfills,
    activeBackfill,
    change_category,
    categories,
    modified,
    added,
    removed,
    logicalUpdateDescription,
    forward_only,
    skip_backfill,
  } = usePlan()

  const planState = useStorePlan(s => s.state)
  const planAction = useStorePlan(s => s.action)

  const filterActiveBackfillsTasks = useCallback(
    (tasks: PlanTasks): PlanTasks => {
      return Object.entries(tasks).reduce(
        (acc: PlanTasks, [taskModelName, task]) => {
          if (change_category.id === EnumCategoryType.NonBreakingChange) {
            const directChanges = modified?.direct
            const isTaskDirectChange =
              directChanges == null
                ? false
                : directChanges.some(
                    ({ model_name }) => model_name === taskModelName,
                  )

            if (isTaskDirectChange) {
              acc[taskModelName] = task
            }
          } else {
            acc[taskModelName] = task
          }

          return acc
        },
        {},
      )
    },
    [change_category, modified],
  )

  const filterBackfillsTasks = useCallback(
    (backfills: ContextEnvironmentBackfill[]): PlanTasks => {
      return backfills.reduce((acc: PlanTasks, task) => {
        const taskModelName = task.model_name
        const taskInterval = task.interval as [string, string]
        const taskBackfill: PlanTaskStatus = {
          completed: 0,
          total: task.batches,
          interval: taskInterval,
        }

        if (change_category.id === EnumCategoryType.BreakingChange) {
          acc[taskModelName] = taskBackfill
        }

        if (change_category.id === EnumCategoryType.NonBreakingChange) {
          const directChanges = modified?.direct
          const isTaskDirectChange =
            directChanges == null
              ? false
              : directChanges.some(
                  ({ model_name }) => model_name === taskModelName,
                )

          if (isTaskDirectChange) {
            acc[taskModelName] = taskBackfill
          }
        }

        return acc
      }, {})
    },
    [change_category, modified],
  )

  const tasks: PlanTasks = useMemo(
    (): PlanTasks =>
      activeBackfill?.tasks != null
        ? filterActiveBackfillsTasks(activeBackfill.tasks)
        : filterBackfillsTasks(backfills),
    [backfills, modified, change_category, activeBackfill],
  )

  const hasLogicalUpdate = hasChanges && isFalse(hasBackfills)
  const isProgress = includes(
    [EnumPlanState.Cancelling, EnumPlanState.Applying],
    planState,
  )
  const isFinished = planState === EnumPlanState.Finished
  const hasNoChange = [hasChanges, hasBackfills, isObjectNotEmpty(tasks)].every(
    isFalse,
  )
  const showDetails =
    isFalse(hasNoChange) &&
    (hasBackfills || (hasLogicalUpdate && isFalse(isFinished))) &&
    isFalse(skip_backfill)
  const backfillStepHeadline = getBackfillStepHeadline({
    planAction,
    planState,
    hasBackfills,
    hasLogicalUpdate,
    hasNoChange,
    skip_backfill,
  })

  return (
    <ul className="w-full mx-auto">
      {planAction === EnumPlanAction.Run ? (
        <Plan.StepOptions className="w-full mx-auto md:w-[75%] lg:w-[60%]" />
      ) : (
        <>
          <PlanWizardStep
            headline="Models"
            description="Review Changes"
            disabled={environment == null}
          >
            {hasChanges ? (
              <>
                {(isArrayNotEmpty(added) || isArrayNotEmpty(removed)) && (
                  <div className="flex">
                    {isArrayNotEmpty(added) && (
                      <PlanWizardStepChanges
                        className="w-full"
                        headline="Added Models"
                        type="add"
                      >
                        <PlanWizardStepChangesDefault
                          type="add"
                          changes={added}
                        />
                      </PlanWizardStepChanges>
                    )}
                    {isArrayNotEmpty(removed) && (
                      <PlanWizardStepChanges
                        className="w-full"
                        headline="Removed Models"
                        type="remove"
                      >
                        <PlanWizardStepChangesDefault
                          type="remove"
                          changes={removed}
                        />
                      </PlanWizardStepChanges>
                    )}
                  </div>
                )}
                {isModified(modified) && (
                  <div className="flex">
                    {isArrayNotEmpty(modified?.direct) && (
                      <PlanWizardStepChanges
                        className="w-full"
                        headline="Modified Directly"
                        type="direct"
                      >
                        <PlanWizardStepChangesDirect
                          changes={modified?.direct}
                        />
                      </PlanWizardStepChanges>
                    )}
                    {isArrayNotEmpty(modified?.indirect) && (
                      <PlanWizardStepChanges
                        headline="Modified Indirectly"
                        type="indirect"
                      >
                        <PlanWizardStepChangesDefault
                          type="indirect"
                          changes={modified?.indirect}
                        />
                      </PlanWizardStepChanges>
                    )}
                    {isArrayNotEmpty(modified?.metadata) && (
                      <PlanWizardStepChanges
                        headline="Modified Metadata"
                        type="metadata"
                      >
                        <PlanWizardStepChangesDefault
                          type="metadata"
                          changes={modified?.metadata}
                        />
                      </PlanWizardStepChanges>
                    )}
                  </div>
                )}
              </>
            ) : planAction === EnumPlanAction.Running ? (
              <PlanWizardStepMessage hasSpinner>
                Checking Models...
              </PlanWizardStepMessage>
            ) : (
              <PlanWizardStepMessage>No Changes</PlanWizardStepMessage>
            )}
          </PlanWizardStep>
          <PlanWizardStep
            headline="Backfill"
            description="Progress"
            disabled={environment == null}
          >
            <Disclosure
              key={backfillStepHeadline}
              defaultOpen={hasBackfills}
            >
              {({ open }) => (
                <>
                  <PlanWizardStepMessage
                    hasSpinner={
                      isFalse(open) &&
                      (planAction === EnumPlanAction.Running ||
                        planAction === EnumPlanAction.Applying)
                    }
                  >
                    <div className="flex justify-between items-center w-full">
                      <div className="flex items-center">
                        <h3
                          className={clsx(
                            planState === EnumPlanState.Cancelled &&
                              'text-gray-700',
                            planState === EnumPlanState.Failed &&
                              'text-danger-700',
                            planState === EnumPlanState.Finished &&
                              'text-success-700',
                          )}
                        >
                          {backfillStepHeadline}
                        </h3>
                      </div>
                      {showDetails && (
                        <div className="flex items-center">
                          <p className="mr-2 text-sm">Details</p>
                          <Disclosure.Button className="flex items-center justify-between rounded-lg text-left text-sm">
                            {open ? (
                              <MinusCircleIcon className="h-6 w-6 text-secondary-500" />
                            ) : (
                              <PlusCircleIcon className="h-6 w-6 text-secondary-500" />
                            )}
                          </Disclosure.Button>
                        </div>
                      )}
                    </div>
                  </PlanWizardStepMessage>

                  <Disclosure.Panel className="px-4 pb-2 text-sm text-gray-500">
                    {hasBackfills && isFalse(skip_backfill) && (
                      <>
                        {isModified(modified) && isFalse(forward_only) && (
                          <div className="mb-10 lg:px-4 ">
                            <RadioGroup
                              className={clsx(
                                'flex flex-col',
                                isFinished
                                  ? 'opacity-50 cursor-not-allowed'
                                  : 'cursor-pointer',
                              )}
                              value={change_category}
                              onChange={(c: Category) => {
                                dispatch({
                                  type: EnumPlanActions.Category,
                                  change_category: c,
                                })
                              }}
                              disabled={isFinished}
                            >
                              {categories.map(category => (
                                <RadioGroup.Option
                                  key={category.name}
                                  value={category}
                                  className={({ active, checked }) =>
                                    `${
                                      active
                                        ? 'ring-2 ring-secodary-500 ring-opacity-60 ring-offset ring-offset-sky-300'
                                        : ''
                                    }
                                      ${
                                        checked
                                          ? 'bg-secondary-500 bg-opacity-75 text-white'
                                          : 'bg-secondary-100'
                                      } elative flex  rounded-md px-3 py-2 focus:outline-none mb-2`
                                  }
                                >
                                  {({ checked }) => (
                                    <>
                                      <div className="flex w-full items-center justify-between">
                                        <div className="flex items-center">
                                          <div className="text-sm">
                                            <RadioGroup.Label
                                              as="p"
                                              className={`font-medium  ${
                                                checked
                                                  ? 'text-white'
                                                  : 'text-gray-900'
                                              }`}
                                            >
                                              {category.name}
                                            </RadioGroup.Label>
                                            <RadioGroup.Description
                                              as="span"
                                              className={`inline ${
                                                checked
                                                  ? 'text-sky-100'
                                                  : 'text-gray-500'
                                              } text-xs`}
                                            >
                                              <span>
                                                {category.description}
                                              </span>
                                            </RadioGroup.Description>
                                          </div>
                                        </div>
                                        {checked && (
                                          <div className="shrink-0 text-white">
                                            <CheckCircleIcon className="h-6 w-6" />
                                          </div>
                                        )}
                                      </div>
                                    </>
                                  )}
                                </RadioGroup.Option>
                              ))}
                            </RadioGroup>
                          </div>
                        )}
                        {change_category?.id !== EnumCategoryType.NoChange && (
                          <>
                            <Suspense
                              fallback={<Spinner className="w-4 h-4 mr-2" />}
                            >
                              <TasksProgress
                                environment={environment}
                                tasks={tasks}
                                changes={{
                                  modified,
                                  added,
                                  removed,
                                }}
                                updated_at={activeBackfill?.updated_at}
                                showBatches={hasBackfills}
                                showLogicalUpdate={hasLogicalUpdate}
                                planState={planState}
                              />
                            </Suspense>
                            <form>
                              <fieldset className="flex w-full">
                                <Plan.BackfillDates disabled={isProgress} />
                              </fieldset>
                            </form>
                          </>
                        )}
                      </>
                    )}
                    {hasChanges && isFalse(hasBackfills) && (
                      <div>
                        <small className="text-sm">
                          {logicalUpdateDescription}
                        </small>
                      </div>
                    )}
                  </Disclosure.Panel>
                </>
              )}
            </Disclosure>
          </PlanWizardStep>
        </>
      )}
    </ul>
  )
}

interface PropsPlanWizardStep extends React.HTMLAttributes<HTMLElement> {
  headline: string
  description: string
  disabled?: boolean
}

interface PropsPlanWizardStepMessage extends React.HTMLAttributes<HTMLElement> {
  hasSpinner?: boolean
}

interface PropsPlanWizardStepChanges extends React.HTMLAttributes<HTMLElement> {
  headline: string
  type: PlanChangeType
}

interface PropsPlanWizardStepHeader
  extends React.ButtonHTMLAttributes<HTMLElement> {
  headline?: string
  disabled?: boolean
}

function PlanWizardStepChanges({
  children,
  headline,
  type,
  className,
}: PropsPlanWizardStepChanges): JSX.Element {
  return (
    <div
      className={clsx(
        'flex flex-col rounded-md p-4 mx-2',
        type === EnumPlanChangeType.Add && 'bg-success-100',
        type === EnumPlanChangeType.Remove && 'bg-danger-100',
        type === EnumPlanChangeType.Direct && 'bg-secondary-100',
        type === EnumPlanChangeType.Indirect && 'bg-warning-100',
        type === 'metadata' && 'bg-gray-100',
        className,
      )}
    >
      <h4
        className={clsx(
          `mb-2 font-bold`,
          type === EnumPlanChangeType.Add && 'text-success-700',
          type === EnumPlanChangeType.Remove && 'text-danger-700',
          type === EnumPlanChangeType.Direct && 'text-secondary-500',
          type === EnumPlanChangeType.Indirect && 'text-warning-700',
          type === EnumPlanChangeType.Metadata && 'text-gray-900',
        )}
      >
        {headline}
      </h4>
      <ul>{children}</ul>
    </div>
  )
}

function PlanWizardStepChangesDefault({
  changes = [],
  type,
}: {
  type: PlanChangeType
  changes?: string[]
}): JSX.Element {
  return (
    <>
      {changes.map(change => (
        <li
          key={change}
          className={clsx(
            'px-1',
            type === EnumPlanChangeType.Add && 'text-success-700',
            type === EnumPlanChangeType.Remove && 'text-danger-700',
            type === EnumPlanChangeType.Direct && 'text-secondary-500',
            type === EnumPlanChangeType.Indirect && 'text-warning-700',
            type === EnumPlanChangeType.Metadata && 'text-gray-900',
          )}
        >
          <small>{change}</small>
        </li>
      ))}
    </>
  )
}

function PlanWizardStepChangesDirect({
  changes = [],
}: {
  changes?: ModelsDiffDirectItem[]
}): JSX.Element {
  return (
    <>
      {changes.map(change => (
        <li
          key={change.model_name}
          className="text-secondary-500"
        >
          <PlanWizardStepChangeDiff change={change} />
        </li>
      ))}
    </>
  )
}

function PlanWizardStepChangeDiff({
  change,
}: {
  change: ModelsDiffDirectItem
}): JSX.Element {
  return (
    <Disclosure>
      {({ open }) => (
        <>
          <Disclosure.Button className="flex items-center w-full justify-between rounded-lg text-left">
            <small className="inline-block text-sm">{change.model_name}</small>
            <Divider className="mx-4" />
            {(() => {
              const Tag = open ? MinusCircleIcon : PlusCircleIcon

              return (
                <Tag className="max-h-[1rem] min-w-[1rem] text-secondary-200" />
              )
            })()}
          </Disclosure.Button>
          <Disclosure.Panel className="text-sm text-secondary-100">
            <pre className="my-4 bg-secondary-900 rounded-lg p-4">
              {change.diff?.split('\n').map((line: string, idx: number) => (
                <p
                  key={`${line}-${idx}`}
                  className={clsx(
                    line.startsWith('+') && 'text-success-500',
                    line.startsWith('-') && 'text-danger-500',
                    line.startsWith('@@') && 'text-secondary-300 my-5',
                  )}
                >
                  {line}
                </p>
              ))}
            </pre>
          </Disclosure.Panel>
        </>
      )}
    </Disclosure>
  )
}

function PlanWizardStepMessage({
  hasSpinner = false,
  children,
}: PropsPlanWizardStepMessage): JSX.Element {
  return (
    <span className="mt-1 mb-4 px-4 py-2 border border-secondary-100 flex w-full rounded-lg">
      <span className="flex items-center w-full">
        {hasSpinner && <Spinner className="w-4 h-4 mr-2" />}
        {children}
      </span>
    </span>
  )
}

function PlanWizardStep({
  headline,
  description,
  children,
  disabled = false,
}: PropsPlanWizardStep): JSX.Element {
  return (
    <li className="mb-2 flex items-center w-full p-4">
      <div className="flex flex-col items-start w-full lg:flex-row">
        <PlanWizardStepHeader
          className="min-w-[25%] pr-12 lg:text-right"
          headline={headline}
          disabled={disabled}
        >
          {description}
        </PlanWizardStepHeader>
        <div className="w-full">{!disabled && children}</div>
      </div>
    </li>
  )
}

function PlanWizardStepHeader({
  disabled = false,
  headline,
  children,
  className,
}: PropsPlanWizardStepHeader): JSX.Element {
  return (
    <div
      className={clsx(
        disabled && 'opacity-40 cursor-not-allowed',
        'mb-4 ',
        className,
      )}
    >
      {headline != null && (
        <h3 className="whitespace-nowrap text-gray-600 font-bold text-lg">
          {headline}
        </h3>
      )}
      {children != null && (
        <small className="whitespace-nowrap text-gray-500">{children}</small>
      )}
    </div>
  )
}
