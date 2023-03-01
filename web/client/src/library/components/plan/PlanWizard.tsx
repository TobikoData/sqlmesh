import { Disclosure, RadioGroup } from '@headlessui/react'
import {
  CheckCircleIcon,
  MinusCircleIcon,
  PlusCircleIcon,
} from '@heroicons/react/24/solid'
import clsx from 'clsx'
import { lazy, Suspense, useMemo } from 'react'
import {
  ContextEnvironmentBackfill,
  ContextEnvironmentChanges,
  ModelsDiffDirectItem,
} from '~/api/client'
import { EnvironmentName } from '~/context/context'
import {
  EnumPlanState,
  EnumPlanAction,
  useStorePlan,
  PlanTasks,
  PlanTaskStatus,
  EnumCategoryType,
} from '../../../context/plan'
import {
  includes,
  isArrayNotEmpty,
  isFalse,
  toDate,
  toDateFormat,
} from '../../../utils'
import { Divider } from '../divider/Divider'
import Input from '../input/Input'
import Spinner from '../logo/Spinner'
import { isModified } from './help'
import PlanWizardStepOptions from './PlanWizardStepOptions'

const Tasks = lazy(async () => await import('../plan/Tasks'))

export default function PlanWizard({
  environment,
  changes,
  hasChanges,
  hasBackfill,
  backfills = [],
}: {
  environment: EnvironmentName
  hasChanges: boolean
  changes?: ContextEnvironmentChanges
  backfills?: ContextEnvironmentBackfill[]
  hasBackfill: boolean
}): JSX.Element {
  const planState = useStorePlan(s => s.state)
  const planAction = useStorePlan(s => s.action)

  const category = useStorePlan(s => s.category)
  const categories = useStorePlan(s => s.categories)
  const backfill_start = useStorePlan(s => s.backfill_start)
  const backfill_end = useStorePlan(s => s.backfill_end)
  const activePlan = useStorePlan(s => s.activePlan)
  const mostRecentPlan = useStorePlan(s => s.lastPlan ?? s.activePlan)
  const setCategory = useStorePlan(s => s.setCategory)
  const setBackfillDate = useStorePlan(s => s.setBackfillDate)

  const tasks: PlanTasks = useMemo(
    (): PlanTasks =>
      hasBackfill
        ? backfills.reduce((acc: PlanTasks, task) => {
            const taskModelName = task.model_name
            const taskInterval = task.interval as [string, string]
            const taskBackfill: PlanTaskStatus = {
              completed: 0,
              ...activePlan?.tasks[taskModelName],
              total: task.batches,
              interval: taskInterval,
            }

            if (category?.id === EnumCategoryType.BreakingChange) {
              acc[taskModelName] = taskBackfill
            }

            if (category?.id === EnumCategoryType.NonBreakingChange) {
              const directChanges = changes?.modified.direct
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
        : mostRecentPlan?.tasks ?? {},
    [backfills, changes, category, mostRecentPlan, activePlan],
  )

  const isDone = mostRecentPlan != null && planAction === EnumPlanAction.Done
  const isPlanInProgress = includes(
    [EnumPlanState.Cancelling, EnumPlanState.Applying],
    planState,
  )
  const shouldDisplayTaskProgress =
    (hasBackfill || isDone) &&
    planAction !== EnumPlanAction.Running &&
    planAction !== EnumPlanAction.Closing

  return (
    <ul className="w-full mx-auto">
      {planAction === EnumPlanAction.Run ? (
        <PlanWizardStepOptions className="w-full mx-auto md:w-[75%] lg:w-[60%]" />
      ) : (
        <>
          <PlanWizardStep
            headline="Models"
            description="Review Changes"
            disabled={environment == null}
          >
            {hasChanges ? (
              <>
                {(isArrayNotEmpty(changes?.added) ||
                  isArrayNotEmpty(changes?.removed)) && (
                  <div className="flex">
                    {isArrayNotEmpty(changes?.added) && (
                      <PlanWizardStepChanges
                        className="w-full"
                        headline="Added Models"
                        type="add"
                      >
                        <PlanWizardStepChangesDefault
                          type="add"
                          changes={changes?.added}
                        />
                      </PlanWizardStepChanges>
                    )}
                    {isArrayNotEmpty(changes?.removed) && (
                      <PlanWizardStepChanges
                        className="w-full"
                        headline="Removed Models"
                        type="remove"
                      >
                        <PlanWizardStepChangesDefault
                          type="remove"
                          changes={changes?.removed}
                        />
                      </PlanWizardStepChanges>
                    )}
                  </div>
                )}
                {isModified(changes?.modified) && (
                  <div className="flex">
                    {isArrayNotEmpty(changes?.modified.direct) && (
                      <PlanWizardStepChanges
                        className="w-full"
                        headline="Modified Directly"
                        type="direct"
                      >
                        <PlanWizardStepChangesDirect
                          changes={changes?.modified.direct}
                        />
                      </PlanWizardStepChanges>
                    )}
                    {isArrayNotEmpty(changes?.modified.indirect) && (
                      <PlanWizardStepChanges
                        headline="Modified Indirectly"
                        type="indirect"
                      >
                        <PlanWizardStepChangesDefault
                          type="indirect"
                          changes={changes?.modified.indirect}
                        />
                      </PlanWizardStepChanges>
                    )}
                    {isArrayNotEmpty(changes?.modified.metadata) && (
                      <PlanWizardStepChanges
                        headline="Modified Metadata"
                        type="metadata"
                      >
                        <PlanWizardStepChangesDefault
                          type="metadata"
                          changes={changes?.modified.metadata}
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
            {isDone && isFalse(hasBackfill) && (
              <PlanWizardStepMessage className="">
                <div className="flex justify-between items-center w-full">
                  <h3
                    className={clsx(
                      'font-bold text-lg text-success-700',
                      planState === EnumPlanState.Cancelled && 'text-gray-700',
                      planState === EnumPlanState.Failed && 'text-danger-700',
                    )}
                  >
                    {planState === EnumPlanState.Failed
                      ? 'Failed'
                      : planState === EnumPlanState.Cancelled
                      ? 'Cancelled'
                      : 'Completed'}
                  </h3>
                  <p className="text-xs text-gray-600">
                    {toDateFormat(
                      toDate(mostRecentPlan?.updated_at),
                      'yyyy-mm-dd hh-mm-ss',
                    )}
                  </p>
                </div>
              </PlanWizardStepMessage>
            )}
            {planAction === EnumPlanAction.Running && (
              <PlanWizardStepMessage hasSpinner>
                Collecting Backfills...
              </PlanWizardStepMessage>
            )}
            {planState === EnumPlanState.Applying &&
              isFalse(isDone) &&
              isFalse(hasBackfill) && (
                <PlanWizardStepMessage hasSpinner>
                  Applying...
                </PlanWizardStepMessage>
              )}
            {shouldDisplayTaskProgress && (
              <>
                {isModified(changes?.modified) &&
                  planState !== EnumPlanState.Applying && (
                    <div className="mb-10 lg:px-4 ">
                      <RadioGroup
                        className="rounded-lg w-full"
                        value={category}
                        onChange={setCategory}
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
                              } elative flex cursor-pointer rounded-md px-3 py-2 focus:outline-none mb-2`
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
                                        <span>{category.description}</span>
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
                {category?.id !== EnumCategoryType.NoChange &&
                  environment != null && (
                    <>
                      <Suspense fallback={<Spinner className="w-4 h-4 mr-2" />}>
                        <Tasks
                          environment={environment}
                          tasks={tasks}
                          changes={changes}
                          updated_at={mostRecentPlan?.updated_at}
                        />
                      </Suspense>
                      <form>
                        <fieldset className="flex w-full">
                          <Input
                            className="w-full"
                            label="Start Date"
                            disabled={
                              isPlanInProgress ||
                              planAction === EnumPlanAction.Done
                            }
                            value={backfill_start}
                            onInput={(
                              e: React.ChangeEvent<HTMLInputElement>,
                            ) => {
                              setBackfillDate('start', e.target.value)
                            }}
                          />
                          <Input
                            className="w-full"
                            label="End Date"
                            disabled={
                              isPlanInProgress ||
                              planAction === EnumPlanAction.Done
                            }
                            value={backfill_end}
                            onInput={(
                              e: React.ChangeEvent<HTMLInputElement>,
                            ) => {
                              setBackfillDate('end', e.target.value)
                            }}
                          />
                        </fieldset>
                      </form>
                    </>
                  )}
              </>
            )}
            {hasChanges &&
              isFalse(isPlanInProgress) &&
              isFalse(hasBackfill) && (
                <PlanWizardStepMessage>
                  <div>
                    <h3 className="font-bold">
                      Logical Update will be applied
                    </h3>
                    <small className="text-sm">
                      All changes and their downstream dependencies can be fully
                      previewed before they get promoted. If during plan
                      creation no data gaps have been detected and only
                      references to new model versions need to be updated, then
                      such update is referred to as logical. Logical updates
                      impose no additional runtime overhead or cost.
                    </small>
                  </div>
                </PlanWizardStepMessage>
              )}
          </PlanWizardStep>
        </>
      )}
    </ul>
  )
}

type PlanChangeType = 'add' | 'remove' | 'direct' | 'indirect' | 'metadata'

interface PropsPlanWizardStep extends React.HTMLAttributes<HTMLElement> {
  headline: number | string
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
  headline: number | string
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
        type === 'add' && 'bg-success-100',
        type === 'remove' && 'bg-danger-100',
        type === 'direct' && 'bg-secondary-100',
        type === 'indirect' && 'bg-warning-100',
        type === 'metadata' && 'bg-gray-100',
        className,
      )}
    >
      <h4
        className={clsx(
          `mb-2 font-bold`,
          type === 'add' && 'text-success-700',
          type === 'remove' && 'text-danger-700',
          type === 'direct' && 'text-secondary-500',
          type === 'indirect' && 'text-warning-700',
          type === 'metadata' && 'text-gray-900',
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
            type === 'add' && 'text-success-700',
            type === 'remove' && 'text-danger-700',
            type === 'direct' && 'text-secondary-500',
            type === 'indirect' && 'text-warning-700',
            type === 'metadata' && 'text-gray-900',
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
  headline = 1,
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
      <h3 className="whitespace-nowrap text-gray-600 font-bold text-lg">
        {headline}
      </h3>
      {children != null && (
        <small className="whitespace-nowrap text-gray-500">{children}</small>
      )}
    </div>
  )
}
