import { Disclosure } from '@headlessui/react'
import { MinusCircleIcon, PlusCircleIcon } from '@heroicons/react/24/solid'
import clsx from 'clsx'
import { type RefObject, Suspense, useCallback, useMemo } from 'react'
import { type ContextEnvironmentBackfill } from '~/api/client'
import { useStoreContext } from '~/context/context'
import {
  EnumPlanState,
  EnumPlanAction,
  useStorePlan,
  type PlanTasks,
  type PlanTaskStatus,
} from '../../../context/plan'
import {
  isArrayEmpty,
  isArrayNotEmpty,
  isFalse,
  isObjectNotEmpty,
} from '../../../utils'
import Spinner from '../logo/Spinner'
import TasksProgress from '../tasksProgress/TasksProgress'
import { EnumPlanChangeType, usePlan } from './context'
import { getBackfillStepHeadline, isModified } from './help'
import Plan from './Plan'
import PlanChangePreview from './PlanChangePreview'

export default function PlanWizard({
  setRefTaskProgress,
}: {
  setRefTaskProgress: RefObject<HTMLDivElement>
}): JSX.Element {
  const {
    backfills,
    hasChanges,
    hasBackfills,
    activeBackfill,
    modified,
    added,
    removed,
    virtualUpdateDescription,
    skip_backfill,
    change_categorization,
    hasVirtualUpdate,
  } = usePlan()

  const environment = useStoreContext(s => s.environment)

  const planState = useStorePlan(s => s.state)
  const planAction = useStorePlan(s => s.action)

  const categories = useMemo(
    () =>
      Array.from(change_categorization.values()).reduce<
        Record<string, boolean[]>
      >((acc, { category, change }) => {
        change?.indirect?.forEach(model => {
          if (acc[model] == null) {
            acc[model] = []
          }

          acc[model]?.push(category.value !== 1)
        })

        if (category.value === 3) {
          acc[change.model_name] = [true]
        }

        return acc
      }, {}),
    [change_categorization],
  )

  const filterActiveBackfillsTasks = useCallback(
    (tasks: PlanTasks): PlanTasks => {
      return Object.entries(tasks).reduce(
        (acc: PlanTasks, [taskModelName, task]) => {
          const choices = categories[taskModelName]

          const shouldExclude = choices != null ? choices.every(Boolean) : false

          if (shouldExclude) return acc

          acc[taskModelName] = task

          return acc
        },
        {},
      )
    },
    [categories],
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
        const choices = categories[taskModelName]

        const shouldExclude = choices != null ? choices.every(Boolean) : false

        if (shouldExclude) return acc

        acc[taskModelName] = taskBackfill

        return acc
      }, {})
    },
    [categories],
  )

  const tasks: PlanTasks = useMemo(
    (): PlanTasks =>
      activeBackfill?.tasks != null
        ? filterActiveBackfillsTasks(activeBackfill.tasks)
        : filterBackfillsTasks(backfills),
    [backfills, change_categorization, activeBackfill],
  )

  const isFinished = planState === EnumPlanState.Finished
  const hasNoChanges = [
    hasChanges,
    hasBackfills,
    isObjectNotEmpty(tasks),
  ].every(isFalse)
  const showDetails =
    (hasVirtualUpdate && isFalse(isFinished)) ||
    (isFalse(hasNoChanges) &&
      hasBackfills &&
      isFalse(skip_backfill) &&
      isArrayNotEmpty(Object.keys(tasks)))

  const backfillStepHeadline = getBackfillStepHeadline({
    planAction,
    planState,
    hasBackfills,
    hasVirtualUpdate,
    hasNoChanges: hasNoChanges || isArrayEmpty(Object.keys(tasks)),
    skip_backfill,
  })

  return (
    <ul className="w-full">
      {planAction === EnumPlanAction.Run ? (
        <Plan.StepOptions className="w-full" />
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
                      <PlanChangePreview
                        className="w-full mx-2"
                        headline="Added Models"
                        type={EnumPlanChangeType.Add}
                      >
                        <PlanChangePreview.Default
                          type={EnumPlanChangeType.Add}
                          changes={added}
                        />
                      </PlanChangePreview>
                    )}
                    {isArrayNotEmpty(removed) && (
                      <PlanChangePreview
                        className="w-full mx-2"
                        headline="Removed Models"
                        type={EnumPlanChangeType.Remove}
                      >
                        <PlanChangePreview.Default
                          type={EnumPlanChangeType.Remove}
                          changes={removed}
                        />
                      </PlanChangePreview>
                    )}
                  </div>
                )}
                {isModified(modified) && (
                  <>
                    {isArrayNotEmpty(modified?.direct) && (
                      <PlanChangePreview
                        className="w-full my-2"
                        headline="Modified Directly"
                        type={EnumPlanChangeType.Direct}
                      >
                        <PlanChangePreview.Direct
                          changes={modified.direct ?? []}
                        />
                      </PlanChangePreview>
                    )}
                    {isArrayNotEmpty(modified.indirect) && (
                      <PlanChangePreview
                        className="w-full my-2"
                        headline="Modified Indirectly"
                        type={EnumPlanChangeType.Indirect}
                      >
                        <PlanChangePreview.Indirect
                          changes={modified.indirect ?? []}
                        />
                      </PlanChangePreview>
                    )}
                    {isArrayNotEmpty(modified?.metadata) && (
                      <PlanChangePreview
                        className="w-full my-2"
                        headline="Modified Metadata"
                        type={EnumPlanChangeType.Metadata}
                      >
                        <PlanChangePreview.Default
                          type={EnumPlanChangeType.Metadata}
                          changes={modified?.metadata ?? []}
                        />
                      </PlanChangePreview>
                    )}
                  </>
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
                              'text-prose',
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
                              <MinusCircleIcon className="h-6 w-6 text-primary-500" />
                            ) : (
                              <PlusCircleIcon className="h-6 w-6 text-primary-500" />
                            )}
                          </Disclosure.Button>
                        </div>
                      )}
                    </div>
                  </PlanWizardStepMessage>

                  <Disclosure.Panel className="px-4 pb-2 text-sm">
                    {hasBackfills &&
                      isFalse(skip_backfill) &&
                      isArrayNotEmpty(Object.keys(tasks)) && (
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
                              showVirtualUpdate={hasVirtualUpdate}
                              planState={planState}
                              setRefTaskProgress={setRefTaskProgress}
                            />
                          </Suspense>
                        </>
                      )}
                    {hasVirtualUpdate && (
                      <div>
                        <small className="text-sm">
                          {virtualUpdateDescription}
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

interface PropsPlanWizardStepHeader
  extends React.ButtonHTMLAttributes<HTMLElement> {
  headline?: string
  disabled?: boolean
}

function PlanWizardStepMessage({
  hasSpinner = false,
  children,
}: PropsPlanWizardStepMessage): JSX.Element {
  return (
    <span className="mt-1 mb-4 px-4 py-2 bg-primary-10 flex w-full rounded-lg">
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
    <li className="mb-2 p-4">
      <PlanWizardStepHeader
        className="min-w-[25%] pr-12"
        headline={headline}
        disabled={disabled}
      >
        {description}
      </PlanWizardStepHeader>
      {!disabled && children}
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
        <h3 className="whitespace-nowrap font-bold text-lg">{headline}</h3>
      )}
      {children != null && (
        <small className="whitespace-nowrap">{children}</small>
      )}
    </div>
  )
}
