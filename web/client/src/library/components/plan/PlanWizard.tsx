import { Disclosure } from '@headlessui/react'
import {
  MinusCircleIcon,
  PlusCircleIcon,
  CheckCircleIcon,
} from '@heroicons/react/24/solid'
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
  isNotNil,
} from '../../../utils'
import Spinner from '../logo/Spinner'
import { EnumPlanChangeType, usePlan } from './context'
import { getBackfillStepHeadline, isModified } from './help'
import Plan from './Plan'
import PlanChangePreview from './PlanChangePreview'
import { EnumSize, EnumVariant, type Variant } from '~/types/enum'
import Banner from '@components/banner/Banner'
import TasksOverview from '../tasksOverview/TasksOverview'
import Loading from '@components/loading/Loading'
import Title from '@components/title/Title'
import ReportTestsErrors from '@components/report/ReportTestsErrors'

interface PropsPlanWizardStepMessage extends React.HTMLAttributes<HTMLElement> {
  hasSpinner?: boolean
  variant?: Variant
  index?: number
}

export default function PlanWizard({
  setRefTasksOverview,
}: {
  setRefTasksOverview: RefObject<HTMLDivElement>
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
    testsReportMessages,
    planReport,
    testsReportErrors,
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
          view_name: task.view_name,
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
  const isFailed = Array.from(planReport.values()).some(
    report => report.status === 'fail',
  )

  console.log({ planReport, isFailed }, Object.values(planReport))

  return (
    <div className="w-full h-full py-4 overflow-hidden">
      <div className="w-full h-full px-4 overflow-y-auto hover:scrollbar scrollbar--vertical ">
        {planAction === EnumPlanAction.Run ? (
          <Plan.StepOptions className="w-full" />
        ) : (
          <>
            <PlanModelChanges />
            {planReport.has('plan') && (
              <div className="px-2">
                {planReport.get('plan')!.status === 'init' && (
                  <Banner
                    className="my-2"
                    variant={EnumVariant.Primary}
                  >
                    <Loading
                      text="Step 1: Validating Plan..."
                      hasSpinner
                      size={EnumSize.lg}
                      variant={EnumVariant.Primary}
                    />
                  </Banner>
                )}
                {planReport.get('plan')!.status === 'success' && (
                  <Banner
                    className="my-2 flex items-center"
                    variant={EnumVariant.Success}
                  >
                    <CheckCircleIcon className="w-5 mr-4" />
                    <Title
                      text="Plan Validated"
                      size={EnumSize.sm}
                      variant={EnumVariant.Success}
                    />
                  </Banner>
                )}
              </div>
            )}
            {planReport.has('tests') && (
              <div className="px-2">
                {planReport.get('tests')!.status === 'init' && (
                  <PlanWizardStepMessage
                    index={2}
                    hasSpinner
                  >
                    Running Tests...
                  </PlanWizardStepMessage>
                )}
                {planReport.get('tests')!.status === 'success' &&
                  isNotNil(testsReportMessages) && (
                    <Banner
                      className="my-2 flex items-center"
                      variant={EnumVariant.Success}
                    >
                      <CheckCircleIcon className="w-5 mr-4" />
                      <Title
                        text={testsReportMessages?.message}
                        size={EnumSize.sm}
                        variant={EnumVariant.Success}
                      />
                    </Banner>
                  )}
                {planReport.get('tests')!.status === 'fail' &&
                  isNotNil(testsReportErrors) &&
                  isObjectNotEmpty(testsReportErrors) && (
                    <Banner variant={EnumVariant.Danger}>
                      <Disclosure defaultOpen={true}>
                        {({ open }) => (
                          <>
                            <div className="flex items-center">
                              <p className="w-full mr-2 text-sm">
                                {testsReportErrors?.title}
                              </p>
                              <Disclosure.Button className="flex items-center justify-between rounded-lg text-left text-sm">
                                {open ? (
                                  <MinusCircleIcon className="w-4 text-danger-500" />
                                ) : (
                                  <PlusCircleIcon className="w-4 text-danger-500" />
                                )}
                              </Disclosure.Button>
                            </div>
                            <Disclosure.Panel className="px-4 pb-2 text-sm">
                              <ReportTestsErrors report={testsReportErrors} />
                            </Disclosure.Panel>
                          </>
                        )}
                      </Disclosure>
                    </Banner>
                  )}
                {planReport.get('tests')!.status === 'skip' && (
                  <PlanWizardStepMessage
                    index={2}
                    variant={EnumVariant.Info}
                  >
                    Tests Skipped
                  </PlanWizardStepMessage>
                )}
              </div>
            )}
            {planReport.has('push') && isFalse(isFailed) && (
              <div className="px-2 mt-2">
                {planReport.get('push')!.status === 'init' && (
                  <PlanWizardStepMessage
                    index={3}
                    hasSpinner
                  >
                    Pushing Changes...
                  </PlanWizardStepMessage>
                )}
                {planReport.get('push')!.status === 'success' && (
                  <Banner
                    className="my-2 flex items-center"
                    variant={EnumVariant.Success}
                  >
                    <CheckCircleIcon className="w-5 mr-4" />
                    <Title
                      text="Changes Pushed"
                      size={EnumSize.sm}
                      variant={EnumVariant.Success}
                    />
                  </Banner>
                )}
                {planReport.get('push')!.status === 'fail' && (
                  <PlanWizardStepMessage
                    index={3}
                    variant={EnumVariant.Danger}
                  >
                    Pushing Failed
                  </PlanWizardStepMessage>
                )}
              </div>
            )}
            {planReport.has('restate') && isFalse(isFailed) && (
              <div className="px-2">
                {planReport.get('restate')!.status === 'init' && (
                  <PlanWizardStepMessage
                    index={4}
                    hasSpinner
                  >
                    Restating Models...
                  </PlanWizardStepMessage>
                )}
                {planReport.get('restate')!.status === 'success' && (
                  <PlanWizardStepMessage
                    index={4}
                    variant={EnumVariant.Success}
                  >
                    Restate Completed
                  </PlanWizardStepMessage>
                )}
                {planReport.get('restate')!.status === 'fail' && (
                  <PlanWizardStepMessage
                    index={4}
                    variant={EnumVariant.Danger}
                  >
                    Restate Failed
                  </PlanWizardStepMessage>
                )}
                {planReport.get('restate')!.status === 'skip' && (
                  <Banner
                    className="my-2 flex items-center"
                    variant={EnumVariant.Info}
                  >
                    <CheckCircleIcon className="w-5 mr-4" />
                    <Title
                      text="No Models To Restate"
                      size={EnumSize.sm}
                      variant={EnumVariant.Info}
                    />
                  </Banner>
                )}
              </div>
            )}
            {(isNotNil(activeBackfill) || hasVirtualUpdate) &&
              isFalse(isFailed) && (
                <Disclosure
                  key={backfillStepHeadline}
                  defaultOpen={hasBackfills}
                >
                  {({ open }) => (
                    <div className="px-2">
                      <Banner
                        className="my-2 flex items-center"
                        variant={
                          planState === EnumPlanState.Finished
                            ? EnumVariant.Success
                            : planState === EnumPlanState.Failed
                            ? EnumVariant.Danger
                            : EnumVariant.Info
                        }
                      >
                        {planState === EnumPlanState.Finished && (
                          <CheckCircleIcon className="w-5 mr-4" />
                        )}
                        <Title
                          text={backfillStepHeadline}
                          size={EnumSize.sm}
                          variant={
                            planState === EnumPlanState.Finished
                              ? EnumVariant.Success
                              : planState === EnumPlanState.Failed
                              ? EnumVariant.Danger
                              : EnumVariant.Info
                          }
                          className="w-full"
                        />
                        {showDetails && (
                          <div className="flex items-center">
                            <p className="mr-2 text-sm">Details</p>
                            <Disclosure.Button className="flex items-center justify-between rounded-lg text-left text-sm">
                              {open ? (
                                <MinusCircleIcon className="w-5" />
                              ) : (
                                <PlusCircleIcon className="w-5" />
                              )}
                            </Disclosure.Button>
                          </div>
                        )}
                      </Banner>
                      <Disclosure.Panel className="px-4 pb-2 text-sm">
                        {hasBackfills &&
                          isFalse(skip_backfill) &&
                          isArrayNotEmpty(Object.keys(tasks)) && (
                            <>
                              <Suspense
                                fallback={<Spinner className="w-4 h-4 mr-2" />}
                              >
                                <TasksOverview
                                  tasks={tasks}
                                  setRefTasksOverview={setRefTasksOverview}
                                >
                                  {({
                                    total,
                                    completed,
                                    models,
                                    completedBatches,
                                    totalBatches,
                                  }) => (
                                    <>
                                      <TasksOverview.Summary
                                        environment={environment.name}
                                        planState={planState}
                                        headline="Target Environment"
                                        completed={completed}
                                        total={total}
                                        completedBatches={completedBatches}
                                        totalBatches={totalBatches}
                                        updateType={
                                          hasVirtualUpdate
                                            ? 'Virtual'
                                            : 'Backfill'
                                        }
                                        updatedAt={activeBackfill?.updated_at}
                                      />
                                      {models != null && (
                                        <TasksOverview.Details
                                          models={models}
                                          changes={{
                                            modified,
                                            added,
                                            removed,
                                          }}
                                          showBatches={hasBackfills}
                                          showVirtualUpdate={hasVirtualUpdate}
                                          showProgress={true}
                                        />
                                      )}
                                    </>
                                  )}
                                </TasksOverview>
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
                    </div>
                  )}
                </Disclosure>
              )}
            {planReport.has('promote') && isFalse(isFailed) && (
              <div className="px-2">
                {planReport.get('promote')!.status === 'init' && (
                  <PlanWizardStepMessage
                    index={6}
                    hasSpinner
                  >
                    Promoting...
                  </PlanWizardStepMessage>
                )}
                {planReport.get('promote')!.status === 'success' && (
                  <PlanWizardStepMessage
                    index={6}
                    variant={EnumVariant.Success}
                  >
                    Promote Completed
                  </PlanWizardStepMessage>
                )}
                {planReport.get('promote')!.status === 'fail' && (
                  <PlanWizardStepMessage
                    index={6}
                    variant={EnumVariant.Danger}
                  >
                    Promote Failed
                  </PlanWizardStepMessage>
                )}
              </div>
            )}
          </>
        )}
      </div>
    </div>
  )
}

function PlanModelChanges(): JSX.Element {
  const planAction = useStorePlan(s => s.action)

  const { hasChanges, modified, added, removed, hasVirtualUpdate } = usePlan()

  const isPlanRunning = planAction === EnumPlanAction.Running
  const shouldBeFullAndCenter =
    (isFalse(hasChanges) &&
      isFalse(isPlanRunning) &&
      isFalse(hasVirtualUpdate)) ||
    isPlanRunning

  return (
    <div className={clsx('w-full', shouldBeFullAndCenter && 'h-full')}>
      {isPlanRunning && (
        <Banner
          isFull
          isCenter
        >
          <Loading
            text="Checking Models..."
            hasSpinner
            size={EnumSize.lg}
          />
        </Banner>
      )}
      {isFalse(hasChanges) && isFalse(isPlanRunning) && (
        <Banner
          isFull={isFalse(hasVirtualUpdate)}
          isCenter={isFalse(hasVirtualUpdate)}
        >
          <Title
            size={hasVirtualUpdate ? EnumSize.md : EnumSize.lg}
            text="No Changes"
          />
        </Banner>
      )}
      {hasChanges && isFalse(isPlanRunning) && (
        <>
          {(isArrayNotEmpty(added) || isArrayNotEmpty(removed)) && (
            <div className="flex">
              {isArrayNotEmpty(added) && (
                <PlanChangePreview
                  className="w-full my-2"
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
                  className="w-full my-2"
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
                  className="m-2"
                  headline="Modified Directly"
                  type={EnumPlanChangeType.Direct}
                >
                  <PlanChangePreview.Direct changes={modified.direct ?? []} />
                </PlanChangePreview>
              )}
              {isArrayNotEmpty(modified.indirect) && (
                <PlanChangePreview
                  className="m-2"
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
                  className="m-2 max-h-[50vh]"
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
      )}
    </div>
  )
}

function PlanWizardStepMessage({
  hasSpinner = false,
  variant = EnumVariant.Primary,
  index,
  children,
}: PropsPlanWizardStepMessage): JSX.Element {
  return (
    <Banner variant={variant}>
      <span className="flex items-center w-full">
        {isNotNil(index) && (
          <span className="inline-block mr-3 font-black whitespace-nowrap">
            Step {index}:
          </span>
        )}
        {hasSpinner && <Spinner className="w-4 h-4 mr-2" />}
        {children}
      </span>
    </Banner>
  )
}
