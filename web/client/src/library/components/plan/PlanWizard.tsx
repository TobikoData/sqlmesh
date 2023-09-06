import { Disclosure } from '@headlessui/react'
import {
  MinusCircleIcon,
  PlusCircleIcon,
  CheckCircleIcon,
  ExclamationCircleIcon,
} from '@heroicons/react/24/solid'
import clsx from 'clsx'
import { useCallback, useMemo } from 'react'
import { type ContextEnvironmentBackfill } from '~/api/client'
import {
  EnumPlanState,
  EnumPlanAction,
  useStorePlan,
  type PlanTasks,
  type PlanTaskStatus,
} from '../../../context/plan'
import {
  isArrayNotEmpty,
  isFalse,
  isObjectNotEmpty,
  isNotNil,
  toRatio,
  isTrue,
  isNil,
} from '../../../utils'
import Spinner from '../logo/Spinner'
import {
  EnumPlanChangeType,
  type TestReportError,
  type TestReportMessage,
  usePlan,
} from './context'
import { isModified } from './help'
import Plan from './Plan'
import PlanChangePreview from './PlanChangePreview'
import { EnumSize, EnumVariant, type Variant } from '~/types/enum'
import Banner from '@components/banner/Banner'
import TasksOverview from '../tasksOverview/TasksOverview'
import Loading from '@components/loading/Loading'
import Title from '@components/title/Title'
import ReportTestsErrors from '@components/report/ReportTestsErrors'
import { Divider } from '@components/divider/Divider'
import Progress from '@components/progress/Progress'

interface PropsPlanWizardStepMessage extends React.HTMLAttributes<HTMLElement> {
  hasSpinner?: boolean
  variant?: Variant
  index?: number
}

export default function PlanWizard(): JSX.Element {
  const {
    planChangesReport,
    planValidateReport,
    applyReport,
    testsReportMessages,
    testsReportErrors,
  } = usePlan()
  const planAction = useStorePlan(s => s.action)

  return (
    <div className="w-full h-full py-4 overflow-hidden">
      <div className="w-full h-full px-4 overflow-y-scroll hover:scrollbar scrollbar--vertical">
        {planAction === EnumPlanAction.Run ? (
          <Plan.StepOptions className="w-full" />
        ) : (
          <>
            {planChangesReport.has('plan') && (
              <StepChanges report={planChangesReport}>
                <PlanModelChanges />
              </StepChanges>
            )}
            {isTrue(planChangesReport.get('skip_tests')) && (
              <PlanWizardStepMessage variant={EnumVariant.Info}>
                Tests Skipped
              </PlanWizardStepMessage>
            )}
            {isNotNil(testsReportMessages) && (
              <StepTestsCompleted report={testsReportMessages} />
            )}
            {isNotNil(testsReportErrors) && (
              <StepTestsFailed report={testsReportErrors} />
            )}
            {planValidateReport.has('plan') && (
              <StepValidate report={planValidateReport} />
            )}
            {isNil(testsReportErrors) && applyReport.has('evaluation') && (
              <StepEvaluate report={applyReport}>
                {applyReport.has('creation') && (
                  <StepCreation report={applyReport} />
                )}
                {applyReport.has('restate') ? (
                  <StepRestate report={applyReport} />
                ) : (
                  <PlanWizardStepMessage
                    variant={EnumVariant.Info}
                    className="mt-2"
                  >
                    No Models To Restate
                  </PlanWizardStepMessage>
                )}
                {applyReport.has('backfill') && (
                  <StepBackfill report={applyReport} />
                )}
                {applyReport.has('promotion') && (
                  <StepPromote report={applyReport} />
                )}
              </StepEvaluate>
            )}
          </>
        )}
      </div>
    </div>
  )
}

function StepChanges({
  report,
  children,
}: {
  report: Map<string, any>
  children?: React.ReactNode
}): JSX.Element {
  const payload = report.get('plan')

  return (
    <Step
      report={payload}
      states={[
        'Plan Changes Collected',
        'Failed Getting Plan Changes',
        'Getting Plan Changes...',
      ]}
      isOpen={true}
      panel={children}
    >
      {isNotNil(payload.backfills) && (
        <p className="mb-0.5">
          Backfills <b>{payload.backfills.length}</b>
        </p>
      )}
      {isNotNil(payload.changes) && (
        <>
          <p className="mb-0.5">
            Added <b>{payload.changes.added.length}</b>
          </p>
          <p className="mb-0.5">
            Modified{' '}
            <b>{Object.values(payload.changes.modified).flat().length}</b>
          </p>
          <p className="mb-0.5">
            Removed <b>{payload.changes.removed.length}</b>
          </p>
        </>
      )}
    </Step>
  )
}

function StepTestsCompleted({
  report,
}: {
  report: TestReportMessage
}): JSX.Element {
  return (
    <Step
      report={{
        status: 'success',
        ...report,
      }}
      trigger={
        <>
          <CheckCircleIcon className="w-6 mr-4" />
          <Title
            text="Tests Completed"
            size={EnumSize.sm}
            variant={EnumVariant.Success}
            className="w-full"
          />
        </>
      }
    >
      <p className="mb-0.5">{report.message}</p>
    </Step>
  )
}

function StepTestsFailed({ report }: { report: TestReportError }): JSX.Element {
  return (
    <Step
      variant={EnumVariant.Danger}
      report={{
        status: 'fail',
        ...report,
      }}
      trigger={
        <>
          <CheckCircleIcon className="w-6 mr-4" />
          <Title
            text="Tests Failed"
            size={EnumSize.sm}
            variant={EnumVariant.Danger}
            className="w-full"
          />
        </>
      }
    >
      <ReportTestsErrors report={report} />
    </Step>
  )
}

function StepValidate({ report }: { report: Map<string, any> }): JSX.Element {
  const payload = report.get('plan')

  return (
    <Step
      report={payload}
      states={[
        'Plan Validated',
        'Plan Validation Failed',
        'Validating Plan...',
      ]}
    />
  )
}

function StepEvaluate({
  report,
  children,
}: {
  report: Map<string, any>
  children: React.ReactNode
}): JSX.Element {
  const payload = report.get('evaluation')

  return (
    <div className="pt-6 pb-2">
      {isNotNil(payload.start_at) && (
        <>
          <small className="text-neutral-500 block px-4 mb-1">
            Evaluation started at{' '}
            <b>{new Date(payload.start_at).toLocaleString()}</b>
          </small>
          <Divider />
          <small className="text-neutral-500 block px-4 mt-1">
            Given a plan, it pushes snapshots into the state and then kicks off
            the backfill process for all affected snapshots. Once backfill is
            done, snapshots that are part of the plan are promoted in the
            environment targeted by this plan.
          </small>
        </>
      )}
      <div className="py-2">{children}</div>
      {isNotNil(payload.stop_at) && (
        <>
          <Divider />
          <small className="text-neutral-500 block px-4 mt-1">
            Evaluation stopped at{' '}
            <b>{new Date(payload.stop_at).toLocaleString()}</b>
          </small>
        </>
      )}
    </div>
  )
}

function StepCreation({
  report,
  isOpen,
}: {
  report: Map<string, any>
  isOpen?: boolean
}): JSX.Element {
  const payload = report.get('creation')

  return (
    <Step
      report={payload}
      states={[
        'Snapshot Tables Created',
        'Snapshot Tables Creation Failed',
        'Creating Snapshot Tables...',
      ]}
      isOpen={isOpen}
    >
      {isNotNil(payload.total_tasks) && (
        <TasksOverview.Block>
          <TasksOverview.Task>
            <TasksOverview.TaskDetails>
              <TasksOverview.TaskInfo>
                <TasksOverview.TaskHeadline headline="Snapshot Tables" />
              </TasksOverview.TaskInfo>
              <TasksOverview.DetailsProgress>
                <TasksOverview.TaskSize
                  completed={payload.total_tasks}
                  total={payload.num_tasks}
                  unit="task"
                />
                <TasksOverview.TaskDivider />
                <TasksOverview.TaskProgress
                  completed={payload.num_tasks}
                  total={payload.total_tasks}
                />
              </TasksOverview.DetailsProgress>
            </TasksOverview.TaskDetails>
            <Progress
              progress={toRatio(payload.num_tasks, payload.total_tasks)}
            />
          </TasksOverview.Task>
        </TasksOverview.Block>
      )}
    </Step>
  )
}

function StepRestate({ report }: { report: Map<string, any> }): JSX.Element {
  const payload = report.get('restate')

  return (
    <Step
      report={payload}
      states={[
        'Restate Models',
        'Restate Models Failed',
        'Restating Models...',
      ]}
    />
  )
}

function StepBackfill({ report }: { report: Map<string, any> }): JSX.Element {
  const {
    backfills,
    hasChanges,
    hasBackfills,
    activeBackfill,
    modified,
    added,
    removed,
    skip_backfill,
    change_categorization,
    hasVirtualUpdate,
  } = usePlan()
  const planState = useStorePlan(s => s.state)

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
  const payload = report.get('backfill')
  const tasks: PlanTasks = useMemo(
    (): PlanTasks =>
      isNil(payload?.tasks)
        ? filterBackfillsTasks(backfills)
        : filterActiveBackfillsTasks(payload?.tasks),
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

  const environment = report.get('environment')

  return (
    <Step
      report={payload}
      states={[
        'Intervals Backfilled',
        'Intervals Backfilling Failed',
        'Backfilling Intervals...',
      ]}
      showDetails={showDetails}
      isOpen={true}
    >
      <TasksOverview tasks={payload.tasks}>
        {({ total, completed, models, completedBatches, totalBatches }) => (
          <>
            <TasksOverview.Summary
              environment={environment}
              planState={planState}
              headline="Target Environment"
              completed={completed}
              total={total}
              completedBatches={completedBatches}
              totalBatches={totalBatches}
              updateType={hasVirtualUpdate ? 'Virtual' : 'Backfill'}
            />
            {isNotNil(models) && (
              <TasksOverview.Details
                models={models}
                queue={payload.queue}
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
    </Step>
  )
}

function StepPromote({ report }: { report: Map<string, any> }): JSX.Element {
  const payload = report.get('promotion')

  return (
    <Step
      report={payload}
      states={[
        'Environment Promoted',
        'Promotion Failed',
        'Promoting Environment...',
      ]}
    >
      <TasksOverview.Block>
        <TasksOverview.Task>
          <TasksOverview.TaskDetails>
            <TasksOverview.TaskInfo>
              <TasksOverview.TaskHeadline
                headline={`Promote Environment: ${payload.target_environment}`}
              />
            </TasksOverview.TaskInfo>
            <TasksOverview.DetailsProgress>
              <TasksOverview.TaskSize
                completed={payload.total_tasks}
                total={payload.num_tasks}
                unit="task"
              />
              <TasksOverview.TaskDivider />
              <TasksOverview.TaskProgress
                completed={payload.num_tasks}
                total={payload.total_tasks}
              />
            </TasksOverview.DetailsProgress>
          </TasksOverview.TaskDetails>
          <Progress
            progress={toRatio(payload.num_tasks, payload.total_tasks)}
          />
        </TasksOverview.Task>
      </TasksOverview.Block>
    </Step>
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
    <div className={clsx('w-full my-2', shouldBeFullAndCenter && 'h-[25vh]')}>
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
                  className="my-2 w-full"
                  headline="Modified Directly"
                  type={EnumPlanChangeType.Direct}
                >
                  <PlanChangePreview.Direct changes={modified.direct ?? []} />
                </PlanChangePreview>
              )}
              {isArrayNotEmpty(modified.indirect) && (
                <PlanChangePreview
                  className="my-2 w-full"
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
                  className="my-2 w-full"
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
  className,
}: PropsPlanWizardStepMessage): JSX.Element {
  return (
    <Banner
      className={className}
      variant={variant}
    >
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

function Step({
  report,
  states = ['Success', 'Failed', 'Running'],
  isOpen = false,
  trigger,
  panel,
  children,
  showDetails = true,
}: {
  variant?: Variant
  report: Record<string, any>
  trigger?: React.ReactNode
  panel?: React.ReactNode
  children?: React.ReactNode
  states?: [string, string, string]
  isOpen?: boolean
  showDetails?: boolean
}): JSX.Element {
  const variant =
    report.status === 'success'
      ? EnumVariant.Success
      : report.status === 'fail'
      ? EnumVariant.Danger
      : EnumVariant.Info
  const [titleSuccess, titleFail, titleDefault] = states
  const text =
    report.status === 'success'
      ? titleSuccess
      : report.status === 'fail'
      ? titleFail
      : titleDefault

  return (
    <Disclosure defaultOpen={isOpen}>
      {({ open }) => (
        <>
          <Banner
            className="my-2 flex items-center"
            variant={variant}
          >
            {isNil(trigger) ? (
              <>
                {report.status === 'success' && (
                  <CheckCircleIcon className="w-6 mr-4" />
                )}
                {report.status === 'fail' && (
                  <ExclamationCircleIcon className="w-6 mr-4" />
                )}
                {report.status === 'init' ? (
                  <Loading
                    text={text}
                    hasSpinner
                    size={EnumSize.sm}
                    variant={EnumVariant.Primary}
                    className="w-full"
                  />
                ) : (
                  <Title
                    text={text}
                    size={EnumSize.sm}
                    variant={variant}
                    className="w-full"
                  />
                )}
              </>
            ) : (
              trigger
            )}
            {showDetails && (
              <div className="flex items-center">
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
          <Disclosure.Panel className={clsx('px-2 text-xs')}>
            <div
              className={clsx(
                'p-4 rounded-md',
                variant === EnumVariant.Danger
                  ? 'bg-danger-10 text-danger-500'
                  : 'bg-neutral-5',
              )}
            >
              {isNotNil(report.start_at) && isNotNil(report.stop_at) && (
                <p className="mb-2">
                  {text} in <b>{(report.stop_at - report.start_at) / 1000}</b>{' '}
                  seconds
                </p>
              )}
              {children}
            </div>
            {report.status !== 'fail' && panel}
          </Disclosure.Panel>
        </>
      )}
    </Disclosure>
  )
}
