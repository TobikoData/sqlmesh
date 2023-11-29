import { Disclosure } from '@headlessui/react'
import {
  MinusCircleIcon,
  PlusCircleIcon,
  CheckCircleIcon,
  ExclamationCircleIcon,
} from '@heroicons/react/24/solid'
import clsx from 'clsx'
import { useEffect, useMemo, useRef } from 'react'
import {
  useStorePlan,
  type PlanTasks,
  type PlanTaskStatus,
} from '../../../context/plan'
import {
  isArrayNotEmpty,
  isFalse,
  isNotNil,
  toRatio,
  isTrue,
  isNil,
  toDateFormat,
  isFalseOrNil,
} from '../../../utils'
import Spinner from '../logo/Spinner'
import { EnumPlanChangeType, usePlan } from './context'
import { getPlanOverviewDetails } from './help'
import PlanChangePreview from './PlanChangePreview'
import { EnumSize, EnumVariant, type Variant } from '~/types/enum'
import Banner from '@components/banner/Banner'
import TasksOverview from '../tasksOverview/TasksOverview'
import Loading from '@components/loading/Loading'
import Title from '@components/title/Title'
import ReportTestsErrors from '@components/report/ReportTestsErrors'
import { Divider } from '@components/divider/Divider'
import Progress from '@components/progress/Progress'
import {
  type PlanStageBackfill,
  type PlanStageCreation,
  type PlanStagePromote,
  type PlanStageRestate,
  type PlanStageBackfills,
  type PlanStageChanges,
  type PlanStageValidation,
  SnapshotChangeCategory,
  type PlanOverviewStageTrackerStart,
  type PlanOverviewStageTrackerEnd,
} from '@api/client'
import { type PlanTrackerMeta } from '@models/tracker-plan'
import { type Tests, useStoreProject } from '@context/project'

interface PropsPlanStageMessage extends React.HTMLAttributes<HTMLElement> {
  hasSpinner?: boolean
  variant?: Variant
  index?: number
}

export default function PlanApplyStageTracker(): JSX.Element {
  const tests = useStoreProject(s => s.tests)

  const planApply = useStorePlan(s => s.planApply)
  const planOverview = useStorePlan(s => s.planOverview)

  const {
    start,
    end,
    hasChanges,
    hasBackfills,
    changes,
    backfills,
    validation,
    plan_options,
    isVirtualUpdate,
  } = getPlanOverviewDetails(planApply, planOverview)

  const showTestsDetails = isNotNil(tests) && Boolean(tests.total)
  const showTestsMessage = isNotNil(tests) && Boolean(tests.message)
  const hasFailedTests = isNotNil(tests) && Boolean(tests.failures)

  return (
    <div className="py-4">
      {isNotNil(start) && isNotNil(end) && (
        <div className="flex justify-between items-center mb-4 whitespace-nowrap">
          <small className="text-neutral-500 block px-4">
            Start Date:&nbsp;<b>{toDateFormat(new Date(start))}</b>
          </small>
          <Divider />
          <small className="text-neutral-500 block px-4">
            End Date:&nbsp;<b>{toDateFormat(new Date(end))}</b>
          </small>
        </div>
      )}
      {isFalseOrNil(hasChanges) || isNil(changes) ? (
        <PlanStageMessage
          variant={EnumVariant.Info}
          className="mt-2"
        >
          No Changes
        </PlanStageMessage>
      ) : (
        <StageChanges
          report={changes}
          isOpen={true}
        />
      )}
      {isFalseOrNil(hasBackfills) || isNil(backfills) ? (
        <PlanStageMessage
          variant={EnumVariant.Info}
          className="mt-2"
        >
          No Backfills
        </PlanStageMessage>
      ) : (
        <StageBackfills report={backfills} />
      )}
      {isTrue(plan_options?.skip_tests) && (
        <PlanStageMessage
          variant={EnumVariant.Info}
          className="mt-2"
        >
          Tests Skipped
        </PlanStageMessage>
      )}
      {showTestsMessage && <StageTestsCompleted report={tests} />}
      {showTestsDetails && <StageTestsFailed report={tests} />}
      {isNotNil(validation) && <StageValidate report={validation} />}
      {isTrue(isVirtualUpdate) && (
        <PlanVirtualUpdate isUpdated={isTrue(planApply.promote?.meta?.done)} />
      )}
      {hasFailedTests ||
        (planApply.shouldShowEvaluation && (
          <StageEvaluate
            start={planApply.evaluationStart}
            end={planApply.evaluationEnd}
          >
            {isNotNil(planApply.creation) && (
              <StageCreation report={planApply.creation} />
            )}
            {isNotNil(planApply.restate) ? (
              <StageRestate report={planApply.restate} />
            ) : (
              <PlanStageMessage
                variant={EnumVariant.Info}
                className="mt-2"
              >
                No Models To Restate
              </PlanStageMessage>
            )}
            {isNotNil(planApply.backfill) &&
              isNotNil(planApply.environment) && (
                <StageBackfill
                  backfill={planApply.backfill}
                  backfills={backfills}
                  isVirtualUpdate={isVirtualUpdate}
                  environment={planApply.environment}
                />
              )}
            {isNotNil(planApply.promote) && (
              <StagePromote report={planApply.promote} />
            )}
          </StageEvaluate>
        ))}
    </div>
  )
}

function StageChanges({
  report,
  isOpen = false,
}: {
  isOpen?: boolean
  report: PlanStageChanges
}): JSX.Element {
  return (
    <Stage
      meta={report.meta!}
      states={[
        'Changes Collected',
        'Failed Getting Plan Changes',
        'Getting Plan Changes...',
      ]}
      isOpen={isOpen}
      panel={<PlanChanges />}
    />
  )
}

function StageBackfills({
  report,
  isOpen = false,
}: {
  isOpen?: boolean
  report: PlanStageBackfills
}): JSX.Element {
  return (
    <Stage
      meta={report.meta!}
      states={[
        'Backfills Collected',
        'Failed Getting Plan Backfills',
        'Getting Plan Backfills...',
      ]}
      isOpen={isOpen}
      panel={
        <PlanChangePreview
          headline={`Models ${report.models?.length ?? 0}`}
          type={EnumPlanChangeType.Default}
        >
          <PlanChangePreview.Default
            type={EnumPlanChangeType.Default}
            changes={
              (report?.models?.map(
                ({ model_name }: any) => model_name,
              ) as string[]) ?? []
            }
          />
        </PlanChangePreview>
      }
    />
  )
}

function StageTestsCompleted({ report }: { report: Tests }): JSX.Element {
  return (
    <Stage
      meta={{
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
    </Stage>
  )
}

function StageTestsFailed({ report }: { report: Tests }): JSX.Element {
  return (
    <Stage
      variant={EnumVariant.Danger}
      meta={{
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
    </Stage>
  )
}

function StageValidate({
  report,
}: {
  report: PlanStageValidation
}): JSX.Element {
  return (
    <Stage
      meta={report.meta!}
      states={[
        'Plan Validated',
        'Plan Validation Failed',
        'Validating Plan...',
      ]}
      showDetails={false}
    />
  )
}

function StageEvaluate({
  start,
  end,
  children,
}: {
  start?: PlanOverviewStageTrackerStart
  end?: PlanOverviewStageTrackerEnd
  children: React.ReactNode
}): JSX.Element {
  const elStageEvaluate = useRef<HTMLDivElement>(null)

  useEffect(() => {
    requestAnimationFrame(() => {
      elStageEvaluate.current?.scrollIntoView({
        behavior: 'smooth',
        block: 'start',
      })
    })
  }, [])

  return (
    <div
      ref={elStageEvaluate}
      className="pt-6 pb-2"
    >
      {isNotNil(start) && (
        <>
          <small className="text-neutral-500 block px-4 mb-1">
            Evaluation started at{' '}
            <b>{toDateFormat(new Date(start), 'yyyy-mm-dd hh-mm-ss')}</b>
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
      {isNotNil(end) && (
        <>
          <Divider />
          <small className="text-neutral-500 block px-4 mt-1">
            Evaluation stopped at{' '}
            <b>{toDateFormat(new Date(end), 'yyyy-mm-dd hh-mm-ss')}</b>
          </small>
        </>
      )}
    </div>
  )
}

function StageCreation({
  report,
  isOpen,
}: {
  report: PlanStageCreation
  isOpen?: boolean
}): JSX.Element {
  return (
    <Stage
      meta={report.meta!}
      states={[
        'Snapshot Tables Created',
        'Snapshot Tables Creation Failed',
        'Creating Snapshot Tables...',
      ]}
      isOpen={isOpen}
    >
      {isNotNil(report.total_tasks) && (
        <TasksOverview.Block>
          <TasksOverview.Task>
            <TasksOverview.TaskDetails>
              <TasksOverview.TaskInfo>
                <TasksOverview.TaskHeadline headline="Snapshot Tables" />
              </TasksOverview.TaskInfo>
              <TasksOverview.DetailsProgress>
                <TasksOverview.TaskSize
                  completed={report.total_tasks}
                  total={report.num_tasks}
                  unit="task"
                />
                <TasksOverview.TaskDivider />
                <TasksOverview.TaskProgress
                  completed={report.num_tasks}
                  total={report.total_tasks}
                />
              </TasksOverview.DetailsProgress>
            </TasksOverview.TaskDetails>
            <Progress
              progress={toRatio(report.num_tasks, report.total_tasks)}
            />
          </TasksOverview.Task>
        </TasksOverview.Block>
      )}
    </Stage>
  )
}

function StageRestate({ report }: { report: PlanStageRestate }): JSX.Element {
  return (
    <Stage
      meta={report.meta!}
      states={[
        'Restate Models',
        'Restate Models Failed',
        'Restating Models...',
      ]}
    />
  )
}

function StageBackfill({
  environment,
  backfill,
  isVirtualUpdate,
  backfills,
}: {
  environment: string
  backfill: PlanStageBackfill
  isVirtualUpdate: boolean
  backfills?: PlanStageBackfills
}): JSX.Element {
  const { change_categorization } = usePlan()

  const categories = useMemo(
    () =>
      Array.from(change_categorization.values()).reduce<
        Record<string, boolean[]>
      >((acc, { category, change }) => {
        change?.indirect?.forEach(model => {
          if (isNil(acc[model])) {
            acc[model] = []
          }

          acc[model]?.push(category.value !== SnapshotChangeCategory.NUMBER_1)
        })

        if (category.value === SnapshotChangeCategory.NUMBER_3) {
          acc[change.model_name] = [true]
        }

        return acc
      }, {}),
    [change_categorization],
  )

  const tasks: PlanTasks = useMemo(
    () =>
      backfills?.models?.reduce((acc: PlanTasks, model) => {
        const taskModelName = model.model_name ?? model.view_name
        const taskInterval = model.interval as [string, string]
        const taskBackfill: PlanTaskStatus = {
          completed: 0,
          total: model.batches,
          interval: taskInterval,
          view_name: model.view_name,
          ...backfill?.tasks?.[taskModelName],
        }
        const choices = categories[taskModelName]
        const shouldExclude = isNil(choices) ? false : choices.every(Boolean)

        if (shouldExclude) return acc

        acc[taskModelName] = taskBackfill

        return acc
      }, {}) ?? {},
    [backfill, backfills, change_categorization],
  )

  return (
    <Stage
      meta={backfill.meta!}
      states={[
        'Intervals Backfilled',
        'Intervals Backfilling Failed',
        'Backfilling Intervals...',
      ]}
      showDetails={true}
      isOpen={true}
    >
      <TasksOverview tasks={tasks}>
        {({ total, completed, models, completedBatches, totalBatches }) => (
          <>
            <TasksOverview.Summary
              environment={environment}
              headline="Target Environment"
              completed={completed}
              total={total}
              completedBatches={completedBatches}
              totalBatches={totalBatches}
              updateType={isVirtualUpdate ? 'Virtual' : 'Backfill'}
            />
            {isNotNil(models) && (
              <TasksOverview.Details
                models={models}
                queue={backfill.queue}
                showBatches={true}
                showVirtualUpdate={isVirtualUpdate}
                showProgress={true}
              />
            )}
          </>
        )}
      </TasksOverview>
    </Stage>
  )
}

function StagePromote({ report }: { report: PlanStagePromote }): JSX.Element {
  const elStagePromote = useRef<HTMLDivElement>(null)

  useEffect(() => {
    requestAnimationFrame(() => {
      elStagePromote.current?.scrollIntoView({
        behavior: 'smooth',
        block: 'start',
      })
    })
  }, [])

  return (
    <div ref={elStagePromote}>
      <Stage
        meta={report.meta!}
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
                  headline={`Promote Environment: ${report.target_environment}`}
                />
              </TasksOverview.TaskInfo>
              <TasksOverview.DetailsProgress>
                <TasksOverview.TaskSize
                  completed={report.total_tasks}
                  total={report.num_tasks}
                  unit="task"
                />
                <TasksOverview.TaskDivider />
                <TasksOverview.TaskProgress
                  completed={report.num_tasks}
                  total={report.total_tasks}
                />
              </TasksOverview.DetailsProgress>
            </TasksOverview.TaskDetails>
            <Progress
              progress={toRatio(report.num_tasks, report.total_tasks)}
            />
          </TasksOverview.Task>
        </TasksOverview.Block>
      </Stage>
    </div>
  )
}

function PlanChanges(): JSX.Element {
  const planOverview = useStorePlan(s => s.planOverview)
  const planApply = useStorePlan(s => s.planApply)

  const { hasChanges, changes, isVirtualUpdate, isRunning } =
    getPlanOverviewDetails(planApply, planOverview)

  const shouldBeFullAndCenter =
    (isFalse(hasChanges) && isFalse(isRunning) && isFalse(isVirtualUpdate)) ||
    isRunning

  const { added = [], removed = [], modified = {} } = changes ?? {}

  return (
    <div className={clsx('w-full my-2', shouldBeFullAndCenter && 'h-[25vh]')}>
      {isRunning && (
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
      {isFalse(hasChanges) && isFalse(isRunning) && (
        <Banner
          isFull={isFalse(isVirtualUpdate)}
          isCenter={isFalse(isVirtualUpdate)}
        >
          <Title
            size={isVirtualUpdate ? EnumSize.md : EnumSize.lg}
            text="No Changes"
          />
        </Banner>
      )}
      {isTrue(hasChanges) && isFalse(isRunning) && (
        <>
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
          {isArrayNotEmpty(modified?.direct) && (
            <PlanChangePreview
              className="my-2 w-full"
              headline="Modified Directly"
              type={EnumPlanChangeType.Direct}
            >
              <PlanChangePreview.Direct changes={modified.direct ?? []} />
            </PlanChangePreview>
          )}
          {isArrayNotEmpty(modified?.indirect) && (
            <PlanChangePreview
              className="my-2 w-full"
              headline="Modified Indirectly"
              type={EnumPlanChangeType.Indirect}
            >
              <PlanChangePreview.Indirect changes={modified.indirect ?? []} />
            </PlanChangePreview>
          )}
          {isArrayNotEmpty(modified?.metadata) && (
            <PlanChangePreview
              className="my-2 w-full"
              headline="Modified Metadata"
              type={EnumPlanChangeType.Default}
            >
              <PlanChangePreview.Default
                type={EnumPlanChangeType.Default}
                changes={modified?.metadata ?? []}
              />
            </PlanChangePreview>
          )}
        </>
      )}
    </div>
  )
}

function PlanVirtualUpdate({
  isUpdated = false,
}: {
  isUpdated: boolean
}): JSX.Element {
  const { virtualUpdateDescription } = usePlan()
  return (
    <Disclosure>
      {({ open }) => (
        <>
          <Banner
            className="my-2 flex items-center"
            variant={isUpdated ? EnumVariant.Success : EnumVariant.Info}
          >
            <>
              {isUpdated && <CheckCircleIcon className="w-6 mr-4" />}
              <Title
                className="w-full"
                text={isUpdated ? 'Virtual Update Completed' : 'Virtual Update'}
                size={EnumSize.sm}
                variant={isUpdated ? EnumVariant.Success : EnumVariant.Info}
              />
            </>

            <div className="flex items-center">
              <Disclosure.Button className="flex items-center justify-between rounded-lg text-left text-sm">
                {open ? (
                  <MinusCircleIcon className="w-5" />
                ) : (
                  <PlusCircleIcon className="w-5" />
                )}
              </Disclosure.Button>
            </div>
          </Banner>
          <Disclosure.Panel className="px-2 text-xs">
            <div className="p-4 rounded-md bg-neutral-5">
              {virtualUpdateDescription}
            </div>
          </Disclosure.Panel>
        </>
      )}
    </Disclosure>
  )
}

function PlanStageMessage({
  hasSpinner = false,
  variant = EnumVariant.Primary,
  index,
  children,
  className,
}: PropsPlanStageMessage): JSX.Element {
  return (
    <Banner
      className={className}
      variant={variant}
    >
      <span className="flex items-center w-full">
        {isNotNil(index) && (
          <span className="inline-block mr-3 font-black whitespace-nowrap">
            Stage {index}:
          </span>
        )}
        {hasSpinner && <Spinner className="w-4 h-4 mr-2" />}
        {children}
      </span>
    </Banner>
  )
}

function Stage({
  meta,
  states = ['Success', 'Failed', 'Running'],
  isOpen = false,
  trigger,
  panel,
  children,
  showDetails = true,
}: {
  variant?: Variant
  meta: PlanTrackerMeta
  trigger?: React.ReactNode
  panel?: React.ReactNode
  children?: React.ReactNode
  states?: [string, string, string]
  isOpen?: boolean
  showDetails?: boolean
}): JSX.Element {
  const variant =
    meta.status === 'success'
      ? EnumVariant.Success
      : meta.status === 'fail'
      ? EnumVariant.Danger
      : EnumVariant.Info
  const [titleSuccess, titleFail, titleDefault] = states
  const text =
    meta.status === 'success'
      ? titleSuccess
      : meta.status === 'fail'
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
                {meta.status === 'success' && (
                  <CheckCircleIcon className="w-6 mr-4" />
                )}
                {meta.status === 'fail' && (
                  <ExclamationCircleIcon className="w-6 mr-4" />
                )}
                {meta.status === 'init' ? (
                  <Loading
                    text={text}
                    hasSpinner
                    size={EnumSize.sm}
                    variant={EnumVariant.Primary}
                    className="w-full"
                  />
                ) : (
                  <span className="flex w-full items-baseline">
                    <Title
                      text={text}
                      size={EnumSize.sm}
                      variant={variant}
                    />
                    {isNotNil(meta.duration) && (
                      <p className="text-xs text-neutral-400 inline-block ml-2">
                        in <b>{meta.duration / 1000}</b> seconds
                      </p>
                    )}
                  </span>
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
          <Disclosure.Panel className="px-2 text-xs">
            {isNotNil(children) && (
              <div
                className={clsx(
                  'p-4 rounded-md',
                  variant === EnumVariant.Danger
                    ? 'bg-danger-10 text-danger-500'
                    : 'bg-neutral-5',
                )}
              >
                {children}
              </div>
            )}
            {meta.status !== 'fail' && panel}
          </Disclosure.Panel>
        </>
      )}
    </Disclosure>
  )
}
