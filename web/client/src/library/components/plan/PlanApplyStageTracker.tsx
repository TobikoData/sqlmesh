import { Disclosure } from '@headlessui/react'
import {
  MinusCircleIcon,
  PlusCircleIcon,
  CheckCircleIcon,
  ExclamationCircleIcon,
} from '@heroicons/react/24/solid'
import clsx from 'clsx'
import { useEffect, useMemo, useRef } from 'react'
import { useStorePlan } from '../../../context/plan'
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
  SnapshotChangeCategory,
  type PlanOverviewStageTrackerStart,
  type PlanOverviewStageTrackerEnd,
  Status,
} from '@api/client'
import { type PlanTrackerMeta } from '@models/tracker-plan'
import { type Tests, useStoreProject } from '@context/project'
import { type ModelSQLMeshChangeDisplay } from '@models/sqlmesh-change-display'

interface PropsPlanStageMessage extends React.HTMLAttributes<HTMLElement> {
  hasSpinner?: boolean
  variant?: Variant
  index?: number
}

export default function PlanApplyStageTracker(): JSX.Element {
  const tests = useStoreProject(s => s.tests)

  const planApply = useStorePlan(s => s.planApply)
  const planOverview = useStorePlan(s => s.planOverview)
  const planAction = useStorePlan(s => s.planAction)

  const { start, end, hasChanges, hasBackfills, plan_options } =
    getPlanOverviewDetails(planApply, planOverview)

  const showTestsDetails = isNotNil(tests) && Boolean(tests.total)
  const showTestsMessage = isNotNil(tests) && Boolean(tests.message)

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
      {planAction.isRunning || isTrue(hasChanges) ? (
        <StageChanges />
      ) : (
        <PlanStageMessage
          variant={EnumVariant.Info}
          className="mt-2"
        >
          No Changes
        </PlanStageMessage>
      )}
      {planAction.isRunning || isTrue(hasBackfills) ? (
        <StageBackfills />
      ) : (
        <PlanStageMessage
          variant={EnumVariant.Info}
          className="mt-2"
        >
          No Backfills
        </PlanStageMessage>
      )}
      {isTrue(plan_options?.skip_tests) ? (
        <PlanStageMessage
          variant={EnumVariant.Info}
          className="mt-2"
        >
          Tests Skipped
        </PlanStageMessage>
      ) : (
        <>
          {showTestsMessage && <StageTestsCompleted report={tests} />}
          {showTestsDetails && <StageTestsFailed report={tests} />}
        </>
      )}
      <StageValidate />
      <StageVirtualUpdate />
      <StageEvaluate
        start={planApply.evaluationStart}
        end={planApply.evaluationEnd}
      >
        <StageCreation />
        <StageRestate />
        <StageBackfill />
        <StagePromote />
      </StageEvaluate>
    </div>
  )
}

function StageChanges({ isOpen = false }: { isOpen?: boolean }): JSX.Element {
  const planApply = useStorePlan(s => s.planApply)
  const planOverview = useStorePlan(s => s.planOverview)

  const { meta, stageChanges } = getPlanOverviewDetails(planApply, planOverview)

  return (
    <Stage
      meta={stageChanges?.meta ?? meta ?? { status: Status.init }}
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

function StageBackfills({ isOpen }: { isOpen?: boolean }): JSX.Element {
  const planApply = useStorePlan(s => s.planApply)
  const planOverview = useStorePlan(s => s.planOverview)

  const { meta, stageBackfills, backfills } = getPlanOverviewDetails(
    planApply,
    planOverview,
  )

  return (
    <Stage
      meta={stageBackfills?.meta ?? meta ?? { status: Status.init }}
      states={[
        'Backfills Collected',
        'Failed Getting Plan Backfills',
        'Getting Plan Backfills...',
      ]}
      isOpen={isOpen}
      panel={
        <PlanChangePreview
          headline={`Models ${backfills.length}`}
          type={EnumPlanChangeType.Default}
        >
          <PlanChangePreview.Default
            type={EnumPlanChangeType.Default}
            changes={backfills}
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

function StageValidate(): JSX.Element {
  const planApply = useStorePlan(s => s.planApply)
  const planOverview = useStorePlan(s => s.planOverview)

  const { stageValidation } = getPlanOverviewDetails(planApply, planOverview)

  return (
    <Stage
      meta={stageValidation?.meta}
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
  const tests = useStoreProject(s => s.tests)
  const planApply = useStorePlan(s => s.planApply)

  const elStageEvaluate = useRef<HTMLDivElement>(null)

  const hasFailedTests = isNotNil(tests) && Boolean(tests.failures)

  useEffect(() => {
    requestAnimationFrame(() => {
      elStageEvaluate.current?.scrollIntoView({
        behavior: 'smooth',
        block: 'start',
      })
    })
  }, [])

  if (isFalse(hasFailedTests) && isFalse(planApply.shouldShowEvaluation))
    return <></>

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

function StageCreation({ isOpen }: { isOpen?: boolean }): JSX.Element {
  const planApply = useStorePlan(s => s.planApply)

  if (isNil(planApply.stageCreation)) return <></>

  return (
    <Stage
      meta={planApply.stageCreation?.meta}
      states={[
        'Snapshot Tables Created',
        'Snapshot Tables Creation Failed',
        'Creating Snapshot Tables...',
      ]}
      isOpen={isOpen}
    >
      <TasksOverview.Block>
        <TasksOverview.Task>
          <TasksOverview.TaskDetails>
            <TasksOverview.TaskInfo>
              <TasksOverview.TaskHeadline headline="Snapshot Tables" />
            </TasksOverview.TaskInfo>
            <TasksOverview.DetailsProgress>
              <TasksOverview.TaskSize
                completed={planApply.stageCreation.total_tasks}
                total={planApply.stageCreation.num_tasks}
                unit="task"
              />
              <TasksOverview.TaskDivider />
              <TasksOverview.TaskProgress
                completed={planApply.stageCreation.num_tasks}
                total={planApply.stageCreation.total_tasks}
              />
            </TasksOverview.DetailsProgress>
          </TasksOverview.TaskDetails>
          <Progress
            progress={toRatio(
              planApply.stageCreation.num_tasks,
              planApply.stageCreation.total_tasks,
            )}
          />
        </TasksOverview.Task>
      </TasksOverview.Block>
    </Stage>
  )
}

function StageRestate(): JSX.Element {
  const planApply = useStorePlan(s => s.planApply)

  return isNil(planApply.stageRestate) ? (
    <PlanStageMessage
      variant={EnumVariant.Info}
      className="mt-2"
    >
      No Models To Restate
    </PlanStageMessage>
  ) : (
    <Stage
      meta={planApply.stageRestate?.meta}
      states={[
        'Restate Models',
        'Restate Models Failed',
        'Restating Models...',
      ]}
    />
  )
}

function StageBackfill(): JSX.Element {
  const planApply = useStorePlan(s => s.planApply)
  const planAction = useStorePlan(s => s.planAction)
  const environment = planApply.environment
  const stageBackfill = planApply.stageBackfill

  const { change_categorization } = usePlan()

  const categories = useMemo(
    () =>
      Array.from(change_categorization.values()).reduce<
        Record<string, boolean[]>
      >((acc, { category, change }) => {
        change.indirect?.forEach(c => {
          if (isNil(acc[c.name])) {
            acc[c.name] = []
          }

          acc[c.name]?.push(category.value !== SnapshotChangeCategory.NUMBER_1)
        })

        if (category.value === SnapshotChangeCategory.NUMBER_3) {
          acc[change.name] = [true]
        }

        return acc
      }, {}),
    [change_categorization],
  )

  const tasks = useMemo(
    () =>
      planApply.backfills.reduce(
        (acc: Record<string, ModelSQLMeshChangeDisplay>, model) => {
          const taskBackfill = planApply.tasks[model.name] ?? model

          taskBackfill.interval = model.interval ?? []

          const choices = categories[model.name]
          const shouldExclude = isNil(choices) ? false : choices.every(Boolean)

          if (shouldExclude) return acc

          acc[model.name] = taskBackfill

          return acc
        },
        {},
      ),
    [planApply.backfills, categories],
  )

  if (isNil(stageBackfill) || isNil(environment)) return <></>

  return (
    <Stage
      meta={stageBackfill.meta}
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
              headline="Target Environment"
              environment={environment}
              completed={completed}
              total={total}
              completedBatches={completedBatches}
              totalBatches={totalBatches}
              updateType={planAction.isApplyVirtual ? 'Virtual' : 'Backfill'}
            />
            {isNotNil(models) && (
              <TasksOverview.Details
                models={models}
                added={planApply.added}
                removed={planApply.removed}
                direct={planApply.direct}
                indirect={planApply.indirect}
                metadata={planApply.metadata}
                queue={planApply.queue}
                showBatches={true}
                showVirtualUpdate={planAction.isApplyVirtual}
                showProgress={true}
              />
            )}
          </>
        )}
      </TasksOverview>
    </Stage>
  )
}

function StagePromote(): JSX.Element {
  const planApply = useStorePlan(s => s.planApply)
  const elStagePromote = useRef<HTMLDivElement>(null)

  useEffect(() => {
    requestAnimationFrame(() => {
      elStagePromote.current?.scrollIntoView({
        behavior: 'smooth',
        block: 'start',
      })
    })
  }, [])

  if (isNil(planApply.stagePromote)) return <></>

  return (
    <div ref={elStagePromote}>
      <Stage
        meta={planApply.stagePromote?.meta}
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
                  headline={`Promote Environment: ${planApply.stagePromote.target_environment}`}
                />
              </TasksOverview.TaskInfo>
              <TasksOverview.DetailsProgress>
                <TasksOverview.TaskSize
                  completed={planApply.stagePromote.total_tasks}
                  total={planApply.stagePromote.num_tasks}
                  unit="task"
                />
                <TasksOverview.TaskDivider />
                <TasksOverview.TaskProgress
                  completed={planApply.stagePromote.num_tasks}
                  total={planApply.stagePromote.total_tasks}
                />
              </TasksOverview.DetailsProgress>
            </TasksOverview.TaskDetails>
            <Progress
              progress={toRatio(
                planApply.stagePromote.num_tasks,
                planApply.stagePromote.total_tasks,
              )}
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
  const planAction = useStorePlan(s => s.planAction)

  const { hasChanges, added, removed, direct, indirect, metadata } =
    getPlanOverviewDetails(planApply, planOverview)

  return (
    <div className="w-full my-2">
      {planAction.isRunning && isNil(hasChanges) && (
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
      {isFalse(hasChanges) && (
        <Banner
          isFull={isFalse(planAction.isApplyVirtual)}
          isCenter={isFalse(planAction.isApplyVirtual)}
        >
          <Title
            size={planAction.isApplyVirtual ? EnumSize.md : EnumSize.lg}
            text="No Changes"
          />
        </Banner>
      )}
      {isTrue(hasChanges) && (
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
          {isArrayNotEmpty(direct) && (
            <PlanChangePreview
              className="my-2 w-full"
              headline="Modified Directly"
              type={EnumPlanChangeType.Direct}
            >
              <PlanChangePreview.Direct changes={direct} />
            </PlanChangePreview>
          )}
          {isArrayNotEmpty(indirect) && (
            <PlanChangePreview
              className="my-2 w-full"
              headline="Modified Indirectly"
              type={EnumPlanChangeType.Indirect}
            >
              <PlanChangePreview.Indirect changes={indirect} />
            </PlanChangePreview>
          )}
          {isArrayNotEmpty(metadata) && (
            <PlanChangePreview
              className="my-2 w-full"
              headline="Modified Metadata"
              type={EnumPlanChangeType.Default}
            >
              <PlanChangePreview.Default
                type={EnumPlanChangeType.Default}
                changes={metadata}
              />
            </PlanChangePreview>
          )}
        </>
      )}
    </div>
  )
}

function StageVirtualUpdate(): JSX.Element {
  const planApply = useStorePlan(s => s.planApply)
  const planOverview = useStorePlan(s => s.planOverview)

  const { virtualUpdateDescription } = usePlan()

  if (
    isFalseOrNil(planApply.overview?.isVirtualUpdate) &&
    isFalse(planOverview.isVirtualUpdate)
  )
    return <></>

  const isUpdated = isTrue(planApply.stagePromote?.meta?.done)

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
  meta?: PlanTrackerMeta
  trigger?: React.ReactNode
  panel?: React.ReactNode
  children?: React.ReactNode
  states?: [string, string, string]
  isOpen?: boolean
  showDetails?: boolean
}): JSX.Element {
  if (isNil(meta)) return <></>

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
