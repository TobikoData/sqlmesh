import { Disclosure, Transition } from '@headlessui/react'
import {
  MinusCircleIcon,
  PlusCircleIcon,
  ExclamationCircleIcon,
} from '@heroicons/react/24/solid'
import { CheckIcon } from '@heroicons/react/20/solid'
import clsx from 'clsx'
import { useEffect, useMemo, useRef, useState } from 'react'
import { useStorePlan } from '../../../context/plan'
import {
  isArrayNotEmpty,
  isNotNil,
  toRatio,
  isTrue,
  isNil,
  toDateFormat,
  isFalse,
  isArrayEmpty,
} from '../../../utils'
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

export default function PlanApplyStageTracker(): JSX.Element {
  const tests = useStoreProject(s => s.tests)

  const planApply = useStorePlan(s => s.planApply)
  const planOverview = useStorePlan(s => s.planOverview)
  const planAction = useStorePlan(s => s.planAction)

  const { plan_options } = getPlanOverviewDetails(planApply, planOverview)

  const hasTestsDetails = isNotNil(tests) && Boolean(tests.total)
  const showTestsMessage =
    isNotNil(tests) && Boolean(tests.message) && isFalse(hasTestsDetails)

  return (
    <Transition
      show={isFalse(planAction.isRun)}
      enter="transition ease duration-500 transform"
      enterFrom="opacity-0 scale-95"
      enterTo="opacity-100 scale-100"
      leave="transition ease duration-500 transform"
      leaveFrom="opacity-100 scale-100"
      leaveTo="opacity-0 scale-95"
    >
      <div className="mt-8 mb-4">
        <StageChanges isOpen={isFalse(planAction.isApplying)} />
        <StageBackfills isOpen={isFalse(planAction.isApplying)} />
        {isTrue(plan_options?.skip_tests) ? (
          <Banner
            className="flex items-center mb-1"
            size={EnumSize.sm}
            hasBackground={false}
          >
            <CheckIcon className="w-5 mr-2" />
            <Banner.Label className="mr-2 text-sm">Tests Skipped</Banner.Label>
          </Banner>
        ) : hasTestsDetails ? (
          <StageTestsFailed
            isOpen={true}
            report={tests}
          />
        ) : showTestsMessage ? (
          <StageTestsCompleted report={tests} />
        ) : (
          <Banner
            className="flex items-center mb-1"
            size={EnumSize.sm}
            hasBackground={false}
          >
            <CheckIcon className="w-5 mr-2" />
            <Banner.Label className="mr-2 text-sm">No Tests</Banner.Label>
          </Banner>
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
    </Transition>
  )
}

function StageChanges({ isOpen = false }: { isOpen?: boolean }): JSX.Element {
  const planApply = useStorePlan(s => s.planApply)
  const planOverview = useStorePlan(s => s.planOverview)
  const planAction = useStorePlan(s => s.planAction)

  const { meta, stageChanges, hasChanges } = getPlanOverviewDetails(
    planApply,
    planOverview,
  )

  const showChanges = planAction.isRunning || isTrue(hasChanges)

  return showChanges ? (
    <Transition
      show={showChanges}
      enter="transition ease duration-700 transform"
      enterFrom="opacity-0 scale-95"
      enterTo="opacity-100 scale-100"
      leave="transition ease duration-1000 transform"
      leaveFrom="opacity-100 scale-100"
      leaveTo="opacity-0 scale-95"
    >
      <Stage
        meta={stageChanges?.meta ?? meta ?? { status: Status.init }}
        states={['Changes', 'Failed Getting Changes', 'Getting Changes...']}
        isOpen={isOpen}
        panel={<PlanChanges />}
      />
    </Transition>
  ) : (
    <Banner
      className="flex items-center mb-1"
      size={EnumSize.sm}
      hasBackground={false}
    >
      <CheckIcon className="w-5 mr-2" />
      <Banner.Label className="mr-2 text-sm">No Changes</Banner.Label>
    </Banner>
  )
}

function StageBackfills({ isOpen }: { isOpen?: boolean }): JSX.Element {
  const planApply = useStorePlan(s => s.planApply)
  const planOverview = useStorePlan(s => s.planOverview)
  const planAction = useStorePlan(s => s.planAction)

  const { meta, stageBackfills, backfills, hasBackfills } =
    getPlanOverviewDetails(planApply, planOverview)

  const showBackfills = planAction.isRunning || isTrue(hasBackfills)

  return showBackfills ? (
    <Transition
      show={showBackfills}
      enter="transition ease duration-700 transform"
      enterFrom="opacity-0 scale-95"
      enterTo="opacity-100 scale-100"
      leave="transition ease duration-1000 transform"
      leaveFrom="opacity-100 scale-100"
      leaveTo="opacity-0 scale-95"
    >
      <Stage
        meta={stageBackfills?.meta ?? meta ?? { status: Status.init }}
        states={[
          'Backfills',
          'Failed Getting Backfills',
          'Getting Backfills...',
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
    </Transition>
  ) : (
    <Banner
      className="flex items-center mb-1"
      size={EnumSize.sm}
      hasBackground={false}
    >
      <CheckIcon className="w-5 mr-2" />
      <Banner.Label className="mr-2 text-sm">No Backfills</Banner.Label>
    </Banner>
  )
}

function StageTestsCompleted({ report }: { report: Tests }): JSX.Element {
  return (
    <Stage
      meta={{
        status: Status.success,
        ...report,
      }}
      states={['Tests Completed', 'Failed Tests', 'Running Tests...']}
    >
      {report.message}
    </Stage>
  )
}

function StageTestsFailed({
  report,
  isOpen = false,
}: {
  report: Tests
  isOpen: boolean
}): JSX.Element {
  return (
    <Stage
      variant={EnumVariant.Danger}
      meta={{
        status: Status.fail,
        ...report,
      }}
      isOpen={isOpen}
      states={['Tests Failed', 'One or More Tests Failed', 'Running Tests...']}
      shouldCollapse={false}
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

  return (
    <Transition
      show={hasFailedTests || planApply.shouldShowEvaluation}
      enter="transition ease duration-500 transform delay-[1000ms]"
      enterFrom="opacity-0 scale-95"
      enterTo="opacity-100 scale-100"
      leave="transition ease duration-500 transform"
      leaveFrom="opacity-100 scale-100"
      leaveTo="opacity-0 scale-95"
    >
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
              Given a plan, it pushes snapshots into the state and then kicks
              off the backfill process for all affected snapshots. Once backfill
              is done, snapshots that are part of the plan are promoted in the
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
    </Transition>
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
    <Banner
      className="flex items-center mb-1"
      size={EnumSize.sm}
      hasBackground={false}
    >
      <CheckIcon className="w-5 mr-2" />
      <Banner.Label className="mr-2 text-sm">No Models To Restate</Banner.Label>
    </Banner>
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
  const elStageBackfill = useRef<HTMLDivElement>(null)

  const planApply = useStorePlan(s => s.planApply)
  const planAction = useStorePlan(s => s.planAction)

  const environment = planApply.environment
  const stageBackfill = planApply.stageBackfill

  useEffect(() => {
    setTimeout(() => {
      elStageBackfill.current?.scrollIntoView({
        behavior: 'smooth',
        block: 'start',
      })
    }, 1000)
  }, [])

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
      isArrayEmpty(planApply.backfills)
        ? Object.entries(planApply.tasks).reduce(
            (acc: Record<string, ModelSQLMeshChangeDisplay>, [name, task]) => {
              const choices = categories[name]
              const shouldExclude = isNil(choices)
                ? false
                : choices.every(Boolean)

              if (shouldExclude) return acc

              acc[name] = task

              return acc
            },
            {},
          )
        : planApply.backfills.reduce(
            (acc: Record<string, ModelSQLMeshChangeDisplay>, model) => {
              const taskBackfill = planApply.tasks[model.name] ?? model

              taskBackfill.interval = model.interval ?? []

              const choices = categories[model.name]
              const shouldExclude = isNil(choices)
                ? false
                : choices.every(Boolean)

              if (shouldExclude) return acc

              acc[model.name] = taskBackfill

              return acc
            },
            {},
          ),
    [planApply.backfills, planApply.tasks, categories],
  )

  if (isNil(stageBackfill) || isNil(environment)) return <></>

  return (
    <div ref={elStageBackfill}>
      <Stage
        meta={stageBackfill.meta}
        states={[
          'Intervals Backfilled',
          'Intervals Backfilling Failed',
          'Backfilling Intervals...',
        ]}
        showDetails={true}
        isOpen={true}
        shouldCollapse={false}
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
    </div>
  )
}

function StagePromote(): JSX.Element {
  const planApply = useStorePlan(s => s.planApply)

  if (isNil(planApply.stagePromote)) return <></>

  return (
    <div>
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

function StageVirtualUpdate(): JSX.Element {
  const { virtualUpdateDescription } = usePlan()

  const planApply = useStorePlan(s => s.planApply)
  const planOverview = useStorePlan(s => s.planOverview)
  const isVirtualUpdate = planOverview.isLatest
    ? isTrue(planApply.overview?.isVirtualUpdate)
    : planOverview.isVirtualUpdate
  const isUpdated = isTrue(planApply.stagePromote?.meta?.done)

  return isVirtualUpdate ? (
    <Stage
      meta={{
        status: Status.success,
        done: isUpdated,
      }}
      states={[
        isUpdated ? 'Virtual Update Completed' : 'Virtual Update',
        'Virtual Update Failed',
        'Applying Virtual Update...',
      ]}
    >
      {virtualUpdateDescription}
    </Stage>
  ) : (
    <></>
  )
}

function PlanChanges(): JSX.Element {
  const planOverview = useStorePlan(s => s.planOverview)
  const planApply = useStorePlan(s => s.planApply)

  const { hasChanges, added, removed, direct, indirect, metadata } =
    getPlanOverviewDetails(planApply, planOverview)

  return (
    <div className="w-full my-2">
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

function Stage({
  meta,
  states = ['Success', 'Failed', 'Running'],
  isOpen = false,
  trigger,
  panel,
  children,
  showDetails = true,
  shouldCollapse = true,
}: {
  variant?: Variant
  meta?: PlanTrackerMeta
  trigger?: React.ReactNode
  panel?: React.ReactNode
  children?: React.ReactNode
  states?: [string, string, string]
  isOpen?: boolean
  showDetails?: boolean
  shouldCollapse?: boolean
}): JSX.Element {
  const elTrigger = useRef<HTMLButtonElement>(null)

  const planOverview = useStorePlan(s => s.planOverview)
  const planApply = useStorePlan(s => s.planApply)

  const hasChildren = isNotNil(panel) || isNotNil(children)

  const [open, setOpen] = useState(false)

  useEffect(() => {
    if (isNil(elTrigger.current)) return

    if (shouldCollapse && (planApply.isFinished || planOverview.isLatest)) {
      if (elTrigger.current.classList.contains('--is-open')) {
        elTrigger.current?.click()
      }
    }
  }, [elTrigger, planOverview, planApply, shouldCollapse])

  useEffect(() => {
    setOpen(isOpen || (meta?.status !== Status.init && hasChildren))
  }, [meta?.status, hasChildren, isOpen])

  if (isNil(meta)) return <></>

  const variant =
    meta.status === Status.success
      ? EnumVariant.Success
      : meta.status === Status.fail
      ? EnumVariant.Danger
      : EnumVariant.Info
  const [titleSuccess, titleFail, titleDefault] = states
  const text =
    meta.status === Status.success
      ? titleSuccess
      : meta.status === Status.fail
      ? titleFail
      : titleDefault

  return (
    <Transition
      appear
      show
      enter="transition-all ease-in duration-500 delay-[200ms]"
      enterFrom="opacity-0 translate-y-6"
      enterTo="opacity-100 translate-y-0"
      leave="transition-all ease-out duration-200"
      leaveFrom="opacity-100"
      leaveTo="opacity-0"
    >
      <Disclosure>
        <Banner
          className="mb-1"
          variant={variant}
          size={EnumSize.sm}
          hasBackground={false}
          hasBackgroundOnHover={hasChildren}
        >
          <Disclosure.Button
            ref={elTrigger}
            className={clsx(
              'w-full flex flex-col',
              open && '--is-open',
              isFalse(hasChildren) && 'cursor-default',
            )}
            onClick={() => setOpen(oldState => !oldState)}
          >
            {isNil(trigger) ? (
              <>
                <Transition
                  show={meta.status === Status.init}
                  className="flex items-center h-full"
                  enter="transition-all ease-in-out duration-500"
                  enterFrom="opacity-0 translate-y-6"
                  enterTo="opacity-100 translate-y-0"
                  leave="transition-all ease-in-out duration-200"
                  leaveFrom="opacity-100"
                  leaveTo="opacity-0"
                >
                  <Loading
                    text={text}
                    hasSpinner
                    size={EnumSize.sm}
                    variant={EnumVariant.Primary}
                    className="w-full"
                  />
                </Transition>
                <Transition
                  show={meta.status !== Status.init}
                  className="flex items-center h-full"
                  enter="transition-all ease-in-out duration-400 delay-[300ms]"
                  enterFrom="opacity-0 translate-y-6"
                  enterTo="opacity-100 translate-y-0"
                  leave="transition-all ease-in-out duration-200"
                  leaveFrom="opacity-100"
                  leaveTo="opacity-0"
                >
                  {showDetails ? (
                    <>
                      {open ? (
                        <MinusCircleIcon className="w-6 mr-2" />
                      ) : (
                        <PlusCircleIcon className="w-6 mr-2" />
                      )}
                    </>
                  ) : (
                    <>
                      {meta.status === Status.success && (
                        <CheckIcon className="w-6 mr-2" />
                      )}
                      {meta.status === Status.fail && (
                        <ExclamationCircleIcon className="w-6 mr-2" />
                      )}
                    </>
                  )}
                  <Banner.Label className="mr-2 text-sm w-full">
                    <Title
                      text={text}
                      size={EnumSize.sm}
                      variant={variant}
                    />
                  </Banner.Label>
                </Transition>
              </>
            ) : (
              trigger
            )}
          </Disclosure.Button>
        </Banner>
        <Transition
          show={open}
          enter="transition-all ease-in-out duration-300 delay-[200ms]"
          enterFrom="opacity-0 translate-y-6"
          enterTo="opacity-100 translate-y-0"
          leave="transition-all ease-in-out duration-200"
          leaveFrom="opacity-100"
          leaveTo="opacity-0"
        >
          <Disclosure.Panel
            static
            className="px-2 text-xs mb-2"
          >
            {isNotNil(children) && (
              <div
                className={clsx(
                  'p-4 rounded-md',
                  variant === EnumVariant.Danger
                    ? 'bg-danger-5 text-danger-500'
                    : 'bg-neutral-5',
                )}
              >
                {children}
              </div>
            )}
            {meta.status !== Status.fail && panel}
          </Disclosure.Panel>
        </Transition>
      </Disclosure>
    </Transition>
  )
}
