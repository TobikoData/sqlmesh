import { Disclosure, Transition } from '@headlessui/react'
import { MinusCircleIcon, PlusCircleIcon } from '@heroicons/react/24/solid'
import { ExclamationCircleIcon } from '@heroicons/react/24/outline'
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
  includes,
  isFalseOrNil,
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

export default function PlanApplyStageTracker(): JSX.Element {
  const tests = useStoreProject(s => s.tests)

  const planApply = useStorePlan(s => s.planApply)
  const planOverview = useStorePlan(s => s.planOverview)
  const planCancel = useStorePlan(s => s.planCancel)
  const planAction = useStorePlan(s => s.planAction)

  const { plan_options, hasChanges, hasBackfills, isFailed } =
    getPlanOverviewDetails(planApply, planOverview, planCancel)

  const hasTestsDetails = isNotNil(tests) && Boolean(tests.total)
  const showTestsMessage =
    isNotNil(tests) && Boolean(tests.message) && isFalse(hasTestsDetails)
  const hasFailedTests =
    isNotNil(tests) && (Boolean(tests.failures) || Boolean(tests.errors))
  const showChangesAndBackfills =
    isFalse(planAction.isProcessing) &&
    isFalse(planAction.isDone) &&
    isFalse(planCancel.isFinished)
  const showFailedMessage =
    isFailed &&
    [hasFailedTests, hasTestsDetails, hasChanges, hasBackfills].every(
      isFalseOrNil,
    )

  return (
    <Transition
      appear
      show={isFalse(planAction.isRun)}
      enter="transition ease duration-300 transform"
      enterFrom="opacity-0 scale-95"
      enterTo="opacity-100 scale-100"
      leave="transition ease duration-300 transform"
      leaveFrom="opacity-100 scale-100"
      leaveTo="opacity-0 scale-95"
      className={clsx('my-2 rounded-xl', isFailed && 'bg-danger-5')}
    >
      {showFailedMessage ? (
        <Banner
          className="flex items-center"
          size={EnumSize.sm}
          variant={EnumVariant.Danger}
        >
          <ExclamationCircleIcon className="w-4 mr-2" />
          <Banner.Label className="mr-2 text-sm">Plan Failed</Banner.Label>
        </Banner>
      ) : (
        <>
          {isFailed && (
            <Banner
              className="flex items-center mb-1"
              size={EnumSize.sm}
              variant={EnumVariant.Danger}
            >
              <ExclamationCircleIcon className="w-4 mr-2" />
              <Banner.Label className="mr-2 text-sm">Plan Failed</Banner.Label>
            </Banner>
          )}
          {isTrue(plan_options?.skip_tests) ? (
            <Banner
              className="flex items-center mb-1"
              size={EnumSize.sm}
              hasBackground={false}
            >
              <CheckIcon className="w-4 mr-2" />
              <Banner.Label className="mr-2 text-sm">
                Tests Skipped
              </Banner.Label>
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
              <CheckIcon className="w-4 mr-2" />
              <Banner.Label className="mr-2 text-sm">No Tests</Banner.Label>
            </Banner>
          )}
          {isFalse(hasFailedTests) && (
            <>
              <StageChanges isOpen={showChangesAndBackfills} />
              <StageBackfills isOpen={showChangesAndBackfills} />
              <StageVirtualUpdate />
              {planApply.shouldShowEvaluation && (
                <StageEvaluate
                  start={planApply.evaluationStart}
                  end={
                    planApply.isFinished
                      ? planApply.evaluationEnd ?? planCancel.meta?.end
                      : undefined
                  }
                >
                  <StageCreation />
                  <StageRestate />
                  <StageBackfill />
                  <StagePromote />
                </StageEvaluate>
              )}
            </>
          )}
        </>
      )}
    </Transition>
  )
}

function StageChanges({ isOpen = false }: { isOpen?: boolean }): JSX.Element {
  const planApply = useStorePlan(s => s.planApply)
  const planOverview = useStorePlan(s => s.planOverview)
  const planCancel = useStorePlan(s => s.planCancel)

  const { meta, stageChanges, hasChanges } = getPlanOverviewDetails(
    planApply,
    planOverview,
    planCancel,
  )
  const tempMeta = stageChanges?.meta ?? meta
  const showChanges =
    includes([Status.init, Status.fail], stageChanges?.meta?.status) ||
    isTrue(hasChanges)

  return showChanges ? (
    <Stage
      meta={tempMeta}
      states={['Changes', 'Failed Getting Changes', 'Getting Changes...']}
      isOpen={isOpen && isTrue(hasChanges)}
      panel={<PlanChanges />}
    />
  ) : (
    <Banner
      className="flex items-center mb-1"
      size={EnumSize.sm}
      hasBackground={false}
    >
      <CheckIcon className="w-4 mr-2" />
      <Banner.Label className="mr-2 text-sm">No Changes</Banner.Label>
    </Banner>
  )
}

function StageBackfills({ isOpen = false }: { isOpen?: boolean }): JSX.Element {
  const { change_categorization } = usePlan()

  const planApply = useStorePlan(s => s.planApply)
  const planOverview = useStorePlan(s => s.planOverview)
  const planCancel = useStorePlan(s => s.planCancel)

  const { meta, stageBackfills, backfills, hasBackfills } =
    getPlanOverviewDetails(planApply, planOverview, planCancel)
  const tempMeta = stageBackfills?.meta ?? meta

  const categories = useMemo(
    () =>
      Array.from(change_categorization.values()).reduce(
        (acc, { category, change }) => {
          if (category?.value !== SnapshotChangeCategory.NUMBER_3) {
            acc.add(change.name)
          }

          if (
            isNil(category) ||
            category.value === SnapshotChangeCategory.NUMBER_1
          ) {
            change.indirect?.forEach(c => acc.add(c.name))
          }

          return acc
        },
        new Set<string>(),
      ),
    [change_categorization],
  )

  const changes = useMemo(
    () =>
      change_categorization.size > 0
        ? backfills.filter(backfill => categories.has(backfill.name))
        : backfills,
    [backfills, categories],
  )

  const showBackfills =
    (tempMeta?.status === Status.init || isTrue(hasBackfills)) &&
    (change_categorization.size > 0 ? changes.length > 0 : true)

  return showBackfills ? (
    <Stage
      meta={tempMeta}
      states={['Backfills', 'Failed Getting Backfills', 'Getting Backfills...']}
      isOpen={isOpen && isTrue(hasBackfills)}
      panel={
        <PlanChangePreview
          headline={`Models ${changes.length}`}
          type={EnumPlanChangeType.Default}
        >
          <PlanChangePreview.Default
            type={EnumPlanChangeType.Default}
            changes={changes}
          />
        </PlanChangePreview>
      }
    />
  ) : (
    <Banner
      className="flex items-center mb-1"
      size={EnumSize.sm}
      hasBackground={false}
    >
      <CheckIcon className="w-4 mr-2" />
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
    setTimeout(() => {
      elStageEvaluate.current?.scrollIntoView({
        behavior: 'smooth',
        block: 'start',
      })
    }, 500)
  }, [])

  return isNil(start) ? (
    <></>
  ) : (
    <div
      ref={elStageEvaluate}
      className="pt-4 pb-2 text-xs"
    >
      <span className="text-neutral-500 block px-4 mb-1">
        Evaluation started at{' '}
        {toDateFormat(new Date(start), 'yyyy-mm-dd hh-mm-ss')}
      </span>
      <Divider />
      <div className="py-2">{children}</div>
      {isNotNil(end) && (
        <>
          <Divider />
          <span className="text-neutral-500 block px-4 mt-1">
            Evaluation stopped at{' '}
            {toDateFormat(new Date(end), 'yyyy-mm-dd hh-mm-ss')}
          </span>
        </>
      )}
    </div>
  )
}

function StageCreation({ isOpen }: { isOpen?: boolean }): JSX.Element {
  const planApply = useStorePlan(s => s.planApply)

  return isNil(planApply.stageCreation) ? (
    <></>
  ) : (
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
                completed={planApply.stageCreation.num_tasks}
                total={planApply.stageCreation.total_tasks}
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
      <CheckIcon className="w-4 mr-2" />
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
  const planApply = useStorePlan(s => s.planApply)
  const planAction = useStorePlan(s => s.planAction)
  const planOverview = useStorePlan(s => s.planOverview)
  const planCancel = useStorePlan(s => s.planCancel)

  const environment = planApply.environment
  const stageBackfill = planApply.stageBackfill

  const { backfills } = getPlanOverviewDetails(
    planApply,
    planOverview,
    planCancel,
  )

  const tasks = useMemo(
    () =>
      Object.values(planApply.tasks).reduce<typeof planApply.tasks>(
        (acc, task) => {
          const backfill = backfills.find(b => b.name === task.name)

          task.interval = backfill?.interval ?? []

          acc[task.name] = task

          return acc
        },
        {},
      ),
    [backfills, planApply.tasks],
  )

  const showStageBackfill = isNotNil(stageBackfill) && isNotNil(environment)

  return showStageBackfill ? (
    <Stage
      meta={stageBackfill.meta}
      states={['Backfilled', 'Backfilling Failed', 'Backfilling Intervals...']}
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
  ) : (
    <></>
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
                  completed={planApply.stagePromote.num_tasks}
                  total={planApply.stagePromote.total_tasks}
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
  const planCancel = useStorePlan(s => s.planCancel)

  const { isFailed } = getPlanOverviewDetails(
    planApply,
    planOverview,
    planCancel,
  )

  const isVirtualUpdate =
    planApply.overview?.isVirtualUpdate ?? planOverview.isVirtualUpdate
  const isUpdated = isTrue(planApply.isFinished)

  return isVirtualUpdate && isFalse(isFailed) ? (
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
  const planAction = useStorePlan(s => s.planAction)
  const planOverview = useStorePlan(s => s.planOverview)
  const planApply = useStorePlan(s => s.planApply)
  const planCancel = useStorePlan(s => s.planCancel)

  const { hasChanges, added, removed, direct, indirect, metadata } =
    getPlanOverviewDetails(planApply, planOverview, planCancel)

  const shouldDisable =
    planAction.isProcessing ||
    planAction.isDone ||
    planApply.isFinished ||
    (planOverview.isLatest && isFalse(planAction.isRun)) ||
    planOverview.isVirtualUpdate

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
              <PlanChangePreview.Direct
                changes={direct}
                disabled={shouldDisable}
              />
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

  const hasChildren =
    (meta?.status !== Status.fail && isNotNil(panel)) || isNotNil(children)

  const [open, setOpen] = useState(isOpen)

  useEffect(() => {
    if (isNil(elTrigger.current)) return

    if (shouldCollapse && (planApply.isFinished || planOverview.isLatest)) {
      if (elTrigger.current.classList.contains('--is-open')) {
        elTrigger.current?.click()
      }
    }
  }, [elTrigger, planOverview, planApply, shouldCollapse])

  useEffect(() => {
    setOpen(isOpen && hasChildren)
  }, [isOpen, hasChildren])

  const isStatusFail = meta?.status === Status.fail
  const isStatusSuccess = meta?.status === Status.success
  const isStatusInit = meta?.status === Status.init

  const variant = isStatusSuccess
    ? EnumVariant.Success
    : isStatusFail
    ? EnumVariant.Danger
    : EnumVariant.Info
  const [titleSuccess, titleFail, titleDefault] = states
  const text = isStatusSuccess
    ? titleSuccess
    : isStatusFail
    ? titleFail
    : titleDefault

  return (
    <Transition
      appear
      show={isNotNil(meta)}
      enter="transition ease duration-300 transform"
      enterFrom="opacity-0 scale-95"
      enterTo="opacity-100 scale-100"
      leave="transition ease duration-300 transform"
      leaveFrom="opacity-100 scale-100"
      leaveTo="opacity-0 scale-95"
      className="my-2"
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
                {isStatusInit && (
                  <Loading
                    text={text}
                    hasSpinner
                    size={EnumSize.sm}
                    variant={EnumVariant.Primary}
                    className="w-full"
                  />
                )}
                {isFalse(isStatusInit) && (
                  <div className="flex items-center h-full">
                    {showDetails && hasChildren ? (
                      <>
                        {open ? (
                          <MinusCircleIcon className="w-4 mr-2" />
                        ) : (
                          <PlusCircleIcon className="w-4 mr-2" />
                        )}
                      </>
                    ) : (
                      <>
                        {meta?.status === Status.success && (
                          <CheckIcon className="min-w-4 max-w-4 mr-2" />
                        )}
                        {meta?.status === Status.fail && (
                          <ExclamationCircleIcon className="min-w-4 max-w-4 mr-2" />
                        )}
                      </>
                    )}
                    <Banner.Label className="mr-2 text-sm">
                      <Title
                        text={text}
                        size={EnumSize.sm}
                        variant={variant}
                      />
                    </Banner.Label>
                  </div>
                )}
              </>
            ) : (
              trigger
            )}
          </Disclosure.Button>
        </Banner>
        <Transition
          appear
          show={open}
          enter="transition ease duration-300 transform"
          enterFrom="opacity-0 scale-95"
          enterTo="opacity-100 scale-100"
          leave="transition ease duration-300 transform"
          leaveFrom="opacity-100 scale-100"
          leaveTo="opacity-0 scale-95"
          className="trasition-all duration-300 ease-in-out"
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
            {meta?.status !== Status.fail && panel}
          </Disclosure.Panel>
        </Transition>
      </Disclosure>
    </Transition>
  )
}
