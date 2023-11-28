import clsx from 'clsx'
import React, { type RefObject, useMemo } from 'react'
import { type PlanTaskStatus, type PlanTasks } from '~/context/plan'
import {
  isArrayNotEmpty,
  isNil,
  isNotNil,
  toDateFormat,
  toRatio,
} from '~/utils'
import { Divider } from '../divider/Divider'
import Progress from '../progress/Progress'
import pluralize from 'pluralize'
import { type EnvironmentName } from '~/models/environment'
import { EnumPlanChangeType, type PlanChangeType } from '../plan/context'
import Title from '@components/title/Title'
import { type PlanStageChanges } from '@api/client'

interface PropsTasks {
  tasks: PlanTasks
  setRefTasksOverview?: RefObject<HTMLDivElement>
  children: (options: {
    models?: Array<[string, PlanTaskStatus]>
    completed: number
    total: number
    completedBatches: number
    totalBatches: number
  }) => JSX.Element
}

const TasksOverview = function TasksOverview({
  children,
  setRefTasksOverview,
  tasks,
}: PropsTasks): JSX.Element {
  const { models, taskCompleted, taskTotal, batchesTotal, batchesCompleted } =
    useMemo(() => {
      const models = Object.entries(tasks)
      const taskTotal = models.length
      let taskCompleted = 0
      let batchesTotal = 0
      let batchesCompleted = 0

      models.forEach(([_, { completed, total }]) => {
        taskCompleted = completed === total ? taskCompleted + 1 : taskCompleted
        batchesTotal += total
        batchesCompleted += completed
      })

      return {
        models,
        taskCompleted,
        taskTotal,
        batchesTotal,
        batchesCompleted,
      }
    }, [tasks])

  return (
    <div
      className="text-prose"
      ref={setRefTasksOverview}
    >
      {children({
        models,
        completed: taskCompleted,
        total: taskTotal,
        totalBatches: batchesTotal,
        completedBatches: batchesCompleted,
      })}
    </div>
  )
}

function TasksSummary({
  className,
  headline,
  environment,
  completed,
  total,
  updatedAt,
  updateType,
  completedBatches,
  totalBatches,
}: {
  className?: string
  headline: string
  environment: EnvironmentName
  completed: number
  total: number
  completedBatches: number
  totalBatches: number
  updateType: string
  updatedAt?: string
}): JSX.Element {
  return (
    <TasksBlock className={className}>
      <Task>
        <TaskDetails>
          <TaskDetailsInfo>
            <TaskHeadline
              headline={headline}
              environment={environment}
            />
          </TaskDetailsInfo>
          <TaskDetailsProgress>
            <TaskSize
              completed={completed}
              total={total}
              unit="task"
            />
            <TaskDivider />
            <TaskSize
              completed={completedBatches}
              total={totalBatches}
              unit="batches"
            />
            <TaskDivider />
            <TaskProgress
              completed={completedBatches}
              total={totalBatches}
            />
          </TaskDetailsProgress>
        </TaskDetails>
        <Progress progress={toRatio(completedBatches, totalBatches)} />
        {updatedAt != null && (
          <TaskCompletedMeta
            updatedAt={updatedAt}
            updateType={updateType}
          />
        )}
      </Task>
    </TasksBlock>
  )
}

function TasksDetails({
  models,
  className,
  changes,
  showBatches = true,
  showProgress = true,
  showVirtualUpdate = false,
  queue,
}: {
  className?: string
  changes?: PlanStageChanges
  models: Array<[string, PlanTaskStatus]>
  queue?: string[]
  showBatches: boolean
  showProgress: boolean
  showVirtualUpdate: boolean
}): JSX.Element {
  const {
    changesAdded,
    changesRemoved,
    changesModifiedDirect,
    changesModifiedIndirect,
  } = useMemo(() => {
    const modified = changes?.modified

    return {
      changesAdded: changes?.added ?? [],
      changesRemoved: changes?.removed ?? [],
      changesModifiedMetadata: modified?.metadata ?? [],
      changesModifiedIndirect: (modified?.indirect ?? []).map(
        ({ model_name }) => model_name,
      ),
      changesModifiedDirect: (modified?.direct ?? []).map(
        ({ model_name }) => model_name,
      ),
    }
  }, [changes])

  const modelsInQueue = useMemo(() => {
    if (isNil(queue)) return []

    return models.filter(([modelName]) => queue.includes(modelName))
  }, [queue, models])

  return (
    <>
      {isArrayNotEmpty(queue) && (
        <div className="p-4 mt-6 shadow-lg bg-neutral-5 rounded-lg">
          <Title text="Currently in proccess" />
          <Tasks models={modelsInQueue}>
            {([modelName, task]) => (
              <Task>
                <TaskDetails>
                  <TaskDetailsInfo>
                    {isNotNil(task.interval) && (
                      <TaskInterval
                        start={task.interval[0]}
                        end={task.interval[1]}
                      />
                    )}
                    <TaskModelName
                      modelName={modelName}
                      changeType={getChangeType({
                        modelName,
                        changesAdded,
                        changesRemoved,
                        changesModifiedDirect,
                        changesModifiedIndirect,
                      })}
                    />
                  </TaskDetailsInfo>
                  <TaskDetailsProgress>
                    {showBatches && (
                      <TaskSize
                        completed={task.completed}
                        total={task.total}
                        unit="batch"
                      />
                    )}
                    <TaskDivider />
                    {showProgress && (
                      <>
                        {isNil(task.end) || isNil(task.start) ? (
                          <TaskProgress
                            total={task.total}
                            completed={task.completed}
                          />
                        ) : (
                          <TaskCompletionTime
                            start={task.start}
                            end={task.end}
                          />
                        )}
                      </>
                    )}
                    {showVirtualUpdate && (
                      <span className="inline-block whitespace-nowrap font-bold ml-2">
                        Updated
                      </span>
                    )}
                  </TaskDetailsProgress>
                </TaskDetails>
                {showProgress ? (
                  <Progress
                    startFromZero={false}
                    progress={toRatio(task.completed, task.total)}
                  />
                ) : (
                  <Divider className="my-1 border-neutral-200 opacity-50" />
                )}
              </Task>
            )}
          </Tasks>
        </div>
      )}
      <TasksBlock className={className}>
        <Tasks models={models}>
          {([modelName, task]) => (
            <Task>
              <TaskDetails>
                <TaskDetailsInfo>
                  {isNotNil(task.interval) && (
                    <TaskInterval
                      start={task.interval[0]}
                      end={task.interval[1]}
                    />
                  )}
                  <TaskModelName
                    modelName={modelName}
                    changeType={getChangeType({
                      modelName,
                      changesAdded,
                      changesRemoved,
                      changesModifiedDirect,
                      changesModifiedIndirect,
                    })}
                  />
                </TaskDetailsInfo>
                <TaskDetailsProgress>
                  {showBatches && (
                    <TaskSize
                      completed={task.completed}
                      total={task.total}
                      unit="batch"
                    />
                  )}
                  <TaskDivider />
                  {showProgress && (
                    <>
                      {isNil(task.end) || isNil(task.start) ? (
                        <TaskProgress
                          total={task.total}
                          completed={task.completed}
                        />
                      ) : (
                        <TaskCompletionTime
                          start={task.start}
                          end={task.end}
                        />
                      )}
                    </>
                  )}
                  {showVirtualUpdate && (
                    <span className="inline-block whitespace-nowrap font-bold ml-2">
                      Updated
                    </span>
                  )}
                </TaskDetailsProgress>
              </TaskDetails>
              {showProgress ? (
                <Progress
                  progress={toRatio(task.completed, task.total)}
                  startFromZero={
                    modelsInQueue.findIndex(([name]) => name === modelName) ===
                    -1
                  }
                />
              ) : (
                <Divider className="my-1 border-neutral-200 opacity-50" />
              )}
            </Task>
          )}
        </Tasks>
      </TasksBlock>
    </>
  )
}

function TasksBlock({
  className,
  children,
}: {
  className?: string
  children: React.ReactNode
}): JSX.Element {
  return (
    <div
      className={clsx(
        'my-3 max-h-[50vh] overflow-auto hover:scrollbar scrollbar--vertical scrollbar--horizontal',
        className,
      )}
    >
      {children}
    </div>
  )
}

function Task({
  className,
  children,
}: {
  className?: string
  children: React.ReactNode
}): JSX.Element {
  return <div className={clsx('px-2', className)}>{children}</div>
}

function Tasks({
  className,
  models,
  children,
}: {
  className?: string
  models: Array<[string, PlanTaskStatus]>
  children: (model: [string, PlanTaskStatus]) => JSX.Element
}): JSX.Element {
  return (
    <ul
      className={clsx(
        'rounded-lg py-4 overflow-auto text-prose hover:scrollbar scrollbar--vertical scrollbar--horizontal',
        className,
      )}
    >
      {models.map(([modelName, task]) => (
        <li
          key={modelName}
          className="mb-2"
        >
          {children([modelName, task])}
        </li>
      ))}
    </ul>
  )
}

function TaskDetails({
  className,
  children,
}: {
  className?: string
  children: React.ReactNode
}): JSX.Element {
  return (
    <div
      className={clsx(
        'flex sm:justify-between sm:items-baseline text-xs',
        className,
      )}
    >
      {children}
    </div>
  )
}

function TaskHeadline({
  className,
  headline,
  environment,
}: {
  headline: string
  environment?: EnvironmentName
  className?: string
}): JSX.Element {
  return (
    <span className={clsx('flex items-center', className)}>
      <span className="block whitespace-nowrap text-sm font-medium">
        {headline}
      </span>
      {isNotNil(environment) && (
        <small className="inline-block ml-1 px-2 py-[0.125rem] text-xs font-bold bg-neutral-10 rounded-md">
          {environment}
        </small>
      )}
    </span>
  )
}

function TaskDetailsInfo({
  className,
  children,
}: {
  className?: string
  children: React.ReactNode
}): JSX.Element {
  return (
    <div
      className={clsx('flex mr-6 w-full sm:w-auto overflow-hidden', className)}
    >
      {children}
    </div>
  )
}

function TaskDetailsProgress({
  className,
  children,
}: {
  className?: string
  children: React.ReactNode
}): JSX.Element {
  return <div className={clsx('flex items-center', className)}>{children}</div>
}

function TaskInterval({
  className,
  start,
  end,
}: {
  start: string
  end: string
  className?: string
}): JSX.Element {
  return (
    <span
      className={clsx(
        'inline-block mr-2 whitespace-nowrap font-mono',
        className,
      )}
    >
      {start}&nbsp;&ndash;&nbsp;{end}
    </span>
  )
}

function TaskSize({
  className,
  total,
  completed,
  unit,
}: {
  total: number
  completed: number
  unit: string
  className?: string
}): JSX.Element {
  return (
    <span className={clsx('inline-block whitespace-nowrap', className)}>
      {completed} of {total} {pluralize(unit, total)}
    </span>
  )
}

function TaskDivider({ className }: { className?: string }): JSX.Element {
  return <span className={clsx('inline-block mx-2', className)}>|</span>
}

function TaskProgress({
  className,
  total,
  completed,
}: {
  total: number
  completed: number
  className?: string
}): JSX.Element {
  return (
    <span
      className={clsx('inline-block whitespace-nowrap font-bold', className)}
    >
      {Math.ceil(toRatio(completed, total))}%
    </span>
  )
}

function TaskCompletionTime({
  className,
  start,
  end,
}: {
  start: number
  end: number
  className?: string
}): JSX.Element {
  return (
    <span
      className={clsx('inline-block whitespace-nowrap font-bold', className)}
    >
      {`${Math.floor((end - start) / 60000)}:${String(
        Math.ceil(((end - start) / 1000) % 60),
      ).padStart(2, '0')}`}
    </span>
  )
}

function TaskCompletedMeta({
  updateType,
  updatedAt,
}: {
  updateType: string
  updatedAt: string
}): JSX.Element {
  return (
    <div className="flex justify-between mt-1">
      <small className="text-xs">
        <b>Update Type:</b>
        <span className="inline-block ml-1">{updateType}</span>
      </small>
      <small className="text-xs">
        <b>Last Update:</b>
        <span className="inline-block ml-1">
          {toDateFormat(new Date(updatedAt), 'yyyy-mm-dd hh-mm-ss', false)}
        </span>
      </small>
    </div>
  )
}

function TaskModelName({
  className,
  modelName,
  changeType,
}: {
  className?: string
  modelName: string
  changeType: PlanChangeType
}): JSX.Element {
  return (
    <span
      className={clsx(
        'font-bold whitespace-nowrap',
        changeType === EnumPlanChangeType.Add &&
          'text-success-600  dark:text-success-500',
        changeType === EnumPlanChangeType.Remove && 'text-danger-500',
        changeType === EnumPlanChangeType.Direct &&
          'text-secondary-500 dark:text-primary-500',
        changeType === EnumPlanChangeType.Indirect && 'text-warning-500',
        changeType === EnumPlanChangeType.Default && 'text-prose',
        className,
      )}
    >
      {modelName}
    </span>
  )
}

TasksOverview.Block = TasksBlock
TasksOverview.Summary = TasksSummary
TasksOverview.Details = TasksDetails
TasksOverview.DetailsProgress = TaskDetailsProgress
TasksOverview.Task = Task
TasksOverview.Tasks = Tasks
TasksOverview.TaskDetails = TaskDetails
TasksOverview.TaskProgress = TaskProgress
TasksOverview.TaskSize = TaskSize
TasksOverview.TaskDivider = TaskDivider
TasksOverview.TaskInfo = TaskDetailsInfo
TasksOverview.TaskHeadline = TaskHeadline

export default TasksOverview

function getChangeType({
  modelName,
  changesAdded,
  changesRemoved,
  changesModifiedDirect,
  changesModifiedIndirect,
}: {
  modelName: string
  changesAdded: string[]
  changesRemoved: string[]
  changesModifiedDirect: string[]
  changesModifiedIndirect: string[]
}): PlanChangeType {
  if (changesAdded.includes(modelName)) return EnumPlanChangeType.Add
  if (changesRemoved.includes(modelName)) return EnumPlanChangeType.Remove
  if (changesModifiedDirect.includes(modelName))
    return EnumPlanChangeType.Direct
  if (changesModifiedIndirect.includes(modelName))
    return EnumPlanChangeType.Indirect

  return EnumPlanChangeType.Default
}
