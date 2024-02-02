import clsx from 'clsx'
import React, { type RefObject, useMemo } from 'react'
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
import { type ModelSQLMeshChangeDisplay } from '@models/sqlmesh-change-display'
import { Transition } from '@headlessui/react'

const TasksOverview = function TasksOverview({
  children,
  setRefTasksOverview,
  tasks,
}: {
  tasks: Record<string, ModelSQLMeshChangeDisplay>
  setRefTasksOverview?: RefObject<HTMLDivElement>
  children: (options: {
    models: ModelSQLMeshChangeDisplay[]
    completed: number
    total: number
    completedBatches: number
    totalBatches: number
  }) => JSX.Element
}): JSX.Element {
  const { models, taskCompleted, taskTotal, batchesTotal, batchesCompleted } =
    useMemo(() => {
      const models = Object.values(tasks)
      const taskTotal = models.length
      let taskCompleted = 0
      let batchesTotal = 0
      let batchesCompleted = 0

      models.forEach(model => {
        taskCompleted =
          model.completed === model.total ? taskCompleted + 1 : taskCompleted
        batchesTotal += model.total
        batchesCompleted += model.completed
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
        {isNotNil(updatedAt) && (
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
  added,
  removed,
  direct,
  indirect,
  metadata,
  showBatches = true,
  showProgress = true,
  showVirtualUpdate = false,
  queue,
}: {
  className?: string
  models: ModelSQLMeshChangeDisplay[]
  added: ModelSQLMeshChangeDisplay[]
  removed: ModelSQLMeshChangeDisplay[]
  direct: ModelSQLMeshChangeDisplay[]
  indirect: ModelSQLMeshChangeDisplay[]
  metadata: ModelSQLMeshChangeDisplay[]
  queue: ModelSQLMeshChangeDisplay[]
  showBatches: boolean
  showProgress: boolean
  showVirtualUpdate: boolean
}): JSX.Element {
  return (
    <>
      <Transition
        show={isArrayNotEmpty(queue)}
        enter="transition ease duration-300 transform"
        enterFrom="opacity-0 scale-95"
        enterTo="opacity-100 scale-100"
        leave="transition ease duration-300 transform"
        leaveFrom="opacity-100 scale-100"
        leaveTo="opacity-0 scale-95"
        className="trasition-all duration-300 ease-in-out"
      >
        <div className="px-4 pt-3 pb-2 mt-4 shadow-lg rounded-lg">
          <Title text="Currently in proccess" />
          <Tasks models={queue}>
            {task => (
              <Task>
                <TaskDetails>
                  <TaskDetailsInfo>
                    {isArrayNotEmpty(task.interval) && (
                      <TaskInterval
                        start={task.interval[0]!}
                        end={task.interval[1]!}
                      />
                    )}
                    <TaskModelName
                      modelName={task.displayViewName}
                      changeType={getChangeType({
                        modelName: task.name,
                        added,
                        removed,
                        direct,
                        indirect,
                        metadata,
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
      </Transition>
      <TasksBlock className={className}>
        <Tasks models={models}>
          {task => (
            <Task>
              <TaskDetails>
                <TaskDetailsInfo>
                  {isArrayNotEmpty(task.interval) && (
                    <TaskInterval
                      start={task.interval[0]!}
                      end={task.interval[1]!}
                    />
                  )}
                  <TaskModelName
                    modelName={task.displayViewName}
                    changeType={getChangeType({
                      modelName: task.name,
                      added,
                      removed,
                      direct,
                      indirect,
                      metadata,
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
                    queue.findIndex(c => c.name === task.name) === -1
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
        'max-h-[50vh] overflow-auto hover:scrollbar scrollbar--vertical scrollbar--horizontal',
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
  models: ModelSQLMeshChangeDisplay[]
  children: (model: ModelSQLMeshChangeDisplay) => JSX.Element
}): JSX.Element {
  return (
    <ul
      className={clsx(
        'rounded-lg pt-4 overflow-auto text-prose hover:scrollbar scrollbar--vertical scrollbar--horizontal',
        className,
      )}
    >
      {models.map(task => (
        <li
          key={task.name}
          className="mb-2"
        >
          {children(task)}
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
  added,
  removed,
  direct,
  indirect,
  metadata,
}: {
  modelName: string
  added: ModelSQLMeshChangeDisplay[]
  removed: ModelSQLMeshChangeDisplay[]
  direct: ModelSQLMeshChangeDisplay[]
  indirect: ModelSQLMeshChangeDisplay[]
  metadata: ModelSQLMeshChangeDisplay[]
}): PlanChangeType {
  if (added.some(c => c.name === modelName)) return EnumPlanChangeType.Add
  if (removed.some(c => c.name === modelName)) return EnumPlanChangeType.Remove
  if (direct.some(c => c.name === modelName)) return EnumPlanChangeType.Direct
  if (indirect.some(c => c.name === modelName))
    return EnumPlanChangeType.Indirect
  if (metadata.some(c => c.name === modelName))
    return EnumPlanChangeType.Metadata

  return EnumPlanChangeType.Default
}
