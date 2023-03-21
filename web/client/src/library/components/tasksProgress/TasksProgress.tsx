import clsx from 'clsx'
import { type RefObject, useMemo } from 'react'
import { EnumPlanState, type PlanState, type PlanTasks } from '~/context/plan'
import { toDateFormat, toRatio } from '~/utils'
import { Divider } from '../divider/Divider'
import Progress from '../progress/Progress'
import pluralize from 'pluralize'
import { type ModelEnvironment } from '~/models/environment'
import { type ContextEnvironmentChanges } from '~/api/client'

interface PropsTasks {
  environment: ModelEnvironment
  tasks: PlanTasks
  headline?: string
  updated_at?: string
  changes?: ContextEnvironmentChanges
  showBatches?: boolean
  showProgress?: boolean
  showVirtualUpdate?: boolean
  planState: PlanState
  setRefTaskProgress: RefObject<HTMLDivElement>
}

export default function TasksProgress({
  environment,
  tasks,
  updated_at,
  changes,
  headline = 'Target Environment',
  showBatches = true,
  showProgress = true,
  showVirtualUpdate = false,
  planState,
  setRefTaskProgress,
}: PropsTasks): JSX.Element {
  const { models, taskCompleted, taskTotal } = useMemo(() => {
    const models = Object.entries(tasks)
    const taskCompleted = models.filter(
      ([_, { completed, total }]) => completed === total,
    ).length
    const taskTotal = models.length

    return {
      models,
      taskCompleted,
      taskTotal,
    }
  }, [tasks])

  const {
    changesAdded,
    changesRemoved,
    changesModifiedDirect,
    changesModifiedIndirect,
    changesModifiedMetadata,
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

  return (
    <div
      ref={setRefTaskProgress}
    >
      <div className="my-3 mx-4">
        <div className="flex justify-between items-baseline">
          <span className="flex items-center">
            <span className="block whitespace-nowrap text-sm font-medium">
              {headline}
            </span>
            <small className="inline-block ml-1 px-2 py-[0.125rem] text-xs font-bold bg-primary-10 rounded-md">
              {environment.name}
            </small>
            {planState !== EnumPlanState.Init && (
              <small className="ml-2">{planState}</small>
            )}
          </span>
          <small className="block whitespace-nowrap text-xs font-bold">
            <span>
              {taskCompleted} of {taskTotal} {pluralize('task', taskTotal)}
            </span>
            <span className="inline-block mx-2">|</span>
            <span className="inline-block whitespace-nowrap font-bold">
              {Math.ceil(toRatio(taskCompleted, taskTotal))}%
            </span>
          </small>
        </div>
        <Progress progress={toRatio(taskCompleted, taskTotal)} />
        {updated_at != null && (
          <div className="flex justify-between mt-1">
            <small className="text-xs">
              <b>Update Type:</b>
              <span className="inline-block ml-1 text-gray-500">
                {showVirtualUpdate ? 'Virtual' : 'Backfill'}
              </span>
            </small>
            <small className="text-xs">
              <b>Last Update:</b>
              <span className="inline-block ml-1">
                {toDateFormat(new Date(updated_at), 'yyyy-mm-dd hh-mm-ss')}
              </span>
            </small>
          </div>
        )}
      </div>
      <div className="my-4 px-4">
        <div className="bg-primary-10 rounded-lg">
          <ul className="p-4 overflow-auto scrollbar scrollbar--vertical scrollbar--horizontal">
            {models.map(([model_name, task]) => (
              <li
                key={model_name}
                className="mb-2"
              >
                <small className="flex sm:justify-between sm:items-baseline text-xs">
                  <span className="flex mr-6 w-full sm:w-auto overflow-hidden">
                    {task.interval != null && (
                      <span className="inline-block mr-2 whitespace-nowrap">
                        {task.interval[0]}&nbsp;&ndash;&nbsp;{task.interval[1]}
                      </span>
                    )}
                    <span
                      className={clsx(
                        'font-bold whitespace-nowrap',
                        changesAdded.includes(model_name) && 'text-success-600',
                        changesRemoved.includes(model_name) &&
                          'text-danger-700',
                        changesModifiedDirect.includes(model_name) &&
                          'text-secondary-500',
                        changesModifiedIndirect.includes(model_name) &&
                          'text-warning-700',
                        changesModifiedMetadata.includes(model_name) &&
                          'text-gray-900',
                      )}
                    >
                      {model_name}
                    </span>
                  </span>
                  <span className="flex items-center">
                    {showBatches && (
                      <>
                        <span className="block whitespace-nowrap">
                          {task.completed} of {task.total}&nbsp;
                          {pluralize('batch', task.total)}
                        </span>
                        <span className="inline-block mx-2">|</span>
                      </>
                    )}
                    {showProgress && (
                      <>
                        {task.end == null || task.start == null ? (
                          <span className="inline-block whitespace-nowrap font-bold text-primary-500">
                            {Math.ceil(toRatio(task.completed, task.total))}%
                          </span>
                        ) : (
                          <span className="inline-block whitespace-nowrap font-bold text-primary-500">
                            {`${Math.floor(
                              (task.end - task.start) / 60000,
                            )}:${String(
                              Math.ceil(((task.end - task.start) / 1000) % 60),
                            ).padStart(2, '0')}`}
                          </span>
                        )}
                      </>
                    )}
                    {showVirtualUpdate && (
                      <>
                        <span className="inline-block whitespace-nowrap font-bold ml-2">
                          Updated
                        </span>
                      </>
                    )}
                  </span>
                </small>
                {showProgress ? (
                  <Progress progress={toRatio(task.completed, task.total)} />
                ) : (
                  <Divider className="my-1 border-gray-200 opacity-50" />
                )}
              </li>
            ))}
          </ul>
        </div>
      </div>
    </div>
  )
}
