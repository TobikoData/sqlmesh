import clsx from 'clsx'
import { useMemo } from 'react'
import { ContextEnvironmentChanges } from '~/api/client'
import { PlanTasks } from '~/context/plan'
import { toDateFormat, toRatio } from '~/utils'
import { Divider } from '../divider/Divider'
import Progress from '../progress/Progress'
import pluralize from 'pluralize'

interface PropsTasks {
  environment: string
  tasks: PlanTasks
  headline?: string
  updated_at?: string
  changes?: ContextEnvironmentChanges
  showBatches?: boolean
  showProgress?: boolean
  showLogicalUpdate?: boolean
}

export default function Tasks({
  environment,
  tasks,
  updated_at,
  changes,
  headline = 'Target Environment',
  showBatches = true,
  showProgress = true,
  showLogicalUpdate = false,
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
      changesModifiedIndirect: modified?.indirect ?? [],
      changesModifiedMetadata: modified?.indirect ?? [],
      changesModifiedDirect: (modified?.direct ?? []).map(
        ({ model_name }) => model_name,
      ),
    }
  }, [changes])

  return (
    <div className="bg-white">
      <div className="my-3 mx-4">
        <div className="flex justify-between items-baseline">
          <span className="flex items-center">
            <span className="block whitespace-nowrap text-sm font-medium text-gray-900">
              {headline}
            </span>
            <small className="inline-block ml-1 px-2 text-secondary-500 text-xs font-bold bg-secondary-100 rounded-md">
              {environment}
            </small>
          </span>
          <small className="block whitespace-nowrap text-xs font-bold text-gray-900">
            <span>
              {taskCompleted} of {taskTotal} task{taskTotal > 1 ? 's' : ''}
            </span>
            <span className="inline-block mx-2 text-gray-500">|</span>
            <span className="inline-block whitespace-nowrap font-bold text-secondary-500">
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
                {showLogicalUpdate ? 'Logical' : 'Backfill'}
              </span>
            </small>
            <small className="text-xs">
              <b>Last Update:</b>
              <span className="inline-block ml-1 text-gray-500">
                {toDateFormat(new Date(updated_at), 'yyyy-mm-dd hh-mm-ss')}
              </span>
            </small>
          </div>
        )}
      </div>
      <div className="my-4 px-4">
        <ul className="p-4 bg-secondary-100 rounded-lg">
          {models.map(([model_name, task]) => (
            <li
              key={model_name}
              className="mb-2"
            >
              <small className="flex justify-between items-baseline text-xs">
                <span className="flex whitespace-nowrap mr-6">
                  {task.interval != null && (
                    <span className="inline-block text-gray-500 mr-2">
                      {task.interval[0]}&nbsp;&ndash;&nbsp;{task.interval[1]}
                    </span>
                  )}
                  <span
                    className={clsx(
                      'font-bold',
                      changesAdded.includes(model_name) && 'text-success-700',
                      changesRemoved.includes(model_name) && 'text-danger-700',
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
                      <span className="block whitespace-nowrap text-gray-500">
                        {task.completed} of {task.total}&nbsp;
                        {pluralize('batch', task.total)}
                      </span>
                      <span className="inline-block mx-2 text-gray-500">|</span>
                    </>
                  )}
                  {showProgress && (
                    <>
                      {task.end == null || task.start == null ? (
                        <span className="inline-block whitespace-nowrap font-bold text-secondary-500">
                          {Math.ceil(toRatio(task.completed, task.total))}%
                        </span>
                      ) : (
                        <span className="inline-block whitespace-nowrap font-bold text-secondary-500">
                          {`${Math.floor(
                            (task.end - task.start) / 60000,
                          )}:${String(
                            Math.ceil(((task.end - task.start) / 1000) % 60),
                          ).padStart(2, '0')}`}
                        </span>
                      )}
                    </>
                  )}
                  {showLogicalUpdate && (
                    <>
                      <span className="inline-block whitespace-nowrap font-bold text-gray-500 ml-2">
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
  )
}
