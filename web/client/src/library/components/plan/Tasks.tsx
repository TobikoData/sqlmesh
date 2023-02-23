import clsx from 'clsx'
import { useMemo } from 'react'
import { ContextEnvironmentChanges } from '~/api/client'
import { PlanTasks } from '~/context/plan'
import { toDateFormat, toRatio } from '~/utils'
import Progress from '../progress/Progress'

interface PropsTasks {
  environment: string
  tasks: PlanTasks
  updated_at?: string
  changes?: ContextEnvironmentChanges
}

export default function Tasks({
  environment,
  tasks,
  updated_at,
  changes,
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
              Target Environment
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
          <div className="flex justify-end mt-2">
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
                      changesAdded.includes(model_name) && 'text-success-500',
                      changesRemoved.includes(model_name) && 'text-danger-500',
                      changesModifiedDirect.includes(model_name) &&
                        'text-secondary-500',
                      changesModifiedIndirect.includes(model_name) &&
                        'text-warning-500',
                      changesModifiedMetadata.includes(model_name) &&
                        'text-gray-900',
                    )}
                  >
                    {model_name}
                  </span>
                </span>
                <span className="flex items-center">
                  <span className="block whitespace-nowrap text-gray-500">
                    {task.completed} of {task.total} batch
                    {task.total > 1 ? 'es' : ''}
                  </span>
                  <span className="inline-block mx-2 text-gray-500">|</span>
                  <span className="inline-block whitespace-nowrap font-bold text-secondary-500">
                    {task.end == null || task.start == null
                      ? `${Math.ceil(toRatio(task.completed, task.total))}%`
                      : `${Math.floor(
                          (task.end - task.start) / 60000,
                        )}:${String(
                          Math.ceil(((task.end - task.start) / 1000) % 60),
                        ).padStart(2, '0')}`}
                  </span>
                </span>
              </small>
              <Progress progress={toRatio(task.completed, task.total)} />
            </li>
          ))}
        </ul>
      </div>
    </div>
  )
}
