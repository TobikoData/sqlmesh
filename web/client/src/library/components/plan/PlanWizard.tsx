import { Disclosure, RadioGroup } from '@headlessui/react'
import {
  CheckCircleIcon,
  MinusCircleIcon,
  PlusCircleIcon,
} from '@heroicons/react/24/solid'
import clsx from 'clsx'
import { lazy, Suspense, useEffect, useMemo } from 'react'
import { ContextEnvironmentChanges } from '~/api/client'
import { EnvironmentName } from '~/context/context'
import {
  EnumPlanState,
  EnumPlanAction,
  useStorePlan,
} from '../../../context/plan'
import {
  includes,
  isArrayNotEmpty,
  isFalse,
  toDate,
  toDateFormat,
} from '../../../utils'
import { Divider } from '../divider/Divider'
import Input from '../input/Input'
import Spinner from '../logo/Spinner'
import { isModified } from './help'
import PlanWizardStepOptions from './PlanWizardStepOptions'

const Tasks = lazy(async () => await import('../plan/Tasks'))

export default function PlanWizard({
  environment,
  changes,
}: {
  environment: EnvironmentName
  changes?: ContextEnvironmentChanges
}): JSX.Element {
  const planState = useStorePlan(s => s.state)
  const planAction = useStorePlan(s => s.action)
  const backfills = useStorePlan(s => s.backfills)

  const category = useStorePlan(s => s.category)
  const categories = useStorePlan(s => s.categories)
  const backfill_start = useStorePlan(s => s.backfill_start)
  const backfill_end = useStorePlan(s => s.backfill_end)
  const mostRecentPlan = useStorePlan(s => s.lastPlan ?? s.activePlan)
  const setCategory = useStorePlan(s => s.setCategory)
  const setWithBackfill = useStorePlan(s => s.setWithBackfill)
  const setBackfillDate = useStorePlan(s => s.setBackfillDate)

  const hasChanges = useMemo(
    () =>
      [
        isModified(changes?.modified),
        isArrayNotEmpty(changes?.added),
        isArrayNotEmpty(changes?.removed),
      ].some(Boolean),
    [changes],
  )

  const tasks = useMemo(
    () =>
      backfills.reduce(
        (acc, task) =>
          Object.assign(acc, {
            [task.model_name]: {
              completed: 0,
              ...(mostRecentPlan?.tasks[task.model_name] ?? {}),
              total: task.batches,
              interval: task.interval,
            },
          }),
        {},
      ),
    [backfills, mostRecentPlan],
  )
  useEffect(() => {
    if (isArrayNotEmpty(backfills)) {
      setCategory(categories[0])
    }
  }, [backfills])

  useEffect(() => {
    setWithBackfill(isArrayNotEmpty(backfills) && category?.id !== 'no-change')
  }, [backfills, category])

  const hasBackfills = isArrayNotEmpty(backfills)
  const isPlanInProgress = includes(
    [EnumPlanState.Cancelling, EnumPlanState.Applying],
    planState,
  )
  const isRun = includes(
    [EnumPlanAction.Running, EnumPlanAction.Run],
    planAction,
  )
  const isDone = mostRecentPlan != null && planAction === EnumPlanAction.Done

  return (
    <ul className="w-full mx-auto md:w-[75%] lg:w-[50%]">
      {isRun ? (
        <PlanWizardStepOptions />
      ) : (
        <>
          <PlanWizardStep
            headline="Models"
            description="Review Changes"
            disabled={environment == null}
          >
            {hasChanges ? (
              <>
                <div className="flex">
                  {isArrayNotEmpty(changes?.added) && (
                    <div className="ml-4 mb-8">
                      <h4 className="text-success-500 mb-2">Added Models</h4>
                      <ul className="ml-2">
                        {changes?.added.map((modelName: string) => (
                          <li
                            key={modelName}
                            className="text-success-500 font-sm h-[1.5rem]"
                          >
                            <small className="inline-block h-[1.25rem] px-1 pl-4 border-l border-success-500">
                              {modelName}
                            </small>
                          </li>
                        ))}
                      </ul>
                    </div>
                  )}
                  {isArrayNotEmpty(changes?.removed?.length) && (
                    <div className="ml-4 mb-8">
                      <h4 className="text-danger-500 mb-2">Removed Models</h4>
                      <ul className="ml-2">
                        {changes?.added.map((modelName: string) => (
                          <li
                            key={modelName}
                            className="text-danger-500 font-sm h-[1.5rem]"
                          >
                            <small className="inline-block h-[1.25rem] px-1 pl-4 border-l border-danger-500">
                              {modelName}
                            </small>
                          </li>
                        ))}
                      </ul>
                    </div>
                  )}
                </div>
                {isModified(changes?.modified) && (
                  <div className="flex">
                    {isArrayNotEmpty(changes?.modified.direct) && (
                      <div className="w-full ml-1">
                        <h4 className="text-secondary-500 mb-2">
                          Modified Directly
                        </h4>
                        <ul className="ml-1 mr-3">
                          {changes?.modified.direct.map(change => (
                            <li
                              key={change.model_name}
                              className="text-secondary-500"
                            >
                              <Disclosure>
                                {({ open }) => (
                                  <>
                                    <Disclosure.Button className="flex items-center w-full justify-between rounded-lg text-left">
                                      <small className="inline-block text-sm">
                                        {change.model_name}
                                      </small>
                                      <Divider className="mx-4" />
                                      {(() => {
                                        const Tag = open
                                          ? MinusCircleIcon
                                          : PlusCircleIcon

                                        return (
                                          <Tag className="max-h-[1rem] min-w-[1rem] text-secondary-500" />
                                        )
                                      })()}
                                    </Disclosure.Button>
                                    <Disclosure.Panel className="text-sm text-gray-500">
                                      <pre className="my-4 bg-secondary-100 rounded-lg p-4">
                                        {change.diff
                                          ?.split('\n')
                                          .map((s, idx) => (
                                            <p
                                              key={idx}
                                              className={clsx(
                                                s.startsWith('+') &&
                                                  'text-success-500',
                                                s.startsWith('-') &&
                                                  'text-danger-500',
                                                s.startsWith('@@') &&
                                                  'text-secondary-500 my-5',
                                              )}
                                            >
                                              {s}
                                            </p>
                                          ))}
                                      </pre>
                                    </Disclosure.Panel>
                                  </>
                                )}
                              </Disclosure>
                            </li>
                          ))}
                        </ul>
                      </div>
                    )}
                    {isArrayNotEmpty(changes?.modified.indirect) && (
                      <div className="ml-1">
                        <h4 className="text-warning-500 mb-2">
                          Modified Indirectly
                        </h4>
                        <ul className="ml-1">
                          {changes?.modified?.indirect.map(
                            (modelName: string) => (
                              <li
                                key={modelName}
                                className="flex text-warning-500"
                              >
                                <small className="inline-block text-sm leading-4">
                                  {modelName}
                                </small>
                              </li>
                            ),
                          )}
                        </ul>
                      </div>
                    )}
                    {isArrayNotEmpty(changes?.modified.metadata) && (
                      <div className="ml-1">
                        <small>Modified Metadata</small>
                        <ul className="ml-1">
                          {changes?.modified?.metadata.map(
                            (modelName: string) => (
                              <li
                                key={modelName}
                                className="text-gray-500 font-sm h-[1.5rem]"
                              >
                                <small className="inline-block h-[1.25rem] px-1 pl-4 border-l border-gray-500">
                                  {modelName}
                                </small>
                              </li>
                            ),
                          )}
                        </ul>
                      </div>
                    )}
                  </div>
                )}
              </>
            ) : planAction === EnumPlanAction.Running ? (
              <span className="flex items-center">
                <Spinner className="w-4 h-4 mr-2" />
                <small>Checking ...</small>
              </span>
            ) : (
              <div className="ml-1 text-gray-700">
                <h3>No Changes</h3>
              </div>
            )}
          </PlanWizardStep>
          <PlanWizardStep
            headline="Backfill"
            description="Progress"
            disabled={environment == null}
          >
            {isDone && isFalse(hasBackfills) && (
              <div className="mb-4 px-4 py-2 border border-secondary-100 flex items-center justify-between  rounded-lg">
                <h3
                  className={clsx(
                    'font-bold text-lg text-success-500',
                    planState === EnumPlanState.Cancelled && 'text-gray-700',
                    planState === EnumPlanState.Failed && 'text-danger-500',
                  )}
                >
                  {planState === EnumPlanState.Failed
                    ? 'Failed'
                    : planState === EnumPlanState.Cancelled
                    ? 'Cancelled'
                    : 'Completed'}
                </h3>
                <p className="text-xs text-gray-600">
                  {toDateFormat(
                    toDate(mostRecentPlan?.updated_at),
                    'yyyy-mm-dd hh-mm-ss',
                  )}
                </p>
              </div>
            )}
            {planState === EnumPlanState.Applying &&
              isFalse(isDone) &&
              isFalse(hasBackfills) && (
                <span className="flex items-center ml-2">
                  <Spinner className="w-3 h-3 mr-1" />
                  <span className="inline-block ">Applying...</span>
                </span>
              )}

            {hasBackfills && (
              <>
                {isModified(changes?.modified) &&
                  planState !== EnumPlanState.Applying && (
                    <div className="mb-4">
                      <RadioGroup
                        className="rounded-lg w-full"
                        value={category}
                        onChange={setCategory}
                      >
                        {categories.map(category => (
                          <RadioGroup.Option
                            key={category.name}
                            value={category}
                            className={({ active, checked }) =>
                              `${
                                active
                                  ? 'ring-2 ring-secodary-500 ring-opacity-60 ring-offset ring-offset-sky-300'
                                  : ''
                              }
                              ${
                                checked
                                  ? 'bg-secondary-500 bg-opacity-75 text-white'
                                  : 'bg-secondary-100'
                              } elative flex cursor-pointer rounded-md px-3 py-2 focus:outline-none mb-2`
                            }
                          >
                            {({ checked }) => (
                              <>
                                <div className="flex w-full items-center justify-between">
                                  <div className="flex items-center">
                                    <div className="text-sm">
                                      <RadioGroup.Label
                                        as="p"
                                        className={`font-medium  ${
                                          checked
                                            ? 'text-white'
                                            : 'text-gray-900'
                                        }`}
                                      >
                                        {category.name}
                                      </RadioGroup.Label>
                                      <RadioGroup.Description
                                        as="span"
                                        className={`inline ${
                                          checked
                                            ? 'text-sky-100'
                                            : 'text-gray-500'
                                        } text-xs`}
                                      >
                                        <span>{category.description}</span>
                                      </RadioGroup.Description>
                                    </div>
                                  </div>
                                  {checked && (
                                    <div className="shrink-0 text-white">
                                      <CheckCircleIcon className="h-6 w-6" />
                                    </div>
                                  )}
                                </div>
                              </>
                            )}
                          </RadioGroup.Option>
                        ))}
                      </RadioGroup>
                    </div>
                  )}
                {category?.id !== 'no-change' && environment != null && (
                  <>
                    <Suspense fallback={<Spinner className="w-4 h-4 mr-2" />}>
                      <Tasks
                        environment={environment}
                        tasks={tasks}
                        changes={changes}
                        updated_at={mostRecentPlan?.updated_at}
                      />
                    </Suspense>
                    <form>
                      <fieldset className="flex w-full">
                        <Input
                          className="w-full"
                          label="Start Date"
                          disabled={
                            isPlanInProgress ||
                            planAction === EnumPlanAction.Done
                          }
                          value={backfill_start}
                          onInput={(e: React.ChangeEvent<HTMLInputElement>) => {
                            setBackfillDate('start', e.target.value)
                          }}
                        />
                        <Input
                          className="w-full"
                          label="End Date"
                          disabled={
                            isPlanInProgress ||
                            planAction === EnumPlanAction.Done
                          }
                          value={backfill_end}
                          onInput={(e: React.ChangeEvent<HTMLInputElement>) => {
                            setBackfillDate('end', e.target.value)
                          }}
                        />
                      </fieldset>
                    </form>
                  </>
                )}
              </>
            )}

            {hasChanges &&
              isFalse(isPlanInProgress) &&
              isFalse(hasBackfills) && (
                <div className="ml-1 text-gray-700">
                  <Divider className="h-1 w-full mb-4" />
                  <h3>Explanation why we dont need to Backfill</h3>
                </div>
              )}
          </PlanWizardStep>
        </>
      )}
    </ul>
  )
}
interface PropsPlanWizardStep extends React.ButtonHTMLAttributes<HTMLElement> {
  headline: number | string
  description: string
  disabled?: boolean
}

function PlanWizardStep({
  headline,
  description,
  children,
  disabled = false,
}: PropsPlanWizardStep): JSX.Element {
  return (
    <li className="mb-2 flex items-center w-full">
      <div className="flex items-start w-full">
        <PlanWizardStepHeader
          className="min-w-[25%] text-right pr-12"
          headline={headline}
          disabled={disabled}
        >
          {description}
        </PlanWizardStepHeader>
        <div className="w-full pt-1">{!disabled && children}</div>
      </div>
    </li>
  )
}

interface PropsPlanWizardStepHeader
  extends React.ButtonHTMLAttributes<HTMLElement> {
  headline: number | string
  disabled?: boolean
}

function PlanWizardStepHeader({
  disabled = false,
  headline = 1,
  children,
  className,
}: PropsPlanWizardStepHeader): JSX.Element {
  return (
    <div
      className={clsx(
        disabled && 'opacity-40 cursor-not-allowed',
        'mb-4 ',
        className,
      )}
    >
      <h3 className="whitespace-nowrap text-gray-600 font-bold text-lg">
        {headline}
      </h3>
      {children != null && (
        <small className="whitespace-nowrap text-gray-500">{children}</small>
      )}
    </div>
  )
}
