import { RadioGroup } from "@headlessui/react";
import { CheckCircleIcon } from "@heroicons/react/24/solid";
import clsx from "clsx";
import { useEffect } from "react";
import { useApiContextByEnvironment } from "../../../api";
import { EnumPlanState, EnumPlanAction, useStorePlan } from "../../../context/plan";
import { includes, isArrayNotEmpty, toDate, toDateFormat, toRatio } from "../../../utils";
import { Divider } from "../divider/Divider";
import { Progress } from "../progress/Progress";
import { isModified } from "./help";

export function PlanWizard({
  id,
}: {
  id: string
}) {
  const planState = useStorePlan(s => s.state)
  const planAction = useStorePlan(s => s.action)
  const setPlanAction = useStorePlan(s => s.setAction)
  const backfills = useStorePlan(s => s.backfills)
  const setBackfills = useStorePlan(s => s.setBackfills)
  const setEnvironment = useStorePlan(s => s.setEnvironment)
  const environment = useStorePlan(s => s.environment)
  const setCategory = useStorePlan(s => s.setCategory)
  const category = useStorePlan(s => s.category)
  const categories = useStorePlan(s => s.categories)
  const setWithBackfill = useStorePlan(s => s.setWithBackfill)
  const setBackfillDate = useStorePlan(s => s.setBackfillDate)
  const backfill_start = useStorePlan(s => s.backfill_start)
  const backfill_end = useStorePlan(s => s.backfill_end)
  const plan = useStorePlan(s => s.lastPlan || s.activePlan)
  const activePlan = useStorePlan(s => s.activePlan)

  const { data: context } = useApiContextByEnvironment(environment)

  useEffect(() => {
    if (context?.environment == null) return

    setBackfills(context.backfills ?? [])
    setEnvironment(context.environment)

    if (isArrayNotEmpty(context.backfills)) {
      setCategory(categories[0])
      setPlanAction(EnumPlanAction.Apply)
    } else if (isModified(context.changes?.modified) || isArrayNotEmpty(context.changes?.added) || isArrayNotEmpty(context.changes?.removed)) {
      setPlanAction(EnumPlanAction.Apply)
    } else {
      setPlanAction(EnumPlanAction.Done)
    }

    setBackfillDate('start', toDateFormat(toDate(context.start)))
    setBackfillDate('end', toDateFormat(toDate(context.end)))
  }, [context])

  useEffect(() => {
    setWithBackfill(isArrayNotEmpty(backfills) && category?.id != 'no-change')
  }, [backfills, category])

  function getContext(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault()
    e.stopPropagation()

    setPlanAction(EnumPlanAction.Running)

    const elForm = e.target as HTMLFormElement
    const data = new FormData(elForm)

    setEnvironment(String(data.get('environment')))

    elForm.reset()
  }

  const isPlanInProgress = planAction === EnumPlanState.Canceling || planState === EnumPlanState.Applying
  const changes = context?.changes
  const withChanges = (isModified(changes?.modified) || isArrayNotEmpty(changes?.added) || isArrayNotEmpty(changes?.removed))

  return (
    <ul>
      <PlanWizardStep headline="Setup" description='Set Options'>
        {environment ? (
          <div>
            <h4 className="ml-1">Current Environment is <b className='px-2 py-1 font-sm rounded-md bg-secondary-100'>{environment}</b></h4>
          </div>
        ) : (
          <form onSubmit={getContext} id={id}>
            <fieldset className='mb-4'>
              <label htmlFor="">
                <small>Environment (Optional)</small>
                <input
                  type="text"
                  name="environment"
                  className="block bg-gray-100 px-2 py-1 rounded-md w-full"
                />
                <div className="flex items-center">
                  <small>Maybe?</small>
                  <ul className="flex ml-2">
                    {['prod', 'dev', 'stage'].map(env => (
                      <li key={env} className="mr-3 border-b cursor-pointer hover:opacity-50" onClick={() => setEnvironment(env)}>
                        <small className="font-sm">
                          {env}
                        </small>
                      </li>
                    ))}
                  </ul>
                </div>
              </label>
            </fieldset>
          </form>
        )}
      </PlanWizardStep>
      <PlanWizardStep headline="Models" description='Review Changes' disabled={context?.environment == null}>
        {withChanges ? (
          <>
            <div className="flex">
              {isArrayNotEmpty(changes?.added) && (
                <div className='ml-4 mb-8'>
                  <h4 className='text-success-500 mb-2'>Added Models</h4>
                  <ul className='ml-2'>
                    {changes?.added.map((modelName: string) => (
                      <li key={modelName} className='text-success-500 font-sm h-[1.5rem]'>
                        <small className="inline-block h-[1.25rem] px-1 pl-4 border-l border-success-500">{modelName}</small>
                      </li>
                    ))}
                  </ul>
                </div>
              )}
              {isArrayNotEmpty(changes?.removed?.length) && (
                <div className='ml-4 mb-8'>
                  <h4 className='text-danger-500 mb-2'>Removed Models</h4>
                  <ul className='ml-2'>
                    {changes?.added.map((modelName: string) => (
                      <li key={modelName} className='text-danger-500 font-sm h-[1.5rem]'>
                        <small className="inline-block h-[1.25rem] px-1 pl-4 border-l border-danger-500">{modelName}</small>
                      </li>
                    ))}
                  </ul>
                </div>
              )}
            </div>
            {isModified(changes?.modified) && (
              <div className="flex">
                {isArrayNotEmpty(changes?.modified.direct) && (
                  <div className="ml-1">
                    <h4 className='text-secondary-500 mb-2'>Modified Directly</h4>
                    <ul className='ml-1 mr-3'>
                      {changes?.modified.direct.map(change => (
                        <li key={change.model_name} className='text-secondary-500 font-sm h-[1.5rem]'>
                          <small className="inline-block h-[1.25rem] px-1 pl-4 border-l border-secondary-500">{change.model_name}</small>
                        </li>
                      ))}
                    </ul>
                  </div>
                )}
                {isArrayNotEmpty(changes?.modified.indirect) && (
                  <div className="ml-1 mr-3">
                    <h4 className='text-warning-500 mb-2'>Modified Indirectly</h4>
                    <ul className='ml-1'>
                      {changes?.modified?.indirect.map((modelName: string) => (
                        <li key={modelName} className='text-warning-500 font-sm h-[1.5rem]'>
                          <small className="inline-block h-[1.25rem] px-1 pl-4 border-l border-warning-500">{modelName}</small>
                        </li>
                      ))}
                    </ul>
                  </div>
                )}
                {isArrayNotEmpty(changes?.modified.metadata) && (
                  <div className="ml-1">
                    <small>Modified Metadata</small>
                    <ul className='ml-1'>
                      {changes?.modified?.metadata.map((modelName: string) => (
                        <li key={modelName} className='text-gray-500 font-sm h-[1.5rem]'>
                          <small className="inline-block h-[1.25rem] px-1 pl-4 border-l border-gray-500">{modelName}</small>
                        </li>
                      ))}
                    </ul>
                  </div>
                )}
              </div>
            )}
          </>
        ) : (
          <div className="ml-1 text-gray-700">
            <Divider className="h-1 w-full mb-4" />
            <h3>No Changes</h3>
          </div>
        )}
      </PlanWizardStep>
      {withChanges && (
        <PlanWizardStep headline="Backfill" description='Progress' disabled={context?.environment == null}>
          {includes([EnumPlanState.Cancelled, EnumPlanState.Finished, EnumPlanState.Failed], planState) && planAction === EnumPlanAction.Done && (
            <div className="mb-4 px-4 py-2 border border-secondary-100 flex items-center justify-between  rounded-lg">
              <h3
                className={clsx(
                  "font-bold text-lg ",
                  planState === EnumPlanState.Cancelled && 'text-gray-700',
                  planState === EnumPlanState.Finished && 'text-success-500',
                  planState === EnumPlanState.Failed && 'text-danger-500',
                )}
              >{planState === EnumPlanState.Finished ? 'Completed' : planState === EnumPlanState.Cancelled ? 'Canceled' : 'Failed'}</h3>
              <p className="text-xs text-gray-600">{toDateFormat(toDate(plan?.updated_at), 'yyyy-mm-dd hh-mm-ss')}</p>
            </div>
          )}
          {isArrayNotEmpty(backfills) ? (
            <>
              {isModified(changes?.modified) && planState !== EnumPlanState.Applying && (
                <div className="mb-4">
                  <RadioGroup
                    className='rounded-lg w-full'
                    value={category}
                    onChange={setCategory}
                  >
                    {categories.map(category => (
                      <RadioGroup.Option
                        key={category.name}
                        value={category}
                        className={({ active, checked }) =>
                          `${active
                            ? 'ring-2 ring-secodary-500 ring-opacity-60 ring-offset ring-offset-sky-300'
                            : ''
                          }
                          ${checked
                            ? 'bg-secondary-500 bg-opacity-75 text-white'
                            : 'bg-secondary-100'
                          }
                      relative flex cursor-pointer rounded-md px-3 py-2 focus:outline-none mb-2`
                        }
                      >
                        {({ checked }) => (
                          <>
                            <div className="flex w-full items-center justify-between">
                              <div className="flex items-center">
                                <div className="text-sm">
                                  <RadioGroup.Label
                                    as="p"
                                    className={`font-medium  ${checked ? 'text-white' : 'text-gray-900'
                                      }`}
                                  >
                                    {category.name}
                                  </RadioGroup.Label>
                                  <RadioGroup.Description
                                    as="span"
                                    className={`inline ${checked ? 'text-sky-100' : 'text-gray-500'} text-xs`}
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
              {category && category.id != 'no-change' && (
                <>
                  <ul className="mb-4">
                    {backfills.filter(item => category.id === 'non-breaking-change'
                      ? !changes?.modified?.indirect?.find(model_name => model_name === item.model_name)
                      : true
                    ).map(snapshot => (
                      <li key={snapshot.model_name} className='text-gray-600 font-light w-full mb-2'>
                        <div className="flex justify-between items-center w-full">
                          <div className="flex justify-end items-center whitespace-nowrap text-gray-900">
                            <p
                              className={clsx(
                                "font-bold text-xs",
                                changes?.modified?.direct?.find(change => change.model_name === snapshot.model_name) && 'text-secondary-500',
                                changes?.added?.find(name => name === snapshot.model_name) && 'text-success-500',
                                changes?.modified?.indirect?.find(name => name === snapshot.model_name) && 'text-warning-500',
                              )}
                            >
                              <span className="inline-block pr-3 text-xs text-gray-500">{`${snapshot.interval[0]} - ${snapshot.interval[1]}`}</span>
                              {snapshot.model_name}
                            </p>

                          </div>
                          <p className="inline-block text-xs">
                            <small>{activePlan?.tasks?.[snapshot.model_name]?.completed ?? 0} / {snapshot.batches} batch{snapshot.batches > 1 ? 'es' : ''}</small>
                            <small className="inline-block ml-2 font-bold">{Math.ceil(toRatio(activePlan?.tasks?.[snapshot.model_name]?.completed, activePlan?.tasks?.[snapshot.model_name]?.total))}%</small>
                          </p>
                        </div>
                        <Progress
                          progress={Math.ceil(toRatio(activePlan?.tasks?.[snapshot.model_name]?.completed, activePlan?.tasks?.[snapshot.model_name]?.total))}
                        />
                      </li>
                    ))}
                  </ul>
                  {<form className={clsx('flex ml-1 mt-1')}>
                    <label className={clsx(
                      'mb-3 mr-4 text-left',
                      (isPlanInProgress || planAction === EnumPlanAction.Done) && 'opacity-50 pointer-events-none cursor-not-allowed'
                    )}>
                      <small>Start Date</small>
                      <input
                        type="text"
                        name="start_date"
                        className="block bg-gray-100 px-2 py-1 rounded-md text-sm text-gray-700"
                        disabled={isPlanInProgress || planAction === EnumPlanAction.Done}
                        value={backfill_start}
                        onChange={(e) => setBackfillDate('start', e.target.value)}
                      />
                      <small className="text-xs text-gray-500">eg. '1 year', '2020-01-01'</small>
                    </label>
                    <label className={clsx(
                      'mb-3 text-left',
                      (isPlanInProgress || planAction === EnumPlanAction.Done) && 'opacity-50 pointer-events-none cursor-not-allowed'
                    )}>
                      <small>End Date</small>
                      <input
                        type="text"
                        name="end_date"
                        className="block bg-gray-100 px-2 py-1 rounded-md text-sm text-gray-700"
                        disabled={isPlanInProgress || planAction === EnumPlanAction.Done}
                        value={backfill_end}
                        onChange={(e) => setBackfillDate('end', e.target.value)}
                      />
                      <small className="text-xs text-gray-500">eg. '1 year', '2020-01-01'</small>
                    </label>
                  </form>}
                </>
              )}
            </>
          ) : (
            <div className="ml-1 text-gray-700">
              <Divider className="h-1 w-full mb-4" />
              <h3>Explanation why we dont need to Backfill</h3>
            </div>
          )}
        </PlanWizardStep>
      )}
    </ul>
  )
}

function PlanWizardStep({ headline, description, children, disabled = false }: any) {
  return (
    <li className='mb-2'>
      <div className="flex items-start">
        <PlanWizardStepHeader className='min-w-[25%] text-right pr-12' headline={headline} disabled={disabled}>
          {description}
        </PlanWizardStepHeader>
        <div className="w-full pt-1">
          {disabled === false && children}
        </div>
      </div>
    </li>
  )
}

function PlanWizardStepHeader({ disabled = false, headline = 1, children = '', className }: any) {
  return (
    <div className={clsx(disabled && 'opacity-40 cursor-not-allowed', 'mb-4 ', className)}>
      <h3 className='whitespace-nowrap text-gray-600 font-bold text-lg'>{headline}</h3>
      <small className="text-gray-500">{children}</small>
    </div>
  )
}