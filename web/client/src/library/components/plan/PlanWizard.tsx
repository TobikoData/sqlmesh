import { RadioGroup } from "@headlessui/react";
import { CheckCircleIcon } from "@heroicons/react/24/solid";
import clsx from "clsx";
import { useEffect } from "react";
import { useApiContextByEnvironment } from "../../../api";
import { EnumPlanState, useStorePlan } from "../../../context/plan";
import { isArrayNotEmpty } from "../../../utils";
import { Divider } from "../divider/Divider";
import { Progress } from "../progress/Progress";

export function PlanWizard({
  id
}: {
  id: string
}) {
  const planState = useStorePlan((s: any) => s.state)
  const setPlanAction = useStorePlan((s: any) => s.setAction)
  const backfills = useStorePlan((s: any) => s.backfills)
  const setBackfills = useStorePlan((s: any) => s.setBackfills)
  const environment = useStorePlan((s: any) => s.environment)
  const setEnvironment = useStorePlan((s: any) => s.setEnvironment)
  const setCategory = useStorePlan((s: any) => s.setCategory)
  const category = useStorePlan((s: any) => s.category)
  const categories = useStorePlan((s: any) => s.categories)
  const setWithBackfill = useStorePlan((s: any) => s.setWithBackfill)

  const { data: context } = useApiContextByEnvironment(environment)

  useEffect(() => {
    if (context == null) return

    setBackfills(context.backfills)

    if (isArrayNotEmpty(context.backfills)) {
      setCategory(categories[0])
      setPlanAction(EnumPlanState.Apply)
    } else if (isModified(context.changes?.modified) || isArrayNotEmpty(context.changes?.added) || isArrayNotEmpty(context.changes?.removed)) {
      setPlanAction(EnumPlanState.Apply)
    } else {
      setPlanAction(EnumPlanState.Done)
    }
  }, [context])

  useEffect(() => {
    setWithBackfill(isArrayNotEmpty(backfills) && category?.id != 'no-change')
  }, [backfills, category])

  function getContext(e: any) {
    e.preventDefault()
    e.stopPropagation()

    setPlanAction(EnumPlanState.Running)

    const elForm = e.target
    const data = new FormData(e.target)

    setEnvironment(String(data.get('environment')))

    elForm.reset()
  }

  return (
    <ul>
      <PlanWizardStep headline="Setup" description='Set Details'>
        {context?.environment ? (
          <div>
            <h4 className="ml-1">Current Environment is <b className='px-2 py-1 font-sm rounded-md bg-secondary-100'>{context?.environment}</b></h4>
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
      <PlanWizardStep headline="Models" description='Review Models' disabled={context?.environment == null}>
        {(isModified(context?.changes?.modified) || isArrayNotEmpty(context?.changes?.added) || isArrayNotEmpty(context?.changes?.removed)) ? (
          <>
            <div className="flex">
              {isArrayNotEmpty(context?.changes?.added) && (
                <div className='ml-4 mb-8'>
                  <h4 className='text-success-500 mb-2'>Added Models</h4>
                  <ul className='ml-2'>
                    {context?.changes?.added.map(modelName => (
                      <li key={modelName} className='text-success-500 font-sm h-[1.5rem]'>
                        <small className="inline-block h-[1.25rem] px-1 pl-4 border-l border-success-500">{modelName}</small>
                      </li>
                    ))}
                  </ul>
                </div>
              )}
              {isArrayNotEmpty(context?.changes?.removed?.length) && (
                <div className='ml-4 mb-8'>
                  <h4 className='text-danger-500 mb-2'>Removed Models</h4>
                  <ul className='ml-2'>
                    {context?.changes?.added.map(modelName => (
                      <li key={modelName} className='text-danger-500 font-sm h-[1.5rem]'>
                        <small className="inline-block h-[1.25rem] px-1 pl-4 border-l border-danger-500">{modelName}</small>
                      </li>
                    ))}
                  </ul>
                </div>
              )}
            </div>
            {isModified(context?.changes?.modified) && (
              <div className="flex">
                {isArrayNotEmpty(context?.changes?.modified.direct) && (
                  <div className="ml-1">
                    <h4 className='text-secondary-500 mb-2'>Modified Directly</h4>
                    <ul className='ml-1 mr-3'>
                      {context?.changes?.modified.direct.map(({ model_name }: any) => (
                        <li key={model_name} className='text-secondary-500 font-sm h-[1.5rem]'>
                          <small className="inline-block h-[1.25rem] px-1 pl-4 border-l border-secondary-500">{model_name}</small>
                        </li>
                      ))}
                    </ul>
                  </div>
                )}
                {isArrayNotEmpty(context?.changes?.modified.indirect) && (
                  <div className="ml-1 mr-3">
                    <h4 className='text-warning-500 mb-2'>Modified Indirectly</h4>
                    <ul className='ml-1'>
                      {(context?.changes?.modified?.indirect || [])?.map((modelName: any) => (
                        <li key={modelName} className='text-warning-500 font-sm h-[1.5rem]'>
                          <small className="inline-block h-[1.25rem] px-1 pl-4 border-l border-warning-500">{modelName}</small>
                        </li>
                      ))}
                    </ul>
                  </div>
                )}
                {isArrayNotEmpty(context?.changes?.modified.metadata) && (
                  <div className="ml-1">
                    <small>Modified Metadata</small>
                    <ul className='ml-1'>
                      {(context?.changes?.modified?.metadata || [])?.map((modelName: any) => (
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
      <PlanWizardStep headline="Backfill" description='Backfill Progress' disabled={context?.environment == null}>
        {isArrayNotEmpty(backfills) ? (
          <>
            {isArrayNotEmpty(backfills) && isModified(context?.changes?.modified) && planState !== EnumPlanState.Applying && (
              <div className="mb-4">
                <RadioGroup
                  className='rounded-lg w-full'
                  value={category}
                  onChange={setCategory}
                >
                  {categories.map((c: any) => (
                    <RadioGroup.Option
                      key={c.name}
                      value={c}
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
                                  {c.name}
                                </RadioGroup.Label>
                                <RadioGroup.Description
                                  as="span"
                                  className={`inline ${checked ? 'text-sky-100' : 'text-gray-500'} text-xs`}
                                >
                                  <span>{c.description}</span>
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
            {category.id != 'no-change' && (
              <>
                <ul className="mb-4">
                  {backfills.filter((item: any) => category.id === 'non-breaking-change' ? !context?.changes?.modified?.indirect?.find(model_name => model_name === item.model_name) : true).map(({ model_name, interval, batches }: any) => (
                    <li key={model_name} className='text-gray-600 font-light w-full mb-2'>
                      <div className="flex justify-between items-center w-full">
                        <div className="flex justify-end items-center whitespace-nowrap text-gray-900">
                          <p className={clsx(
                            "font-bold text-xs ",
                            context?.changes?.modified?.direct?.find(m => m.model_name === model_name) && 'text-secondary-500',
                            context?.changes?.added?.find(name => name === model_name) && 'text-success-500',
                            context?.changes?.modified?.indirect?.find(name => name === model_name) && 'text-warning-500',
                          )}>{model_name}</p>
                          <small className="inline-block pl-3 text-xs text-gray-900">{interval[0]} - {interval[1]}</small>
                        </div>
                        <small className="inline-block text-xs">0 / {batches} batch{batches > 1 ? 'es' : ''}</small>
                      </div>
                      <Progress
                        progress={0}
                        duration={200}
                      />
                    </li>
                  ))}
                </ul>
                {planState !== EnumPlanState.Applying && <form className={clsx('flex ml-1 mt-1')}>
                  <label className='mr-4 mb-3 text-left'>
                    <small>Start Date</small>
                    <input
                      type="text"
                      name="start_date"
                      className="block bg-gray-100 px-2 py-1 rounded-md"
                    />
                    <small className="text-xs text-gray-500">eg. '1 year', '2020-01-01'</small>
                  </label>
                  <label className='mb-3 text-left'>
                    <small>End Date</small>
                    <input
                      type="text"
                      name="end_date"
                      className="block bg-gray-100 px-2 py-1 rounded-md"
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
    </ul >
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

function isModified(modified: unknown): boolean {
  return (Object.values(modified || {}) as any[]).some(isArrayNotEmpty)
}