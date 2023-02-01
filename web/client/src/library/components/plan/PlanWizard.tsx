import { RadioGroup } from "@headlessui/react";
import { CheckCircleIcon } from "@heroicons/react/24/solid";
import clsx from "clsx";
import { useEffect, useRef, useState } from "react";
import { useApiContextByEnvironment } from "../../../api";
import { Divider } from "../divider/Divider";
import { Progress } from "../progress/Progress";
import { EnumStatePlan, StatePlan } from "./Plan";

const strategies = [
  {
    title: 'Breaking Change',
    description: 'This is a breaking change',
  },
  {
    title: 'Non-Breaking Change',
    description: 'This is a non-breaking change',
  },
  {
    title: 'No Change',
    description: 'This is a no change',
  },
]

export function PlanWizard({
  id,
  backfillStatus,
  setPlanState,
  setWithModified,
  planState,
  setEnvironment,
  environment
}: {
  id: string,
  setPlanState: any,
  setWithModified: any,
  backfillStatus: any,
  planState: StatePlan,
  setEnvironment: any,
  environment?: string
}) {
  const elFormOverrideDates = useRef(null)
  const elFormBackfillDates = useRef(null)

  const [selected, setSelected] = useState(strategies[0])


  const { data: context } = useApiContextByEnvironment(environment)
  const backfills = context?.backfills ?? []

  useEffect(() => {
    const withEnironemnt = Boolean(context?.environment != null)
    const withBackfills = Array.isArray(backfills) && backfills.length > 0

    if (withEnironemnt && withBackfills) {
      setPlanState(EnumStatePlan.Backfill)
    }

    if (withEnironemnt && !withBackfills) {
      setPlanState(EnumStatePlan.Apply)
    }


    setWithModified(Object.keys(context?.changes?.modified ?? {}).length > 0)
  }, [context?.environment, backfills, context?.changes])

  function getContext(e: any) {
    e.preventDefault()
    e.stopPropagation()

    const elForm = e.target
    const data = new FormData(e.target)

    setEnvironment(String(data.get('environment')))

    elForm.reset()
  }

  return (
    <ul>
      <PlanWizardStep step={1} title='Details'>
        {context?.environment ? (
          <div className='ml-4'>
            <h4>Current Environment is <b className='px-2 py-1 font-sm cursor-pointer rounded-md bg-secondary-100' onClick={() => {
              setEnvironment(undefined)
              setPlanState(EnumStatePlan.Run)
            }}><u>{context?.environment}</u></b></h4>
          </div>
        ) : (
          <form className='ml-4' onSubmit={getContext} id={id}>
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
      <PlanWizardStep step={2} title='Review Models' disabled={context?.environment == null}>
        <div className="flex">
          {Boolean(context?.changes?.added?.length) && (
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
          {Boolean(context?.changes?.removed?.length) && (
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

          {isModified(context?.changes?.modified) && (
            <div className='ml-4 mb-8'>
              <h4 className='text-gray-700 mb-2'>Modified Models</h4>
              <div className="flex">
                {isModifiedDirect(context?.changes?.modified.direct) && (
                  <div className="ml-1">
                    <small>Direct</small>
                    <ul className='ml-1 mr-3'>
                      {Object.keys(context?.changes?.modified.direct || []).map(modelName => (
                        <li key={modelName} className='text-success-500 font-sm h-[1.5rem]'>
                          <small className="inline-block h-[1.25rem] px-1 pl-4 border-l border-success-500">{modelName}</small>
                        </li>
                      ))}
                    </ul>
                  </div>
                )}
                {isModifiedIndirect(context?.changes?.modified.indirect) && (
                  <div className="ml-1 mr-3">
                    <small>Indirect</small>
                    <ul className='ml-1'>
                      {(context?.changes?.modified?.indirect || [])?.map((modelName: any) => (
                        <li key={modelName} className='text-warning-500 font-sm h-[1.5rem]'>
                          <small className="inline-block h-[1.25rem] px-1 pl-4 border-l border-warning-500">{modelName}</small>
                        </li>
                      ))}
                    </ul>
                  </div>
                )}
                {isModifiedMetadata(context?.changes?.modified.metadata) && (
                  <div className="ml-1">
                    <small>Metadata</small>
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
            </div>
          )}
        </div>

        {backfills.length > 0 ? (
          <div className='mx-4 mb-4 p-8 bg-secondary-100 rounded-lg overflow-hidden'>
            <div className="text-center flex flex-col mb-6">
              <h4 className='font-black text-gray-700'>Models needing backfill (missing dates):</h4>
              {planState === EnumStatePlan.Backfill && <PlanDates refDates={elFormBackfillDates} prefix='Backfill' show={[true, false]} className="inline-block mx-auto" />}
            </div>
            <ul>
              {backfills.map(({ model_name, interval, batches }: any) => (
                <li key={model_name} className='text-gray-600 font-light w-full mb-3'>
                  <div className="flex justify-between items-center w-full">
                    <p className="font-bold">{model_name}</p>
                    <div className="flex justify-end items-center whitespace-nowrap text-sm text-gray-900">
                      <small className="inline-block">0 / {batches} batch{batches > 1 ? 'es' : ''}</small>
                      <small className="inline-block pl-6 font-bold">{interval[0]} - {interval[1]}</small>
                    </div>

                  </div>
                  <Progress
                    progress={backfillStatus != null ? 100 : 0}
                    delay={Math.round(Math.random() * 1000)}
                    duration={Math.round(Math.random() * 1000)}
                  />
                </li>
              ))}
            </ul>

          </div>
        ) : (
          <div className="ml-4 text-gray-700">
            <Divider className="h-1 w-full mb-4" />
            <h3>Latest Data</h3>
          </div>
        )}
      </PlanWizardStep>
      <PlanWizardStep step={3} title='Select Change Strategy' disabled={context?.environment == null}>
        {Object.keys(context?.changes?.modified ?? {}).length > 0 ? (
          <RadioGroup
            className='p-4 pt-5 bg-secondary-100 rounded-lg'
            value={selected}
            onChange={setSelected}
          >
            {strategies.map((strategy) => (
              <RadioGroup.Option
                key={strategy.title}
                value={strategy}
                className={({ active, checked }) =>
                  `${active
                    ? 'ring-2 ring-secodary-500 ring-opacity-60 ring-offset-2 ring-offset-sky-300'
                    : ''
                  }
                  ${checked
                    ? 'bg-secondary-500 bg-opacity-75 text-white'
                    : 'bg-white'
                  }
                          relative flex cursor-pointer rounded-lg px-5 py-4 shadow-md focus:outline-none mb-2`
                }
              >
                {({ active, checked }) => (
                  <>
                    <div className="flex w-full items-center justify-between">
                      <div className="flex items-center">
                        <div className="text-sm">
                          <RadioGroup.Label
                            as="p"
                            className={`font-medium  ${checked ? 'text-white' : 'text-gray-900'
                              }`}
                          >
                            {strategy.title}
                          </RadioGroup.Label>
                          <RadioGroup.Description
                            as="span"
                            className={`inline ${checked ? 'text-sky-100' : 'text-gray-500'
                              }`}
                          >
                            <span>{strategy.description}</span>
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
        ) : (
          <h3>No Changes</h3>
        )}
      </PlanWizardStep>
      <PlanWizardStep step={4} title='Override Dates' disabled={context?.environment == null}>
        {Object.keys(context?.changes?.modified ?? {}).length > 0 ? (
          <PlanDates refDates={elFormOverrideDates} show={[true, false]} />
        ) : (
          <h3>Not Applicable</h3>
        )}
      </PlanWizardStep>
    </ul >
  )
}

function isModifiedDirect(direct: any): boolean {
  return Object.keys(direct ?? {}).length > 0
}

function isModifiedIndirect(indirect: any): boolean {
  return Array.isArray(indirect ?? []) && indirect?.length > 0
}

function isModifiedMetadata(metadata: any): boolean {
  return Array.isArray(metadata ?? []) && metadata?.length > 0
}

function isModified(modified: any): boolean {
  return isModifiedDirect(modified?.direct ?? {})
    || isModifiedIndirect(modified?.indirect)
    || isModifiedMetadata(modified?.metadata)
}

function PlanWizardStep({ step, title, children, disabled = false }: any) {
  return (
    <li className='mb-8'>
      <div className="flex items-start">
        <PlanWizardStepHeader className='min-w-[25%] text-right pr-12' step={step} disabled={disabled}>
          {title}
        </PlanWizardStepHeader>
        <div className="w-full h-full pt-1">
          {disabled === false && children}
        </div>
      </div>
    </li>
  )
}

function PlanWizardStepHeader({ disabled = false, step = 1, children = '', className }: any) {
  return (
    <div className={clsx(disabled && 'opacity-40 cursor-not-allowed', 'mb-4 ', className)}>
      <h3 className='text-secondary-600 font-bold text-lg'>Step {step}</h3>
      <small>{children}</small>
      <Divider className="h-12 mt-3 mr-6" orientation="vertical" />
    </div>
  )
}

function PlanDates({ prefix = '', hint = 'eg. "1 year", "2020-01-01"', refDates, show = [true, true], className }: { prefix?: string, hint?: string, refDates: React.RefObject<HTMLFormElement>, show: [boolean, boolean], className?: string }) {
  const labelStartDate = `${prefix} Start Date (Optional)`.trim()
  const labelEndDate = `${prefix} End Date (Optional)`.trim()
  const [showStartDate, showEndDate] = show;

  return (
    <form ref={refDates} className={clsx('flex mt-4', className)}>
      {showStartDate && (
        <label className='mx-4 text-left'>
          <small>{labelStartDate}</small>
          <input
            type="text"
            name="start_date"
            className="block bg-gray-100 px-2 py-1 rounded-md"
          />
          <small>eg. '1 year', '2020-01-01'</small>
        </label>
      )}
      {showEndDate && (
        <label className='mx-4 text-left'>
          <small>{labelEndDate}</small>
          <input
            type="text"
            name="end_date"
            className="block bg-gray-100 px-2 py-1 rounded-md"
          />
          <small>{hint}</small>
        </label>
      )}
    </form>
  )
}