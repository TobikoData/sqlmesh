import { RadioGroup } from "@headlessui/react";
import { CheckCircleIcon } from "@heroicons/react/24/solid";
import clsx from "clsx";
import { useEffect, useRef, useState } from "react";
import { useApiContextByEnvironment } from "../../../api";
import { Divider } from "../divider/Divider";
import { Progress } from "../progress/Progress";

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
  setShouldShowNext,
  setShouldShowBackfill,
  setShouldShowApply,
  shouldShowBackfill,
  backfillTasks,
  backfillStatus,
}: {
  id: string,
  setShouldShowNext: any,
  setShouldShowBackfill: any,
  setShouldShowApply: any,
  shouldShowBackfill: boolean,
  backfillTasks?: Array<[string, number]>,
  backfillStatus?: boolean,
}) {
  const elFormOverrideDates = useRef(null)
  const elFormBackfillDates = useRef(null)

  const [selected, setSelected] = useState(strategies[0])
  const [environment, setEnvironment] = useState<string>()

  const { data: context } = useApiContextByEnvironment(environment)

  useEffect(() => {
    setShouldShowNext(Boolean(!context?.environment))
  }, [context?.environment])

  useEffect(() => {
    setShouldShowBackfill(Boolean(context?.backfills))
  }, [context?.backfills])

  useEffect(() => {
    setShouldShowApply(Boolean(context?.environment) && !Boolean(context?.backfills))
  }, [context?.environment, context?.backfills])

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
      <PlanWizardStep step={1} title='Define Environment'>
        {context?.environment ? (
          <div className='ml-4'>
            <h4>Current Environment is <b className='px-2 py-1 font-sm cursor-pointer rounded-md bg-secondary-100' onClick={() => setEnvironment(undefined)}><u>{context?.environment}</u></b></h4>
          </div>
        ) : (
          <form className='ml-4' onSubmit={getContext} id={id}>
            <fieldset className='mb-4'>
              <label htmlFor="">
                <small>Environment (Optional)</small>
                <input
                  type="text"
                  name="environment"
                  className="block bg-gray-100 px-2 py-1 rounded-md"
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
        <div className='ml-4 mb-4'>
          {context?.changes?.added && (
            <>
              <h4 className='text-success-500'>Added Models</h4>
              <ul className='ml-4'>
                {context?.changes.added.map(modelName => (
                  <li key={modelName} className='text-gray-600 font-light'>{modelName}</li>
                ))}
              </ul>
            </>
          )}
        </div>
        {context?.backfills && (
          <div className='ml-4 mb-4 p-8 bg-secondary-100 rounded-lg overflow-hidden'>
            <div className="text-center flex flex-col mb-6">
              <h4 className='font-black text-gray-700'>Models needing backfill (missing dates):</h4>
              {shouldShowBackfill && <PlanDates refDates={elFormBackfillDates} prefix='Backfill' show={[true, false]} className="inline-block mx-auto" />}
            </div>
            <ul>
              {context?.backfills.map(([modelName, [start, end], batches]: any = []) => (
                <li key={modelName} className='text-gray-600 font-light w-full mb-3'>
                  <div className="flex justify-between items-center w-full">
                    <p className="font-bold">{modelName}</p>
                    <div className="flex justify-end items-center whitespace-nowrap text-sm text-gray-900">
                      <small className="inline-block">0 / {batches} batch{batches > 1 ? 'es' : ''}</small>
                      <small className="inline-block pl-6 font-bold">{start} - {end}</small>
                    </div>

                  </div>
                  <Progress
                    progress={backfillStatus != null ? 100 : 2}
                    delay={Math.round(Math.random() * 1000)}
                    duration={Math.round(Math.random() * 1000)}
                  />
                </li>
              ))}
            </ul>

          </div>
        )}
      </PlanWizardStep>
      {!context?.changes?.added && (
        <PlanWizardStep step={3} title='Select Change Strategy' disabled={context?.environment == null}>
          <RadioGroup
            className='p-4 bg-secondary-900 rounded-lg'
            value={selected}
            onChange={setSelected}
          >
            {strategies.map((strategy) => (
              <RadioGroup.Option
                key={strategy.title}
                value={strategy}
                className={({ active, checked }) =>
                  `${active
                    ? 'ring-2 ring-white ring-opacity-60 ring-offset-2 ring-offset-sky-300'
                    : ''
                  }
                  ${checked
                    ? 'bg-sky-900 bg-opacity-75 text-white'
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
        </PlanWizardStep>
      )}
      {!context?.changes?.added && (
        <PlanWizardStep step={4} title='Override Dates' disabled={context?.environment == null}>
          <PlanDates refDates={elFormOverrideDates} show={[true, false]} />
        </PlanWizardStep>
      )}
    </ul >
  )
}

function PlanWizardStep({ step, title, children, disabled = false }: any) {
  return (
    <li className='mb-8'>
      <PlanWizardStepHeader step={step} disabled={disabled}>
        {title}
      </PlanWizardStepHeader>
      {disabled === false && children}
    </li>
  )
}

function PlanWizardStepHeader({ disabled = false, step = 1, children = '' }: any) {
  return (
    <div className={clsx(disabled && 'opacity-40 cursor-not-allowed', 'flex items-center mb-4')}>
      <h3 className='text-secondary-600'>Step {step}</h3>
      <Divider className='mx-6' orientation='vertical' />
      <small>{children}</small>
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