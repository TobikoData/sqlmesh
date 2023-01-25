import { RadioGroup } from "@headlessui/react";
import { CheckCircleIcon } from "@heroicons/react/24/solid";
import clsx from "clsx";
import { useEffect, useRef, useState } from "react";
import { useApiContextByEnvironment } from "../../../api";
import { EnumSize } from "../../../types/enum";
import { Button } from "../button/Button";
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

export function PlanWizard({ id, setShouldShowApply, setShouldShowNext }: { id: string, setShouldShowApply: any, setShouldShowNext: any }) {
  const elFormOverrideDates = useRef(null)
  const elFormBackfillDates = useRef(null)

  const [shouldShowBackfill, setShouldShowBackfill] = useState(true)
  const [selected, setSelected] = useState(strategies[0])
  const [environment, setEnvironment] = useState<string>()
  const [backfillStatus, setBackfillStatus] = useState(false)
  const [backfillTasks, setBackfillTasks] = useState<Array<[string, number]>>([])

  const { data: context } = useApiContextByEnvironment(environment)

  useEffect(() => {
    setShouldShowNext(Boolean(!context?.environment))
  }, [context?.environment])

  useEffect(() => {
    setShouldShowApply(!shouldShowBackfill)
  }, [shouldShowBackfill])

  function getContext(e: any) {
    e.preventDefault()
    e.stopPropagation()

    const elForm = e.target
    const data = new FormData(e.target)

    setEnvironment(String(data.get('environment')))

    elForm.reset()
  }

  function backfill() {
    context?.backfills && setBackfillTasks(context?.backfills.map(([name]) => [name, Math.round(Math.random() * 40)]))

    setTimeout(() => {
      setBackfillStatus(true)
    }, 200)

    setShouldShowBackfill(Boolean(selected == null))
  }

  return (
    <ul>
      <PlanWizardStep step={1} title='Define Environment'>
        {context?.environment ? (
          <div className='ml-4'>
            <h4>Current Environment is <b className='px-2 pb-1 cursor-pointer rounded-md bg-secondary-100' onClick={() => { }}><u>{context?.environment}</u></b></h4>
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
              </label>
              <small>If not provided defaults to <i><b>prod</b></i></small>
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
            {shouldShowBackfill ? (
              <>
                <h4 className='text-gray-700'>Models needing backfill (missing dates):</h4>
                <ul className='ml-4'>
                  {context?.backfills.map(([modelName, dates]) => (
                    <li key={modelName} className='text-gray-600 font-light'>
                      <p>{modelName}: <b><small>{dates}</small></b></p>
                    </li>
                  ))}
                </ul>
                <PlanDates refDates={elFormBackfillDates} prefix='Backfill' />
                <div className='flex justify-end mt-4'>
                  <Button size={EnumSize.sm} onClick={backfill}>
                    Backfill
                  </Button>
                  <Button size={EnumSize.sm} variant='alternative' onClick={() => setShouldShowBackfill(Boolean(selected == null))}>
                    Skip
                  </Button>
                </div>
              </>
            ) : (
              <div>
                <h4 className='text-gray-700 mb-4'>Backfilling Models</h4>
                <ul>
                  {backfillTasks?.map(([name, count]) => (
                    <li key={name} className="mb-4">
                      <p className="flex justify-between">
                        <small>{name}</small>
                        <small>{count} batch{count < 1 || count > 1 ? 'es' : ''}</small>
                      </p>

                      {/* Fake Progress for now. Remove once API works */}
                      <Progress
                        progress={backfillStatus ? 100 : 2}
                        delay={Math.round(Math.random() * 1000)}
                        duration={Math.round(Math.random() * 1000)}
                      />
                    </li>
                  ))}
                </ul>
              </div>
            )}
          </div>
        )}
      </PlanWizardStep>
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
      <PlanWizardStep step={4} title='Override Dates' disabled={context?.environment == null}>
        <PlanDates refDates={elFormOverrideDates} />
      </PlanWizardStep>
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

function PlanDates({ prefix = '', hint = 'eg. "1 year", "2020-01-01"', refDates }: { prefix?: string, hint?: string, refDates: React.RefObject<HTMLFormElement> }) {
  const labelStartDate = `${prefix} Start Date (Optional)`.trim()
  const labelEndDate = `${prefix} End Date (Optional)`.trim()

  return (
    <form ref={refDates} className='flex mt-4'>
      <label className='mx-4'>
        <small>{labelStartDate}</small>
        <input
          type="text"
          name="start_date"
          className="block bg-gray-100 px-2 py-1 rounded-md"
        />
        <small>eg. '1 year', '2020-01-01'</small>
      </label>
      <label className='mx-4'>
        <small>{labelEndDate}</small>
        <input
          type="text"
          name="end_date"
          className="block bg-gray-100 px-2 py-1 rounded-md"
        />
        <small>{hint}</small>
      </label>
    </form>
  )
}