import { RadioGroup } from '@headlessui/react'
import clsx from 'clsx'
import { useRef, useState, useEffect } from 'react'
import { useStorePlan } from '~/context/plan'
import { isNil, isStringEmptyOrNil } from '../../../utils'
import Toggle from '../toggle/Toggle'

const ENVIRONMENTS = ['Prod', 'Dev', 'Stage']
const CUSTOM = 'custom'

export default function PlanWizardStepOptions(): JSX.Element {
  const environment = useStorePlan(s => s.environment)

  return (
    <li className="mt-6 mb-2 flex justify-center items-center w-full overflow-hidden overflow-y-auto scrollbar scrollbar--vertical mb-6">
      {isNil(environment) ? (
        <FormPlanOptions />
      ) : (
        <div className="flex h-full w-full bg-secondary-100 rounded-xl overflow-hidden p-4">
          <h4 className="text-xl">
            <span className="font-bold">Current Environment is</span>
            <b className="ml-1 px-2 py-1 font-sm rounded-md bg-secondary-500 text-secondary-100">
              {environment}
            </b>
          </h4>
        </div>
      )}
    </li>
  )
}

function FormPlanOptions(): JSX.Element {
  const elInputEnvironment = useRef<HTMLInputElement>(null)

  const [selected, setSelected] = useState(CUSTOM)

  const environment = useStorePlan(s => s.environment)
  const planOptions = useStorePlan(s => s.planOptions)
  const setPlanOptions = useStorePlan(s => s.setPlanOptions)

  useEffect(() => {
    if (selected !== CUSTOM) {
      setPlanOptions({ environment: selected.toLowerCase() })
    } else {
      elInputEnvironment.current?.focus()

      setPlanOptions({
        environment: elInputEnvironment.current?.value.toLowerCase(),
      })
    }
  }, [selected])

  useEffect(() => {
    if (environment == null) {
      setSelected(CUSTOM)
    }
  }, [environment])

  return (
    <form className="h-full w-[70%]">
      <fieldset className="mb-4">
        <h2 className="whitespace-nowrap text-xl font-bold mb-1 text-gray-900">
          Select Environment
        </h2>
        <p>
          Plan a migration of the current context&apos;s models with the given
          environment
        </p>
        <div className="flex flex-col w-full mt-3 py-1">
          <RadioGroup
            className="rounded-lg w-full flex flex-wrap"
            value={selected}
            name="environment"
            onChange={(value: string) => {
              setSelected(value)
            }}
          >
            {ENVIRONMENTS.map(option => (
              <div
                key={option}
                className="w-[50%] py-2 odd:pr-2 even:pl-2"
              >
                <RadioGroup.Option
                  value={option}
                  className={({ active, checked }) =>
                    clsx(
                      'relative flex cursor-pointer rounded-md px-3 py-3 focus:outline-none border-2 border-secondary-100',
                      active &&
                        'ring-4 ring-secondary-300 ring-opacity-60 ring-offset ring-offset-secondary-100',
                      checked
                        ? 'border-secondary-500 bg-secondary-100'
                        : 'bg-secondary-100 text-gray-900',
                    )
                  }
                >
                  {({ checked }) => (
                    <RadioGroup.Label
                      as="p"
                      className="font-medium"
                    >
                      {option}
                    </RadioGroup.Label>
                  )}
                </RadioGroup.Option>
              </div>
            ))}
            <div className="w-[50%] py-2 pl-2">
              <RadioGroup.Option
                value={CUSTOM}
                className={({ active, checked }) =>
                  clsx(
                    'relative flex cursor-pointer rounded-md focus:outline-none border-2 border-secondary-100 pointer-events-auto',
                    active &&
                      'ring-4 ring-secondary-300 ring-opacity-60 ring-offset ring-offset-secondary-100',
                    checked
                      ? 'border-secondary-500 bg-secondary-100'
                      : 'bg-secondary-100 text-gray-900',
                  )
                }
              >
                <input
                  ref={elInputEnvironment}
                  type="text"
                  name="environment"
                  className="bg-gray-100 px-3 py-3 rounded-md w-full m-0  focus:outline-none"
                  placeholder="Environment Name"
                  onChange={(e: React.ChangeEvent<HTMLInputElement>) => {
                    e.stopPropagation()

                    setPlanOptions({ environment: e.target.value })
                  }}
                />
              </RadioGroup.Option>
            </div>
          </RadioGroup>
          <small className="block text-xs mt-1 px-3 text-gray-500">
            Please select from suggested environments or provide custom name
          </small>
        </div>
      </fieldset>
      <fieldset
        className={clsx(
          'mb-4 mt-6',
          isStringEmptyOrNil(planOptions.environment) &&
            'opacity-25 pointer-events-none cursor-not-allowed',
        )}
      >
        <h2 className="whitespace-nowrap text-xl font-bold mb-1 text-gray-900">
          Additional Options
        </h2>
        <div className="mt-3">
          <div className="flex">
            <Input
              className="w-[50%] ml-0"
              label="Start Date"
              info="The start datetime of the interval"
              placeholder="01/01/2023"
              value={planOptions.start}
              onInput={(e: React.ChangeEvent<HTMLInputElement>) => {
                e.stopPropagation()

                setPlanOptions({ start: e.target.value })
              }}
            />
            <Input
              className="w-[50%] ml-0"
              label="End Date"
              info="The end datetime of the interval"
              placeholder="02/13/2023"
              value={planOptions.end}
              onInput={(e: React.ChangeEvent<HTMLInputElement>) => {
                e.stopPropagation()

                setPlanOptions({ end: e.target.value })
              }}
            />
          </div>
          <div className="flex">
            <Input
              className="w-[50%] ml-0"
              label="From Environment"
              info="The environment to base the plan on rather than local files"
              placeholder={
                isStringEmptyOrNil(planOptions.environment)
                  ? 'Envoirnment Name'
                  : planOptions.environment
              }
              value={planOptions.from}
              onInput={(e: React.ChangeEvent<HTMLInputElement>) => {
                e.stopPropagation()

                setPlanOptions({ from: e.target.value })
              }}
            />
            <Input
              className="w-[50%] ml-0"
              label="Restate Models"
              info="Restate data for specified models and models
              downstream from the one specified. For production
              environment, all related model versions will have
              their intervals wiped, but only the current
              versions will be backfilled. For development
              environment, only the current model versions will
              be affected"
              placeholder="project.model1, project.model2"
              value={planOptions.restateModel}
              onInput={(e: React.ChangeEvent<HTMLInputElement>) => {
                e.stopPropagation()

                setPlanOptions({ restateModel: e.target.value })
              }}
            />
          </div>
        </div>
        <div className="flex w-full mt-3">
          <div className="w-full mr-2">
            <div className="block my-2">
              <div className="flex justify-between">
                <InputToggle
                  label="Skip Tests"
                  info="Skip tests prior to generating the plan if they
                  are defined"
                  enabled={Boolean(planOptions.skipTests)}
                  setEnabled={(value: boolean) => {
                    setPlanOptions({ skipTests: value })
                  }}
                />
              </div>
            </div>
            <div className="block my-2">
              <div className="flex justify-between">
                <InputToggle
                  label="No Gaps"
                  info="Ensure that new snapshots have no data gaps when
                  comparing to existing snapshots for matching
                  models in the target environment"
                  enabled={Boolean(planOptions.noGaps)}
                  setEnabled={(value: boolean) => {
                    setPlanOptions({ noGaps: value })
                  }}
                />
              </div>
            </div>
            <div className="block my-2">
              <InputToggle
                label="Skip Backfill"
                info="Skip the backfill step"
                enabled={Boolean(planOptions.skipBackfill)}
                setEnabled={(value: boolean) => {
                  setPlanOptions({ skipBackfill: value })
                }}
              />
            </div>
          </div>
          <div className="w-full ml-2">
            <div className="block my-2">
              <InputToggle
                label="Forward Only"
                info="Create a plan for forward-only changes"
                enabled={Boolean(planOptions.forwardOnly)}
                setEnabled={(value: boolean) => {
                  setPlanOptions({ forwardOnly: value })
                }}
              />
            </div>
            <div className="block my-2">
              <InputToggle
                label=" Auto Apply"
                info="Automatically apply the plan after it is generated"
                enabled={Boolean(planOptions.autoApply)}
                setEnabled={(value: boolean) => {
                  setPlanOptions({ autoApply: value })
                }}
              />
            </div>
          </div>
        </div>
      </fieldset>
    </form>
  )
}

function Input({
  type = 'text',
  label,
  info,
  value,
  placeholder,
  onInput,
  className,
}: any): JSX.Element {
  return (
    <div className={clsx('relative block m-2', className)}>
      <label className="block mb-1 px-3 text-sm font-bold">{label}</label>
      <input
        className={clsx(
          'w-full px-3 py-2 bg-secondary-100 rounded-md focus:outline-none border-2 border-secondary-100 focus:ring-4 ring-secondary-300 ring-opacity-60 ring-offset ring-offset-secondary-100 focus:border-secondary-500 bg-secondary-100',
        )}
        type={type}
        value={value}
        placeholder={placeholder}
        onInput={onInput}
      />
      <small className="block text-xs mt-1 px-3 text-gray-500">{info}</small>
    </div>
  )
}

function InputToggle({ label, info, setEnabled, enabled }: any): JSX.Element {
  return (
    <div className="flex justify-between">
      <label className="block mb-1 px-3 text-sm font-bold">
        {label}
        <small className="block text-xs text-gray-500">{info}</small>
      </label>
      <Toggle
        className="mt-2"
        enabled={enabled}
        setEnabled={setEnabled}
      />
    </div>
  )
}
