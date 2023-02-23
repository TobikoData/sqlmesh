import { Disclosure } from '@headlessui/react'
import { PlusCircleIcon, MinusCircleIcon } from '@heroicons/react/24/solid'
import clsx from 'clsx'
import { useStoreContext } from '~/context/context'
import { useStorePlan } from '~/context/plan'
import { isStringEmptyOrNil } from '../../../utils'
import Input from '../input/Input'
import Toggle from '../toggle/Toggle'

export default function PlanWizardStepOptions(): JSX.Element {
  return (
    <li className="mt-6 mb-2 mb-6">
      <FormPlanOptions />
    </li>
  )
}

function FormPlanOptions(): JSX.Element {
  const planOptions = useStorePlan(s => s.planOptions)
  const setPlanOptions = useStorePlan(s => s.setPlanOptions)

  const environment = useStoreContext(s => s.environment)

  return (
    <form className="w-full h-full">
      <fieldset className={clsx('mb-10 mt-6')}>
        <h2 className="whitespace-nowrap text-xl font-bold mb-1 text-gray-900">
          Set Dates
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
              className="w-[50%] ml-0  mr-0"
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
        </div>
      </fieldset>
      <fieldset className={clsx('mb-4 mt-6')}>
        <Disclosure>
          {({ open }) => (
            <>
              <Disclosure.Button className="flex items-center w-full justify-between rounded-lg text-left text-sm">
                <h2 className="whitespace-nowrap text-xl font-bold mb-1 text-gray-900">
                  Additional Options
                </h2>

                {open ? (
                  <MinusCircleIcon className="h-6 w-6 text-secondary-500" />
                ) : (
                  <PlusCircleIcon className="h-6 w-6 text-secondary-500" />
                )}
              </Disclosure.Button>
              <Disclosure.Panel className="px-4 pb-2 text-sm text-gray-500">
                <div className="mt-3">
                  <div className="flex">
                    <Input
                      className="w-[50%] ml-0"
                      label="From Environment"
                      info="The environment to base the plan on rather than local files"
                      placeholder={
                        isStringEmptyOrNil(environment)
                          ? 'Envoirnment Name'
                          : environment
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
                    <div className="block my-2">
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
                        label="Auto Apply"
                        info="Automatically apply the plan after it is generated"
                        enabled={Boolean(planOptions.autoApply)}
                        setEnabled={(value: boolean) => {
                          setPlanOptions({ autoApply: value })
                        }}
                      />
                    </div>
                  </div>
                </div>
              </Disclosure.Panel>
            </>
          )}
        </Disclosure>
      </fieldset>
    </form>
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
