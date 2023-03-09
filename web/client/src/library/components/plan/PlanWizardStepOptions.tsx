import clsx from 'clsx'
import { Disclosure } from '@headlessui/react'
import { PlusCircleIcon, MinusCircleIcon } from '@heroicons/react/24/solid'
import Input from '../input/Input'
import InputToggle from '../input/InputToggle'
import { EnumPlanActions, usePlan, usePlanDispatch } from './context'
import Plan from './Plan'

interface PropsPlanWizardStepOptions
  extends React.HTMLAttributes<HTMLElement> {}

export default function PlanWizardStepOptions({
  className,
}: PropsPlanWizardStepOptions): JSX.Element {
  const dispatch = usePlanDispatch()
  const {
    skip_tests,
    no_gaps,
    skip_backfill,
    forward_only,
    auto_apply,
    from,
    no_auto_categorization,
    restate_models,
  } = usePlan()

  return (
    <li className={clsx('mt-6 mb-2 mb-6', className)}>
      <form className="w-full h-full">
        <fieldset className={clsx('mb-10 mt-6')}>
          <h2 className="whitespace-nowrap text-xl font-bold mb-1 text-gray-900">
            Set Dates
          </h2>
          <div className="mt-3">
            <Plan.BackfillDates />
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
                    <div className="flex flex-wrap md:flex-nowrap">
                      <Input
                        className="w-full md:w-[50%]"
                        label="From Environment"
                        info="The environment to base the plan on rather than local files"
                        placeholder="prod"
                        value={from ?? ''}
                        onInput={(e: React.ChangeEvent<HTMLInputElement>) => {
                          e.stopPropagation()

                          dispatch({
                            type: EnumPlanActions.AdditionalOptions,
                            from: e.target.value,
                          })
                        }}
                      />
                      <Input
                        className="w-full md:w-[50%]"
                        label="Restate Models"
                        info="Restate data for specified models and models
              downstream from the one specified. For production
              environment, all related model versions will have
              their intervals wiped, but only the current
              versions will be backfilled. For development
              environment, only the current model versions will
              be affected"
                        placeholder="project.model1, project.model2"
                        value={restate_models ?? ''}
                        onInput={(e: React.ChangeEvent<HTMLInputElement>) => {
                          e.stopPropagation()

                          dispatch({
                            type: EnumPlanActions.AdditionalOptions,
                            restate_models: e.target.value,
                          })
                        }}
                      />
                    </div>
                  </div>
                  <div className="flex flex-wrap md:flex-nowrap w-full mt-3">
                    <div className="w-full md:mr-2">
                      <div className="block my-2">
                        <InputToggle
                          label="Skip Tests"
                          info="Skip tests prior to generating the plan if they
                  are defined"
                          enabled={Boolean(skip_tests)}
                          setEnabled={(value: boolean) => {
                            dispatch({
                              type: EnumPlanActions.AdditionalOptions,
                              skip_tests: value,
                            })
                          }}
                        />
                      </div>
                      <div className="block my-2">
                        <InputToggle
                          label="No Gaps"
                          info="Ensure that new snapshots have no data gaps when
                  comparing to existing snapshots for matching
                  models in the target environment"
                          enabled={Boolean(no_gaps)}
                          setEnabled={(value: boolean) => {
                            dispatch({
                              type: EnumPlanActions.AdditionalOptions,
                              no_gaps: value,
                            })
                          }}
                        />
                      </div>
                      <div className="block my-2">
                        <InputToggle
                          label="Skip Backfill"
                          info="Skip the backfill step"
                          enabled={Boolean(skip_backfill)}
                          setEnabled={(value: boolean) => {
                            dispatch({
                              type: EnumPlanActions.AdditionalOptions,
                              skip_backfill: value,
                            })
                          }}
                        />
                      </div>
                    </div>
                    <div className="w-full md:ml-2">
                      <div className="block my-2">
                        <InputToggle
                          label="Forward Only"
                          info="Create a plan for forward-only changes"
                          enabled={Boolean(forward_only)}
                          setEnabled={(value: boolean) => {
                            dispatch({
                              type: EnumPlanActions.AdditionalOptions,
                              forward_only: value,
                            })
                          }}
                        />
                      </div>
                      <div className="block my-2">
                        <InputToggle
                          label="Auto Apply"
                          info="Automatically apply the plan after it is generated"
                          enabled={Boolean(auto_apply)}
                          setEnabled={(value: boolean) => {
                            dispatch({
                              type: EnumPlanActions.AdditionalOptions,
                              auto_apply: value,
                            })
                          }}
                        />
                      </div>
                      <div className="block my-2">
                        <InputToggle
                          label="No Auto Categorization"
                          info="Set category manually"
                          enabled={Boolean(no_auto_categorization)}
                          setEnabled={(value: boolean) => {
                            dispatch({
                              type: EnumPlanActions.AdditionalOptions,
                              no_auto_categorization: value,
                            })
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
    </li>
  )
}
