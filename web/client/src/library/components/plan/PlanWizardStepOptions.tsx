import clsx from 'clsx'
import { Disclosure } from '@headlessui/react'
import { PlusCircleIcon, MinusCircleIcon } from '@heroicons/react/24/solid'
import Input from '../input/Input'
import InputToggle from '../input/InputToggle'
import { EnumPlanActions, usePlan, usePlanDispatch } from './context'
import Plan from './Plan'
import { isFalse } from '~/utils'
import { ModelEnvironment } from '~/models/environment'
import { useStoreContext } from '~/context/context'

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
    no_auto_categorization,
    restate_models,
    isInitialPlanRun,
    create_from,
    include_unmodified,
  } = usePlan()

  const environment = useStoreContext(s => s.environment)
  const environments = useStoreContext(s => s.environments)

  const synchronizedEnvironments = ModelEnvironment.getOnlySynchronized(
    Array.from(environments),
  )

  return (
    <li className={clsx('mt-6 mb-6', className)}>
      <form className="w-full h-full">
        <fieldset className={clsx('mb-10 mt-6')}>
          <h2 className="whitespace-nowrap text-xl font-bold mb-1 px-4">
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
                <Disclosure.Button className="flex items-center w-full justify-between rounded-lg text-left text-sm px-4 pt-3 pb-2 bg-neutral-10 hover:bg-theme-darker dark:hover:bg-theme-lighter">
                  <h2 className="whitespace-nowrap text-xl font-bold mb-1">
                    Additional Options
                  </h2>
                  {open ? (
                    <MinusCircleIcon className="h-6 w-6 text-primary-500" />
                  ) : (
                    <PlusCircleIcon className="h-6 w-6 text-primary-500" />
                  )}
                </Disclosure.Button>
                <Disclosure.Panel className="px-4 pb-2 text-sm text-neutral-500">
                  <div className="mt-3">
                    <div className="flex flex-wrap md:flex-nowrap">
                      {isFalse(environment.isDefault) && (
                        <Input
                          className="w-full"
                          label="Create From Environment"
                          info="The environment to base the plan on rather than local files"
                          disabled={synchronizedEnvironments.length < 2}
                        >
                          {({ className, disabled }) => (
                            <Input.Selector
                              className={clsx(className, 'w-full')}
                              list={ModelEnvironment.getOnlySynchronized(
                                Array.from(environments),
                              ).map(env => ({
                                value: env.name,
                                text: env.name,
                              }))}
                              onChange={env => {
                                dispatch({
                                  type: EnumPlanActions.PlanOptions,
                                  create_from: env,
                                })
                              }}
                              value={create_from}
                              disabled={disabled}
                            />
                          )}
                        </Input>
                      )}
                      <Input
                        className="w-full"
                        label="Restate Models"
                        info="Restate data for specified models and models
                        downstream from the one specified. For production
                        environment, all related model versions will have
                        their intervals wiped, but only the current
                        versions will be backfilled. For development
                        environment, only the current model versions will
                        be affected"
                      >
                        {({ className }) => (
                          <Input.Textfield
                            className={clsx(className, 'w-full')}
                            placeholder="project.model1, project.model2"
                            disabled={isInitialPlanRun}
                            value={restate_models ?? ''}
                            onInput={(
                              e: React.ChangeEvent<HTMLInputElement>,
                            ) => {
                              e.stopPropagation()

                              dispatch({
                                type: EnumPlanActions.PlanOptions,
                                restate_models: e.target.value,
                              })
                            }}
                          />
                        )}
                      </Input>
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
                              type: EnumPlanActions.PlanOptions,
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
                          disabled={isInitialPlanRun}
                          setEnabled={(value: boolean) => {
                            dispatch({
                              type: EnumPlanActions.PlanOptions,
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
                          disabled={isInitialPlanRun}
                          setEnabled={(value: boolean) => {
                            dispatch({
                              type: EnumPlanActions.PlanOptions,
                              skip_backfill: value,
                            })
                          }}
                        />
                      </div>
                    </div>
                    <div className="w-full md:ml-2">
                      <div className="block my-2">
                        <InputToggle
                          label="Include Unmodified"
                          info="Indicates whether to create views for all models in the target development environment or only for modified ones"
                          enabled={Boolean(include_unmodified)}
                          disabled={isInitialPlanRun}
                          setEnabled={(value: boolean) => {
                            dispatch({
                              type: EnumPlanActions.PlanOptions,
                              include_unmodified: value,
                            })
                          }}
                        />
                        <InputToggle
                          label="Forward Only"
                          info="Create a plan for forward-only changes"
                          enabled={Boolean(forward_only)}
                          disabled={isInitialPlanRun}
                          setEnabled={(value: boolean) => {
                            dispatch({
                              type: EnumPlanActions.PlanOptions,
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
                              type: EnumPlanActions.PlanOptions,
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
                          disabled={isInitialPlanRun}
                          setEnabled={(value: boolean) => {
                            dispatch({
                              type: EnumPlanActions.PlanOptions,
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
