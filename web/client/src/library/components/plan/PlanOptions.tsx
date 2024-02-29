import clsx from 'clsx'
import { Disclosure } from '@headlessui/react'
import { PlusCircleIcon, MinusCircleIcon } from '@heroicons/react/24/solid'
import Input from '../input/Input'
import InputToggle from '../input/InputToggle'
import { EnumPlanActions, usePlan, usePlanDispatch } from './context'
import { isFalse, isNil } from '~/utils'
import { ModelEnvironment } from '~/models/environment'
import { useStoreContext } from '~/context/context'
import PlanBackfillDates from './PlanBackfillDates'
import { useEffect, useRef } from 'react'
import { useStorePlan } from '@context/plan'
import Banner from '@components/banner/Banner'

export default function PlanOptions(): JSX.Element {
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
  const elTrigger = useRef<HTMLButtonElement>(null)

  const planAction = useStorePlan(s => s.planAction)
  const planOverview = useStorePlan(s => s.planOverview)
  const planApply = useStorePlan(s => s.planApply)

  const environment = useStoreContext(s => s.environment)
  const environments = useStoreContext(s => s.environments)

  const remoteEnvironments = ModelEnvironment.getOnlyRemote(
    Array.from(environments),
  )

  useEffect(() => {
    dispatch({
      type: EnumPlanActions.PlanOptions,
      ...planOverview.plan_options,
      skip_tests: false,
    })
  }, [])

  const shouldDisable =
    planAction.isProcessing ||
    planAction.isDone ||
    planApply.isFinished ||
    (planOverview.isLatest && isFalse(planAction.isRun)) ||
    planOverview.isVirtualUpdate

  useEffect(() => {
    if (isNil(elTrigger.current)) return

    if (planAction.isProcessing) {
      if (elTrigger.current.classList.contains('--is-open')) {
        elTrigger.current?.click()
      }
    }
  }, [elTrigger, planAction])

  return (
    <form className="w-full">
      <fieldset
        className={clsx(shouldDisable && 'opacity-50 cursor-not-allowed')}
      >
        <PlanBackfillDates
          className={clsx(shouldDisable && 'pointer-events-none')}
        />
      </fieldset>
      <fieldset className="my-2">
        <Banner>
          <Disclosure
            key={String(planAction.isRun)}
            defaultOpen={planAction.isRun}
          >
            {({ open }) => (
              <>
                <Disclosure.Button
                  ref={elTrigger}
                  className={clsx(
                    'w-full flex items-center',
                    open && '--is-open',
                  )}
                >
                  <Banner.Label className="mr-2 text-sm w-full">
                    <span>Additional Options</span>
                    {shouldDisable && <span className="ml-1">(Read Only)</span>}
                  </Banner.Label>
                  {open ? (
                    <MinusCircleIcon className="h-5 w-5 text-neutral-400" />
                  ) : (
                    <PlusCircleIcon className="h-5 w-5 text-neutral-400" />
                  )}
                </Disclosure.Button>
                <Disclosure.Panel
                  unmount={false}
                  className={clsx(
                    'py-4 text-sm text-neutral-500',
                    shouldDisable && 'opacity-50 cursor-not-allowed',
                  )}
                >
                  <div className={clsx(shouldDisable && 'pointer-events-none')}>
                    <div className="flex flex-wrap md:flex-nowrap">
                      {isFalse(environment.isProd) && (
                        <Input
                          className="w-full"
                          label="Create From Environment"
                          info="The environment to base the plan on rather than local files"
                          disabled={remoteEnvironments.length < 2}
                        >
                          {({ className, disabled }) => (
                            <Input.Selector
                              className={clsx(className, 'w-full')}
                              list={ModelEnvironment.getOnlyRemote(
                                Array.from(environments),
                              )
                                .filter(env => env !== environment)
                                .map(env => ({
                                  value: env.name,
                                  text: env.name,
                                }))}
                              onChange={create_from => {
                                dispatch({
                                  type: EnumPlanActions.PlanOptions,
                                  create_from,
                                })
                              }}
                              value={create_from as string}
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
                  <div
                    className={clsx(
                      'flex flex-wrap md:flex-nowrap w-full mt-3',
                      shouldDisable && 'pointer-events-none',
                    )}
                  >
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
                          enabled={
                            Boolean(no_gaps) ||
                            (Boolean(skip_backfill) &&
                              environment.isInitialProd) ||
                            environment.isInitialProd
                          }
                          disabled={
                            isInitialPlanRun ||
                            (Boolean(skip_backfill) &&
                              environment.isInitialProd) ||
                            environment.isInitialProd
                          }
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
                          disabled={
                            isInitialPlanRun || environment.isInitialProd
                          }
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
                          disabled={
                            isInitialPlanRun || environment.isInitialProd
                          }
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
                          disabled={
                            isInitialPlanRun || environment.isInitialProd
                          }
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
                          disabled={
                            isInitialPlanRun || environment.isInitialProd
                          }
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
        </Banner>
      </fieldset>
    </form>
  )
}
