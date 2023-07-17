import { Menu, Popover, Transition } from '@headlessui/react'
import { ChevronDownIcon, CheckCircleIcon } from '@heroicons/react/24/solid'
import clsx from 'clsx'
import {
  useState,
  useEffect,
  Fragment,
  type MouseEvent,
  useCallback,
} from 'react'
import { useApiPlanRun } from '~/api'
import { type ContextEnvironment } from '~/api/client'
import { useStoreContext } from '~/context/context'
import {
  useStorePlan,
  EnumPlanState,
  EnumPlanAction,
  type PlanAction,
  type PlanState,
} from '~/context/plan'
import { type ModelEnvironment } from '~/models/environment'
import { EnumSide, EnumSize, EnumVariant, type Side } from '~/types/enum'
import {
  isArrayNotEmpty,
  includes,
  isFalse,
  isStringEmptyOrNil,
  debounceAsync,
} from '~/utils'
import { Button, makeButton, type ButtonSize } from '@components/button/Button'
import { Divider } from '@components/divider/Divider'
import Input from '@components/input/Input'
import Spinner from '@components/logo/Spinner'
import {
  EnumPlanChangeType,
  type PlanChangeType,
} from '@components/plan/context'
import PlanChangePreview from '@components/plan/PlanChangePreview'
import { useIDE } from './context'

export default function RunPlan(): JSX.Element {
  const { errors, setIsPlanOpen } = useIDE()

  const planState = useStorePlan(s => s.state)
  const planAction = useStorePlan(s => s.action)
  const setPlanState = useStorePlan(s => s.setState)
  const setPlanAction = useStorePlan(s => s.setAction)
  const setActivePlan = useStorePlan(s => s.setActivePlan)

  const addConfirmation = useStoreContext(s => s.addConfirmation)
  const setShowConfirmation = useStoreContext(s => s.setShowConfirmation)
  const environment = useStoreContext(s => s.environment)
  const environments = useStoreContext(s => s.environments)
  const setInitialDates = useStoreContext(s => s.setInitialDates)
  const hasSynchronizedEnvironments = useStoreContext(
    s => s.hasSynchronizedEnvironments,
  )

  const [hasChanges, setHasChanges] = useState(false)
  const [plan, setPlan] = useState<ContextEnvironment | undefined>()
  const [shouldStartPlanAutomatically, setShouldStartPlanAutomatically] =
    useState(false)

  const {
    refetch: planRun,
    data: dataPlan,
    isFetching,
  } = useApiPlanRun(environment.name, {
    planOptions: { skip_tests: true, include_unmodified: true },
  })

  const debouncedRunPlan = useCallback(debounceAsync(planRun, 1000, true), [
    planRun,
  ])

  useEffect(() => {
    if (shouldStartPlanAutomatically) {
      startPlan()
      setShouldStartPlanAutomatically(false)
    }

    if (isFalse(environment.isSynchronized)) return

    void debouncedRunPlan()
  }, [environment])

  useEffect(() => {
    if (dataPlan == null) return

    setPlan(dataPlan)
    setInitialDates(dataPlan.start, dataPlan.end)
    setHasChanges(
      [
        dataPlan.changes?.added,
        dataPlan.changes?.removed,
        dataPlan.changes?.modified?.direct,
        dataPlan.changes?.modified?.indirect,
        dataPlan.changes?.modified?.metadata,
      ].some(isArrayNotEmpty),
    )
  }, [dataPlan])

  function startPlan(): void {
    setActivePlan(undefined)
    setPlanState(EnumPlanState.Init)
    setPlanAction(EnumPlanAction.Run)
    setIsPlanOpen(true)
  }

  const hasErrors = errors.size > 0
  const showRunButton =
    isFalse(environment.isDefault) || hasSynchronizedEnvironments()
  const showSelectEnvironmentButton =
    showRunButton &&
    (isFalse(environment.isDefault) || isFalse(environment.isInitial))
  const isProcessingPlan = includes(
    [EnumPlanState.Applying, EnumPlanState.Running, EnumPlanState.Cancelling],
    planState,
  )
  const shouldDisableActions =
    hasErrors ||
    isFetching ||
    planAction !== EnumPlanAction.None ||
    planState === EnumPlanState.Applying ||
    planState === EnumPlanState.Running ||
    planState === EnumPlanState.Cancelling

  return (
    <div
      className={clsx(
        'flex items-center',
        environment == null &&
          'opacity-50 pointer-events-none cursor-not-allowed',
      )}
    >
      <div className="flex items-center relative">
        <Button
          className={clsx(
            'mx-0',
            isFalse(environment.isInitial && environment.isDefault) &&
              'rounded-none rounded-l-lg border-r',
          )}
          disabled={shouldDisableActions}
          variant={EnumVariant.Alternative}
          size={EnumSize.sm}
          onClick={(e: React.MouseEvent) => {
            e.stopPropagation()

            if (environment.isDefault && isFalse(environment.isInitial)) {
              addConfirmation({
                headline: 'Running Plan Directly On Prod Environment!',
                description: `Are you sure you want to run your changes directly on prod? Safer choice will be to select or add new environment first.`,
                yesText: `Yes, Run ${environment.name}`,
                noText: 'No, Cancel',
                action() {
                  startPlan()
                },
                children: (
                  <div className="mt-5 pt-4">
                    <h4 className="mb-2">{`${
                      environments.size > 1 ? 'Select or ' : ''
                    }Add Environment`}</h4>
                    <div className="flex items-center relative">
                      {environments.size > 1 && (
                        <SelectEnvironemnt
                          className="mr-2"
                          side="left"
                          environment={environment}
                          showAddEnvironment={false}
                          onSelect={() => {
                            setShouldStartPlanAutomatically(true)
                            setShowConfirmation(false)
                          }}
                          size={EnumSize.md}
                          disabled={
                            isFetching ||
                            planAction !== EnumPlanAction.None ||
                            planState === EnumPlanState.Applying ||
                            planState === EnumPlanState.Cancelling
                          }
                        />
                      )}
                      <AddEnvironemnt
                        className="w-full"
                        size={EnumSize.md}
                        onAdd={() => {
                          setShouldStartPlanAutomatically(true)
                          setShowConfirmation(false)
                        }}
                      />
                    </div>
                  </div>
                ),
              })
            } else {
              startPlan()
            }
          }}
        >
          {isProcessingPlan && <Spinner className="w-3 h-3 mr-1" />}
          <span className="inline-block">
            {getPlanStatus(planState, planAction)}
          </span>
        </Button>
        {showSelectEnvironmentButton && (
          <SelectEnvironemnt
            className="rounded-none rounded-r-lg border-l mx-0"
            environment={environment}
            disabled={shouldDisableActions}
          />
        )}
      </div>
      <PlanChanges
        environment={environment}
        plan={plan}
        isLoading={isFetching}
        hasChanges={hasChanges}
      />
    </div>
  )
}

function PlanChanges({
  isLoading,
  hasChanges,
  environment,
  plan,
}: {
  environment: ModelEnvironment
  plan?: ContextEnvironment
  isLoading: boolean
  hasChanges: boolean
}): JSX.Element {
  return (
    <span className="flex align-center h-full w-full">
      <>
        {isLoading ? (
          <span className="flex items-center ml-2">
            <Spinner className="w-3 h-3 mr-1" />
            <span className="inline-block text-xs text-neutral-500">
              Checking...
            </span>
          </span>
        ) : (
          <>
            {environment.isInitial && environment.isLocal && (
              <span
                title="New"
                className="block ml-1 px-2 first-child:ml-0 rounded-full bg-success-10 text-success-500 text-xs text-center font-bold"
              >
                New
              </span>
            )}
            {[hasChanges, isLoading, environment.isLocal].every(isFalse) && (
              <span
                title="Latest"
                className="block ml-1 px-2 first-child:ml-0 rounded-full bg-neutral-10 text-xs text-center"
              >
                <span>Latest</span>
              </span>
            )}
            {isArrayNotEmpty(plan?.changes?.added) && (
              <ChangesPreview
                headline="Added Models"
                type={EnumPlanChangeType.Add}
                changes={plan!.changes!.added}
              />
            )}
            {isArrayNotEmpty(plan?.changes?.modified.direct) && (
              <ChangesPreview
                headline="Direct Changes"
                type={EnumPlanChangeType.Direct}
                changes={plan!.changes!.modified.direct!.map(
                  ({ model_name }) => model_name,
                )}
              />
            )}
            {isArrayNotEmpty(plan?.changes?.modified.indirect) && (
              <ChangesPreview
                headline="Indirectly Modified"
                type={EnumPlanChangeType.Indirect}
                changes={plan!.changes!.modified.indirect!.map(
                  ci => ci.model_name,
                )}
              />
            )}
            {isArrayNotEmpty(plan?.changes?.removed) && (
              <ChangesPreview
                headline="Removed Models"
                type={EnumPlanChangeType.Remove}
                changes={plan!.changes!.removed}
              />
            )}
          </>
        )}
      </>
    </span>
  )
}

function SelectEnvironemnt({
  onSelect,
  environment,
  disabled,
  side = EnumSide.Right,
  className,
  showAddEnvironment = true,
  size = EnumSize.sm,
}: {
  environment: ModelEnvironment
  disabled: boolean
  className?: string
  size?: ButtonSize
  side?: Side
  onSelect?: () => void
  showAddEnvironment?: boolean
}): JSX.Element {
  const environments = useStoreContext(s => s.environments)
  const setEnvironment = useStoreContext(s => s.setEnvironment)
  const removeLocalEnvironment = useStoreContext(s => s.removeLocalEnvironment)

  const ButtonMenu = makeButton<HTMLDivElement>(Menu.Button)

  return (
    <Menu>
      {({ close }) => (
        <>
          <ButtonMenu
            variant={EnumVariant.Alternative}
            size={size}
            disabled={disabled}
            className={clsx(className)}
          >
            <span
              className={clsx(
                'block overflow-hidden truncate',
                (environment.isLocal || disabled) && 'text-neutral-500',
                environment.isSynchronized && 'text-primary-500',
              )}
            >
              {environment.name}
            </span>
            <span className="pointer-events-none inset-y-0 right-0 flex items-center pl-2">
              <ChevronDownIcon
                className="h-4 w-4"
                aria-hidden="true"
              />
            </span>
          </ButtonMenu>
          <Transition
            as={Fragment}
            leave="transition ease-in duration-100"
            leaveFrom="opacity-100"
            leaveTo="opacity-0"
          >
            <div
              className={clsx(
                'absolute top-9 overflow-hidden shadow-xl bg-theme border-2 border-primary-20 rounded-md flex flex-col z-10',
                side === EnumSide.Left && 'left-0',
                side === EnumSide.Right && 'right-0',
              )}
            >
              <Menu.Items className="overflow-auto max-h-80 py-2 hover:scrollbar scrollbar--vertical">
                {Array.from(environments).map(env => (
                  <Menu.Item key={env.name}>
                    {({ active }) => (
                      <div
                        onClick={(e: MouseEvent) => {
                          e.stopPropagation()

                          setEnvironment(env)

                          onSelect?.()
                        }}
                        className={clsx(
                          'flex justify-between items-center px-4 py-1 cursor-pointer overflow-auto',
                          active && 'bg-primary-10',
                          env === environment &&
                            'pointer-events-none cursor-default bg-secondary-10',
                        )}
                      >
                        <div className="flex items-start">
                          <CheckCircleIcon
                            className={clsx(
                              'w-4 h-4 text-primary-500 mt-1',
                              active && 'opacity-10',
                              env !== environment && 'opacity-0',
                            )}
                          />
                          <span className="block">
                            <span className="flex items-center">
                              <span
                                className={clsx(
                                  'block truncate ml-2',
                                  env.isSynchronized && 'text-primary-500',
                                )}
                              >
                                {env.name}
                              </span>
                              <small className="block ml-2">({env.type})</small>
                            </span>
                            {env.isDefault && (
                              <span className="flex ml-2">
                                <small className="text-xs text-neutral-500">
                                  Default Environment
                                </small>
                              </span>
                            )}
                          </span>
                        </div>
                        {env.isLocal && env !== environment && (
                          <Button
                            className="my-0 mx-0"
                            size={EnumSize.xs}
                            variant={EnumVariant.Neutral}
                            onClick={(e: MouseEvent) => {
                              e.stopPropagation()

                              removeLocalEnvironment(env)
                            }}
                          >
                            -
                          </Button>
                        )}
                      </div>
                    )}
                  </Menu.Item>
                ))}
                {showAddEnvironment && (
                  <>
                    <Divider />
                    <AddEnvironemnt
                      onAdd={close}
                      className="mt-2 px-2"
                    />
                  </>
                )}
              </Menu.Items>
              <Divider />
            </div>
          </Transition>
        </>
      )}
    </Menu>
  )
}

function AddEnvironemnt({
  onAdd,
  className,
  label = 'Add',
  size = EnumSize.sm,
}: {
  size?: ButtonSize
  onAdd?: () => void
  className?: string
  label?: string
}): JSX.Element {
  const getNextEnvironment = useStoreContext(s => s.getNextEnvironment)
  const isExistingEnvironment = useStoreContext(s => s.isExistingEnvironment)
  const addLocalEnvironment = useStoreContext(s => s.addLocalEnvironment)

  const [customEnvironment, setCustomEnvironment] = useState<string>('')
  const [createdFrom, setCreatedFrom] = useState<string>(
    getNextEnvironment().name,
  )

  return (
    <div className={clsx('flex w-full items-center', className)}>
      <Input
        className="my-0 mx-0 mr-4 min-w-[10rem] w-full"
        size={size}
      >
        {({ className }) => (
          <Input.Textfield
            className={clsx(className, 'w-full')}
            placeholder="Environment"
            value={customEnvironment}
            onInput={(e: React.ChangeEvent<HTMLInputElement>) => {
              e.stopPropagation()

              setCustomEnvironment(e.target.value)
            }}
          />
        )}
      </Input>
      <Button
        className="my-0 mx-0 font-bold"
        size={size}
        disabled={
          isStringEmptyOrNil(customEnvironment) ||
          isExistingEnvironment(customEnvironment)
        }
        onClick={(e: MouseEvent) => {
          e.stopPropagation()

          addLocalEnvironment(customEnvironment, createdFrom)

          setCustomEnvironment('')
          setCreatedFrom(getNextEnvironment().name)

          onAdd?.()
        }}
      >
        {label}
      </Button>
    </div>
  )
}

function ChangesPreview({
  headline,
  type,
  changes,
}: {
  headline?: string
  type: PlanChangeType
  changes: string[]
}): JSX.Element {
  const [isShowing, setIsShowing] = useState(false)

  return (
    <Popover
      onMouseEnter={() => {
        setIsShowing(true)
      }}
      onMouseLeave={() => {
        setIsShowing(false)
      }}
      className="relative flex"
    >
      {() => (
        <>
          <span
            className={clsx(
              'inline-block ml-1 px-2 rounded-full text-xs font-bold text-neutral-100 cursor-default border border-inherit',
              type === EnumPlanChangeType.Add &&
                'bg-success-500 border-success-500',
              type === EnumPlanChangeType.Remove &&
                'bg-danger-500 border-danger-500',
              type === EnumPlanChangeType.Direct &&
                'bg-secondary-500 border-secondary-500',
              type === EnumPlanChangeType.Indirect &&
                'bg-warning-500 border-warning-500',
              type === 'metadata' && 'bg-neutral-500 border-neutral-500',
            )}
          >
            {changes.length}
          </span>
          <Transition
            show={isShowing}
            as={Fragment}
            enter="transition ease-out duration-200"
            enterFrom="opacity-0 translate-y-1"
            enterTo="opacity-100 translate-y-0"
            leave="transition ease-in duration-150"
            leaveFrom="opacity-100 translate-y-0"
            leaveTo="opacity-0 translate-y-1"
          >
            <Popover.Panel className="absolute right-1 z-10 mt-8 transform flex p-2 bg-theme-lighter shadow-xl focus:ring-2 ring-opacity-5 rounded-lg ">
              <PlanChangePreview
                headline={headline}
                type={type}
                className="w-full h-full max-h-[40vh] overflow-hidden overflow-y-auto "
              >
                <PlanChangePreview.Default
                  type={type}
                  changes={changes}
                />
              </PlanChangePreview>
            </Popover.Panel>
          </Transition>
        </>
      )}
    </Popover>
  )
}

function getPlanStatus(planState: PlanState, planAction: PlanAction): string {
  if (planState === EnumPlanState.Running) return 'Running Plan...'
  if (planState === EnumPlanState.Applying) return 'Applying Plan...'
  if (planState === EnumPlanState.Cancelling) return 'Cancelling Plan...'
  if (planAction === EnumPlanAction.None) return 'Run Plan'

  return 'Setting Plan...'
}
