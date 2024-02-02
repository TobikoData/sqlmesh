import {
  FolderIcon,
  DocumentTextIcon,
  DocumentCheckIcon,
  ShieldCheckIcon,
  ExclamationTriangleIcon,
  PlayCircleIcon,
  ChevronRightIcon,
  ChevronLeftIcon,
} from '@heroicons/react/20/solid'
import clsx from 'clsx'
import {
  FolderIcon as OutlineFolderIcon,
  DocumentTextIcon as OutlineDocumentTextIcon,
  ExclamationTriangleIcon as OutlineExclamationTriangleIcon,
  DocumentCheckIcon as OutlineDocumentCheckIcon,
  ShieldCheckIcon as OutlineShieldCheckIcon,
  PlayCircleIcon as OutlinePlayCircleIcon,
  CheckCircleIcon,
  ChevronDownIcon,
  MinusCircleIcon,
  StarIcon,
} from '@heroicons/react/24/outline'
import { NavLink, useNavigate } from 'react-router-dom'
import { EnumRoutes } from '~/routes'
import { Divider } from '@components/divider/Divider'
import SplitPane from '@components/splitPane/SplitPane'
import { useStoreContext } from '@context/context'
import { EnumErrorKey, useIDE } from '../ide/context'
import { useStorePlan } from '@context/plan'
import {
  apiDeleteEnvironment,
  useApiEnvironments,
  useApiModels,
  useApiPlanRun,
} from '@api/index'
import { EnumSize, EnumVariant } from '~/types/enum'
import { Modules } from '@api/client'
import {
  isArrayNotEmpty,
  isFalse,
  isStringEmptyOrNil,
  isTrue,
} from '@utils/index'
import Spinner from '@components/logo/Spinner'
import Loading from '@components/loading/Loading'
import {
  type ButtonSize,
  makeButton,
  Button,
  EnumButtonFormat,
} from '@components/button/Button'
import PlanChangePreview from '@components/plan/PlanChangePreview'
import {
  EnumPlanChangeType,
  type PlanChangeType,
} from '@components/plan/context'
import { Menu, Transition, Popover } from '@headlessui/react'
import { type ModelEnvironment } from '@models/environment'
import { type ModelSQLMeshChangeDisplay } from '@models/sqlmesh-change-display'
import { Fragment, useState, type MouseEvent, useEffect } from 'react'
import Input from '@components/input/Input'
import ReportErrors from '@components/report/ReportErrors'
import { ModelPlanOverviewTracker } from '@models/tracker-plan-overview'

export default function Page({
  sidebar,
  content,
}: {
  sidebar: React.ReactNode
  content: React.ReactNode
}): JSX.Element {
  const navigate = useNavigate()

  const splitPaneSizes = useStoreContext(s => s.splitPaneSizes)
  const setSplitPaneSizes = useStoreContext(s => s.setSplitPaneSizes)

  const modules = useStoreContext(s => s.modules)

  return (
    <>
      <SplitPane
        sizes={splitPaneSizes}
        minSize={[0, 0]}
        snapOffset={0}
        className="flex w-full h-full overflow-hidden"
        onDragEnd={setSplitPaneSizes}
      >
        <div className="flex flex-col h-full overflow-hidden">
          {modules.length > 1 && (
            <>
              <div className="flex min-w-[10rem] px-2 h-8 w-full">
                <SelectModule />
              </div>
              <Divider />
            </>
          )}

          <div className="w-full h-full overflow-hidden">{sidebar}</div>
        </div>
        <div className="flex flex-col w-full h-full overflow-hidden">
          {modules.includes(Modules.environments) && (
            <>
              <div className="min-w-[10rem] px-2 h-8 w-full flex items-center">
                <div className="flex">
                  <button
                    onClick={() => navigate(-1)}
                    className="inline-block text-neutral-500 text-xs hover:bg-neutral-5 px-1 py-1 rounded-full"
                  >
                    <ChevronLeftIcon className="min-w-4 h-4" />
                  </button>
                  <button
                    onClick={() => navigate(1)}
                    className="inline-block text-neutral-500 text-xs hover:bg-neutral-5 px-1 py-1 rounded-full"
                  >
                    <ChevronRightIcon className="min-w-4 h-4" />
                  </button>
                </div>
                <EnvironmentDetails />
              </div>
              <Divider />
            </>
          )}
          <div className="w-full h-full overflow-hidden">{content}</div>
        </div>
      </SplitPane>
    </>
  )
}

function EnvironmentDetails(): JSX.Element {
  const environment = useStoreContext(s => s.environment)
  const modules = useStoreContext(s => s.modules)

  const planAction = useStorePlan(s => s.planAction)
  const planOverview = useStorePlan(s => s.planOverview)

  const { isFetching: isFetchingModels } = useApiModels()
  const { isFetching: isFetchingEnvironments } = useApiEnvironments()

  const { isFetching: isFetchingPlanRun } = useApiPlanRun(environment.name, {
    planOptions: { skip_tests: true, include_unmodified: true },
  })

  const withPlanModule =
    modules.includes(Modules.plans) ||
    modules.includes(Modules['plan-progress'])

  return (
    <div className="h-8 flex w-full items-center justify-end text-neutral-500">
      {isFetchingEnvironments ? (
        <ActionStatus>Loading Environments...</ActionStatus>
      ) : isFetchingModels ? (
        <ActionStatus>Loading Models...</ActionStatus>
      ) : (
        <>
          {withPlanModule && (
            <ModuleLink
              title={
                planAction.isProcessing
                  ? planAction.displayStatus(planOverview)
                  : 'Plan'
              }
              to={EnumRoutes.Plan}
              icon={
                planAction.isProcessing ? (
                  <Spinner
                    variant={EnumVariant.Success}
                    className="w-3 py-1"
                  />
                ) : (
                  <OutlinePlayCircleIcon
                    className={clsx(
                      'w-5',
                      planOverview.isFailed
                        ? 'text-danger-500 dark:text-danger-100'
                        : 'text-success-100',
                    )}
                  />
                )
              }
              iconActive={
                planAction.isProcessing ? (
                  <Spinner
                    variant={EnumVariant.Success}
                    className="w-3 py-1"
                  />
                ) : (
                  <PlayCircleIcon
                    className={clsx(
                      'w-5',
                      planOverview.isFailed
                        ? 'text-danger-500 dark:text-danger-100'
                        : 'text-success-100',
                    )}
                  />
                )
              }
              before={
                <p
                  className={clsx(
                    'block mx-1 text-xs font-bold',
                    planOverview.isFailed
                      ? 'text-danger-500 dark:text-danger-100'
                      : 'text-success-100',
                  )}
                >
                  {planAction.isProcessing
                    ? planAction.displayStatus(planOverview)
                    : 'Plan'}
                </p>
              }
              className={clsx(
                'px-2',
                planOverview.isFailed
                  ? 'bg-danger-10 dark:bg-danger-500'
                  : 'bg-success-500',
              )}
              classActive={clsx(
                planOverview.isFailed
                  ? 'bg-danger-10 dark:bg-danger-500'
                  : 'bg-success-500',
              )}
            />
          )}
          <SelectEnvironemnt
            className="border-none h-6 "
            size={EnumSize.sm}
            showAddEnvironment={modules.includes(Modules.plans)}
            disabled={
              isFetchingPlanRun ||
              planAction.isProcessing ||
              environment.isInitialProd
            }
          />
          {[Modules['plan-progress'], Modules.plans].some(m =>
            modules.includes(m),
          ) && <PlanChanges />}
          {modules.includes(Modules.errors) && <ReportErrors />}
        </>
      )}
    </div>
  )
}

function SelectModule(): JSX.Element {
  const { errors } = useIDE()

  const models = useStoreContext(s => s.models)
  const modules = useStoreContext(s => s.modules)

  const modelsCount = Array.from(new Set(models.values())).length

  return (
    <div className="h-8 flex w-full items-center justify-center px-1 py-0.5 text-neutral-500">
      {modules.includes(Modules.editor) && (
        <ModuleLink
          title="File Explorer"
          to={EnumRoutes.Editor}
          className="text-primary-500"
          classActive="px-2 bg-primary-10 text-primary-500"
          icon={<OutlineFolderIcon className="w-4" />}
          iconActive={<FolderIcon className="w-4" />}
        />
      )}
      {modules.includes(Modules.docs) && (
        <ModuleLink
          title="Docs"
          to={EnumRoutes.Docs}
          icon={<OutlineDocumentTextIcon className="w-4" />}
          iconActive={<DocumentTextIcon className="w-4" />}
        >
          <span className="block ml-1 text-xs">{modelsCount}</span>
        </ModuleLink>
      )}
      {modules.includes(Modules.errors) && (
        <ModuleLink
          title="Errors"
          to={EnumRoutes.Errors}
          className={errors.size > 0 ? 'text-danger-500' : ''}
          classActive="px-2 bg-danger-10 text-danger-500"
          icon={<OutlineExclamationTriangleIcon className="w-4" />}
          iconActive={<ExclamationTriangleIcon className="w-4" />}
          disabled={errors.size === 0}
        >
          {errors.size > 0 && (
            <span className="block ml-1 text-xs">{errors.size}</span>
          )}
        </ModuleLink>
      )}
      {modules.includes(Modules.tests) && (
        <ModuleLink
          title="Tests"
          to={EnumRoutes.Tests}
          icon={<OutlineDocumentCheckIcon className="w-4" />}
          iconActive={<DocumentCheckIcon className="w-4" />}
        />
      )}
      {modules.includes(Modules.audits) && (
        <ModuleLink
          title="Audits"
          to={EnumRoutes.Audits}
          icon={<OutlineShieldCheckIcon className="w-4" />}
          iconActive={<ShieldCheckIcon className="w-4" />}
        />
      )}
    </div>
  )
}

function ModuleLink({
  title,
  to,
  icon,
  iconActive,
  classActive = 'px-2 bg-neutral-10',
  disabled = false,
  children,
  before,
  className,
}: {
  title: string
  to: string
  icon: React.ReactNode
  iconActive: React.ReactNode
  classActive?: string
  children?: React.ReactNode
  before?: React.ReactNode
  disabled?: boolean
  className?: string
}): JSX.Element {
  return (
    <NavLink
      title={title}
      to={to}
      className={({ isActive }) =>
        clsx(
          'mx-1 py-0.5 flex items-center rounded-full',
          disabled && 'opacity-50 cursor-not-allowed',
          isActive && isFalse(disabled) && classActive,
          className,
        )
      }
      style={({ isActive }) =>
        isActive || disabled ? { pointerEvents: 'none' } : {}
      }
    >
      {({ isActive }) => (
        <span
          className={clsx(
            'flex items-center',
            isActive ? 'font-bold' : 'font-normal',
          )}
        >
          {before}
          {isActive ? iconActive : icon}
          {children}
        </span>
      )}
    </NavLink>
  )
}

function ActionStatus({
  children,
}: {
  children: React.ReactNode
}): JSX.Element {
  return (
    <Loading className="inline-block">
      <Spinner className="w-3 h-3 border border-neutral-10 mr-2" />
      <span className="inline-block text-xs">{children}</span>
    </Loading>
  )
}

function PlanChanges(): JSX.Element {
  const planOverview = useStorePlan(s => s.planOverview)

  const [planOverviewTracker, setPlanOverviewTracker] =
    useState<ModelPlanOverviewTracker>(planOverview)

  useEffect(() => {
    if (isFalse(planOverview.isEmpty) && planOverview.isFinished) {
      setPlanOverviewTracker(new ModelPlanOverviewTracker(planOverview))
    }
  }, [planOverview])

  const shouldShow =
    (isFalse(planOverview.isLatest) || planOverview.isFetching) &&
    planOverviewTracker.hasUpdates

  return shouldShow ? (
    <span className="flex group items-center bg-neutral-10 px-1 py-1 rounded-full">
      {isTrue(planOverviewTracker.hasChanges) && (
        <p className="flex text-xs ml-2 mr-2 dark:text-neutral-300">Changes</p>
      )}
      {isArrayNotEmpty(planOverviewTracker.added) && (
        <ChangesPreview
          headline="Added"
          type={EnumPlanChangeType.Add}
          changes={planOverviewTracker.added}
          className="-mr-2 group-hover:mr-0 z-[5]"
        />
      )}
      {isArrayNotEmpty(planOverviewTracker.direct) && (
        <ChangesPreview
          headline="Directly Modified"
          type={EnumPlanChangeType.Direct}
          changes={planOverviewTracker.direct}
          className="-mr-2 group-hover:mr-0 z-[4]"
        />
      )}
      {isArrayNotEmpty(planOverviewTracker.indirect) && (
        <ChangesPreview
          headline="Indirectly Modified"
          type={EnumPlanChangeType.Indirect}
          changes={planOverviewTracker.indirect}
          className="-mr-2 group-hover:mr-0 z-[3]"
        />
      )}
      {isArrayNotEmpty(planOverviewTracker.metadata) && (
        <ChangesPreview
          headline="Metadata"
          type={EnumPlanChangeType.Metadata}
          changes={planOverviewTracker.metadata}
          className="-mr-2 group-hover:mr-0 z-[2]"
        />
      )}
      {isArrayNotEmpty(planOverviewTracker.removed) && (
        <ChangesPreview
          headline="Removed"
          type={EnumPlanChangeType.Remove}
          changes={planOverviewTracker.removed}
          className="-mr-2 group-hover:mr-0 z-[1]"
        />
      )}
      {isArrayNotEmpty(planOverviewTracker.backfills) ? (
        <ChangesPreview
          headline="Backfills"
          type={EnumPlanChangeType.Default}
          changes={planOverviewTracker.backfills}
          className="ml-4 group-hover:ml-2 z-[6]"
        />
      ) : (
        <div className="ml-2 group-hover:ml-2 z-[6]"></div>
      )}
    </span>
  ) : (
    <span className="block ml-1 px-3 py-1 first-child:ml-0 rounded-full whitespace-nowrap font-bold text-xs text-center bg-neutral-5 dark:bg-neutral-20 cursor-default text-neutral-500 dark:text-neutral-300">
      No Changes
    </span>
  )
}

export function SelectEnvironemnt({
  disabled = false,
  className,
  showAddEnvironment = true,
  size = EnumSize.sm,
  onSelect,
}: {
  className?: string
  size?: ButtonSize
  disabled?: boolean
  showAddEnvironment?: boolean
  onSelect?: () => void
}): JSX.Element {
  const { addError } = useIDE()

  const environment = useStoreContext(s => s.environment)
  const environments = useStoreContext(s => s.environments)
  const setEnvironment = useStoreContext(s => s.setEnvironment)
  const removeLocalEnvironment = useStoreContext(s => s.removeLocalEnvironment)

  const ButtonMenu = makeButton<HTMLDivElement>(Menu.Button)

  function deleteEnvironment(env: ModelEnvironment): void {
    apiDeleteEnvironment(env.name)
      .then(() => {
        removeLocalEnvironment(env)
      })
      .catch(error => {
        addError(EnumErrorKey.Environments, error)
      })
  }

  return (
    <Menu
      as="div"
      className="relative"
    >
      {({ close }) => (
        <>
          <ButtonMenu
            variant={EnumVariant.Info}
            size={size}
            disabled={disabled}
            className={clsx(className, 'bg-neutral-10')}
          >
            <span
              className={clsx(
                'block overflow-hidden truncate',
                (environment.isLocal || disabled) && 'text-neutral-500',
                environment.isRemote &&
                  'text-primary-600 dark:text-primary-300',
              )}
            >
              {environment.name}
            </span>
            <span className="pointer-events-none inset-y-0 right-0 flex items-center pl-2">
              <ChevronDownIcon
                className={clsx(
                  'w-4',
                  disabled
                    ? 'text-neutral-400 dark:text-neutral-200'
                    : 'text-neutral-800 dark:text-neutral-200',
                )}
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
            <div className="absolute top-10 right-1 min-w-[20rem] max-w-[25vw] overflow-hidden shadow-xl bg-theme border-2 border-neutral-100 dark:border-neutral-800 rounded-md flex flex-col z-10">
              <Menu.Items className="overflow-auto max-h-80 pb-2 hover:scrollbar scrollbar--vertical">
                {Array.from(environments).map(env => (
                  <Menu.Item
                    key={env.name}
                    disabled={env === environment}
                  >
                    {({ active }) => (
                      <div
                        onClick={(e: MouseEvent) => {
                          e.stopPropagation()

                          setEnvironment(env)

                          onSelect?.()
                        }}
                        className={clsx(
                          'flex justify-between items-center pl-2 pr-1 py-1 overflow-auto',
                          env === environment
                            ? 'cursor-default'
                            : 'cursor-pointer',
                        )}
                      >
                        <div className="flex items-start">
                          <CheckCircleIcon
                            className={clsx(
                              'w-4 h-4 mt-[6px]',
                              env === environment &&
                                'opacity-100 text-primary-500',
                              active && 'opacity-100 text-neutral-500',
                              env !== environment && 'opacity-0',
                            )}
                          />
                          <span className="block">
                            <span className="flex items-baseline">
                              <span
                                className={clsx(
                                  'block truncate ml-2',
                                  env.isRemote && 'text-primary-500',
                                )}
                              >
                                {env.name}
                              </span>
                              <small className="block ml-2">({env.type})</small>
                            </span>
                            {env.isProd && (
                              <span className="flex ml-2">
                                <small className="text-xs text-neutral-500">
                                  Production Environment
                                </small>
                              </span>
                            )}
                            {env.isDefault && (
                              <span className="flex ml-2">
                                <small className="text-xs text-neutral-500">
                                  Default Environment
                                </small>
                              </span>
                            )}
                          </span>
                        </div>
                        <div className="flex items-center">
                          {env.isPinned && (
                            <StarIcon
                              title="Pinned"
                              className="w-4 text-primary-500 dark:text-primary-100 mx-1"
                            />
                          )}
                          {isFalse(env.isPinned) &&
                            env.isLocal &&
                            env !== environment && (
                              <Button
                                className="!m-0 !px-1 bg-transparent hover:bg-transparent border-none"
                                size={EnumSize.xs}
                                format={EnumButtonFormat.Ghost}
                                variant={EnumVariant.Neutral}
                                onClick={(e: MouseEvent) => {
                                  e.stopPropagation()

                                  removeLocalEnvironment(env)
                                }}
                              >
                                <MinusCircleIcon className="w-4 text-neutral-500 dark:text-neutral-100" />
                              </Button>
                            )}
                          {isFalse(env.isPinned) &&
                            env.isRemote &&
                            env !== environment && (
                              <Button
                                className="!px-2 !my-0"
                                size={EnumSize.xs}
                                variant={EnumVariant.Danger}
                                onClick={(e: MouseEvent) => {
                                  e.stopPropagation()

                                  deleteEnvironment(env)
                                }}
                              >
                                Delete
                              </Button>
                            )}
                        </div>
                      </div>
                    )}
                  </Menu.Item>
                ))}
                {showAddEnvironment && (
                  <>
                    <Divider />
                    <AddEnvironment
                      onAdd={close}
                      className="mt-2 px-2"
                    />
                  </>
                )}
              </Menu.Items>
            </div>
          </Transition>
        </>
      )}
    </Menu>
  )
}

export function AddEnvironment({
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
  className,
}: {
  headline?: string
  type: PlanChangeType
  changes: ModelSQLMeshChangeDisplay[]
  className?: string
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
              'transition-all flex min-w-[1rem] h-4 text-center mx-0.5 px-1 rounded-full text-[10px] font-black text-neutral-100 cursor-default border border-inherit',
              type === EnumPlanChangeType.Add &&
                'bg-success-500 border-success-500',
              type === EnumPlanChangeType.Remove &&
                'bg-danger-500 border-danger-500',
              type === EnumPlanChangeType.Direct &&
                'bg-secondary-500 border-secondary-500',
              type === EnumPlanChangeType.Indirect &&
                'bg-warning-500 border-warning-500',
              type === EnumPlanChangeType.Metadata &&
                'bg-neutral-400 border-neutral-400',
              type === EnumPlanChangeType.Default &&
                'bg-neutral-600 border-neutral-600',
              className,
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
            <Popover.Panel className="absolute top-6 right-0 z-10 transform flex p-2 bg-theme-lighter shadow-xl focus:ring-2 ring-opacity-5 rounded-lg ">
              <PlanChangePreview
                headline={headline}
                type={type}
                className="w-full h-full max-w-[50vw] max-h-[40vh] overflow-hidden overflow-y-auto hover:scrollbar scrollbar--vertical"
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
