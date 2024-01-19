import {
  FolderIcon,
  DocumentTextIcon,
  DocumentCheckIcon,
  ShieldCheckIcon,
  ExclamationTriangleIcon,
  PlayCircleIcon,
} from '@heroicons/react/20/solid'
import clsx from 'clsx'
import {
  FolderIcon as OutlineFolderIcon,
  DocumentTextIcon as OutlineDocumentTextIcon,
  ExclamationTriangleIcon as OutlineExclamationTriangleIcon,
  DocumentCheckIcon as OutlineDocumentCheckIcon,
  ShieldCheckIcon as OutlineShieldCheckIcon,
  PlayCircleIcon as OutlinePlayCircleIcon,
} from '@heroicons/react/24/outline'
import { NavLink } from 'react-router-dom'
import { EnumRoutes } from '~/routes'
import { useStoreProject } from '@context/project'
import { Divider } from '@components/divider/Divider'
import SplitPane from '@components/splitPane/SplitPane'
import { useStoreContext } from '@context/context'
import { useIDE } from '../ide/context'
import { useStorePlan } from '@context/plan'
import { useApiEnvironments, useApiModels, useApiPlanRun } from '@api/index'
import { EnumSize } from '~/types/enum'
import {
  EnvironmentStatus,
  PlanChanges,
  SelectEnvironemnt,
} from '../ide/EnvironmentDetails'
import { Modules } from '@api/client'
import { isFalse } from '@utils/index'
import Spinner from '@components/logo/Spinner'
import Loading from '@components/loading/Loading'

export default function Page({
  sidebar,
  content,
}: {
  sidebar: React.ReactNode
  content: React.ReactNode
}): JSX.Element {
  const splitPaneSizes = useStoreContext(s => s.splitPaneSizes)
  const setSplitPaneSizes = useStoreContext(s => s.setSplitPaneSizes)

  return (
    <SplitPane
      sizes={splitPaneSizes}
      minSize={[0, 0]}
      snapOffset={0}
      className="flex w-full h-full overflow-hidden"
      onDragEnd={setSplitPaneSizes}
    >
      <div className="flex flex-col h-full overflow-hidden">
        <div className="px-1 flex max-h-8 w-full items-center relative">
          <ProjectName />
          <EnvironmentDetails />
        </div>
        <Divider />
        <div className="px-1 flex max-h-8 w-full items-center relative">
          <SelectModule />
        </div>
        <Divider />
        <div className="w-full h-full overflow-hidden">{sidebar}</div>
      </div>
      <div className="w-full h-full overflow-hidden">{content}</div>
    </SplitPane>
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

  const withPlanModule = modules.includes(Modules.plans)

  return (
    <div className="h-8 flex w-full items-center justify-end py-0.5 text-neutral-500">
      {isFetchingEnvironments ? (
        <ActionStatus>Loading Environments...</ActionStatus>
      ) : isFetchingModels ? (
        <ActionStatus>Loading Models...</ActionStatus>
      ) : (
        <>
          <SelectEnvironemnt
            className="border-none h-6 !m-0"
            size={EnumSize.sm}
            showAddEnvironment={withPlanModule}
            disabled={
              isFetchingPlanRun ||
              planAction.isProcessing ||
              environment.isInitialProd
            }
          />
          {planAction.isProcessing ? (
            <ActionStatus>
              {planAction.displayStatus(planOverview)}
            </ActionStatus>
          ) : (
            <>
              {withPlanModule && (
                <div className="px-2 flex items-center">
                  <PlanChanges />
                  <EnvironmentStatus />
                </div>
              )}
            </>
          )}
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
      {modules.includes(Modules.plans) && (
        <ModuleLink
          title="Plan"
          to={EnumRoutes.Plan}
          icon={<OutlinePlayCircleIcon className="w-5 text-success-500" />}
          iconActive={<PlayCircleIcon className="w-5 text-success-500" />}
          before={<b className="block mx-1 text-xs text-success-500">Plan</b>}
          className="px-2 bg-success-10"
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
          'mx-1 py-1 flex items-center rounded-full',
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
        <>
          {before}
          {isActive ? iconActive : icon}
          {children}
        </>
      )}
    </NavLink>
  )
}

function ProjectName(): JSX.Element {
  const project = useStoreProject(s => s.project)

  return (
    <span className="flex items-center px-2 py-0.5 rounded-md font-bold text-neutral-600 dark:text-neutral-300 bg-neutral-5 text-sm text-ellipsis whitespace-nowrap">
      {project?.name}
    </span>
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
