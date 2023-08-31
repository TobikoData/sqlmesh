import React, { useEffect, useState } from 'react'
import { isArrayNotEmpty, isFalse, isNil } from '~/utils'
import {
  FolderIcon,
  DocumentTextIcon,
  DocumentCheckIcon,
  ShieldCheckIcon,
  ExclamationTriangleIcon,
  PlayCircleIcon,
} from '@heroicons/react/24/solid'
import {
  FolderIcon as OutlineFolderIcon,
  DocumentTextIcon as OutlineDocumentTextIcon,
  ExclamationTriangleIcon as OutlineExclamationTriangleIcon,
  DocumentCheckIcon as OutlineDocumentCheckIcon,
  ShieldCheckIcon as OutlineShieldCheckIcon,
  PlayCircleIcon as OutlinePlayCircleIcon,
} from '@heroicons/react/24/outline'
import { Link, NavLink, useLocation } from 'react-router-dom'
import { EnumRoutes } from '~/routes'
import { useStoreProject } from '@context/project'
import { Divider } from '@components/divider/Divider'
import SplitPane from '@components/splitPane/SplitPane'
import { useStoreContext } from '@context/context'
import { useIDE } from '../ide/context'
import { PlanChanges, SelectEnvironemnt } from '../ide/RunPlan'
import { useApiPlanRun } from '@api/index'
import clsx from 'clsx'
import { EnumPlanState, type PlanState, useStorePlan } from '@context/plan'
import { EnumSize, EnumVariant } from '~/types/enum'
import { type ContextEnvironment } from '@api/client'
import Spinner from '@components/logo/Spinner'

export default function Page({
  sidebar,
  content,
}: {
  sidebar: React.ReactNode
  content: React.ReactNode
}): JSX.Element {
  const location = useLocation()
  const { errors } = useIDE()

  const models = useStoreContext(s => s.models)
  const splitPaneSizes = useStoreContext(s => s.splitPaneSizes)
  const setSplitPaneSizes = useStoreContext(s => s.setSplitPaneSizes)

  const project = useStoreProject(s => s.project)

  const modelsCount = Array.from(new Set(models.values())).length

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
          <div className="px-2">
            <h3 className="flex items-center h-8 font-bold text-primary-500 text-sm">
              <span className="inline-block">/</span>
              {project?.name}
            </h3>
          </div>
          <EnvironmentDetails />
        </div>
        <Divider />
        <div className="px-1 flex max-h-8 w-full items-center relative">
          <div className="h-8 flex w-full items-center justify-center px-1 py-0.5 text-neutral-500">
            <Link
              title="File Explorer"
              to={EnumRoutes.Editor}
              className={clsx(
                'mx-1 py-1 flex items-center rounded-full',
                location.pathname.startsWith(EnumRoutes.Editor) &&
                  'px-2 bg-neutral-10',
              )}
            >
              {location.pathname.startsWith(EnumRoutes.Editor) ? (
                <FolderIcon className="w-4" />
              ) : (
                <OutlineFolderIcon className="w-4" />
              )}
            </Link>
            <Link
              title="Docs"
              to={EnumRoutes.Docs}
              className={clsx(
                'mx-1 py-1 flex items-center rounded-full',
                location.pathname.startsWith(EnumRoutes.Docs) &&
                  'px-2 bg-neutral-10',
              )}
            >
              {location.pathname.startsWith(EnumRoutes.Docs) ? (
                <DocumentTextIcon className="w-4" />
              ) : (
                <OutlineDocumentTextIcon className="w-4" />
              )}
              <span className="block ml-1 text-xs">{modelsCount}</span>
            </Link>
            <NavLink
              title="Errors"
              to={errors.size === 0 ? '' : EnumRoutes.Errors}
              className={clsx(
                'mx-1 py-1 flex items-center rounded-full',
                errors.size === 0
                  ? 'opacity-50 cursor-not-allowed'
                  : 'px-2 bg-danger-10 text-danger-500',
              )}
            >
              {({ isActive }) => (
                <>
                  {isActive ? (
                    <ExclamationTriangleIcon className="w-4" />
                  ) : (
                    <OutlineExclamationTriangleIcon className="w-4" />
                  )}
                  {errors.size > 0 && (
                    <span className="block ml-1 text-xs">{errors.size}</span>
                  )}
                </>
              )}
            </NavLink>
            <Link
              title="Tests"
              to={EnumRoutes.Tests}
              className={clsx(
                'mx-0.5 py-1 flex items-center rounded-full',
                location.pathname.startsWith(EnumRoutes.Tests) &&
                  'px-2 bg-neutral-10',
              )}
            >
              {location.pathname.startsWith(EnumRoutes.Tests) ? (
                <DocumentCheckIcon className="w-4" />
              ) : (
                <OutlineDocumentCheckIcon className="w-4" />
              )}
            </Link>
            <Link
              title="Audits"
              to={EnumRoutes.Audits}
              className={clsx(
                'mx-1 py-1 flex items-center rounded-full',
                location.pathname.startsWith(EnumRoutes.Audits) &&
                  'px-2 bg-neutral-10',
              )}
            >
              {location.pathname.startsWith(EnumRoutes.Audits) ? (
                <ShieldCheckIcon className="w-4" />
              ) : (
                <OutlineShieldCheckIcon className="w-4" />
              )}
            </Link>
            <NavLink
              title="Plan"
              to={EnumRoutes.Plan}
              className="mx-1 py-0.5 px-2 flex items-center rounded-full bg-success-10"
            >
              <b className="block mx-1 text-xs text-success-500">Plan</b>
              {location.pathname.startsWith(EnumRoutes.Plan) ? (
                <PlayCircleIcon className="text-success-500 w-5" />
              ) : (
                <OutlinePlayCircleIcon className="text-success-500 w-5" />
              )}
            </NavLink>
          </div>
        </div>
        <Divider />
        <div className="w-full h-full">{sidebar}</div>
      </div>
      <div className="w-full h-full">{content}</div>
    </SplitPane>
  )
}

function EnvironmentDetails(): JSX.Element {
  const environment = useStoreContext(s => s.environment)

  const planState = useStorePlan(s => s.state)

  const setInitialDates = useStoreContext(s => s.setInitialDates)
  const hasSynchronizedEnvironments = useStoreContext(
    s => s.hasSynchronizedEnvironments,
  )

  const [hasChanges, setHasChanges] = useState(false)
  const [plan, setPlan] = useState<ContextEnvironment | undefined>()

  const { data: dataPlan, isFetching } = useApiPlanRun(
    environment.name,
    {
      planOptions: { skip_tests: true, include_unmodified: true },
    },
    undefined,
    { enabled: true },
  )

  useEffect(() => {
    if (isNil(dataPlan)) return

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

  const showSelectEnvironmentButton =
    (isFalse(environment.isDefault) || hasSynchronizedEnvironments()) &&
    (isFalse(environment.isDefault) || isFalse(environment.isInitial))

  return (
    <div className="h-8 flex w-full items-center justify-end py-0.5 text-neutral-500">
      <div className="px-2 flex items-center">
        <PlanStatus
          planState={planState}
          isLoading={isFetching}
          className="mr-2"
        />
        <PlanChanges
          environment={environment}
          plan={plan}
          hasChanges={hasChanges}
          isLoading={isFetching}
        />
      </div>
      {showSelectEnvironmentButton && (
        <SelectEnvironemnt
          className="border-none h-6 !m-0"
          size={EnumSize.sm}
          onSelect={env => {
            setPlan(undefined)
            setHasChanges(false)
          }}
        />
      )}
    </div>
  )
}

function PlanStatus({
  planState,
  isLoading,
  className,
}: {
  isLoading: boolean
  planState: PlanState
  className?: string
}): JSX.Element {
  const shouldSkip =
    planState === EnumPlanState.Init || planState === EnumPlanState.Finished
  const isRunning =
    planState === EnumPlanState.Running ||
    planState === EnumPlanState.Applying ||
    planState === EnumPlanState.Cancelling

  return shouldSkip && isFalse(isLoading) ? (
    <></>
  ) : (
    <span
      className={clsx(
        'flex items-center ml-2 py-0.5 px-3 bg-neutral-10 rounded-full',
        className,
      )}
    >
      {isRunning && (
        <Spinner
          className="w-3 h-3 mr-2 !fill-neutral-50"
          variant={EnumVariant.Neutral}
        />
      )}
      <span className="inline-block whitespace-nowrap text-xs text-neutral-500">
        {getPlanStatus(planState, isLoading)}
      </span>
    </span>
  )
}

function getPlanStatus(planState: PlanState, isLoading: boolean): string {
  if (isLoading) return 'Getting Changes...'
  if (planState === EnumPlanState.Running) return 'Running Plan...'
  if (planState === EnumPlanState.Applying) return 'Applying Plan...'
  if (planState === EnumPlanState.Cancelling) return 'Cancelling Plan...'
  if (planState === EnumPlanState.Failed) return 'Last Plan Failed'
  if (planState === EnumPlanState.Cancelled) return 'Last Plan Cancelled'

  return ''
}
