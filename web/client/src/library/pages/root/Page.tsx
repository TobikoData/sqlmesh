import React from 'react'
import { isArrayNotEmpty } from '~/utils'
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
import { PlanChanges } from '../ide/RunPlan'
import { useApiPlanRun } from '@api/index'
import clsx from 'clsx'

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
  const environment = useStoreContext(s => s.environment)
  const splitPaneSizes = useStoreContext(s => s.splitPaneSizes)
  const setSplitPaneSizes = useStoreContext(s => s.setSplitPaneSizes)

  const project = useStoreProject(s => s.project)

  const { data: dataPlan, isFetching } = useApiPlanRun(environment.name, {
    planOptions: { skip_tests: true, include_unmodified: true },
  })

  const modelsCount = Array.from(new Set(models.values())).length
  const hasChanges = [
    dataPlan?.changes?.added,
    dataPlan?.changes?.removed,
    dataPlan?.changes?.modified?.direct,
    dataPlan?.changes?.modified?.indirect,
    dataPlan?.changes?.modified?.metadata,
  ].some(isArrayNotEmpty)

  return (
    <SplitPane
      sizes={splitPaneSizes}
      minSize={[0, 0]}
      snapOffset={0}
      className="flex w-full h-full overflow-hidden"
      onDragEnd={setSplitPaneSizes}
    >
      <div className="flex flex-col h-full overflow-hidden">
        <div className="px-2 flex max-h-8 justify-center w-full">
          <div className="h-8 flex items-center px-1 py-0.5 text-neutral-500">
            <Link
              title="File Explorer"
              to={EnumRoutes.Editor}
              className="mx-0.5 py-1 flex items-center rounded-full"
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
              className="mx-0.5 py-1 px-2 flex items-center rounded-full bg-neutral-10"
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
                'mx-0.5 py-1 flex items-center rounded-full',
                errors.size === 0
                  ? 'opacity-50 cursor-not-allowed'
                  : 'px-2 bg-danger-10',
              )}
            >
              {({ isActive }) => (
                <>
                  {isActive}
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
              className="px-1"
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
              className="px-1"
            >
              {location.pathname.startsWith(EnumRoutes.Audits) ? (
                <ShieldCheckIcon className="w-4" />
              ) : (
                <OutlineShieldCheckIcon className="w-4" />
              )}
            </Link>
            <Link
              title="Plan"
              to={EnumRoutes.Plan}
              className="mx-0.5 py-1 px-2 flex items-center rounded-full bg-success-10"
            >
              {location.pathname.startsWith(EnumRoutes.Plan) ? (
                <PlayCircleIcon className="text-success-500 w-4" />
              ) : (
                <OutlinePlayCircleIcon className="text-success-500 w-4" />
              )}
              <PlanChanges
                environment={environment}
                plan={dataPlan}
                isLoading={isFetching}
                hasChanges={hasChanges}
              />
            </Link>
          </div>
        </div>
        <Divider />
        <div className="h-full">{sidebar}</div>
        <Divider />
        <div className="flex h-8 items-center">
          <h3 className="px-2 font-bold text-primary-500 text-sm">
            <span className="inline-block">/</span>
            {project?.name}
          </h3>
        </div>
      </div>
      <div className="h-full">{content}</div>
    </SplitPane>
  )
}
