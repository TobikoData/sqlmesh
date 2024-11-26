import {
  FolderIcon,
  DocumentTextIcon,
  DocumentCheckIcon,
  ShieldCheckIcon,
  ExclamationTriangleIcon,
  PlayCircleIcon,
  TableCellsIcon,
  SparklesIcon,
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
import { EnumRoutes } from '~/routes'
import { useStoreContext } from '@context/context'
import ModuleLink from './ModuleLink'
import { useNotificationCenter } from '~/library/pages/root/context/notificationCenter'

export default function ModuleNavigation({
  className,
}: {
  className?: string
}): JSX.Element {
  const { errors } = useNotificationCenter()

  const models = useStoreContext(s => s.models)
  const modules = useStoreContext(s => s.modules)

  const modelsCount = Array.from(new Set(models.values())).length

  return (
    <div className={className}>
      {modules.hasEditor && (
        <ModuleLink
          title="File Explorer"
          to={EnumRoutes.Editor}
          className="px-2 py-1 h-10 hover:bg-neutral-5 dark:hover:bg-neutral-10 text-primary-500 rounded-xl my-1"
          classActive="bg-primary-10 text-primary-500"
          icon={<OutlineFolderIcon className="min-w-6 w-6" />}
          iconActive={<FolderIcon className="min-w-6 w-6" />}
        />
      )}
      {modules.hasDataCatalog && (
        <ModuleLink
          title="Data Catalog"
          to={EnumRoutes.DataCatalog}
          icon={<OutlineDocumentTextIcon className="min-w-6 w-6" />}
          iconActive={<DocumentTextIcon className="min-w-6 w-6" />}
          className="relative rounded-xl my-1 px-2 py-1 h-10 hover:bg-neutral-5 dark:hover:bg-neutral-10 text-neutral-600 dark:text-neutral-300"
        >
          <span className="min-w-[1rem] px-1 h-4 py-0.5 text-center text-[9px] absolute bottom-0 right-0 font-black rounded-full bg-primary-500 text-primary-100">
            {modelsCount}
          </span>
        </ModuleLink>
      )}
      {modules.hasErrors && (
        <ModuleLink
          title="Errors"
          to={EnumRoutes.Errors}
          className={clsx(
            'relative px-2 py-1 rounded-xl my-1 h-10 hover:bg-neutral-5 dark:hover:bg-neutral-10',
            errors.size > 0 ? 'text-danger-500' : 'text-neutral-600',
          )}
          classActive="px-2 bg-danger-10 text-danger-500"
          icon={<OutlineExclamationTriangleIcon className="w-6" />}
          iconActive={<ExclamationTriangleIcon className="w-6" />}
          disabled={errors.size === 0}
        >
          {errors.size > 0 && (
            <span className="min-w-[1rem] px-1 h-4 py-0.5 text-center text-[9px] absolute bottom-0 right-0 font-black rounded-full bg-danger-500 text-danger-100">
              {errors.size}
            </span>
          )}
        </ModuleLink>
      )}
      {modules.hasPlans && (
        <ModuleLink
          title="Plan"
          to={EnumRoutes.Plan}
          className="px-2 py-1 h-10 hover:bg-neutral-5 dark:hover:bg-neutral-10 text-success-500 rounded-xl my-1"
          classActive="bg-success-10 text-success-500"
          icon={<OutlinePlayCircleIcon className="w-6" />}
          iconActive={<PlayCircleIcon className="w-6" />}
        />
      )}
      {modules.hasTests && (
        <ModuleLink
          title="Tests"
          to={EnumRoutes.Tests}
          icon={<OutlineDocumentCheckIcon className="w-6" />}
          iconActive={<DocumentCheckIcon className="w-6" />}
          className="rounded-xl my-1 px-2 py-1 h-10 hover:bg-neutral-5 dark:hover:bg-neutral-10"
        />
      )}
      {modules.hasAudits && (
        <ModuleLink
          title="Audits"
          to={EnumRoutes.Audits}
          icon={<OutlineShieldCheckIcon className="w-6" />}
          iconActive={<ShieldCheckIcon className="w-6" />}
          className="rounded-xl my-1 px-2 py-1 h-10 hover:bg-neutral-5 dark:hover:bg-neutral-10"
        />
      )}
      {modules.hasData && (
        <ModuleLink
          title="Data"
          to={EnumRoutes.Data}
          icon={<TableCellsIcon className="w-6" />}
          iconActive={<TableCellsIcon className="w-6" />}
          className="rounded-xl my-1 px-2 py-1 h-10 hover:bg-neutral-5 dark:hover:bg-neutral-10 text-neutral-500 dark:text-neutral-300"
        />
      )}
      {modules.hasLineage && (
        <ModuleLink
          title="Lineage"
          to={EnumRoutes.Lineage}
          icon={<SparklesIcon className="w-6" />}
          iconActive={<SparklesIcon className="w-6" />}
          className="rounded-xl my-1 px-2 py-1 h-10 hover:bg-neutral-5 dark:hover:bg-neutral-10 text-neutral-500 dark:text-neutral-300"
        />
      )}
    </div>
  )
}
