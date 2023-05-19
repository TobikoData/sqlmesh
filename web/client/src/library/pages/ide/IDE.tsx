import { useEffect, useCallback, lazy, Suspense } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import {
  useApiFiles,
  useApiEnvironments,
  apiCancelGetEnvironments,
  apiCancelFiles,
  useApiModels,
  apiCancelModels,
  useApiPlanRun,
} from '../../../api'
import { useStorePlan } from '../../../context/plan'
import { useChannelEvents } from '../../../api/channels'
import { debounceAsync, isFalse, isObjectEmpty } from '~/utils'
import { useStoreContext } from '~/context/context'
import { ArrowLongRightIcon } from '@heroicons/react/24/solid'
import { EnumSize, EnumVariant } from '~/types/enum'
import { Link, Outlet, useLocation, useNavigate } from 'react-router-dom'
import { EnumRoutes } from '~/routes'
import { useStoreFileTree } from '@context/fileTree'
import { Divider } from '@components/divider/Divider'
import { Button } from '@components/button/Button'
import Container from '@components/container/Container'

const PlanSidebar = lazy(async () => await import('./PlanSidebar'))
const ErrorsReport = lazy(async () => await import('./ErrorsReport'))
const RunPlan = lazy(async () => await import('./RunPlan'))
const ActivePlan = lazy(async () => await import('./ActivePlan'))

export default function IDE(): JSX.Element {
  const location = useLocation()
  const navigate = useNavigate()

  const client = useQueryClient()

  const models = useStoreContext(s => s.models)
  const environment = useStoreContext(s => s.environment)
  const addSyncronizedEnvironments = useStoreContext(
    s => s.addSyncronizedEnvironments,
  )
  const hasSyncronizedEnvironments = useStoreContext(
    s => s.hasSyncronizedEnvironments,
  )
  const setModels = useStoreContext(s => s.setModels)

  const activePlan = useStorePlan(s => s.activePlan)
  const updateTasks = useStorePlan(s => s.updateTasks)

  const project = useStoreFileTree(s => s.project)
  const setFiles = useStoreFileTree(s => s.setFiles)
  const setProject = useStoreFileTree(s => s.setProject)

  const [subscribe] = useChannelEvents()

  const { data: dataPlan } = useApiPlanRun(environment.name)
  const { data: dataFiles, refetch: getFiles } = useApiFiles()
  const { data: dataModels, refetch: getModels } = useApiModels()
  const { data: dataEnvironments, refetch: getEnvironments } =
    useApiEnvironments()

  const debouncedGetEnvironemnts = useCallback(
    debounceAsync(getEnvironments, 1000, true),
    [getEnvironments],
  )
  const debouncedGetFiles = useCallback(debounceAsync(getFiles, 1000, true), [
    getFiles,
  ])
  const debouncedGetModels = useCallback(debounceAsync(getModels, 1000, true), [
    getModels,
  ])

  useEffect(() => {
    const unsubscribeTasks = subscribe('tasks', updateTasks)
    const unsubscribeModels = subscribe('models', setModels)

    void debouncedGetFiles()
    void debouncedGetEnvironemnts()
    void debouncedGetModels()

    return () => {
      debouncedGetEnvironemnts.cancel()
      debouncedGetFiles.cancel()
      debouncedGetModels.cancel()

      apiCancelModels(client)
      apiCancelFiles(client)
      apiCancelGetEnvironments(client)

      unsubscribeTasks?.()
      unsubscribeModels?.()
    }
  }, [])

  useEffect(() => {
    if (location.pathname === EnumRoutes.Ide) {
      navigate(EnumRoutes.IdeEditor)
    }
  }, [location])

  useEffect(() => {
    setFiles(project?.allFiles ?? [])
  }, [project])

  useEffect(() => {
    if (dataPlan == null) return

    if (isFalse(hasSyncronizedEnvironments())) {
      void debouncedGetEnvironemnts()
    }

    if (models.size === 0) {
      void debouncedGetModels()
    }
  }, [dataPlan])

  useEffect(() => {
    setModels(dataModels)
  }, [dataModels])

  useEffect(() => {
    setProject(dataFiles)
  }, [dataFiles])

  useEffect(() => {
    if (dataEnvironments == null || isObjectEmpty(dataEnvironments)) return

    addSyncronizedEnvironments(Object.values(dataEnvironments))
  }, [dataEnvironments])

  const isActivePageEditor = location.pathname === EnumRoutes.IdeEditor

  return (
    <Container.Page>
      <PlanSidebar />
      <div className="w-full flex justify-between items-center min-h-[2rem] z-50">
        <div className="px-3 flex items-center whitespace-nowrap">
          <h3 className="font-bold text-primary-500">
            <span className="inline-block">/</span>
            {project?.name}
          </h3>
          <ArrowLongRightIcon className="w-8 mx-4 text-neutral-50" />
          <Button
            size={EnumSize.sm}
            variant={EnumVariant.Neutral}
          >
            {isActivePageEditor ? (
              <Link to={EnumRoutes.IdeDocs}>Docs</Link>
            ) : (
              <Link to={EnumRoutes.IdeEditor}>Editor</Link>
            )}
          </Button>
        </div>
        <div className="px-3 flex items-center min-w-[10rem] justify-end">
          <RunPlan />
          <Suspense>
            {activePlan != null && <ActivePlan plan={activePlan} />}
          </Suspense>
          <ErrorsReport />
        </div>
      </div>
      <Divider />
      <Outlet />
    </Container.Page>
  )
}
