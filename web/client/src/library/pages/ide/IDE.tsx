import { useEffect, useCallback, lazy, Suspense } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import {
  useApiModels,
  apiCancelModels,
  useApiFiles,
  apiCancelFiles,
  useApiEnvironments,
  useApiPlanRun,
  apiCancelGetEnvironments,
  apiCancelPlanRun,
} from '../../../api'
import {
  EnumPlanState,
  type PlanProgress,
  useStorePlan,
  type PlanTasks,
} from '../../../context/plan'
import { useChannelEvents } from '../../../api/channels'
import { debounceAsync, isFalse, isObject, isObjectEmpty } from '~/utils'
import { useStoreContext } from '~/context/context'
import { ArrowLongRightIcon } from '@heroicons/react/24/solid'
import { EnumSize, EnumVariant } from '~/types/enum'
import { Link, Outlet, useLocation, useNavigate } from 'react-router-dom'
import { EnumRoutes } from '~/routes'
import { useStoreFileTree } from '@context/fileTree'
import { EnumErrorKey, ErrorIDE, useIDE } from './context'
import { type Model } from '@api/client'
import { Button } from '@components/button/Button'
import { Divider } from '@components/divider/Divider'
import Container from '@components/container/Container'

const ReportErrors = lazy(
  async () => await import('../../components/report/ReportErrors'),
)
const RunPlan = lazy(async () => await import('./RunPlan'))
const ActivePlan = lazy(async () => await import('./ActivePlan'))
const PlanSidebar = lazy(async () => await import('./PlanSidebar'))

export default function PageIDE(): JSX.Element {
  const location = useLocation()
  const navigate = useNavigate()

  const client = useQueryClient()

  const { removeError, addError } = useIDE()

  const models = useStoreContext(s => s.models)
  const environment = useStoreContext(s => s.environment)
  const setModels = useStoreContext(s => s.setModels)
  const addSynchronizedEnvironments = useStoreContext(
    s => s.addSynchronizedEnvironments,
  )
  const hasSynchronizedEnvironments = useStoreContext(
    s => s.hasSynchronizedEnvironments,
  )

  const planState = useStorePlan(s => s.state)
  const activePlan = useStorePlan(s => s.activePlan)
  const setState = useStorePlan(s => s.setState)
  const setActivePlan = useStorePlan(s => s.setActivePlan)

  const project = useStoreFileTree(s => s.project)
  const setProject = useStoreFileTree(s => s.setProject)
  const setFiles = useStoreFileTree(s => s.setFiles)

  const subscribe = useChannelEvents()

  // We need to fetch from IDE level to make sure
  // all pages have access to models and files
  const { refetch: getModels } = useApiModels()
  const { refetch: getFiles } = useApiFiles()
  const { data: dataEnvironments, refetch: getEnvironments } =
    useApiEnvironments()
  const { refetch: planRun } = useApiPlanRun(environment.name, {
    planOptions: {
      skip_tests: true,
    },
  })

  const debouncedGetModels = useCallback(debounceAsync(getModels, 1000, true), [
    getModels,
  ])

  const debouncedGetFiles = useCallback(debounceAsync(getFiles, 1000, true), [
    getFiles,
  ])

  const debouncedGetEnvironemnts = useCallback(
    debounceAsync(getEnvironments, 1000, true),
    [getEnvironments],
  )

  const debouncedRunPlan = useCallback(debounceAsync(planRun, 1000, true), [
    planRun,
  ])

  useEffect(() => {
    const unsubscribeTasks = subscribe<PlanProgress>('tasks', updateTasks)
    const unsubscribeModels = subscribe<Model[]>('models', updateModels)
    const unsubscribeErrors = subscribe<ErrorIDE>('errors', displayErrors)
    const unsubscribePromote = subscribe<any>(
      'promote-environment',
      handlePromote,
    )

    void debouncedGetModels().then(({ data }) => {
      updateModels(data)
    })

    void debouncedGetFiles().then(({ data }) => {
      setProject(data)
    })

    return () => {
      debouncedGetEnvironemnts.cancel()
      debouncedGetModels.cancel()
      debouncedGetFiles.cancel()
      debouncedRunPlan.cancel()

      apiCancelFiles(client)
      apiCancelModels(client)
      apiCancelGetEnvironments(client)
      apiCancelPlanRun(client)

      unsubscribeTasks?.()
      unsubscribeModels?.()
      unsubscribePromote?.()
      unsubscribeErrors?.()
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
    if (dataEnvironments == null || isObjectEmpty(dataEnvironments)) return

    addSynchronizedEnvironments(Object.values(dataEnvironments))

    // This use case is happening when user refreshes the page
    // while plan is still applying
    if (planState !== EnumPlanState.Applying) {
      void debouncedRunPlan()
    }
  }, [dataEnvironments])

  useEffect(() => {
    if (models.size > 0 && isFalse(hasSynchronizedEnvironments())) {
      void debouncedGetEnvironemnts()
    }

    if (hasSynchronizedEnvironments()) {
      void debouncedRunPlan()
    }
  }, [models])

  function updateModels(models?: Model[]): void {
    removeError(EnumErrorKey.General)
    removeError(EnumErrorKey.Models)
    setModels(models)
  }

  function updateTasks(data?: PlanProgress): void {
    if (data == null) return

    if (isFalse(isObject(data.tasks))) {
      setState(EnumPlanState.Init)

      return
    }

    const plan: PlanProgress = {
      ok: data.ok,
      tasks: data.tasks,
      updated_at: data.updated_at ?? new Date().toISOString(),
    }

    setActivePlan(plan)

    if (isFalse(data.ok)) {
      setState(EnumPlanState.Failed)
    } else if (isAllTasksCompleted(data.tasks)) {
      setState(EnumPlanState.Finished)
    } else {
      setState(EnumPlanState.Applying)
    }
  }

  function displayErrors(data: ErrorIDE): void {
    addError(EnumErrorKey.General, data)
  }

  function handlePromote(): void {
    setActivePlan(undefined)

    void debouncedGetEnvironemnts()
  }

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
          <ReportErrors />
        </div>
      </div>
      <Divider />
      <Outlet />
    </Container.Page>
  )
}

function isAllTasksCompleted(tasks: PlanTasks = {}): boolean {
  return Object.values(tasks).every(t => t.completed === t.total)
}
