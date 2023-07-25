import React, { useEffect, useCallback, lazy, Suspense } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import {
  useApiModels,
  apiCancelModels,
  useApiFiles,
  apiCancelFiles,
  useApiEnvironments,
  useApiPlanRun,
  apiCancelGetEnvironments,
} from '../../../api'
import {
  EnumPlanState,
  type PlanProgress,
  useStorePlan,
  type PlanTasks,
} from '../../../context/plan'
import { useChannelEvents } from '../../../api/channels'
import {
  debounceAsync,
  isArrayEmpty,
  isFalse,
  isNil,
  isNotNil,
  isObject,
  isObjectEmpty,
} from '~/utils'
import { useStoreContext } from '~/context/context'
import { ArrowLongRightIcon } from '@heroicons/react/24/solid'
import { EnumSize, EnumVariant } from '~/types/enum'
import { Link, Outlet, useLocation, useNavigate } from 'react-router-dom'
import { EnumRoutes } from '~/routes'
import { useStoreProject } from '@context/project'
import { EnumErrorKey, type ErrorIDE, useIDE } from './context'
import { type Model } from '@api/client'
import { Button } from '@components/button/Button'
import { Divider } from '@components/divider/Divider'
import Container from '@components/container/Container'
import { useStoreEditor, createLocalFile } from '@context/editor'
import { type ModelFile } from '@models/file'
import ModalConfirmation, {
  type Confirmation,
} from '@components/modal/ModalConfirmation'
import { ModelDirectory } from '@models/directory'

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

  const showConfirmation = useStoreContext(s => s.showConfirmation)
  const setShowConfirmation = useStoreContext(s => s.setShowConfirmation)
  const confirmations = useStoreContext(s => s.confirmations)
  const removeConfirmation = useStoreContext(s => s.removeConfirmation)
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

  const project = useStoreProject(s => s.project)
  const setProject = useStoreProject(s => s.setProject)
  const setFiles = useStoreProject(s => s.setFiles)

  const storedTabs = useStoreEditor(s => s.storedTabs)
  const storedTabId = useStoreEditor(s => s.storedTabId)
  const selectTab = useStoreEditor(s => s.selectTab)
  const createTab = useStoreEditor(s => s.createTab)
  const addTabs = useStoreEditor(s => s.addTabs)

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
      include_unmodified: true,
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
      if (isNil(data)) return

      const project = new ModelDirectory(data)
      const files = project.allFiles

      setFiles(files)
      restoreEditorTabsFromSaved(files)
      setProject(project)
    })

    return () => {
      debouncedGetEnvironemnts.cancel()
      debouncedGetModels.cancel()
      debouncedGetFiles.cancel()
      debouncedRunPlan.cancel()

      apiCancelFiles(client)
      apiCancelModels(client)
      apiCancelGetEnvironments(client)

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

  useEffect(() => {
    setShowConfirmation(confirmations.length > 0)
  }, [confirmations])

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

  function restoreEditorTabsFromSaved(files: ModelFile[]): void {
    if (isArrayEmpty(storedTabs)) return

    const tabs = storedTabs.map(({ id, content }) => {
      const file = files.find(file => file.id === id) ?? createLocalFile(id)
      const storedTab = createTab(file)

      storedTab.file.content = content ?? storedTab.file.content ?? ''

      return storedTab
    })
    const tab = tabs.find(tab => tab.file.id === storedTabId)

    addTabs(tabs)

    if (isNotNil(tab)) {
      selectTab(tab)
    }
  }

  function closeModalConfirmation(confirmation?: Confirmation): void {
    confirmation?.cancel?.()

    setShowConfirmation(false)
  }

  const isActivePageEditor = location.pathname === EnumRoutes.IdeEditor
  const confirmation = confirmations[0]

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
      <ModalConfirmation
        show={showConfirmation}
        onClose={() => {
          closeModalConfirmation(confirmation)
        }}
        afterLeave={() => {
          removeConfirmation()
        }}
        onKeyDown={(e: React.KeyboardEvent) => {
          if (e.key === 'Escape') {
            closeModalConfirmation(confirmation)
          }
        }}
      >
        <ModalConfirmation.Main>
          {confirmation?.headline != null && (
            <ModalConfirmation.Headline>
              {confirmation?.headline}
            </ModalConfirmation.Headline>
          )}
          {confirmation?.description != null && (
            <ModalConfirmation.Description>
              {confirmation?.description}
            </ModalConfirmation.Description>
          )}
          {confirmation?.children}
        </ModalConfirmation.Main>
        <ModalConfirmation.Actions>
          <Button
            className="font-bold"
            size="md"
            variant="danger"
            onClick={(e: React.MouseEvent) => {
              e.stopPropagation()

              confirmation?.action?.()
              setShowConfirmation(false)
            }}
          >
            {confirmation?.yesText ?? 'Confirm'}
          </Button>
          <Button
            size="md"
            variant="alternative"
            onClick={(e: React.MouseEvent) => {
              e.stopPropagation()

              closeModalConfirmation(confirmation)
            }}
          >
            {confirmation?.noText ?? 'Cancel'}
          </Button>
        </ModalConfirmation.Actions>
      </ModalConfirmation>
    </Container.Page>
  )
}

function isAllTasksCompleted(tasks: PlanTasks = {}): boolean {
  return Object.values(tasks).every(t => t.completed === t.total)
}
