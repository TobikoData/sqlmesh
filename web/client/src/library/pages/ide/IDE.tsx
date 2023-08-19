import React, { useEffect, lazy, Suspense } from 'react'
import {
  useApiModels,
  useApiFiles,
  useApiEnvironments,
  useApiPlanRun,
} from '../../../api'
import {
  EnumPlanState,
  type PlanProgress,
  useStorePlan,
  type PlanTasks,
} from '../../../context/plan'
import { useChannelEvents } from '../../../api/channels'
import {
  camelCaseToRegularCase,
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
import { type Directory, type Model } from '@api/client'
import { Button } from '@components/button/Button'
import { Divider } from '@components/divider/Divider'
import Container from '@components/container/Container'
import { useStoreEditor, createLocalFile } from '@context/editor'
import { ModelFile } from '@models/file'
import ModalConfirmation, {
  type Confirmation,
} from '@components/modal/ModalConfirmation'
import { ModelDirectory } from '@models/directory'
import {
  EnumFileExplorerChange,
  type FileExplorerChange,
} from '@components/fileExplorer/context'
import { EnumAction, useStoreActionManager } from '@context/manager'
import Spinner from '@components/logo/Spinner'

const ReportErrors = lazy(
  async () => await import('../../components/report/ReportErrors'),
)
const RunPlan = lazy(async () => await import('./RunPlan'))
const ActivePlan = lazy(async () => await import('./ActivePlan'))
const PlanSidebar = lazy(async () => await import('./PlanSidebar'))

export default function PageIDE(): JSX.Element {
  const location = useLocation()
  const navigate = useNavigate()

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

  const activePlan = useStorePlan(s => s.activePlan)
  const setState = useStorePlan(s => s.setState)
  const setActivePlan = useStorePlan(s => s.setActivePlan)

  const project = useStoreProject(s => s.project)
  const setProject = useStoreProject(s => s.setProject)
  const setFiles = useStoreProject(s => s.setFiles)
  const refreshFiles = useStoreProject(s => s.refreshFiles)
  const findArtifactByPath = useStoreProject(s => s.findArtifactByPath)
  const setActiveRange = useStoreProject(s => s.setActiveRange)

  const storedTabs = useStoreEditor(s => s.storedTabs)
  const storedTabId = useStoreEditor(s => s.storedTabId)
  const selectTab = useStoreEditor(s => s.selectTab)
  const createTab = useStoreEditor(s => s.createTab)
  const addTabs = useStoreEditor(s => s.addTabs)
  const closeTabs = useStoreEditor(s => s.closeTabs)
  const closeTab = useStoreEditor(s => s.closeTab)
  const inTabs = useStoreEditor(s => s.inTabs)

  const subscribe = useChannelEvents()

  const enqueueMany = useStoreActionManager(s => s.enqueueMany)
  const enqueue = useStoreActionManager(s => s.enqueue)
  const isActiveAction = useStoreActionManager(s => s.isActiveAction)
  const resetQueueCurrentByAction = useStoreActionManager(
    s => s.resetQueueCurrentByAction,
  )

  const currentActions = useStoreActionManager(s => s.currentActions)
  const queues = useStoreActionManager(s => s.queues)

  // We need to fetch from IDE level to make sure
  // all pages have access to models and files
  const { refetch: getModels, cancel: cancelRequestModels } = useApiModels()
  const { refetch: getFiles, cancel: cancelRequestFiles } = useApiFiles()
  const {
    data: dataEnvironments,
    refetch: getEnvironments,
    cancel: cancelRequestEnvironments,
  } = useApiEnvironments()
  const { refetch: planRun, cancel: cancelRequestPlan } = useApiPlanRun(
    environment.name,
    {
      planOptions: {
        skip_tests: true,
        include_unmodified: true,
      },
    },
  )

  useEffect(() => {
    const unsubscribeTasks = subscribe<PlanProgress>('tasks', updateTasks)
    const unsubscribeModels = subscribe<Model[]>('models', updateModels)
    const unsubscribeErrors = subscribe<ErrorIDE>('errors', displayErrors)
    const unsubscribePromote = subscribe<any>(
      'promote-environment',
      handlePromote,
    )
    const unsubscribeFile = subscribe<{
      changes: Array<{
        change: FileExplorerChange
        path: string
        file: ModelFile
      }>
      directories: Record<string, Directory>
    }>('file', ({ changes, directories }) => {
      changes.sort((a: any) =>
        a.change === EnumFileExplorerChange.Deleted ? -1 : 1,
      )

      changes.forEach(({ change, path, file }) => {
        if (change === EnumFileExplorerChange.Modified) {
          const currentFile = findArtifactByPath(file.path) as
            | ModelFile
            | undefined

          if (isNil(currentFile) || isNil(file)) return

          currentFile.update(file)
        }

        if (change === EnumFileExplorerChange.Deleted) {
          const artifact = findArtifactByPath(path)

          if (isNil(artifact)) return

          if (artifact instanceof ModelDirectory) {
            artifact.parent?.removeDirectory(artifact)
          }

          if (artifact instanceof ModelFile) {
            artifact.parent?.removeFile(artifact)

            if (inTabs(artifact)) {
              closeTab(artifact)
            }
          }
        }
      })

      for (const path in directories) {
        const directory = directories[path]!

        const currentDirectory = findArtifactByPath(path) as
          | ModelDirectory
          | undefined

        if (isNil(currentDirectory)) continue

        directory.directories?.forEach((d: any) => {
          const directory = findArtifactByPath(d.path) as
            | ModelDirectory
            | undefined

          if (isNil(directory)) {
            currentDirectory.addDirectory(
              new ModelDirectory(d, currentDirectory),
            )
          }
        })

        directory.files?.forEach((f: any) => {
          const file = findArtifactByPath(f.path) as ModelFile | undefined

          if (isNil(file)) {
            currentDirectory.addFile(new ModelFile(f, currentDirectory))
          }
        })

        currentDirectory.directories.sort((a, b) => (a.name > b.name ? 1 : -1))
        currentDirectory.files.sort((a, b) => (a.name > b.name ? 1 : -1))
      }

      refreshFiles()
      setActiveRange()
    })

    enqueueMany([
      {
        action: 'request-models',
        callback: getModels,
        onCallbackSuccess({ data }) {
          updateModels(data as Model[])
        },
      },
      {
        action: EnumAction.FileExplorerGet,
        callback: getFiles,
        onCallbackSuccess({ data }) {
          closeTabs()
          setProject(new ModelDirectory())

          if (isNil(data)) return

          const project = new ModelDirectory(data)
          const files = project.allFiles

          restoreEditorTabsFromSaved(files)
          setFiles(files)
          setProject(project)
        },
      },
    ])

    return () => {
      void cancelRequestModels()
      void cancelRequestFiles()
      void cancelRequestEnvironments()
      void cancelRequestPlan()

      unsubscribeTasks?.()
      unsubscribeModels?.()
      unsubscribePromote?.()
      unsubscribeErrors?.()
      unsubscribeFile?.()
    }
  }, [])

  useEffect(() => {
    if (location.pathname === EnumRoutes.Ide) {
      navigate(EnumRoutes.IdeEditor)
    }
  }, [location])

  useEffect(() => {
    if (isNil(dataEnvironments) || isObjectEmpty(dataEnvironments)) return

    addSynchronizedEnvironments(Object.values(dataEnvironments))

    // This use case is happening when user refreshes the page
    // while plan is still applying
    enqueue({
      action: EnumAction.Plan,
      callback: planRun,
      cancel: cancelRequestPlan,
    })
  }, [dataEnvironments])

  useEffect(() => {
    if (models.size > 0 && isFalse(hasSynchronizedEnvironments())) {
      enqueue({
        action: 'request-environments',
        callback: getEnvironments,
        cancel: cancelRequestEnvironments,
      })
    }

    if (hasSynchronizedEnvironments()) {
      enqueue({
        action: EnumAction.Plan,
        callback: planRun,
        cancel: cancelRequestPlan,
      })
    }
  }, [models])

  useEffect(() => {
    setShowConfirmation(confirmations.length > 0)
  }, [confirmations])

  function updateModels(models?: Model[]): void {
    if (isNotNil(models)) {
      removeError(EnumErrorKey.Models)
      setModels(models)
    }
  }

  function updateTasks(data?: PlanProgress): void {
    if (isNil(data)) return

    if (isFalse(isObject(data.tasks))) {
      setState(EnumPlanState.Init)
      resetQueueCurrentByAction(EnumAction.Backfill)

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
      resetQueueCurrentByAction(EnumAction.Backfill)
    } else if (isAllTasksCompleted(data.tasks)) {
      setState(EnumPlanState.Finished)
      resetQueueCurrentByAction(EnumAction.Backfill)
    } else {
      setState(EnumPlanState.Applying)
      if (isFalse(isActiveAction(EnumAction.Backfill))) {
        enqueue({ action: EnumAction.Backfill })
      }
    }
  }

  function displayErrors(data: ErrorIDE): void {
    addError(EnumErrorKey.General, data)
  }

  function handlePromote(): void {
    setActivePlan(undefined)

    void getEnvironments()
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
  const actionsInQueue = Array.from(queues.values())
    .map(({ queue }) => queue)
    .flat()

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
        {currentActions.length > 0 && (
          <div className="flex overflow-hidden text-xs text-neutral-400 px-2 py-1 rounded-md bg-neutral-10">
            <Spinner
              variant={EnumVariant.Info}
              className="w-4 h-4"
            />
            <span className="flex">
              <span className="ml-2">Running</span>
              <b className="block w-full ml-2 text-neutral-600 whitespace-nowrap overflow-hidden text-ellipsis">
                {currentActions.length > 4
                  ? `${currentActions.length} actions`
                  : currentActions.map(camelCaseToRegularCase).join(', ')}
                {actionsInQueue.length > 0 && (
                  <span className="ml-2 text-neutral-500 whitespace-nowrap">
                    with{' '}
                    {actionsInQueue.length > 2
                      ? actionsInQueue.length
                      : `${actionsInQueue
                          .map(q => camelCaseToRegularCase(q.action))
                          .join(', ')}`}{' '}
                    queued
                  </span>
                )}
              </b>
            </span>
          </div>
        )}
        <div className="flex px-3 items-center justify-end">
          <RunPlan />
          <Suspense>
            {isNotNil(activePlan) && <ActivePlan plan={activePlan} />}
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
