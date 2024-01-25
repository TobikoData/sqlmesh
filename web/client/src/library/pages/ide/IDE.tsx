import React, { useEffect } from 'react'
import {
  useApiModels,
  useApiFiles,
  useApiEnvironments,
  useApiPlanRun,
  useApiPlanApply,
  useApiCancelPlan,
} from '../../../api'
import { useChannelEvents } from '../../../api/channels'
import {
  isArrayEmpty,
  isFalse,
  isNil,
  isNotNil,
  isObjectNotEmpty,
  isTrue,
} from '~/utils'
import { useStoreContext } from '~/context/context'
import { Outlet, useLocation, useNavigate } from 'react-router-dom'
import { EnumRoutes } from '~/routes'
import { type Tests, useStoreProject } from '@context/project'
import { EnumErrorKey, type ErrorIDE, useIDE } from './context'
import {
  type Directory,
  type Model,
  type Environments,
  type EnvironmentsEnvironments,
  Status,
  Modules,
} from '@api/client'
import { Button } from '@components/button/Button'
import Container from '@components/container/Container'
import { useStoreEditor, createLocalFile } from '@context/editor'
import { type FormatFileStatus, ModelFile } from '@models/file'
import ModalConfirmation, {
  type Confirmation,
} from '@components/modal/ModalConfirmation'
import { ModelDirectory } from '@models/directory'
import {
  EnumFileExplorerChange,
  type FileExplorerChange,
} from '@components/fileExplorer/context'
import { type PlanOverviewTracker } from '@models/tracker-plan-overview'
import { type PlanApplyTracker } from '@models/tracker-plan-apply'
import { type PlanCancelTracker } from '@models/tracker-plan-cancel'
import { EnumPlanAction, ModelPlanAction } from '@models/plan-action'
import { useStorePlan } from '@context/plan'

export default function PageIDE(): JSX.Element {
  const location = useLocation()
  const navigate = useNavigate()

  const { removeError, addError } = useIDE()

  const models = useStoreContext(s => s.models)
  const modules = useStoreContext(s => s.modules)
  const showConfirmation = useStoreContext(s => s.showConfirmation)
  const setShowConfirmation = useStoreContext(s => s.setShowConfirmation)
  const confirmations = useStoreContext(s => s.confirmations)
  const removeConfirmation = useStoreContext(s => s.removeConfirmation)
  const environment = useStoreContext(s => s.environment)
  const setModels = useStoreContext(s => s.setModels)
  const addRemoteEnvironments = useStoreContext(s => s.addRemoteEnvironments)

  const planOverview = useStorePlan(s => s.planOverview)
  const planApply = useStorePlan(s => s.planApply)
  const planCancel = useStorePlan(s => s.planCancel)
  const planAction = useStorePlan(s => s.planAction)
  const setPlanOverview = useStorePlan(s => s.setPlanOverview)
  const setPlanApply = useStorePlan(s => s.setPlanApply)
  const setPlanCancel = useStorePlan(s => s.setPlanCancel)
  const setPlanAction = useStorePlan(s => s.setPlanAction)

  const files = useStoreProject(s => s.files)
  const selectedFile = useStoreProject(s => s.selectedFile)
  const setProject = useStoreProject(s => s.setProject)
  const setFiles = useStoreProject(s => s.setFiles)
  const refreshFiles = useStoreProject(s => s.refreshFiles)
  const findArtifactByPath = useStoreProject(s => s.findArtifactByPath)
  const setActiveRange = useStoreProject(s => s.setActiveRange)
  const setTests = useStoreProject(s => s.setTests)

  const storedTabs = useStoreEditor(s => s.storedTabs)
  const storedTabId = useStoreEditor(s => s.storedTabId)
  const selectTab = useStoreEditor(s => s.selectTab)
  const createTab = useStoreEditor(s => s.createTab)
  const addTabs = useStoreEditor(s => s.addTabs)
  const closeTab = useStoreEditor(s => s.closeTab)
  const inTabs = useStoreEditor(s => s.inTabs)

  const channel = useChannelEvents()

  // We need to fetch from IDE level to make sure
  // all pages have access to models and files
  const { refetch: getModels, cancel: cancelRequestModels } = useApiModels()
  const { refetch: getFiles, cancel: cancelRequestFiles } = useApiFiles()
  const { refetch: getEnvironments, cancel: cancelRequestEnvironments } =
    useApiEnvironments()
  const { isFetching: isFetchingPlanApply } = useApiPlanApply(environment.name)
  const { refetch: cancelPlanOrPlanApply, isFetching: isFetchingPlanCancel } =
    useApiCancelPlan()
  const {
    refetch: planRun,
    cancel: cancelRequestPlan,
    isFetching: isFetchingPlanRun,
  } = useApiPlanRun(environment.name, {
    planOptions: {
      skip_tests: true,
      include_unmodified: true,
    },
  })

  useEffect(() => {
    const channelModels = channel<Model[]>('models', updateModels)
    const channelTests = channel<Tests>('tests', updateTests)
    const channelErrors = channel<ErrorIDE>('errors', displayErrors)
    const channelPlanOverview = channel<any>(
      'plan-overview',
      updatePlanOverviewTracker,
    )
    const channelPlanApply = channel<any>('plan-apply', updatePlanApplyTracker)
    const channelPlanCancel = channel<any>(
      'plan-cancel',
      updatePlanCancelTracker,
    )
    const channelFile = channel<{
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
    const channelFormatFile = channel<FormatFileStatus>(
      'format-file',
      formatFileStatus => {
        const file = files.get(formatFileStatus.path)

        if (isNotNil(file)) {
          file.isFormatted = formatFileStatus.status === Status.success

          refreshFiles()
        }
      },
    )

    void getFiles().then(({ data }) => {
      if (isNil(data)) return

      const project = new ModelDirectory(data)
      const files = project.allFiles

      restoreEditorTabsFromSaved(files)
      setFiles(files)
      setProject(project)
    })

    void getEnvironments().then(({ data }) => updateEnviroments(data))

    void getModels().then(({ data }) => {
      updateModels(data as Model[])
    })

    channelFormatFile.subscribe()
    channelModels.subscribe()
    channelErrors.subscribe()
    channelPlanOverview.subscribe()
    channelPlanApply.subscribe()
    channelFile.subscribe()
    channelPlanCancel.subscribe()
    channelTests.subscribe()

    return () => {
      void cancelRequestModels()
      void cancelRequestFiles()
      void cancelRequestEnvironments()
      void cancelRequestPlan()

      channelFormatFile.unsubscribe()
      channelTests.unsubscribe()
      channelModels.unsubscribe()
      channelErrors.unsubscribe()
      channelPlanOverview.unsubscribe()
      channelPlanApply.unsubscribe()
      channelFile.unsubscribe()
      channelPlanCancel.unsubscribe()
    }
  }, [])

  useEffect(() => {
    if (location.pathname === EnumRoutes.Ide) {
      navigate(
        modules.includes(Modules.editor)
          ? EnumRoutes.IdeEditor
          : EnumRoutes.IdeDocs,
        { replace: true },
      )
    }
  }, [location])

  useEffect(() => {
    setShowConfirmation(confirmations.length > 0)
  }, [confirmations])

  useEffect(() => {
    if (
      models.size > 0 &&
      isFalse(planAction.isProcessing) &&
      isFalse(planAction.isRunningTask)
    ) {
      planApply.reset()

      void planRun()
    }
  }, [models, environment])

  useEffect(() => {
    planOverview.isFetching = isFetchingPlanRun

    setPlanOverview(planOverview)
  }, [isFetchingPlanRun])

  useEffect(() => {
    planOverview.isFetching = isFetchingPlanApply

    setPlanApply(planApply)
  }, [isFetchingPlanApply])

  useEffect(() => {
    planOverview.isFetching = isFetchingPlanCancel

    setPlanCancel(planCancel)
  }, [isFetchingPlanCancel])

  useEffect(() => {
    const value = ModelPlanAction.getPlanAction({
      planOverview,
      planApply,
      planCancel,
    })

    // This inconsistency can happen when we receive flag from the backend
    // that some task is runninng but not yet recived SSE event with data
    if (value === EnumPlanAction.Done && planAction.isRunningTask) return

    setPlanAction(new ModelPlanAction({ value }))
  }, [planOverview, planApply, planCancel])

  function updateModels(models?: Model[]): void {
    if (isNotNil(models)) {
      removeError(EnumErrorKey.Models)
      setModels(models)
    }
  }

  function displayErrors(data: ErrorIDE): void {
    if (planAction.isApplying || planAction.isRunning) {
      void cancelPlanOrPlanApply()
    }

    addError(EnumErrorKey.General, data)
  }

  function updateTests(tests?: Tests): void {
    setTests(tests)
  }

  function updatePlanOverviewTracker(data: PlanOverviewTracker): void {
    planOverview.update(data)

    setPlanOverview(planOverview)
  }

  function updatePlanCancelTracker(data: PlanCancelTracker): void {
    planCancel.update(data)

    setPlanCancel(planCancel)
  }

  function updatePlanApplyTracker(data: PlanApplyTracker): void {
    planApply.update(data, planOverview)

    const isFinished =
      isTrue(data.meta?.done) && data.meta?.status !== Status.init

    if (isFinished) {
      void getEnvironments().then(({ data }) => {
        void planRun()

        updateEnviroments(data)
      })
    }

    setPlanApply(planApply)
  }

  function updateEnviroments(data: Optional<Environments>): void {
    const { environments, default_target_environment, pinned_environments } =
      data ?? {}

    if (isObjectNotEmpty<EnvironmentsEnvironments>(environments)) {
      addRemoteEnvironments(
        Object.values(environments),
        default_target_environment,
        pinned_environments,
      )
    }
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

    if (isNotNil(tab) && isNil(selectedFile)) {
      selectTab(tab)
    }
  }

  function closeModalConfirmation(confirmation?: Confirmation): void {
    confirmation?.cancel?.()

    setShowConfirmation(false)
  }

  const confirmation = confirmations[0]

  return (
    <Container.Page>
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
