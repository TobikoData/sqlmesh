import React, { useEffect, lazy } from 'react'
import {
  useApiModels,
  useApiFiles,
  useApiEnvironments,
  useApiPlanRun,
} from '../../../api'
import { useStorePlan } from '../../../context/plan'
import { useChannelEvents } from '../../../api/channels'
import {
  isArrayEmpty,
  isFalse,
  isNil,
  isNotNil,
  isObjectEmpty,
  isTrue,
} from '~/utils'
import { useStoreContext } from '~/context/context'
import { ArrowLongRightIcon } from '@heroicons/react/24/solid'
import { EnumSize, EnumVariant } from '~/types/enum'
import { Link, Outlet, useLocation, useNavigate } from 'react-router-dom'
import { EnumRoutes } from '~/routes'
import { type Tests, useStoreProject } from '@context/project'
import { EnumErrorKey, type ErrorIDE, useIDE } from './context'
import { Status, type Directory, type Model } from '@api/client'
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
import { type PlanOverviewTracker } from '@models/tracker-plan-overview'
import { type PlanApplyTracker } from '@models/tracker-plan-apply'
import { type PlanCancelTracker } from '@models/tracker-plan-cancel'

const ReportErrors = lazy(
  async () => await import('../../components/report/ReportErrors'),
)
const RunPlan = lazy(async () => await import('./RunPlan'))
const PlanSidebar = lazy(async () => await import('./PlanSidebar'))

export default function PageIDE(): JSX.Element {
  const location = useLocation()
  const navigate = useNavigate()

  const { removeError, addError } = useIDE()

  const isRunningPlan = useStoreContext(s => s.isRunningPlan)
  const setIsRunningPlan = useStoreContext(s => s.setIsRunningPlan)
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

  const planOverview = useStorePlan(s => s.planOverview)
  const planApply = useStorePlan(s => s.planApply)
  const planCancel = useStorePlan(s => s.planCancel)
  const setPlanOverview = useStorePlan(s => s.setPlanOverview)
  const setPlanApply = useStorePlan(s => s.setPlanApply)
  const setPlanCancel = useStorePlan(s => s.setPlanCancel)

  const project = useStoreProject(s => s.project)
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

    void getModels().then(({ data }) => {
      updateModels(data as Model[])
    })

    void getFiles().then(({ data }) => {
      if (isNil(data)) return

      const project = new ModelDirectory(data)
      const files = project.allFiles

      restoreEditorTabsFromSaved(files)
      setFiles(files)
      setProject(project)
    })

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
      navigate(EnumRoutes.IdeEditor)
    }
  }, [location])

  useEffect(() => {
    if (
      isNil(dataEnvironments) ||
      isNil(dataEnvironments.environments) ||
      isObjectEmpty(dataEnvironments) ||
      isObjectEmpty(dataEnvironments.environments)
    )
      return

    const { environments, default_target_environment, pinned_environments } =
      dataEnvironments

    addSynchronizedEnvironments(
      Object.values(environments),
      default_target_environment,
      pinned_environments,
    )

    // This use case is happening when user refreshes the page
    // while plan is still applying
    if (isFalse(isRunningPlan) && isFalse(planCancel.isCancelling)) {
      void planRun()
    }
  }, [dataEnvironments])

  useEffect(() => {
    if (models.size > 0 && isFalse(hasSynchronizedEnvironments())) {
      void getEnvironments()
    }

    if (hasSynchronizedEnvironments() && isFalse(planCancel.isCancelling)) {
      void planRun()
    }
  }, [models])

  useEffect(() => {
    setShowConfirmation(confirmations.length > 0)
  }, [confirmations])

  useEffect(() => {
    const { promote, meta } = planApply

    if (
      isNotNil(promote) &&
      isTrue(meta?.done) &&
      meta?.status === Status.success
    ) {
      void getEnvironments()
    }
  }, [planApply])

  function updateModels(models?: Model[]): void {
    if (isNotNil(models)) {
      removeError(EnumErrorKey.Models)
      setModels(models)
    }
  }

  function displayErrors(data: ErrorIDE): void {
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
    if (isNotNil(data)) {
      setIsRunningPlan(isFalse(data?.meta?.done))
    } else {
      setIsRunningPlan(false)
    }

    planApply.update(data, planOverview)

    setPlanApply(planApply)
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
