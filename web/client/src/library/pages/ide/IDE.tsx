import { useEffect, useCallback } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import {
  useApiFiles,
  useApiEnvironments,
  apiCancelGetEnvironments,
  apiCancelFiles,
  useApiModels,
  apiCancelModels,
} from '../../../api'
import { useStorePlan } from '../../../context/plan'
import { useChannelEvents } from '../../../api/channels'
import { isArrayEmpty, debounceAsync } from '~/utils'
import { useStoreContext } from '~/context/context'
import { Divider } from '@components/divider/Divider'
import Container from '@components/container/Container'
import RunPlan from './RunPlan'
import ActivePlan from './ActivePlan'
import { ArrowLongRightIcon } from '@heroicons/react/24/solid'
import { Button } from '@components/button/Button'
import { EnumSize, EnumVariant } from '~/types/enum'
import { Link, Outlet, useLocation, useNavigate } from 'react-router-dom'
import { EnumRoutes } from '~/routes'
import { useIDE } from './context'
import { useStoreFileTree } from '@context/fileTree'

export default function IDE(): JSX.Element {
  const location = useLocation()
  const navigate = useNavigate()

  const client = useQueryClient()

  const { setIsPlanOpen } = useIDE()

  const addSyncronizedEnvironments = useStoreContext(
    s => s.addSyncronizedEnvironments,
  )
  const setModels = useStoreContext(s => s.setModels)

  const activePlan = useStorePlan(s => s.activePlan)
  const updateTasks = useStorePlan(s => s.updateTasks)

  const directory = useStoreFileTree(s => s.project)
  const setFiles = useStoreFileTree(s => s.setFiles)
  const setProject = useStoreFileTree(s => s.setProject)

  const [subscribe] = useChannelEvents()

  const { data: dataModels, refetch: getModels } = useApiModels()
  const { data: project, refetch: getFiles } = useApiFiles()
  const { data: contextEnvironemnts, refetch: getEnvironments } =
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

    void debouncedGetEnvironemnts()
    void debouncedGetFiles()
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
    if (
      contextEnvironemnts == null ||
      isArrayEmpty(Object.keys(contextEnvironemnts))
    )
      return

    addSyncronizedEnvironments(Object.values(contextEnvironemnts))
  }, [contextEnvironemnts])

  useEffect(() => {
    setModels(dataModels)
  }, [dataModels])

  useEffect(() => {
    setFiles(directory?.allFiles ?? [])
  }, [directory])

  useEffect(() => {
    setProject(project)
  }, [project])

  function showRunPlan(): void {
    setIsPlanOpen(true)
  }

  const isActivePageEditor = location.pathname === EnumRoutes.IdeEditor

  return (
    <Container.Page>
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
        {isActivePageEditor && (
          <div className="px-3 flex items-center min-w-[10rem] justify-end">
            <RunPlan showRunPlan={showRunPlan} />
            {activePlan != null && <ActivePlan plan={activePlan} />}
          </div>
        )}
      </div>
      <Divider />
      <Outlet />
    </Container.Page>
  )
}
