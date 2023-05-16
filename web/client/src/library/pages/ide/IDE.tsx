import { useEffect, useCallback, useState, Fragment } from 'react'
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
import { ERROR_KEY_GENERAL, useIDE } from './context'
import { useStoreFileTree } from '@context/fileTree'
import { Popover, Transition } from '@headlessui/react'
import clsx from 'clsx'

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
    const unsubscribeModels = subscribe('models', data => {
      console.log('models', data)
      setModels(data)
    })

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
        <div className="px-3 flex items-center min-w-[10rem] justify-end">
          {isActivePageEditor && (
            <>
              <RunPlan showRunPlan={showRunPlan} />
              {activePlan != null && <ActivePlan plan={activePlan} />}
            </>
          )}
          <ErrorsPreview />
        </div>
      </div>
      <Divider />
      <Outlet />
    </Container.Page>
  )
}

function ErrorsPreview(): JSX.Element {
  const [subscribe] = useChannelEvents()

  const { errors, addError } = useIDE()

  const [isShowing, setIsShowing] = useState(false)

  useEffect(() => {
    const unsubscribeErrors = subscribe('errors', displayErrors)

    return () => {
      unsubscribeErrors?.()
    }
  }, [])

  function displayErrors(data: any): void {
    addError(ERROR_KEY_GENERAL, data)
    console.log(data)
  }

  const hasError = errors.size > 0

  return (
    <Popover
      onMouseEnter={() => {
        setIsShowing(true)
      }}
      onMouseLeave={() => {
        setIsShowing(false)
      }}
      className="relative flex"
    >
      {() => (
        <>
          <span
            className={clsx(
              'block ml-1 px-2 first-child:ml-0 rounded-full border whitespace-nowrap text-neutral-100 text-xs text-center',
              hasError
                ? 'border-danger-500 bg-danger-500 cursor-pointer'
                : 'border-neutral-500 cursor-default',
            )}
          >
            {hasError ? (
              <span>{errors.size} Errors</span>
            ) : (
              <span>No Errors</span>
            )}
          </span>
          <Transition
            show={isShowing && hasError}
            as={Fragment}
            enter="transition ease-out duration-200"
            enterFrom="opacity-0 translate-y-1"
            enterTo="opacity-100 translate-y-0"
            leave="transition ease-in duration-150"
            leaveFrom="opacity-100 translate-y-0"
            leaveTo="opacity-0 translate-y-1"
          >
            <Popover.Panel className="absolute rounded-md right-0 z-[1000] bg-light p-2 mt-8 transform flex min-w-[10rem] max-w-[40vw] max-h-[50-vh] text-danger-700">
              <ul className="w-full h-full bg-danger-10 p-4 rounded-md overflow-auto scrollbar scrollbar--vertical scrollbar--horizontal">
                {Array.from(errors.entries())
                  .reverse()
                  .map(([key, error], idx) => (
                    <li
                      key={error.traceback ?? error.message}
                      className="mb-4"
                    >
                      <h4 className={clsx('mb-2 font-bold whitespace-nowrap')}>
                        <small className="block">{key}</small>
                        <span>
                          {idx + 1}.&nbsp;{error.message}
                        </span>
                      </h4>
                      <pre>
                        <code>{error.traceback}</code>
                      </pre>
                    </li>
                  ))}
              </ul>
            </Popover.Panel>
          </Transition>
        </>
      )}
    </Popover>
  )
}
