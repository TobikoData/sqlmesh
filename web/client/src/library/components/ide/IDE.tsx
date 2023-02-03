import { ViewUpdate } from '@codemirror/view'
import ContextIDE from '../../../context/Ide'
import { Button } from '../button/Button'
import { Divider } from '../divider/Divider'
import { Editor } from '../editor/Editor'
import { FolderTree } from '../folderTree/FolderTree'
import Tabs from '../tabs/Tabs'
import { Fragment, useEffect, useState } from 'react'
import clsx from 'clsx'
import {
  XCircleIcon,
  PlayIcon,
} from '@heroicons/react/24/solid'
import { EnumSize } from '../../../types/enum'
import { Transition, Dialog, Popover } from '@headlessui/react'
import {
  useApiFileByPath,
  useMutationApiSaveFile,
  useApiFiles,
} from '../../../api'
import type { File } from '../../../api/client'
import { useQueryClient } from '@tanstack/react-query'
import { Plan } from '../plan/Plan'
import { EnumPlanState, useStorePlan } from '../../../context/plan'
import { Progress } from '../progress/Progress'
import { Spinner } from '../logo/Spinner'
import fetchAPI from '../../../api/instance'
import { isObject, isObjectEmpty } from '../../../utils'

export function IDE() {
  const client = useQueryClient()

  const planState = useStorePlan((s: any) => s.state)
  const setPlanState = useStorePlan((s: any) => s.setState)
  const setPlanAction = useStorePlan((s: any) => s.setAction)
  const activePlan = useStorePlan((s: any) => s.activePlan)
  const environment = useStorePlan((s: any) => s.environment)
  const setActivePlan = useStorePlan((s: any) => s.setActivePlan)

  const [openedFiles, setOpenedFiles] = useState<Set<File>>(new Set())
  const [activeFile, setActiveFile] = useState<File | null>(null)
  const [fileContent, setFileContent] = useState<string>('')
  const [isOpenModalPlan, setIsOpenModalPlan] = useState(false)
  const [status, setStatus] = useState('editing')
  const [isPlanCompleted, setIsPlanCompleted] = useState<boolean | null>(null)

  const saveFile = useMutationApiSaveFile(client)
  const { data: project } = useApiFiles()
  const { data: fileData } = useApiFileByPath(activeFile?.path)

  useEffect(() => {
    if (isPlanCompleted == null) return

    if (planState === EnumPlanState.Applying) {
      setPlanAction(EnumPlanState.Done)
    }

    if (isOpenModalPlan) return setPlanState(EnumPlanState.Setting)
    if (isPlanCompleted) return setPlanState(EnumPlanState.Init)

    setPlanState(EnumPlanState.Failed)
  }, [isPlanCompleted])

  useEffect(() => {
    setFileContent(fileData?.content ?? '')
  }, [fileData])

  function subscribe(channel: any, callback: any) {
    channel.onmessage = (event: any) => {
      callback(JSON.parse(event.data), channel)
    }

    return channel
  }

  function updateTasks(data: any, channel: any) {
    setActivePlan(null)

    if (data.environment == null) return channel.close()

    if (!data.ok) {
      setIsPlanCompleted(false)

      return channel.close()
    }

    if (!isObject(data.tasks)) return channel.close()

    if (isObjectEmpty(data.tasks)) {
      setIsPlanCompleted(true)

      return channel.close()
    } else {
      setActivePlan({
        environment: data.environment,
        tasks: data.tasks,
      })
    }

  }

  function closeIdeTab(f: File) {
    if (!f) return

    openedFiles.delete(f)

    if (openedFiles.size === 0) {
      setActiveFile(null)
    } else if (!activeFile || !openedFiles.has(activeFile)) {
      setActiveFile([...openedFiles][0])
    }

    setOpenedFiles(new Set([...openedFiles]))
  }

  async function applyPlan() {
    setIsPlanCompleted(null)

    setPlanState(EnumPlanState.Applying)
    setPlanAction(EnumPlanState.Applying)

    try {
      const data: any = await fetchAPI({ url: `/api/apply?environment=${environment}`, method: 'post' })

      if (data.ok) {
        subscribe(new EventSource('/api/tasks'), updateTasks)
      }
    } catch (error) {
      console.log(error)

      setIsPlanCompleted(false)
    }
  }

  function closePlan() {
    setPlanAction(EnumPlanState.Closing)

    if (planState !== EnumPlanState.Applying && planState !== EnumPlanState.Canceling) {
      setPlanState(EnumPlanState.Init)
    }

    setIsOpenModalPlan(false)
  }

  function cancelPlan() {
    setPlanAction(EnumPlanState.Canceling)

    if (planState === EnumPlanState.Applying) {
      setPlanState(EnumPlanState.Canceling)
    }

    console.log('cancel plan')

    // Cancel request

    if (isOpenModalPlan) {
      setPlanState(EnumPlanState.Setting)
    } else {
      setPlanState(EnumPlanState.Init)
    }
  }

  function startPlan() {
    setPlanState(EnumPlanState.Setting)
    setIsOpenModalPlan(true)
  }

  return (
    <ContextIDE.Provider
      value={{
        openedFiles,
        activeFile,
        setActiveFile: (file) => {
          if (!file) return setActiveFile(null)

          setActiveFile(file)
          setOpenedFiles(new Set([...openedFiles, file]))
        },
        setOpenedFiles,
      }}
    >
      <div className="w-full flex justify-between items-center min-h-[2rem] z-50">
        <div className="px-3 flex items-center whitespace-nowrap">
          <h3 className="font-bold"><span className='inline-block text-secondary-500'>/</span> {project?.name}</h3>
        </div>

        <div className='flex w-full justify-center'>
          <ul className='flex w-full items-center justify-center'>
            {['Editor', 'Graph', 'Audits', 'Tests'].map((name, i) => (
              <li key={name} >
                <div className={clsx(
                  'mx-2 text-sm opacity-85 flex',
                  name === 'Editor' && 'font-bold opacity-100 border-b-2 border-secondary-500 text-secondary-500 cursor-default',
                  ['Audits', 'Graph', 'Tests'].includes(name) && 'opacity-25 cursor-not-allowed'
                )}>
                  {i > 0 && <Divider orientation='vertical' className='h-3 mx-2' />}
                  {name}
                </div>
              </li>
            ))}
          </ul>
        </div>

        <div className="px-3 flex items-center">
          {planState === 'applying' && activePlan ? (
            <Popover className="relative">
              {({ open }) => (
                <>
                  <Popover.Button
                    className={`
                      ${open ? '' : 'text-opacity-90'}
                      group text-base font-medium text-white hover:text-opacity-100 focus:outline-none focus-visible:ring-2 focus-visible:ring-white focus-visible:ring-opacity-75`}
                  >
                    <div className='flex items-center text-xs whitespace-nowrap m-1 bg-secondary-100 rounded-md px-2 pr-1 py-1'>
                      <Spinner className='w-3 h-3 mr-2' />
                      <span className="inline-block mr-3 min-w-20 text-secondary-500">
                        {planState === EnumPlanState.Init
                          ? 'Run Plan'
                          : planState === EnumPlanState.Setting
                            ? 'Setting Plan'
                            : planState === EnumPlanState.Canceling
                              ? 'Canceling Plan...'
                              : 'Applying Plan...'
                        }
                      </span>
                      <span className='inline-block px-2 rounded-[4px] text-xs bg-secondary-500 text-secondary-100 font-bold'>1</span>
                    </div>
                  </Popover.Button>
                  <Transition
                    as={Fragment}
                    enter="transition ease-out duration-200"
                    enterFrom="opacity-0 translate-y-1"
                    enterTo="opacity-100 translate-y-0"
                    leave="transition ease-in duration-150"
                    leaveFrom="opacity-100 translate-y-0"
                    leaveTo="opacity-0 translate-y-1"
                  >
                    <Popover.Panel className="absolute right-1 z-10 mt-3 transform">
                      <div className="overflow-hidden rounded-lg shadow-lg ring-1 ring-black ring-opacity-5">
                        <div className="relative grid gap-8 py-3 bg-white">
                          <div key={activePlan.environment} className="mx-4">
                            <div className='flex justify-between items-baseline'>
                              <small className="block whitespace-nowrap text-sm font-medium text-gray-900">
                                Environemnt: {activePlan.environment}
                              </small>
                              <small className="block whitespace-nowrap text-xs font-medium text-gray-900">
                                {Object.values(activePlan.tasks).filter((t: any) => t.completed === t.total).length} of {Object.values(activePlan.tasks).length}
                              </small>
                            </div>
                            <Progress
                              progress={Math.ceil(Object.values(activePlan.tasks).filter((t: any) => t.completed === t.total).length / Object.values(activePlan.tasks).length * 100)}
                            />
                            <div className='my-4 px-4 py-2 bg-secondary-100 rounded-lg'>
                              {(Object.entries(activePlan.tasks)).map(([model_name, task]: any) => (
                                <div key={model_name}>
                                  <div className='flex justify-between items-baselin'>
                                    <small className="text-xs block whitespace-nowrap font-medium text-gray-900 mr-6">
                                      {model_name}
                                    </small>
                                    <small className="block whitespace-nowrap text-xs font-medium text-gray-900">
                                      {task.completed} of {task.total}
                                    </small>
                                  </div>
                                  <Progress
                                    progress={Math.ceil(task.completed / task.total * 100)}
                                  />
                                </div>
                              ))}
                            </div>
                            <div className='flex justify-end items-center px-2'>
                              <Button size='sm' variant='danger' className='mx-0' onClick={e => {
                                e.stopPropagation()
                                cancelPlan()
                              }}>Cancel</Button>
                            </div>
                          </div>
                        </div>

                      </div>
                    </Popover.Panel>

                  </Transition>
                </>
              )}
            </Popover>
          ) : (
            <Button
              disabled={planState === EnumPlanState.Setting}
              variant='primary'
              size={EnumSize.sm}
              onClick={e => {
                e.stopPropagation()
                startPlan()
              }}
              className='min-w-[6rem] justify-between'
            >
              <span className="inline-block mr-3 min-w-20">
                {planState === EnumPlanState.Init ? 'Run Plan' : planState === EnumPlanState.Setting ? 'Setting Plan' : 'Applying Plan'}
              </span>
              <PlayIcon className="w-[1rem] h-[1rem] text-inherit" />
            </Button>
          )}

        </div>
      </div>
      <Divider />
      <div className="flex w-full h-full overflow-hidden">
        <div className="w-[16rem] overflow-hidden overflow-y-auto">
          <FolderTree project={project} />
        </div>
        <Divider orientation="vertical" />
        <div className="h-full w-full flex flex-col overflow-hidden">
          {Boolean(activeFile) && (
            <>
              <div className="w-full h-full flex overflow-hidden">
                <div className="w-full flex flex-col overflow-hidden">
                  <ul className="w-full whitespace-nowrap pl-[40px] min-h-[2rem] max-h-[2rem] overflow-hidden overflow-x-auto">
                    {openedFiles.size > 0 &&
                      [...openedFiles].map((file) => (
                        <li
                          key={file.path}
                          className={clsx(
                            'inline-block py-1 pr-2 last-child:pr-0 overflow-hidden text-center overflow-ellipsis cursor-pointer',

                          )}
                          onClick={() => setActiveFile(file)}
                        >
                          <span className={clsx(
                            "flex justify-between items-center px-2 py-[0.25rem] min-w-[8rem] rounded-md",
                            file.path === activeFile?.path
                              ? 'bg-secondary-100'
                              : 'bg-transparent'
                          )}>
                            <small className="text-xs">{file.name}</small>
                            <XCircleIcon
                              onClick={(e) => {
                                e.stopPropagation()

                                closeIdeTab(file)
                              }}
                              className={`inline-block text-gray-700 w-4 h-4 ml-2 cursor-pointer`}
                            />
                          </span>
                        </li>
                      ))}
                  </ul>
                  <Divider />
                  <div className="w-full h-full flex flex-col overflow-hidden">
                    <div className="w-full h-full overflow-hidden ">
                      <Editor
                        className="h-full w-full"
                        extension={activeFile?.extension}
                        value={fileContent}
                        onChange={debounce(
                          (value: string, viewUpdate: ViewUpdate) => {
                            const shouldMutate =
                              Boolean(value) && value !== fileContent

                            if (shouldMutate) {
                              saveFile.mutate({
                                path: activeFile?.path,
                                body: viewUpdate.state.doc.toString(),
                              })

                              setStatus('saved')
                            } else {
                              setStatus('editing')
                            }
                          },
                          () => {
                            setStatus('saving...')
                          },
                          2000
                        )}
                      />
                    </div>
                  </div>
                  <Divider />
                  <div className="px-2 flex justify-between items-center min-h-[2rem]">
                    <small>validation: ok</small>
                    <small>File Status: {status}</small>
                    <div className="flex">
                      <Button size={EnumSize.sm} variant="secondary">
                        Run Query
                      </Button>
                      <Button size={EnumSize.sm} variant="alternative">
                        Validate
                      </Button>
                      <Button size={EnumSize.sm} variant="alternative">
                        Format
                      </Button>
                      <Button size={EnumSize.sm} variant="success">
                        Save
                      </Button>
                    </div>
                  </div>
                </div>
              </div>
              <Divider />
              <div className="w-full min-h-[10rem] overflow-auto">
                <Tabs />
              </div>
            </>
          )}

          {!Boolean(activeFile) && (
            <div className="w-full h-full flex justify-center items-center text-center">
              <div className="prose">
                <h2>Instructions on how to start</h2>
                <p>Select file</p>
              </div>
            </div>
          )}
        </div>
        <Divider orientation="vertical" />
      </div>
      <Divider />
      <div className="p-1">ide footer</div>
      <Transition appear show={isOpenModalPlan} as={Fragment} afterLeave={() => setPlanAction(EnumPlanState.None)}>
        <Dialog as="div" className="relative z-[100]" onClose={() => undefined}>
          <Transition.Child
            as={Fragment}
            enter="ease-out duration-300"
            enterFrom="opacity-0"
            enterTo="opacity-100"
            leave="ease-in duration-200"
            leaveFrom="opacity-100"
            leaveTo="opacity-0"
          >
            <div className="fixed inset-0 bg-black bg-opacity-25" />
          </Transition.Child>

          <div className="fixed inset-0 overflow-y-auto">
            <div className="flex min-h-full items-center justify-center p-4 text-center">
              <Transition.Child
                as={Fragment}
                enter="ease-out duration-300"
                enterFrom="opacity-0 scale-95"
                enterTo="opacity-100 scale-100"
                leave="ease-in duration-200"
                leaveFrom="opacity-100 scale-100"
                leaveTo="opacity-0 scale-95"
              >
                <Dialog.Panel className="w-full transform overflow-hidden rounded-2xl bg-white text-left align-middle shadow-xl transition-all">
                  <Plan onClose={() => closePlan()} onCancel={() => cancelPlan()} onApply={() => applyPlan()} />
                </Dialog.Panel>
              </Transition.Child>
            </div>
          </div>
        </Dialog>
      </Transition>
    </ContextIDE.Provider>
  )
}

function debounce(
  fn: (...args: any) => void,
  before: () => void,
  delay: number = 500
) {
  let timer: any
  return function (...args: any) {
    clearTimeout(timer)

    before && before()

    timer = setTimeout(() => {
      fn(...args)
    }, delay)
  }
}