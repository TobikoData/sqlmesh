import { ViewUpdate } from '@codemirror/view'
import ContextIDE from '../../../context/Ide'
import { Button } from '../button/Button'
import { Divider } from '../divider/Divider'
import { DropdownPlan, DropdownAudits } from '../dropdown/Dropdown'
import { Editor } from '../editor/Editor'
import { FolderTree } from '../folderTree/FolderTree'
import Tabs from '../tabs/Tabs'
import { Fragment, useEffect, useState } from 'react'
import clsx from 'clsx'
import {
  XCircleIcon,
  CheckCircleIcon,
  PlayIcon,
} from '@heroicons/react/24/solid'
import { EnumSize } from '../../../types/enum'
import { Transition, Dialog, RadioGroup } from '@headlessui/react'
import {
  useApiFileByPath,
  useMutationApiSaveFile,
  useApiFiles,
} from '../../../api'
import { type File } from '../../../api/endpoints'
import { useQueryClient } from '@tanstack/react-query'
import { Plan } from '../plan/Plan'

export function IDE() {
  const client = useQueryClient()

  const [openedFiles, setOpenedFiles] = useState<Set<File>>(new Set())
  const [activeFile, setActiveFile] = useState<File | null>(null)
  const [fileContent, setFileContent] = useState<string>('')
  const [isOpenModalPlan, setIsOpenModalPlan] = useState(false)
  const [activePlan, setActivePlan] = useState<{
    text: string
    value: string
  }>()

  const [status, setStatus] = useState('editing')

  const mutationSaveFile = useMutationApiSaveFile(client)
  const { data: project } = useApiFiles()
  const { data: fileData } = useApiFileByPath(activeFile?.path)

  useEffect(() => {
    setFileContent(fileData?.content ?? '')
  }, [fileData])

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
        {/* Project Name */}
        <div className="px-3 flex">
          <p className="mr-1">Project:</p>
          <h3 className="font-bold">{project?.name}</h3>
        </div>

        {/* Git */}
        {/* <div className='px-4 flex'>
          <div className='px-4'>TobikoData/wursthall</div>
          <div className='px-4'>main</div>
        </div> */}

        {/* Search */}
        {/* <div className='px-4 w-full'>search</div> */}

        <div className="px-4 flex items-center">
          <Button size={EnumSize.sm} onClick={() => setIsOpenModalPlan(true)}>
            <span className="inline-block mr-3">Run Plan</span>
            <PlayIcon className="w-4 h-4 text-gray-100" />
          </Button>
          <DropdownAudits />
          <Button size={EnumSize.sm} variant="alternative">
            Run Tests
          </Button>
        </div>
      </div>
      <Divider />
      <div className="flex w-full h-full overflow-hidden">
        <div className="min-w-[12rem] w-full max-w-[16rem] overflow-hidden overflow-y-auto">
          <FolderTree project={project} />
        </div>
        <Divider orientation="vertical" />
        <div className="h-full w-full flex flex-col overflow-hidden">
          {/* Breadcrubms */}
          {/* <div className='w-full overflow-hidden px-2 min-h-[1.5rem] flex items-center'>
            <small className='inline-block cursor-pointer hover:text-gray-300'>audit</small>
            <small className='inline-block px-2'>/</small>
            <small className='inline-block cursor-pointer hover:text-gray-300'>items.sql</small>
          </div>
          <Divider /> */}

          {Boolean(activeFile) && (
            <>
              <div className="w-full h-full flex overflow-hidden">
                <div className="w-full flex flex-col overflow-hidden overflow-x-auto">
                  <div className="w-full flex min-h-[2rem] overflow-hidden overflow-x-auto">
                    <ul className="w-full whitespace-nowrap">
                      {openedFiles.size > 0 &&
                        [...openedFiles].map((file) => (
                          <li
                            key={file.path}
                            className={clsx(
                              'inline-block justify-between items-center py-1 px-3 overflow-hidden min-w-[10rem] text-center overflow-ellipsis cursor-pointer',
                              file.path === activeFile?.path
                                ? 'bg-white'
                                : 'bg-gray-300'
                            )}
                            onClick={() => setActiveFile(file)}
                          >
                            <span className="flex justify-between items-center">
                              <small>{file.name}</small>
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
                  </div>

                  {/* <div className='text-center flex'>
                    <div className='p-1 min-w-[10rem] '>
                      Tab 1
                    </div>
                    <div className='p-1 min-w-[10rem]'>
                      Inactive
                    </div>            
                    <div className='p-1 w-full'>
                      ide editor tabs
                    </div>
                  </div> */}
                  {/* <Divider /> */}
                  <div className="w-full h-full flex flex-col overflow-hidden">
                    <div className="w-full h-full overflow-hidden">
                      <Editor
                        className="h-full w-full"
                        extension={activeFile?.extension}
                        value={fileContent}
                        onChange={debounce(
                          (value: string, viewUpdate: ViewUpdate) => {
                            const shouldMutate =
                              Boolean(value) && value !== fileContent

                            if (shouldMutate) {
                              mutationSaveFile.mutate({
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
                  <div className="px-2 py-1 flex justify-between min-h-[1.5rem]">
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

                {/* <Divider orientation='vertical' /> */}
                {/* <div className='h-full min-w-[15%] w-full max-w-[25%] p-2'>Inspector</div> */}
              </div>
              <Divider />
              <div className="w-full min-h-[10rem] overflow-auto">
                {/* <div className='text-center flex'>
                  <div className='p-1 min-w-[10rem]'>
                    Table
                  </div>
                  <div className='p-1 min-w-[10rem]'>
                    DAG
                  </div>                
                  <div className='p-1 w-full'>
                    ide preview tabs
                  </div>
                </div> */}
                {/* <Divider /> */}
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
        <div className="min-w-[3.5rem] overflow-hidden py-2">
          <ul className="flex flex-col items-center">
            <li className="prose text-secondary-500 cursor-pointer text-center w-[2.5rem] h-[2.5rem] rounded-lg bg-secondary-100 flex justify-center items-center mb-2">
              <small>DAG</small>
            </li>
            <li className="prose text-secondary-500 cursor-pointer text-center w-[2.5rem] h-[2.5rem] rounded-lg bg-secondary-100 flex justify-center items-center mb-2">
              <small>QA</small>
            </li>
          </ul>
        </div>
      </div>
      <Divider />
      <div className="p-1">ide footer</div>
      <Transition appear show={isOpenModalPlan} as={Fragment}>
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
                  <Plan onCancel={setIsOpenModalPlan} />
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
