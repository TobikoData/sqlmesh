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
import { Transition, Dialog } from '@headlessui/react'
import {
  useApiFileByPath,
  useMutationApiSaveFile,
  useApiFiles,
} from '../../../api'
import type { File } from '../../../api/client'
import { useQueryClient } from '@tanstack/react-query'
import { Plan } from '../plan/Plan'

export function IDE() {
  const client = useQueryClient()

  const [openedFiles, setOpenedFiles] = useState<Set<File>>(new Set())
  const [activeFile, setActiveFile] = useState<File | null>(null)
  const [fileContent, setFileContent] = useState<string>('')
  const [isOpenModalPlan, setIsOpenModalPlan] = useState(false)

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
          <Button variant='primary' size={EnumSize.sm} onClick={() => setIsOpenModalPlan(true)} className='w-[6rem] justify-between'>
            <span className="inline-block mr-3 min-w-20">Run Plan</span>
            <PlayIcon className="w-4 h-4 text-inherit" />
          </Button>
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
                  <Plan onClose={() => setIsOpenModalPlan(false)} />
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
