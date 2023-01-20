import { ViewUpdate } from "@codemirror/view";
import ContextIDE from "../../../context/Ide";
import { Button } from "../button/Button";
import { Divider } from "../divider/Divider";
import { DropdownPlan, DropdownAudits } from "../dropdown/Dropdown";
import { Editor } from "../editor/Editor";
import { FolderTree } from "../folderTree/FolderTree";
import Tabs from "../tabs/Tabs";
import { Fragment, useEffect, useState } from "react";
import clsx from "clsx";
import { XCircleIcon, CheckCircleIcon } from "@heroicons/react/24/solid";
import { EnumSize } from "../../../types/enum";
import { Transition, Dialog, RadioGroup } from "@headlessui/react";
import { useApiFileByPath, useMutationApiSaveFile, type File, useApiFiles } from '../../../api';
import { useQueryClient } from "@tanstack/react-query";

const plans = [
  {
    title: 'Breaking Change',
    description: 'This is a breaking change',
  },
  {
    title: 'Non-Breaking Change',
    description: 'This is a non-breaking change',
  },
  {
    title: 'No Change',
    description: 'This is a no change',
  },
]

export function IDE() {
  const client = useQueryClient();
  
  const [files, setFiles] = useState<Set<File>>(new Set())
  const [file, setFile] = useState<File>()
  const [fileContent, setFileContent] = useState<string>('')
  const [isOpenModalPlan, setIsOpenModalPlan] = useState(false)
  const [activePlan, setActivePlan] = useState<{ text: string, value: string }>()
  const [selected, setSelected] = useState(plans[0])
  const [status, setStatus] = useState('editing')

  const mutationSaveFile = useMutationApiSaveFile(client)
  const { data: project } = useApiFiles();
  const { data: fileData } = useApiFileByPath(client, file?.path)

  useEffect(() => {
    setFileContent(fileData?.content ?? '')
  }, [fileData])

  function closeIdeTab(f: File) {
    if (!f) return

    files.delete(f)

    if (files.size === 0) {
      setFile(undefined)
    } else if (!file || !files.has(file)) {
      setFile([...files][0])
    }

    setFiles(new Set([...files]))
  }

  return (
    <ContextIDE.Provider value={{
      files,
      file,
      setFile: file => {
        if (!file) return setFile(undefined)

        setFile(file)
        setFiles(new Set([...files, file]))
      },
      setFiles,
    }}>
      <div className='w-full flex justify-between items-center min-h-[2rem] z-50'>
        
        {/* Project Name */}
        <div className="px-3 flex">
          <p className="mr-1">Project:</p>
          <h3 className='font-bold'>{project?.name}</h3>
        </div>
        

        {/* Git */}
        {/* <div className='px-4 flex'>
          <div className='px-4'>TobikoData/wursthall</div>
          <div className='px-4'>main</div>
        </div> */}

        {/* Search */}
        {/* <div className='px-4 w-full'>search</div> */}

        <div className='px-4 flex items-center'>
          <DropdownPlan onSelect={item => {
            setIsOpenModalPlan(true)
            setActivePlan(item)
          }} />
          <DropdownAudits />
          <Button size={EnumSize.sm} variant='alternative'>
            Run Tests
          </Button>        
          
        </div>
      </div>
      <Divider />
      <div className='flex w-full h-full overflow-hidden'>
        <div className='min-w-[12rem] w-full max-w-[16rem] overflow-hidden overflow-y-auto'>
          <FolderTree project={project} />
        </div>
        <Divider orientation='vertical' />
        <div className='h-full w-full flex flex-col overflow-hidden'>

          {/* Breadcrubms */}
          {/* <div className='w-full overflow-hidden px-2 min-h-[1.5rem] flex items-center'>
            <small className='inline-block cursor-pointer hover:text-gray-300'>audit</small>
            <small className='inline-block px-2'>/</small>
            <small className='inline-block cursor-pointer hover:text-gray-300'>items.sql</small>
          </div>
          <Divider /> */}

          {Boolean(file) && (
            <>
              <div className='w-full h-full flex overflow-hidden'>
                <div className='w-full flex flex-col overflow-hidden overflow-x-auto'>
                  <div className='w-full flex min-h-[2rem] overflow-hidden overflow-x-auto'> 
                    <ul className='w-full whitespace-nowrap'>
                      {files.size > 0 && [...files].map((f) => (
                        <li key={f.name} className={clsx(
                          'inline-block justify-between items-center py-1 px-3 overflow-hidden min-w-[10rem] text-center overflow-ellipsis cursor-pointer',
                          f.path === file?.path ? 'bg-white' : 'bg-gray-300'
                        )} onClick={() =>  setFile(f)}>
                          <span className='flex justify-between items-center'>
                            <small>{f.name}</small>
                            <XCircleIcon
                              onClick={e => {
                                e.stopPropagation()

                                closeIdeTab(f)
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
                  <div className='w-full h-full flex flex-col overflow-hidden'>
                    <div className='w-full h-full overflow-hidden'>
                      <Editor
                        className='h-full w-full'
                        extension={file?.extension}
                        value={fileContent}
                        onChange={debounce((value: string, viewUpdate: ViewUpdate) => {
                          const shouldMutate = Boolean(value) && value !== fileContent

                          if (shouldMutate) {
                            mutationSaveFile.mutate({
                              path: file?.path,
                              body: viewUpdate.state.doc.toString()
                            })

                            setStatus('saved')
                          } else {
                            setStatus('editing')
                          }
                        }, () => {
                          setStatus('savind')
                        }, 2000)}
                      />
                    </div>

                  </div>
                  <Divider />
                  <div className='px-2 py-1 flex justify-between min-h-[1.5rem]'>
                    <small>
                      validation: ok
                    </small>
                    <small>
                      File Status: {status}
                    </small>
                    <div className='flex'>
                        <Button size={EnumSize.sm} variant='secondary'>Run Query</Button>
                        <Button size={EnumSize.sm} variant='alternative'>Validate</Button>
                        <Button size={EnumSize.sm} variant='alternative'>Format</Button>
                        <Button size={EnumSize.sm} variant='success'>Save</Button>
                      </div>
                  </div>           
                </div>
                                      
                {/* <Divider orientation='vertical' /> */}
                {/* <div className='h-full min-w-[15%] w-full max-w-[25%] p-2'>Inspector</div> */}
              </div>
              <Divider />
              <div className='w-full min-h-[10rem] overflow-auto'>
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

          {!Boolean(file) && (
            <div className='w-full h-full flex justify-center items-center text-center'>
              <div className="prose">
                <h2>Instractions on how to start</h2>
                <p>
                  Select file
                </p>
              </div>
            </div>
          )}


        </div>
        <Divider orientation='vertical' />
        <div className='min-w-[3.5rem] overflow-hidden py-2'>
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
      <div className='p-1'>ide footer</div>
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
                <Dialog.Panel className="w-full transform overflow-hidden rounded-2xl bg-white p-6 text-left align-middle shadow-xl transition-all">
                  <Dialog.Title
                    as="h3"
                    className="text-lg font-medium leading-6 text-gray-900"
                  >
                    {activePlan?.text}
                  </Dialog.Title>
                  <div className="mt-2">
                    <h3>Dependencies</h3>
                    <div className="flex justify-between">
                      <div>
                        <p>Directly Modified:</p>
                        <ul>
                          <li>Model A</li>
                          <li>Model B</li>
                        </ul>
                      </div>
                      <div>
                        <p>Indirectly Modified:</p>
                        <ul>
                          <li>Model C</li>
                          <li>Model D</li>
                        </ul>                      
                      </div>
                    </div>
                  </div>
                  <Divider />
                  <div className="mt-2">
                    <h3>Diff</h3>
                  </div>
                  <Divider />
                  <div className="mt-2 bg-gray-600 p-4">
                    <h3 className="mb-2 text-gray-100">Change</h3>
                    <RadioGroup value={selected} onChange={setSelected}>
                      <RadioGroup.Label className="sr-only">Server size</RadioGroup.Label>
                      <div className="space-y-2">
                        {plans.map((plan) => (
                          <RadioGroup.Option
                            key={plan.title}
                            value={plan}
                            className={({ active, checked }) =>
                              `${
                                active
                                  ? 'ring-2 ring-white ring-opacity-60 ring-offset-2 ring-offset-sky-300'
                                  : ''
                              }
                              ${
                                checked ? 'bg-sky-900 bg-opacity-75 text-white' : 'bg-white'
                              }
                                relative flex cursor-pointer rounded-lg px-5 py-4 shadow-md focus:outline-none`
                            }
                          >
                            {({ active, checked }) => (
                              <>
                                <div className="flex w-full items-center justify-between">
                                  <div className="flex items-center">
                                    <div className="text-sm">
                                      <RadioGroup.Label
                                        as="p"
                                        className={`font-medium  ${
                                          checked ? 'text-white' : 'text-gray-900'
                                        }`}
                                      >
                                        {plan.title}
                                      </RadioGroup.Label>
                                      <RadioGroup.Description
                                        as="span"
                                        className={`inline ${
                                          checked ? 'text-sky-100' : 'text-gray-500'
                                        }`}
                                      >
                                        <span>
                                          {plan.description}
                                        </span>
                                      </RadioGroup.Description>
                                    </div>
                                  </div>
                                  {checked && (
                                    <div className="shrink-0 text-white">
                                      <CheckCircleIcon className="h-6 w-6" />
                                    </div>
                                  )}
                                </div>
                              </>
                            )}
                          </RadioGroup.Option>
                        ))}
                      </div>
                    </RadioGroup>
                  </div>
                  <Divider />
                  <div>
                    <h3>Dates</h3>
                    <div className="flex items-center">
                      <label htmlFor="">
                        Start Date
                        <input className="block" type="date" />
                      </label>
                      <label htmlFor="">
                        End Date
                        <input className="block" type="date" />
                      </label> 
                    </div>
                    {/* <DateRangePicker label={'Set period'} /> */}
                  </div>          
                  <Divider />
                  <div>
                    <h3>Actions</h3>
                    <div className="flex justify-end">
                      <Button>
                        Apply
                      </Button>
                      <Button onClick={() => setIsOpenModalPlan(false)} variant='alternative'>
                        Cancel
                      </Button>
                    </div>
                  </div>                  
                </Dialog.Panel>
              </Transition.Child>
            </div>
          </div>
        </Dialog>
      </Transition>
    </ContextIDE.Provider>
  )
}

function debounce(fn: (...args: any) => void, before: () => void, delay: number = 500) {
  let timer: any;
  return function(...args: any) {
    clearTimeout(timer)

    before && before()

    timer = setTimeout(() => {
      fn(...args)
    }, delay);
  }
}