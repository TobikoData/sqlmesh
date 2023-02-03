import ContextIDE from '../../../context/Ide'
import { Button } from '../button/Button'
import { Divider } from '../divider/Divider'
import { Editor } from '../editor/Editor'
import { FolderTree } from '../folderTree/FolderTree'
import { Fragment, useEffect, useState } from 'react'
import clsx from 'clsx'
import { PlayIcon } from '@heroicons/react/24/solid'
import { EnumSize } from '../../../types/enum'
import { Transition, Dialog } from '@headlessui/react'
import { useApiFiles } from '../../../api'
import { Plan } from '../plan/Plan'
import Tabs from '../tabs/Tabs';
import { ModelFile } from '../../../models'

export function IDE() {
  const [isOpenModalPlan, setIsOpenModalPlan] = useState(false)
  const [openedFiles, setOpenedFiles] = useState<Set<ModelFile>>(new Set([new ModelFile()]))
  const [activeFile, setActiveFile] = useState<ModelFile | null>(null)

  const { data: project } = useApiFiles()

  useEffect(() => {
    if (activeFile === null) {
      setActiveFile([...openedFiles][0])
    }
  }, [openedFiles])

  return (
    <ContextIDE.Provider
      value={{
        openedFiles,
        activeFile,
        setActiveFile: (file) => {
          if (!file) return setActiveFile(null)

          openedFiles.add(file)

          setActiveFile(file)
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

        <div className="px-3 flex items-center min-w-[10rem] justify-end">
          <Button
            disabled={planAction !== EnumPlanAction.None || planState === EnumPlanState.Applying || planState === EnumPlanState.Canceling}
            variant='primary'
            size={EnumSize.sm}
            onClick={e => {
              e.stopPropagation()

              startPlan()
            }}
            className='min-w-[6rem] justify-between'
          >
            {planState === EnumPlanState.Applying || planState === EnumPlanState.Canceling && <Spinner className='w-3 h-3 mr-1' />}
            <span className="inline-block mr-3 min-w-20">
              {planState === EnumPlanState.Applying
                ? 'Applying Plan...'
                : planState === EnumPlanState.Canceling
                  ? 'Canceling Plan...'
                  : planAction !== EnumPlanAction.None
                    ? 'Setting Plan...'
                    : 'Run Plan'
              }
            </span>
            <PlayIcon className="w-[1rem] h-[1rem] text-inherit" />
          </Button>
        </div>
      </div>
      <Divider />
      <div className="flex w-full h-full overflow-hidden">
        <div className="w-[16rem] overflow-hidden overflow-y-auto">
          <FolderTree project={project} />
        </div>
        <Divider orientation="vertical" />
        <div className={clsx('h-full w-full flex flex-col overflow-hidden')}>
          {Boolean(activeFile) && (
            <>
              <div className="w-full h-full flex overflow-hidden">
                <Editor />
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
      </div>
      <Divider />
      <div className="px-2 text-xs py-1">ide footer</div>
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


