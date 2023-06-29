import { useMemo, type MouseEvent, useEffect, useRef } from 'react'
import { PlusIcon, XCircleIcon } from '@heroicons/react/24/solid'
import { EnumSize, EnumVariant } from '~/types/enum'
import { Button } from '../button/Button'
import clsx from 'clsx'
import { type EditorTab, useStoreEditor } from '~/context/editor'
import { useStoreProject } from '@context/project'
import { ModelDirectory } from '@models/directory'
import { ModelFile } from '@models/file'
import { isFalse } from '@utils/index'

export default function EditorTabs(): JSX.Element {
  const selectedFile = useStoreProject(s => s.selectedFile)

  const tab = useStoreEditor(s => s.tab)
  const tabs = useStoreEditor(s => s.tabs)
  const addTab = useStoreEditor(s => s.addTab)
  const selectTab = useStoreEditor(s => s.selectTab)
  const createTab = useStoreEditor(s => s.createTab)
  const replaceTab = useStoreEditor(s => s.replaceTab)

  const [tabsLocal, tabsRemote] = useMemo(() => {
    const local: EditorTab[] = []
    const remote: EditorTab[] = []

    tabs.forEach(tab => {
      if (tab.file.isLocal) {
        local.push(tab)
      }

      if (tab.file.isRemote) {
        remote.push(tab)
      }
    })

    return [local, remote]
  }, [tabs])

  useEffect(() => {
    if (
      tab == null ||
      selectedFile == null ||
      tab?.file === selectedFile ||
      selectedFile instanceof ModelDirectory
    )
      return

    const newTab = createTab(selectedFile)
    const shouldReplaceTab =
      tab.file instanceof ModelFile &&
      isFalse(tab.file.isChanged) &&
      tab.file.isRemote &&
      isFalse(tabs.has(selectedFile))

    if (shouldReplaceTab) {
      replaceTab(tab, newTab)
    } else {
      addTab(newTab)
    }

    selectTab(newTab)
  }, [selectedFile])

  useEffect(() => {
    setTimeout(() => {
      tab?.el?.scrollIntoView({
        behavior: 'smooth',
        inline: 'center',
      })
    }, 100)
  }, [tab?.id])

  function addTabAndSelect(): void {
    const tab = createTab()

    addTab(tab)
    selectTab(tab)
  }

  return (
    <div className="flex items-center">
      <Button
        className="h-6 m-0 ml-1 mr-3 bg-primary-10  hover:bg-secondary-10 active:bg-secondary-10 border-none"
        variant={EnumVariant.Alternative}
        size={EnumSize.sm}
        onClick={(e: MouseEvent) => {
          e.stopPropagation()

          addTabAndSelect()
        }}
      >
        <PlusIcon className="inline-block w-3 h-4 text-secondary-500 dark:text-primary-500" />
      </Button>
      <ul className="w-full whitespace-nowrap min-h-[2rem] max-h-[2rem] overflow-hidden overflow-x-auto hover:scrollbar scrollbar--horizontal">
        {tabsLocal.map((tab, idx) => (
          <Tab
            key={tab.id}
            tab={tab}
            title={`Custom SQL ${idx + 1}`}
          />
        ))}
        {tabsRemote.map(tab => (
          <Tab
            key={tab.id}
            tab={tab}
            title={tab.file.name}
          />
        ))}
      </ul>
    </div>
  )
}

function Tab({ tab, title }: { tab: EditorTab; title: string }): JSX.Element {
  const elTab = useRef<HTMLLIElement>(null)

  const activeTab = useStoreEditor(s => s.tab)
  const selectTab = useStoreEditor(s => s.selectTab)
  const closeTab = useStoreEditor(s => s.closeTab)

  useEffect(() => {
    if (elTab.current == null) return

    tab.el = elTab.current
  }, [elTab])

  function closeEditorTab(tab: EditorTab): void {
    closeTab(tab.file)
  }

  return (
    <li
      ref={elTab}
      className={clsx(
        'inline-block py-1 pr-2 last-child:pr-0 overflow-hidden text-center overflow-ellipsis cursor-pointer',
      )}
      onClick={(e: MouseEvent) => {
        e.stopPropagation()

        selectTab(tab)
      }}
    >
      <span
        className={clsx(
          'flex border-2 justify-between items-center pl-2 pr-1 py-[0.125rem] min-w-[8rem] rounded-md group border-transparent border-r border-r-theme-darker dark:border-r-theme-lighter',
          tab.file.id === activeTab?.file.id
            ? 'bg-neutral-200 border-neutral-200 text-neutral-600 dark:bg-dark-lighter dark:border-dark-lighter dark:text-primary-500'
            : 'bg-trasparent hover:bg-theme-darker dark:hover:bg-theme-lighter',
        )}
      >
        <small className="text-xs">{title}</small>
        <small
          className={clsx(
            'group-hover:hidden text-xs inline-block mx-2 w-2 h-2  rounded-full',
            tab.file.isChanged ? 'bg-warning-500' : 'bg-transparent',
          )}
        ></small>
        <XCircleIcon
          className="hidden group-hover:inline-block text-neutral-600 dark:text-neutral-100 w-4 h-4 ml-2 cursor-pointer"
          onClick={(e: MouseEvent) => {
            e.stopPropagation()

            closeEditorTab(tab)
          }}
        />
      </span>
    </li>
  )
}
