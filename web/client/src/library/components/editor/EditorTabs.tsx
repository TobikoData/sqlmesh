import { useMemo, type MouseEvent } from 'react'
import { PlusIcon, XCircleIcon } from '@heroicons/react/24/solid'
import { EnumSize, EnumVariant } from '~/types/enum'
import { Button } from '../button/Button'
import clsx from 'clsx'
import { isFalse } from '~/utils'
import { type EditorTab, useStoreEditor } from '~/context/editor'

export default function EditorTabs(): JSX.Element {
  const tabs = useStoreEditor(s => s.tabs)
  const selectTab = useStoreEditor(s => s.selectTab)
  const createTab = useStoreEditor(s => s.createTab)

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

  function addNewFileAndSelect(): void {
    selectTab(createTab())
  }

  return (
    <div className="flex items-center">
      <Button
        className="m-0 ml-1 mr-3 bg-primary-10  hover:bg-secondary-10 active:bg-secondary-10 border-none"
        variant={EnumVariant.Alternative}
        size={EnumSize.sm}
        onClick={(e: MouseEvent) => {
          e.stopPropagation()

          addNewFileAndSelect()
        }}
      >
        <PlusIcon className="inline-block w-3 h-4 text-secondary-500 dark:text-primary-500" />
      </Button>
      <ul className="w-full whitespace-nowrap min-h-[2rem] max-h-[2rem] overflow-hidden overflow-x-auto scrollbar scrollbar--horizontal">
        {tabsLocal.map((tab, idx) => (
          <Tab
            key={tab.file.id}
            tab={tab}
            isClosable={isFalse(tab.isInitial)}
            title={`Untitled-${idx + 1}`}
          />
        ))}
        {tabsRemote.map(tab => (
          <Tab
            key={tab.file.id}
            tab={tab}
            title={tab.file.name}
          />
        ))}
      </ul>
    </div>
  )
}

function Tab({
  tab,
  title,
  isClosable = true,
}: {
  tab: EditorTab
  title: string
  isClosable?: boolean
}): JSX.Element {
  const activeTab = useStoreEditor(s => s.tab)
  const selectTab = useStoreEditor(s => s.selectTab)
  const closeTab = useStoreEditor(s => s.closeTab)

  function closeEditorTab(tab: EditorTab): void {
    closeTab(tab.file.id)
  }

  return (
    <li
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
          tab.file.id === activeTab.file.id
            ? 'bg-neutral-200 border-neutral-200 text-neutral-900 dark:bg-dark-lighter dark:border-dark-lighter dark:text-primary-500'
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
        {isClosable && (
          <XCircleIcon
            className="hidden group-hover:inline-block text-neutral-600 dark:text-neutral-100 w-4 h-4 ml-2 cursor-pointer"
            onClick={(e: MouseEvent) => {
              e.stopPropagation()

              closeEditorTab(tab)
            }}
          />
        )}
      </span>
    </li>
  )
}
