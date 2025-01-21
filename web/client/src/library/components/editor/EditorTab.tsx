import { type MouseEvent, useEffect, useRef } from 'react'
import { XCircleIcon } from '@heroicons/react/24/solid'
import clsx from 'clsx'
import { type EditorTab, useStoreEditor } from '~/context/editor'
import { useStoreProject } from '@context/project'
import { isNil } from '@utils/index'
import { useStoreContext } from '@context/context'

export default function Tab({
  tab,
  title,
}: {
  tab: EditorTab
  title: string
}): JSX.Element {
  const elTab = useRef<HTMLLIElement>(null)

  const addConfirmation = useStoreContext(s => s.addConfirmation)
  const setSelectedFile = useStoreProject(s => s.setSelectedFile)
  const closeTab = useStoreEditor(s => s.closeTab)
  const activeTab = useStoreEditor(s => s.tab)
  const isActive = tab.file.id === activeTab?.file.id

  useEffect(() => {
    if (isNil(tab)) return

    tab.el = elTab.current ?? undefined
  }, [elTab, tab])

  useEffect(() => {
    if (isNil(activeTab)) return

    setTimeout(() => {
      activeTab.el?.scrollIntoView({
        behavior: 'smooth',
        inline: 'center',
      })
    }, 300)
  }, [activeTab])

  return (
    <li
      ref={elTab}
      className={clsx(
        'inline-block py-1 pr-2 last-child:pr-0 overflow-hidden text-center overflow-ellipsis cursor-pointer',
      )}
      onClick={handleSelectTab}
    >
      <span
        className={clsx(
          'flex border-2 justify-between items-center pl-1 pr-1 py-[0.125rem] min-w-[8rem] rounded-md group border-transparent border-r border-r-theme-darker dark:border-r-theme-lighter',
          isActive
            ? 'bg-neutral-200 border-neutral-200 text-neutral-900 dark:bg-dark-lighter dark:border-dark-lighter dark:text-primary-500'
            : 'bg-trasparent hover:bg-theme-darker dark:hover:bg-theme-lighter',
        )}
      >
        <small className="text-xs">{title}</small>
        <small
          className={clsx(
            'group-hover:hidden text-xs inline-block ml-3 mr-1 w-2 h-2 rounded-full',
            tab.file.isChanged ? 'bg-warning-500' : 'bg-transparent',
          )}
        ></small>
        <XCircleIcon
          className="hidden group-hover:inline-block text-neutral-600 dark:text-neutral-100 w-4 h-4 ml-2 mr-0 cursor-pointer"
          onClick={handleCloseTab}
        />
      </span>
    </li>
  )

  function handleSelectTab(e: MouseEvent): void {
    e.stopPropagation()

    setSelectedFile(tab.file)
  }

  function handleCloseTab(e: MouseEvent): void {
    e.stopPropagation()

    closeEditorTabWithConfirmation()
  }

  function closeEditorTabWithConfirmation(): void {
    if (tab.file.isChanged) {
      addConfirmation({
        headline: 'Closing Tab',
        description:
          'All unsaved changes will be lost. Do you want to close the tab anyway?',
        yesText: 'Yes, Close Tab',
        noText: 'No, Cancel',
        action: () => closeTab(tab.file),
      })
    } else {
      closeTab(tab.file)
    }
  }
}
