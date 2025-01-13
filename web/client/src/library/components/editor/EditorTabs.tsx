import { useMemo, type MouseEvent, useEffect } from 'react'
import { PlusIcon } from '@heroicons/react/24/solid'
import { EnumSize, EnumVariant } from '~/types/enum'
import { Button } from '../button/Button'
import { type EditorTab, useStoreEditor } from '~/context/editor'
import { useStoreProject } from '@context/project'
import { isFalse, isNil, isNotNil } from '@utils/index'
import { useStoreContext } from '@context/context'
import { ModelDirectory } from '@models/directory'
import { ModelFile } from '@models/file'
import Tab from './EditorTab'

export default function EditorTabs(): JSX.Element {
  const modules = useStoreContext(s => s.modules)
  const models = useStoreContext(s => s.models)
  const lastSelectedModel = useStoreContext(s => s.lastSelectedModel)
  const setLastSelectedModel = useStoreContext(s => s.setLastSelectedModel)

  const files = useStoreProject(s => s.files)
  const selectedFile = useStoreProject(s => s.selectedFile)
  const setSelectedFile = useStoreProject(s => s.setSelectedFile)

  const tab = useStoreEditor(s => s.tab)
  const tabs = useStoreEditor(s => s.tabs)
  const replaceTab = useStoreEditor(s => s.replaceTab)
  const createTab = useStoreEditor(s => s.createTab)
  const selectTab = useStoreEditor(s => s.selectTab)
  const addTab = useStoreEditor(s => s.addTab)

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
      isNil(selectedFile) ||
      tab?.file === selectedFile ||
      selectedFile instanceof ModelDirectory ||
      isFalse(modules.hasFiles)
    )
      return

    setLastSelectedModel(models.get(selectedFile.path))

    const newTab = createTab(selectedFile)
    const shouldReplaceTab =
      isNotNil(tab) &&
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
  }, [selectedFile, modules])

  useEffect(() => {
    if (isNil(lastSelectedModel)) return

    const file = files.get(lastSelectedModel.path)

    if (isNil(file)) return

    setSelectedFile(file)
  }, [lastSelectedModel])

  return (
    <div className="flex items-center bg-neutral-5">
      <Button
        className="h-6 m-0 ml-1 mr-2 border-none"
        variant={EnumVariant.Alternative}
        size={EnumSize.sm}
        onClick={handleAddTab}
      >
        <PlusIcon className="inline-block w-3 h-4" />
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

  function handleAddTab(e: MouseEvent): void {
    e.stopPropagation()

    addTabAndSelect()
  }

  function addTabAndSelect(): void {
    const tab = createTab()

    addTab(tab)
    selectTab(tab)
    setSelectedFile(tab.file)
  }
}
