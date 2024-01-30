import { useState } from 'react'
import clsx from 'clsx'
import { MagnifyingGlassIcon, XCircleIcon } from '@heroicons/react/24/solid'
import SearchList from '@components/search/SearchList'
import { EnumSize, EnumVariant } from '~/types/enum'

import { Button, EnumButtonShape } from '../button/Button'

interface ModelLineageSearchProps {
  currentModels: Array<{ name: string; description: string }>
  handleSelect: ({
    name,
    description,
  }: {
    name: string
    description: string
  }) => void
}

export default function ModelLineageSearch({
  currentModels,
  handleSelect,
}: ModelLineageSearchProps): JSX.Element {
  const [showSearchInput, setShowSearchInput] = useState(false)

  function showSearch(): void {
    // TODO: accessibility- also focus the search input after it is shown
    setShowSearchInput(true)
  }

  function hideSearch(): void {
    // TODO: accessibility- refocus the "open search" button after it reappears
    setShowSearchInput(false)
  }

  return (
    <div
      className={clsx(
        'w-full',
        showSearchInput
          ? 'block absolute top-0 left-0 right-0 z-10 pr-10 bg-light dark:bg-dark @[40rem]:items-end @[40rem]:justify-end @[40rem]:flex @[40rem]:static @[40rem]:pr-0'
          : 'items-end justify-end flex',
      )}
    >
      <Button
        shape={EnumButtonShape.Circle}
        className={clsx(
          'flex @[40rem]:hidden !py-1 border-transparent',
          showSearchInput ? 'hidden' : 'flex',
        )}
        variant={EnumVariant.Alternative}
        size={EnumSize.sm}
        aria-label="Show search"
        onClick={showSearch}
      >
        <MagnifyingGlassIcon className="w-3 h-3 text-primary-500" />
      </Button>
      <SearchList<{ name: string; description: string }>
        list={currentModels}
        placeholder="Find"
        searchBy="displayName"
        displayBy="displayName"
        direction="top"
        descriptionBy="description"
        showIndex={false}
        size={EnumSize.sm}
        onSelect={handleSelect}
        className={clsx(
          'w-full @sm:min-w-[12rem] @[40rem]:flex',
          showSearchInput ? 'flex max-w-none' : 'hidden max-w-[20rem]',
        )}
        isFullWidth={true}
      />
      <button
        className={clsx(
          'flex @[40rem]:hidden bg-none border-none px-2 py-1 absolute right-0 top-0',
          showSearchInput ? 'flex' : 'hidden',
        )}
        aria-label="Hide search"
        onClick={hideSearch}
      >
        <XCircleIcon className="w-6 h-6 text-primary-500" />
      </button>
    </div>
  )
}
