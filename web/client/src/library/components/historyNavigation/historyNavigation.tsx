import { useNavigate } from 'react-router'
import { ChevronRightIcon, ChevronLeftIcon } from '@heroicons/react/20/solid'

export default function HistoryNavigation(): JSX.Element {
  const navigate = useNavigate()

  return (
    <div className="flex">
      <button
        onClick={() => navigate(-1)}
        className="inline-block text-neutral-700 dark:text-neutral-300 text-xs hover:bg-neutral-5 dark:hover:bg-neutral-10 px-1 py-1 rounded-full"
      >
        <ChevronLeftIcon className="min-w-4 h-4" />
      </button>
      <button
        onClick={() => navigate(1)}
        className="inline-block text-neutral-700 dark:text-neutral-300 text-xs hover:bg-neutral-5 dark:hover:bg-neutral-10 px-1 py-1 rounded-full"
      >
        <ChevronRightIcon className="min-w-4 h-4" />
      </button>
    </div>
  )
}
