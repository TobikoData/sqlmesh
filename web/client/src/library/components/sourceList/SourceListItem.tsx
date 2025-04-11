import { isNotNil } from '@utils/index'
import clsx from 'clsx'
import { NavLink } from 'react-router-dom'
import { EnumVariant, type Variant } from '~/types/enum'

export default function SourceListItem({
  name,
  description,
  to,
  text,
  variant,
  disabled = false,
  handleDelete,
}: {
  name: string
  description?: string
  to: string
  variant?: Variant
  disabled?: boolean
  text?: string
  handleDelete?: () => void
}): JSX.Element {
  function handleKeyUp(e: React.KeyboardEvent<HTMLAnchorElement>): void {
    if (e.key === 'Delete' || e.key === 'Backspace') {
      e.preventDefault()
      e.stopPropagation()

      handleDelete?.()
    }
  }

  return (
    <NavLink
      onKeyUp={handleKeyUp}
      to={to}
      className={({ isActive }) =>
        clsx(
          'block overflow-hidden px-2 py-1.5 rounded-md w-full font-semibold',
          disabled && 'opacity-50 pointer-events-none',
          isActive
            ? variant === EnumVariant.Primary
              ? 'text-primary-500 bg-primary-10'
              : variant === EnumVariant.Danger
              ? 'text-danger-500 bg-danger-5'
              : 'text-neutral-600 dark:text-neutral-100 bg-neutral-10'
            : 'hover:bg-neutral-5 text-neutral-500 dark:text-neutral-400',
        )
      }
    >
      <div className="flex items-center">
        <span className="whitespace-nowrap overflow-ellipsis overflow-hidden min-w-10">
          {name}
        </span>
        {isNotNil(text) && (
          <span className=" ml-2 px-2 rounded-md leading-0 text-[0.5rem] bg-neutral-10 text-neutral-700 dark:text-neutral-200">
            {text}
          </span>
        )}
      </div>
      {isNotNil(description) && (
        <p className="text-xs overflow-hidden whitespace-nowrap overflow-ellipsis text-neutral-300 dark:text-neutral-500">
          {description}
        </p>
      )}
    </NavLink>
  )
}
