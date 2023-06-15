import { Switch } from '@headlessui/react'
import clsx from 'clsx'
import { EnumSize, type Size } from '~/types/enum'

export default function Toggle({
  label,
  enabled,
  setEnabled,
  a11yTitle,
  size = EnumSize.md,
  disabled = false,
  className,
}: {
  enabled: boolean
  setEnabled: (enabled: boolean) => void
  size?: Size
  label?: string
  a11yTitle?: string
  disabled?: boolean
  className?: string
}): JSX.Element {
  return (
    <Switch.Group
      as="div"
      className="flex items-center m-1"
    >
      <Switch
        checked={disabled ? false : enabled}
        onChange={setEnabled}
        className={clsx(
          'flex relative border-secondary-30 rounded-full m-0',
          'shrink-0 focus:outline-none ring-secondary-300 ring-opacity-60 ring-offset ring-offset-secondary-100 focus:border-secondary-500 focus-visible:ring-opacity-75',
          'transition duration-200 ease-in-out',
          enabled ? 'bg-secondary-500' : 'bg-secondary-20',
          className,
          disabled ? 'opacity-50 cursor-not-allowed' : 'cursor-pointer',
          size === EnumSize.sm && 'h-[14px] w-6 focus:ring-1 border',
          size === EnumSize.md && 'h-5 w-10 focus:ring-2 border-2',
          size === EnumSize.lg && 'h-7 w-14 focus:ring-4 border-2',
        )}
        disabled={disabled}
      >
        <span className="sr-only">{a11yTitle}</span>
        <span
          aria-hidden="true"
          className={clsx(
            'pointer-events-none inline-block transform rounded-full shadow-md transition duration-200 ease-in-out',
            'bg-light',
            size === EnumSize.sm && 'h-3 w-3',
            size === EnumSize.md && 'h-4 w-4',
            size === EnumSize.lg && 'h-6 w-6',
            enabled && size === EnumSize.sm && 'translate-x-[10px]',
            enabled && size === EnumSize.md && 'translate-x-5',
            enabled && size === EnumSize.lg && 'translate-x-7',
          )}
        />
      </Switch>
      {label != null && (
        <Switch.Label
          className={clsx(
            'text-xs font-light ml-1 text-neutral-600 dark:text-neutral-400',
          )}
        >
          {label}
        </Switch.Label>
      )}
    </Switch.Group>
  )
}
