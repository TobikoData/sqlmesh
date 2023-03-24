import { Switch } from '@headlessui/react'
import clsx from 'clsx'

interface PropsToggle extends React.HTMLAttributes<HTMLElement> {
  enabled: boolean
  setEnabled: (enabled: boolean) => void
  a11yTitle?: string
  disabled?: boolean
}

export default function Toggle({
  enabled,
  setEnabled,
  a11yTitle,
  className,
  disabled = false,
}: PropsToggle): JSX.Element {
  return (
    <Switch
      checked={disabled ? false : enabled}
      onChange={setEnabled}
      className={clsx(
        'relative inline-flex h-8 w-16 shrink-0 rounded-full border-2 transition-colors duration-200 ease-in-out focus:outline-none focus:ring-4 ring-secondary-300 ring-opacity-60 ring-offset ring-offset-secondary-100 focus:border-secondary-500 focus-visible:ring-opacity-75',
        'border-secondary-30',
        enabled ? 'bg-secondary-500' : 'bg-secondary-10',
        className,
        disabled ? 'opacity-50 cursor-not-allowed' : 'cursor-pointer',
      )}
      disabled={disabled}
    >
      <span className="sr-only">{a11yTitle}</span>
      <span
        aria-hidden="true"
        className={clsx(
          'pointer-events-none inline-block h-6 w-6 transform rounded-full shadow-md  transition duration-200 ease-in-out',
          'bg-light translate-y-[0.125rem]',
          enabled
            ? 'translate-x-8 shadow-primary-800'
            : 'translate-x-1 shadow-neutral-300 dark:shadow-neutral-600',
        )}
      />
    </Switch>
  )
}
