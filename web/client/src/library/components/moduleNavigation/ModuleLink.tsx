import clsx from 'clsx'
import { NavLink } from 'react-router'
import { isFalse } from '@utils/index'

export default function ModuleLink({
  title,
  to,
  icon,
  iconActive,
  classActive = 'px-2 bg-neutral-10',
  disabled = false,
  children,
  before,
  className,
}: {
  title: string
  to: string
  icon: React.ReactNode
  iconActive: React.ReactNode
  classActive?: string
  children?: React.ReactNode
  before?: React.ReactNode
  disabled?: boolean
  className?: string
}): JSX.Element {
  return (
    <NavLink
      title={title}
      to={to}
      className={({ isActive }) =>
        clsx(
          'flex items-center',
          disabled && 'opacity-50 cursor-not-allowed',
          isActive && isFalse(disabled) && classActive,
          className,
        )
      }
      style={({ isActive }) =>
        isActive || disabled ? { pointerEvents: 'none' } : {}
      }
    >
      {({ isActive }) => (
        <span
          className={clsx(
            'flex items-center',
            isActive ? 'font-bold' : 'font-normal',
          )}
        >
          {before}
          {isActive ? iconActive : icon}
          {children}
        </span>
      )}
    </NavLink>
  )
}
