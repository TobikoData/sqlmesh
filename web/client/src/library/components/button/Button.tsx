import clsx from "clsx"

export type ButtonVariant = 'primary' | 'secondary' | 'success' | 'danger' | 'warning' | 'alternative';
export type ButtonSize = 'small' | 'medium' | 'large';
export type ButtonShape = "square" | "rounded" | "circle" | "pill";

export interface PropsButton extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: ButtonVariant
  size?: ButtonSize
  shape?: ButtonShape
  value?: string
  onClick?: (e: React.MouseEvent<HTMLButtonElement>) => void
}

const VARIANT = new Map([
  ['primary', 'bg-primary-500 hover:bg-primary-600 active:bg-primary-400 text-gray-100'],
  ['secondary', 'bg-secondary-500 hover:bg-secondary-600 active:bg-secondary-400 text-gray-100'],
  ['success', 'bg-success-500 hover:bg-success-600 active:bg-success-400 text-gray-100'],
  ['danger', 'bg-danger-500 hover:bg-danger-600 active:bg-danger-400 text-gray-100'],
  ['warning', 'bg-warning-500 hover:bg-warning-600 active:bg-warning-400 text-gray-100'],
  ['alternative', 'bg-gray-500 hover:bg-gray-600 active:bg-gray-400 text-gray-100'],
]);

const SHAPE = new Map([
  ['rounded', `rounded-md`],
]);

const SIZE = new Map([
  ['small', `px-2 py-1 text-sm`],
  ['medium', `px-3 py-2 text-base`],
  ['large', `px-4 py-3 text-lg`],
]);

export function Button({ variant = 'secondary', shape = 'rounded', size = 'medium', children = [], onClick }: PropsButton) {
  return (
    <button 
      className={clsx(
        'flex m-1 items-center justify-center font-medium focus:outline-none focus:ring-2 focus:ring-blue-500',
        VARIANT.get(variant),
        SHAPE.get(shape),
        SIZE.get(size),
      )}
      onClick={onClick}
    >
      {children}
    </button>
  )
};