import clsx from "clsx"
import { EnumSize } from "../../../types/enum";
import { Menu } from '@headlessui/react'

export type ButtonVariant = 'primary' | 'secondary' | 'success' | 'danger' | 'warning' | 'alternative';
export type ButtonSize = Subset<Size, typeof EnumSize.sm | typeof EnumSize.md | typeof EnumSize.lg>;
export type ButtonShape = "square" | "rounded" | "circle" | "pill";

export interface PropsButton extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: ButtonVariant
  size?: ButtonSize
  shape?: ButtonShape
  value?: string
  form?: string
  onClick?: (e: React.MouseEvent<HTMLButtonElement>) => void
}

const VARIANT = new Map<ButtonVariant, string>([
  ['primary', 'bg-secondary-100 hover:bg-secondary-100 active:bg-secondary-100 text-secondary-600'],
  ['secondary', 'bg-secondary-500 hover:bg-secondary-600 active:bg-secondary-400 text-gray-100'],
  ['success', 'bg-success-500 hover:bg-success-600 active:bg-success-400 text-gray-100'],
  ['danger', 'bg-danger-500 hover:bg-danger-600 active:bg-danger-400 text-gray-100'],
  ['warning', 'bg-warning-500 hover:bg-warning-600 active:bg-warning-400 text-gray-100'],
  ['alternative', 'bg-gray-100 hover:bg-gray-200 active:bg-gray-200 text-gray-800'],
]);

const SHAPE = new Map<ButtonShape, string>([
  ['rounded', `rounded-md`],
]);

const SIZE = new Map<ButtonSize, string>([
  [EnumSize.sm, `px-2 py-1 text-xs`],
  [EnumSize.md, `px-3 py-2 text-base`],
  [EnumSize.lg, `px-4 py-3 text-lg`],
]);

export function Button({ variant = 'secondary', shape = 'rounded', size = EnumSize.md, children = [], form, onClick, className }: PropsButton) {
  return (
    <button
      form={form}
      className={clsx(
        'flex m-1 items-center justify-center font-medium focus:outline-none focus:ring-2 focus:ring-blue-500',
        VARIANT.get(variant),
        SHAPE.get(shape),
        SIZE.get(size),
        className
      )}
      onClick={onClick}
    >
      {children}
    </button>
  )
};

export function ButtonMenu({ variant = 'secondary', shape = 'rounded', size = EnumSize.md, children = [] }: PropsButton) {
  return (
    <Menu.Button
      className={clsx(
        'flex m-1 items-center justify-center font-medium focus:outline-none focus:ring-2 focus:ring-blue-500',
        VARIANT.get(variant),
        SHAPE.get(shape),
        SIZE.get(size),
      )}>
      {children}
    </Menu.Button>
  )
};