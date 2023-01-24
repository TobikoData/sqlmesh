import clsx from 'clsx'
import { EnumSize } from '../../../types/enum'

interface PropsDivider extends React.HTMLAttributes<HTMLSpanElement> {
  size?: Subset<
    Size,
    typeof EnumSize.sm | typeof EnumSize.md | typeof EnumSize.lg
  >
  orientation?: 'horizontal' | 'vertical'
}

export const SIZE = new Map([
  [EnumSize.sm, ''],
  [EnumSize.md, '-2'],
  [EnumSize.lg, '-4'],
])

export function Divider({
  size = EnumSize.sm,
  orientation = 'horizontal',
  className,
}: PropsDivider) {
  const clsOrientation =
    orientation === 'horizontal'
      ? `w-full border-b${SIZE.get(size)}`
      : `h-full border-r${SIZE.get(size)}`

  return (
    <span
      className={clsx(className, ['block', clsOrientation, 'border-gray-100'])}
    ></span>
  )
}
