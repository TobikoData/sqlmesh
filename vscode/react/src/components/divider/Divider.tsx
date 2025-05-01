import clsx from 'clsx'
import { EnumSize } from '@/style/variants'

interface PropsDivider extends React.HTMLAttributes<HTMLSpanElement> {
  size?: typeof EnumSize.sm | typeof EnumSize.md | typeof EnumSize.lg
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
}: PropsDivider): JSX.Element {
  return (
    <span
      className={clsx(
        [
          'block border-divider',
          orientation === 'horizontal' &&
            `w-full border-b${SIZE.get(size) ?? ''}`,
          orientation === 'vertical' &&
            `h-full  border-r${SIZE.get(size) ?? ''}`,
        ],
        className,
      )}
    ></span>
  )
}
