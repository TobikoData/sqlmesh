import Split, { type SplitProps } from 'react-split'
import './SplitPane.css'
import { useRef } from 'react'

export default function SplitPane({
  className,
  children,
  sizes,
  minSize,
  maxSize,
  direction,
  expandToMin = false,
  snapOffset,
  cursor,
  handleDrag,
  onDragEnd,
}: SplitProps & {
  className?: string
  children: React.ReactNode
  handleDrag?: (sizes: number[], elSplit?: Maybe<HTMLElement & any>) => void
  onDragEnd?: (sizes: number[], elSplit?: Maybe<HTMLElement & any>) => void
}): JSX.Element {
  const elSplit = useRef(null)
  return (
    <Split
      ref={elSplit}
      className={className}
      sizes={sizes}
      cursor={cursor}
      expandToMin={expandToMin}
      gutterAlign="center"
      gutterSize={2}
      direction={direction}
      minSize={minSize}
      maxSize={maxSize}
      snapOffset={snapOffset}
      onDrag={sizes => handleDrag?.(sizes, elSplit.current)}
      onDragEnd={sizes => onDragEnd?.(sizes, elSplit.current)}
    >
      {children}
    </Split>
  )
}
