import Split, { type SplitProps } from 'react-split'
import './SplitPane.css'

export default function SplitPane({
  className,
  children,
  sizes,
  minSize,
  maxSize,
  direction,
  expandToMin = false,
  snapOffset,
  onDragEnd,
}: SplitProps): JSX.Element {
  return (
    <Split
      className={className}
      sizes={sizes}
      expandToMin={expandToMin}
      gutterAlign="center"
      gutterSize={2}
      direction={direction}
      minSize={minSize}
      maxSize={maxSize}
      snapOffset={snapOffset}
      onDragEnd={onDragEnd}
    >
      {children}
    </Split>
  )
}
