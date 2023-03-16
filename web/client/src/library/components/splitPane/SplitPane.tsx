import Split, { type SplitProps } from 'react-split'
import './SplitPane.css'

export default function SplitPane({
  className,
  children,
  sizes,
  minSize,
  maxSize,
  direction,
  expandToMin,
  snapOffset,
}: SplitProps): JSX.Element {
  return (
    <Split
      className={className}
      sizes={sizes}
      expandToMin={expandToMin}
      gutterAlign="center"
      gutterSize={3}
      direction={direction}
      minSize={minSize}
      maxSize={maxSize}
      snapOffset={snapOffset}
    >
      {children}
    </Split>
  )
}
