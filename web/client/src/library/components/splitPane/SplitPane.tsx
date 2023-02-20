import Split, { SplitProps } from 'react-split'
import './SplitPane.css'

export default function SplitPane({
  className,
  children,
  sizes,
  minSize,
  maxSize,
  direction,
}: SplitProps): JSX.Element {
  return (
    <Split
      className={className}
      sizes={sizes}
      expandToMin={false}
      gutterAlign="center"
      gutterSize={3}
      direction={direction}
      minSize={minSize}
      maxSize={maxSize}
    >
      {children}
    </Split>
  )
}
