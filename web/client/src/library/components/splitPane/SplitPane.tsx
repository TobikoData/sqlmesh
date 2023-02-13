import Split, { SplitProps } from 'react-split'
import './SplitPane.css'

export function SplitPane({
  className,
  children,
  sizes,
  direction,
}: SplitProps): JSX.Element {
  return (
    <Split
      className={className}
      sizes={sizes}
      expandToMin={false}
      gutterAlign="center"
      gutterSize={5}
      direction={direction}
    >
      {children}
    </Split>
  )
}
