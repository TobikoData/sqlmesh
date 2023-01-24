import React, { MouseEventHandler, useEffect, useRef, useState } from 'react'
import SplitPaneContext from '../../../context/SplitPaneContext'

export function SplitPane({
  children,
  ...props
}: {
  children: React.ReactNode
}) {
  const [clientHeight, setClientHeight] = useState(0)
  const [clientWidth, setClientWidth] = useState(0)
  const yDividerPos = useRef(0)
  const xDividerPos = useRef(0)

  const onMouseDown: MouseEventHandler = (e: React.MouseEvent) => {
    yDividerPos.current = e.clientY
    xDividerPos.current = e.clientX
  }

  const onMouseUp = () => {
    yDividerPos.current = 0
    xDividerPos.current = 0
  }

  const onMouseMove = (e: MouseEvent) => {
    if (!yDividerPos.current && !xDividerPos.current) return

    setClientHeight(clientHeight + e.clientY - yDividerPos.current)
    setClientWidth(clientWidth + e.clientX - xDividerPos.current)

    yDividerPos.current = e.clientY
    xDividerPos.current = e.clientX
  }

  useEffect(() => {
    document.addEventListener('mouseup', onMouseUp)
    document.addEventListener('mousemove', onMouseMove)

    return () => {
      document.removeEventListener('mouseup', onMouseUp)
      document.removeEventListener('mousemove', onMouseMove)
    }
  })

  return (
    <div {...props}>
      <SplitPaneContext.Provider
        value={{
          clientHeight,
          setClientHeight,
          clientWidth,
          setClientWidth,
          onMouseDown,
        }}
      >
        {children}
      </SplitPaneContext.Provider>
    </div>
  )
}
