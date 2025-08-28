import { createRef } from 'react'
import { describe, expect, it } from 'vitest'
import { render, screen } from '@testing-library/react'

import { ScrollContainer } from './ScrollContainer'

describe('ScrollContainer', () => {
  it('renders children correctly', () => {
    render(
      <ScrollContainer>
        <div>Test Child</div>
      </ScrollContainer>,
    )
    expect(screen.getByText('Test Child')).toBeInTheDocument()
  })

  it('applies custom className', () => {
    render(
      <ScrollContainer className="custom-class">
        <div>Child</div>
      </ScrollContainer>,
    )
    const container = screen.getByText('Child').parentElement
    expect(container).toHaveClass('custom-class')
  })

  it('applies vertical and horizontal scroll classes based on direction', () => {
    const { rerender } = render(
      <ScrollContainer direction="vertical">
        <div>Child</div>
      </ScrollContainer>,
    )
    let container = screen.getByText('Child').parentElement
    expect(container).toHaveClass('overflow-y-scroll scrollbar-w-[6px]')
    expect(container).toHaveClass('overflow-x-hidden')

    rerender(
      <ScrollContainer direction="horizontal">
        <div>Child</div>
      </ScrollContainer>,
    )
    container = screen.getByText('Child').parentElement
    expect(container).toHaveClass('overflow-y-hidden')
    expect(container).toHaveClass('overflow-x-scroll scrollbar-h-[6px]')

    rerender(
      <ScrollContainer direction="both">
        <div>Child</div>
      </ScrollContainer>,
    )
    container = screen.getByText('Child').parentElement
    expect(container).toHaveClass('overflow-y-scroll scrollbar-w-[6px]')
    expect(container).toHaveClass('overflow-x-scroll scrollbar-h-[6px]')
  })

  it('forwards ref to the span element', () => {
    const ref = createRef<HTMLSpanElement>()
    render(
      // @ts-expect-error: ScrollContainer's ref type is HTMLDivElement, but it renders a span
      <ScrollContainer ref={ref}>
        <div>Child</div>
      </ScrollContainer>,
    )
    expect(ref.current).toBeInstanceOf(HTMLElement)
    expect(ref.current?.tagName).toBe('DIV')
  })
})
