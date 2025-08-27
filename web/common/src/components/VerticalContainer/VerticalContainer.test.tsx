import { createRef } from 'react'
import { describe, expect, it } from 'vitest'

import { render, screen } from '@testing-library/react'
import { VerticalContainer } from './VerticalContainer'

describe('VerticalContainer', () => {
  it('renders children correctly', () => {
    render(
      <VerticalContainer>
        <div>Test Child</div>
      </VerticalContainer>,
    )
    expect(screen.getByText('Test Child')).toBeInTheDocument()
  })

  it('should force layout to be vertical (still having flex-col)', () => {
    render(
      <VerticalContainer className="flex-row">
        <div>Child</div>
      </VerticalContainer>,
    )
    const container = screen.getByText('Child').parentElement
    expect(container).toHaveClass('flex-col')
  })

  it('renders ScrollContainer when scroll is true', () => {
    render(
      <VerticalContainer scroll={true}>
        <div>Scroll Child</div>
      </VerticalContainer>,
    )
    expect(
      screen.getByText('Scroll Child').parentElement?.parentElement,
    ).toHaveClass('overflow-y-scroll scrollbar-w-[6px]')
  })

  it('renders a div when scroll is false', () => {
    render(
      <VerticalContainer scroll={false}>
        <div>Div Child</div>
      </VerticalContainer>,
    )
    const container = screen.getByText('Div Child').parentElement
    expect(container).toHaveClass('overflow-hidden')
  })

  it('forwards ref to the div element when scroll is false', () => {
    const ref = createRef<HTMLDivElement>()
    render(
      <VerticalContainer
        ref={ref}
        scroll={false}
      >
        <div>Ref Child</div>
      </VerticalContainer>,
    )
    expect(ref.current).toBeInstanceOf(HTMLElement)
    expect(ref.current?.tagName).toBe('DIV')
  })
})
