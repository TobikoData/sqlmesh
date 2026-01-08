import { createRef } from 'react'
import { describe, expect, it } from 'vitest'

import { render, screen } from '@testing-library/react'
import { HorizontalContainer } from './HorizontalContainer'

describe('HorizontalContainer', () => {
  it('renders children correctly', () => {
    render(
      <HorizontalContainer>
        <div>Test Child</div>
      </HorizontalContainer>,
    )
    expect(screen.getByText('Test Child')).toBeInTheDocument()
  })

  it('should force layout to be horizontal (still having flex-row)', () => {
    render(
      <HorizontalContainer className="flex-col">
        <div>Child</div>
      </HorizontalContainer>,
    )
    const container = screen.getByText('Child').parentElement
    expect(container).toHaveClass('flex-row')
  })

  it('renders ScrollContainer when scroll is true', () => {
    render(
      <HorizontalContainer scroll={true}>
        <div>Scroll Child</div>
      </HorizontalContainer>,
    )
    expect(
      screen.getByText('Scroll Child').parentElement?.parentElement,
    ).toHaveClass('overflow-x-scroll scrollbar-h-[6px]')
  })

  it('renders a div when scroll is false', () => {
    render(
      <HorizontalContainer scroll={false}>
        <div>Div Child</div>
      </HorizontalContainer>,
    )
    const container = screen.getByText('Div Child').parentElement
    expect(container).toHaveClass('overflow-hidden')
  })

  it('forwards ref to the div element when scroll is false', () => {
    const ref = createRef<HTMLDivElement>()
    render(
      <HorizontalContainer
        ref={ref}
        scroll={false}
      >
        <div>Ref Child</div>
      </HorizontalContainer>,
    )
    expect(ref.current).toBeInstanceOf(HTMLElement)
    expect(ref.current?.tagName).toBe('DIV')
  })
})
